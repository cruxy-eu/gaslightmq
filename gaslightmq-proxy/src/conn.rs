//! One proxied client connection.
//!
//! Each client connection gets its own upstream connection. A reader moves
//! requests upstream, a responder moves responses back. Kafka answers
//! requests on a connection in order, so the reader queues one [`InFlight`]
//! entry per request that expects a response, and the responder works
//! through them in the same order: reading the next upstream response,
//! answering with a synthesized one, or stalling forever (blackhole).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use gaslightmq_proxy_api::catalog::api;
use gaslightmq_proxy_api::{NodeState, RuleSet};
use gaslightmq_wire::{
    RequestPeek, decode_response, encode_response, peek_request, peek_response_correlation_id,
    read_frame, split_request, write_frame,
};
use kafka_protocol::messages::{
    ApiVersionsResponse, DescribeClusterResponse, DescribeConfigsResponse, FindCoordinatorResponse,
    MetadataResponse, ResponseHeader,
};
use kafka_protocol::protocol::Encodable;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use crate::decide::{Fired, roll};
use crate::entity::{self, EntityRequest, ErrorPlan, SynthResponse};
use crate::nodes::Target;
use crate::rewrite;
use crate::shared::{ClusterMeta, Shared};
use crate::stats::Event;

/// Max requests a client may have in flight before the proxy stops reading.
const MAX_IN_FLIGHT: usize = 256;

struct InFlight {
    correlation_id: i32,
    api_key: i16,
    api_version: i16,
    delay: Option<Duration>,
    plan: RespPlan,
}

enum RespPlan {
    /// Read the response from upstream, then patch it.
    Upstream(Patch),
    /// Answer with a synthesized response.
    Synthetic(SynthResponse),
    /// Never answer.
    Blackhole,
}

/// Fault-related changes to an upstream response. Address rewrites happen
/// regardless. Requests themselves are never changed.
enum Patch {
    None,
    /// Set an error code on the response entries in scope.
    Mutate { code: i16, topic: Option<String> },
}

/// What the reader does with one request.
struct Step {
    forward: Option<Bytes>,
    in_flight: Option<InFlight>,
    /// Stop forwarding anything on this connection.
    swallow: bool,
}

struct DecRef(Arc<std::sync::atomic::AtomicU32>);

impl Drop for DecRef {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn wait_live(state: &mut watch::Receiver<NodeState>) {
    if state.wait_for(|s| *s == NodeState::Live).await.is_err() {
        std::future::pending::<()>().await;
    }
}

async fn wait_down(mut state: watch::Receiver<NodeState>) {
    if state.wait_for(|s| *s == NodeState::Down).await.is_err() {
        std::future::pending::<()>().await;
    }
}

pub(crate) async fn run(shared: Arc<Shared>, client: TcpStream, target: Target) {
    let Some(info) = shared.target_info(target) else { return };
    info.conns.fetch_add(1, Ordering::Relaxed);
    let _conn = DecRef(info.conns.clone());
    let mut shutdown = shared.shutdown.clone();
    let mut state = info.state;

    if *state.borrow() == NodeState::Down {
        return;
    }

    let down = wait_down(state.clone());
    let session = async {
        // An unresponsive broker accepts connections, then says nothing.
        wait_live(&mut state).await;
        let upstream = match TcpStream::connect(&info.upstream).await {
            Ok(s) => s,
            Err(e) => {
                warn!(upstream = %info.upstream, error = %e, "upstream connect failed");
                return;
            }
        };
        let _ = client.set_nodelay(true);
        let _ = upstream.set_nodelay(true);
        let (client_r, client_w) = client.into_split();
        let (upstream_r, upstream_w) = upstream.into_split();
        let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);

        let rng = match shared.next_conn_seed() {
            Some(seed) => SmallRng::seed_from_u64(seed),
            None => SmallRng::from_os_rng(),
        };
        let mut reader = Reader {
            shared: shared.clone(),
            state: state.clone(),
            rules: shared.rules.subscribe(),
            node_requests: info.requests.clone(),
            rng,
            swallowing: false,
        };
        let mut responder = Responder { shared: shared.clone(), state: state.clone() };

        let result = tokio::select! {
            r = reader.run(client_r, upstream_w, tx) => r,
            r = responder.run(upstream_r, client_w, rx) => r,
        };
        if let Err(e) = result {
            debug!(error = %e, "connection closed");
        }
    };

    tokio::select! {
        _ = session => {}
        _ = down => debug!("node down, dropping connection"),
        _ = crate::until_shutdown(&mut shutdown) => {}
    }
}

struct Reader {
    shared: Arc<Shared>,
    state: watch::Receiver<NodeState>,
    rules: watch::Receiver<Arc<RuleSet>>,
    node_requests: Option<Arc<std::sync::atomic::AtomicU64>>,
    rng: SmallRng,
    swallowing: bool,
}

impl Reader {
    async fn run(
        &mut self,
        mut client: OwnedReadHalf,
        mut upstream: OwnedWriteHalf,
        tx: mpsc::Sender<InFlight>,
    ) -> Result<()> {
        while let Some(frame) = read_frame(&mut client).await? {
            // While unresponsive, hold the request back.
            wait_live(&mut self.state).await;

            let peek = peek_request(&frame)?;
            self.shared.stats.record(peek.api_key, Event::Request);
            if let Some(n) = &self.node_requests {
                n.fetch_add(1, Ordering::Relaxed);
            }
            if self.swallowing {
                self.shared.stats.record(peek.api_key, Event::Blackhole);
                continue;
            }

            let rules = self.rules.borrow().clone();
            let step = self.plan(peek, frame, &rules);
            self.swallowing = step.swallow;
            if let Some(entry) = step.in_flight {
                tx.send(entry).await.context("responder gone")?;
            }
            if let Some(bytes) = step.forward {
                write_frame(&mut upstream, &bytes).await?;
            }
        }
        Ok(())
    }

    fn plan(&mut self, peek: RequestPeek, frame: Bytes, rules: &RuleSet) -> Step {
        let RequestPeek { api_key, api_version, correlation_id } = peek;
        let stats = &self.shared.stats;
        let row = rules.row(api_key).filter(|r| r.is_active());
        let entity_api = matches!(api_key, api::PRODUCE | api::METADATA | api::DESCRIBE_CONFIGS);

        // Only decode what we must: Produce (acks=0 has no response) and
        // APIs that currently have faults configured.
        let decoded = if api_key == api::PRODUCE || (row.is_some() && entity_api) {
            match split_request(frame.clone()).and_then(|p| EntityRequest::decode(api_key, api_version, p.body)) {
                Ok(entity) => entity,
                Err(e) => {
                    warn!(api_key, api_version, error = %e, "could not decode request, forwarding as is");
                    None
                }
            }
        } else {
            None
        };
        let expects_response =
            !matches!(&decoded, Some(EntityRequest::Produce(p)) if p.acks == 0);
        let topic = row.and_then(|r| r.topic.as_deref()).filter(|_| entity_api);

        let mut fired = row.map(|r| roll(r, &mut self.rng)).unwrap_or_default();
        if let (Some(t), Some(entity)) = (topic, &decoded)
            && !entity.touches(Some(t))
        {
            fired = Fired::default();
        }

        let in_flight = |delay, plan| {
            expects_response.then_some(InFlight { correlation_id, api_key, api_version, delay, plan })
        };

        if fired.blackhole {
            stats.record(api_key, Event::Blackhole);
            return Step { forward: None, in_flight: in_flight(None, RespPlan::Blackhole), swallow: true };
        }
        if fired.delay.is_some() {
            stats.record(api_key, Event::Delay);
        }

        if let (Some(code), Some(entity)) = (fired.error, &decoded) {
            match entity.plan_error(code, topic, &self.shared.cluster_info()) {
                ErrorPlan::NoMatch => {}
                ErrorPlan::Synthetic(resp) => {
                    stats.record(api_key, Event::Error);
                    return Step {
                        forward: None,
                        in_flight: in_flight(fired.delay, RespPlan::Synthetic(resp)),
                        swallow: false,
                    };
                }
                ErrorPlan::Mutate { code, topic } => {
                    stats.record(api_key, Event::Error);
                    return Step {
                        forward: Some(frame),
                        in_flight: in_flight(fired.delay, RespPlan::Upstream(Patch::Mutate { code, topic })),
                        swallow: false,
                    };
                }
            }
        }

        Step { forward: Some(frame), in_flight: in_flight(fired.delay, RespPlan::Upstream(Patch::None)), swallow: false }
    }
}

struct Responder {
    shared: Arc<Shared>,
    state: watch::Receiver<NodeState>,
}

impl Responder {
    async fn run(
        &mut self,
        mut upstream: OwnedReadHalf,
        mut client: OwnedWriteHalf,
        mut rx: mpsc::Receiver<InFlight>,
    ) -> Result<()> {
        while let Some(entry) = rx.recv().await {
            let payload = match entry.plan {
                RespPlan::Blackhole => std::future::pending().await,
                RespPlan::Synthetic(resp) => {
                    let header = ResponseHeader::default().with_correlation_id(entry.correlation_id);
                    encode_synth(&header, entry.api_key, entry.api_version, resp)?
                }
                RespPlan::Upstream(patch) => {
                    let Some(frame) = read_frame(&mut upstream).await? else {
                        bail!("upstream closed the connection");
                    };
                    let cid = peek_response_correlation_id(&frame)?;
                    if cid != entry.correlation_id {
                        bail!("upstream answered correlation id {cid}, expected {}", entry.correlation_id);
                    }
                    self.patch(entry.api_key, entry.api_version, patch, frame)?
                }
            };
            if let Some(delay) = entry.delay {
                tokio::time::sleep(delay).await;
            }
            // While unresponsive, hold the response back.
            wait_live(&mut self.state).await;
            write_frame(&mut client, &payload).await?;
        }
        client.shutdown().await?;
        Ok(())
    }

    fn patch(&self, api_key: i16, api_version: i16, patch: Patch, frame: Bytes) -> Result<Bytes> {
        let shared = &self.shared;
        match (api_key, patch) {
            (api::METADATA, patch) => {
                let (header, mut resp): (_, MetadataResponse) = decode_response(frame, api_key, api_version)?;
                *shared.cluster.lock().unwrap() =
                    ClusterMeta { cluster_id: resp.cluster_id.clone(), controller_id: resp.controller_id.0 };
                rewrite::metadata(&mut resp, shared);
                if let Patch::Mutate { code, topic } = patch {
                    entity::mutate_metadata(&mut resp, code, topic.as_deref());
                }
                encode_response(&header, api_key, api_version, &resp)
            }
            (api::FIND_COORDINATOR, _) => {
                let (header, mut resp): (_, FindCoordinatorResponse) = decode_response(frame, api_key, api_version)?;
                rewrite::find_coordinator(&mut resp, shared);
                encode_response(&header, api_key, api_version, &resp)
            }
            (api::DESCRIBE_CLUSTER, _) => {
                let (header, mut resp): (_, DescribeClusterResponse) = decode_response(frame, api_key, api_version)?;
                rewrite::describe_cluster(&mut resp, shared);
                encode_response(&header, api_key, api_version, &resp)
            }
            (api::API_VERSIONS, _) => {
                // A version the broker doesn't support gets a v0 error body;
                // anything we can't read is passed through untouched.
                match decode_response::<ApiVersionsResponse>(frame.clone(), api_key, api_version) {
                    Ok((header, mut resp)) if resp.error_code == 0 => {
                        rewrite::cap_api_versions(&mut resp);
                        encode_response(&header, api_key, api_version, &resp)
                    }
                    _ => Ok(frame),
                }
            }
            (api::DESCRIBE_CONFIGS, Patch::Mutate { code, topic }) => {
                let (header, mut resp): (_, DescribeConfigsResponse) = decode_response(frame, api_key, api_version)?;
                entity::mutate_describe_configs(&mut resp, code, topic.as_deref());
                encode_response(&header, api_key, api_version, &resp)
            }
            _ => Ok(frame),
        }
    }
}

fn encode_synth(header: &ResponseHeader, api_key: i16, api_version: i16, resp: SynthResponse) -> Result<Bytes> {
    fn enc<E: Encodable>(h: &ResponseHeader, k: i16, v: i16, body: &E) -> Result<Bytes> {
        encode_response(h, k, v, body)
    }
    match resp {
        SynthResponse::Produce(r) => enc(header, api_key, api_version, &r),
        SynthResponse::Metadata(r) => enc(header, api_key, api_version, &r),
        SynthResponse::DescribeConfigs(r) => enc(header, api_key, api_version, &r),
    }
}
