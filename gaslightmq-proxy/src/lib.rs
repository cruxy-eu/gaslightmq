//! A Kafka proxy that injects faults between clients and a real cluster.
//!
//! Clients bootstrap from the proxy. Every broker address in responses is
//! rewritten to a proxy listener, so all traffic keeps flowing through it.
//! Faults are configured per Kafka API (error, delay, blackhole) and per
//! broker (live, unresponsive, down), and can be changed at runtime through
//! a [`ProxyHandle`] or the HTTP/WebSocket control plane.

mod config;
mod conn;
mod control;
mod decide;
mod entity;
mod nodes;
mod rewrite;
mod shared;
mod stats;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use gaslightmq_proxy_api::{ApiRow, NodeInfo, NodeState, RuleSet, ServerMsg, Snapshot, StatsTick};
use gaslightmq_wire::{decode_response, encode_request, read_frame, write_frame};
use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, RequestHeader};
use kafka_protocol::protocol::StrBytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::{NodePorts, ProxyBuilder, ProxyConfig};
pub use gaslightmq_proxy_api as api;

use nodes::Target;
use shared::{ClusterMeta, Shared};

const STATS_INTERVAL: Duration = Duration::from_millis(100);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// A running proxy. Dropping it leaves the tasks running; call
/// [`Proxy::shutdown`] to stop.
pub struct Proxy {
    handle: ProxyHandle,
    bootstrap_addr: SocketAddr,
    control_addr: Option<SocketAddr>,
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Proxy {
    pub fn builder() -> ProxyBuilder {
        ProxyBuilder::new()
    }

    pub async fn start(config: impl Into<ProxyConfig>) -> Result<Proxy> {
        let config = config.into();
        anyhow::ensure!(!config.upstream.is_empty(), "no upstream bootstrap address configured");
        let listener = nodes::bind_listener(config.bind)
            .with_context(|| format!("cannot bind bootstrap listener on {}", config.bind))?;
        let bootstrap_addr = listener.local_addr()?;
        let control_bind = config.control;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shared = Arc::new(Shared::new(config, bootstrap_addr.port(), shutdown_rx));

        if let Err(e) = probe(&shared).await {
            warn!(upstream = %shared.config.upstream, error = %e,
                "could not discover upstream brokers yet, will learn them from client traffic");
        }

        let mut tasks = vec![
            tokio::spawn(accept_loop(shared.clone(), listener, Target::Bootstrap)),
            tokio::spawn(stats_loop(shared.clone())),
        ];

        let control_addr = match control_bind {
            Some(addr) => {
                let listener = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("cannot bind control plane on {addr}"))?;
                let addr = listener.local_addr()?;
                tasks.push(tokio::spawn(control::serve(shared.clone(), listener)));
                Some(addr)
            }
            None => None,
        };

        info!(bootstrap = %bootstrap_addr, upstream = %shared.config.upstream, "proxy listening");
        Ok(Proxy { handle: ProxyHandle { shared }, bootstrap_addr, control_addr, shutdown_tx, tasks })
    }

    pub fn handle(&self) -> ProxyHandle {
        self.handle.clone()
    }

    /// Where clients should bootstrap from.
    pub fn bootstrap_addr(&self) -> SocketAddr {
        self.bootstrap_addr
    }

    pub fn control_addr(&self) -> Option<SocketAddr> {
        self.control_addr
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.shared.stop_nodes();
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// Runtime control over a running proxy. Cheap to clone.
#[derive(Clone)]
pub struct ProxyHandle {
    shared: Arc<Shared>,
}

impl ProxyHandle {
    pub fn rules(&self) -> RuleSet {
        (**self.shared.rules.borrow()).clone()
    }

    /// Replace the fault row of one API.
    pub fn set_row(&self, row: ApiRow) {
        self.shared.set_row(row);
    }

    /// Replace all fault rows.
    pub fn set_rules(&self, rules: RuleSet) {
        self.shared.set_rules(rules);
    }

    /// Remove every fault row.
    pub fn clear_rules(&self) {
        self.shared.set_rules(RuleSet::default());
    }

    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.shared.node_infos()
    }

    pub async fn set_node_state(&self, node_id: i32, state: NodeState) -> Result<()> {
        self.shared.set_node_state(node_id, state).await
    }

    pub fn snapshot(&self) -> Snapshot {
        self.shared.snapshot()
    }

    /// Live events: row and node changes, and stats ticks.
    pub fn subscribe(&self) -> broadcast::Receiver<ServerMsg> {
        self.shared.events.subscribe()
    }
}

pub(crate) async fn accept_loop(shared: Arc<Shared>, listener: TcpListener, target: Target) {
    let mut shutdown = shared.shutdown.clone();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => {
                    tokio::spawn(conn::run(shared.clone(), stream, target));
                }
                Err(e) => {
                    warn!(?target, error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            _ = until_shutdown(&mut shutdown) => break,
        }
    }
}

/// Resolves once the proxy is shutting down.
pub(crate) async fn until_shutdown(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

/// Ask the bootstrap broker for the cluster's brokers so every node has a
/// listener (and a pad in the UI) before the first client shows up.
async fn probe(shared: &Arc<Shared>) -> Result<()> {
    const VERSION: i16 = 9;
    let fut = async {
        let mut stream = TcpStream::connect(&shared.config.upstream).await?;
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(VERSION)
            .with_correlation_id(1)
            .with_client_id(Some(StrBytes::from_static_str("gaslightmq-proxy")));
        let request = MetadataRequest::default().with_topics(Some(vec![]));
        write_frame(&mut stream, &encode_request(&header, &request)?).await?;
        let frame = read_frame(&mut stream).await?.context("upstream closed the connection")?;
        let (_, resp): (_, MetadataResponse) = decode_response(frame, ApiKey::Metadata as i16, VERSION)?;
        anyhow::Ok(resp)
    };
    let resp = tokio::time::timeout(PROBE_TIMEOUT, fut).await.context("timed out")??;
    *shared.cluster.lock().unwrap() =
        ClusterMeta { cluster_id: resp.cluster_id.clone(), controller_id: resp.controller_id.0 };
    for b in &resp.brokers {
        shared.ensure_node(b.node_id.0, &b.host, b.port);
    }
    Ok(())
}

async fn stats_loop(shared: Arc<Shared>) {
    let mut interval = tokio::time::interval(STATS_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut api_before = Vec::new();
    let mut node_before = Vec::new();
    let mut seq = 0;
    loop {
        interval.tick().await;
        let totals = shared.stats.totals();
        let apis = stats::deltas(&totals, &api_before);
        api_before = totals;
        let nodes = shared.node_ticks(&mut node_before);
        seq += 1;
        shared.publish(ServerMsg::Stats(StatsTick {
            seq,
            interval_ms: STATS_INTERVAL.as_millis() as u32,
            bootstrap_connections: shared.bootstrap_conns.load(std::sync::atomic::Ordering::Relaxed),
            apis,
            nodes,
        }));
    }
}
