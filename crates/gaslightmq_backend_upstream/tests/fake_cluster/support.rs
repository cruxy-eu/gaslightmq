//! A cluster of hand-written brokers on sockets of this machine, and what a test needs to start a backend in front of it.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_session::CallIds;
use gaslightmq_wire::messages::api_versions_response::ApiVersion;
use gaslightmq_wire::messages::metadata_response::MetadataResponseBroker;
use gaslightmq_wire::messages::{
    ApiVersionsResponse, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, FrameDecoder, FrameError, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The host under which clients reach the listeners of every test.
pub const ADVERTISED_HOST: &str = "gaslight.test";
/// Long enough for anything on this machine to happen, and for a test to fail instead of hang.
pub const SOON: Duration = Duration::from_secs(10);
/// Long enough to tell that something does not happen.
pub const A_WHILE: Duration = Duration::from_millis(200);

/// What a fake broker does with a request.
pub enum Reaction {
    /// Write this response.
    Respond(Response),
    /// Write these bytes as they are.
    Send(Bytes),
    /// Write nothing, and read the next request.
    Nothing,
    /// Close the connection.
    HangUp,
}

/// One request that reached a fake broker.
#[derive(Debug, Clone)]
pub struct Arrival {
    /// Which connection it came on: 0 for the first one the broker accepted, 1 for the next.
    pub connection: usize,
    pub request: Request,
}

/// What a fake broker saw.
#[derive(Debug)]
struct Seen {
    arrivals: Mutex<Vec<Arrival>>,
    arrived: watch::Sender<usize>,
    opened: watch::Sender<usize>,
    closed: watch::Sender<usize>,
}

type React = Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Reaction> + Send>> + Send + Sync>;

/// The brokers that the cluster names in a Metadata response, each with its real address.
type Members = Arc<Mutex<BTreeMap<i32, SocketAddr>>>;

/// A cluster of fake brokers. Every one of them names the same members.
#[derive(Debug, Default)]
pub struct FakeCluster {
    members: Members,
}

impl FakeCluster {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start broker `id` on a free port, as a member of the cluster.
    ///
    /// The broker answers `ApiVersions` and Metadata itself, and every other request as `react` says.
    pub async fn broker<F, Fut>(&self, id: i32, react: F) -> FakeBroker
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Reaction> + Send + 'static,
    {
        let broker = FakeBroker::start(Arc::clone(&self.members), move |request| Box::pin(react(request))).await;
        self.name(id, broker.address());
        broker
    }

    /// Start broker `id`, which answers every request it does not answer itself as a heartbeat.
    pub async fn calm_broker(&self, id: i32) -> FakeBroker {
        self.broker(id, |request| async move { Reaction::Respond(calm(&request)) })
            .await
    }

    /// The cluster names broker `id` at `address` from now on.
    pub fn name(&self, id: i32, address: SocketAddr) {
        self.members.lock().unwrap().insert(id, address);
    }

    /// The cluster no longer names broker `id`.
    pub fn forget(&self, id: i32) {
        self.members.lock().unwrap().remove(&id);
    }
}

/// A listener that answers each connection as a broker does: one request at a time, in order.
///
/// Letting go of it closes the listener and every connection, as a broker that goes down.
#[derive(Debug)]
pub struct FakeBroker {
    address: SocketAddr,
    seen: Arc<Seen>,
    down: watch::Sender<bool>,
    accepting: JoinHandle<()>,
}

impl FakeBroker {
    async fn start(
        members: Members,
        react: impl Fn(Request) -> Pin<Box<dyn Future<Output = Reaction> + Send>> + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Seen {
            arrivals: Mutex::default(),
            arrived: watch::Sender::new(0),
            opened: watch::Sender::new(0),
            closed: watch::Sender::new(0),
        });
        let down = watch::Sender::new(false);
        let react: React = Arc::new(react);
        let accepting = tokio::spawn({
            let (seen, down) = (Arc::clone(&seen), down.clone());
            async move {
                let mut connection = 0;
                while let Ok((stream, _)) = listener.accept().await {
                    let serving = Serving {
                        connection,
                        members: Arc::clone(&members),
                        seen: Arc::clone(&seen),
                        react: Arc::clone(&react),
                    };
                    let mut down = down.subscribe();
                    seen.opened.send_modify(|opened| *opened += 1);
                    tokio::spawn(async move {
                        tokio::select! {
                            () = serving.serve(stream) => {}
                            _ = down.wait_for(|down| *down) => {}
                        }
                        serving.seen.closed.send_modify(|closed| *closed += 1);
                    });
                    connection += 1;
                }
            }
        });
        Self {
            address,
            seen,
            down,
            accepting,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The address as an upstream names it.
    pub fn server(&self) -> ServerAddress {
        self.address.to_string().parse().unwrap()
    }

    /// Every request that arrived so far, in the order of arrival.
    pub fn arrivals(&self) -> Vec<Arrival> {
        self.seen.arrivals.lock().unwrap().clone()
    }

    /// The requests of clients that arrived so far: everything but the questions of the backend itself.
    pub fn of_clients(&self) -> Vec<Arrival> {
        let of_a_client = |arrival: &Arrival| arrival.request.client_id() != Some("gaslightmq");
        self.arrivals().into_iter().filter(of_a_client).collect()
    }

    /// Waits until `count` requests arrived.
    pub async fn arrived(&self, count: usize) {
        soon(self.seen.arrived.subscribe().wait_for(|arrived| *arrived >= count))
            .await
            .unwrap();
    }

    /// Waits until `count` connections ended.
    pub async fn closed(&self, count: usize) {
        soon(self.seen.closed.subscribe().wait_for(|closed| *closed >= count))
            .await
            .unwrap();
    }

    /// How many connections the broker accepted so far.
    pub fn connections(&self) -> usize {
        *self.seen.opened.borrow()
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.accepting.abort();
        self.down.send_replace(true);
    }
}

/// One connection of a fake broker.
struct Serving {
    connection: usize,
    members: Members,
    seen: Arc<Seen>,
    react: React,
}

impl Serving {
    async fn serve(&self, mut stream: TcpStream) {
        let mut frames = FrameDecoder::new();
        loop {
            let frame = loop {
                match frames.next_frame() {
                    Ok(Some(frame)) => break frame,
                    Ok(None) => {}
                    Err(FrameError::Empty | FrameError::TooLarge { .. }) => return,
                }
                match stream.read_buf(frames.buffer_mut()).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            };
            let request = Request::decode(frame).unwrap();
            self.seen.arrivals.lock().unwrap().push(Arrival {
                connection: self.connection,
                request: request.clone(),
            });
            self.seen.arrived.send_modify(|arrived| *arrived += 1);
            let reaction = match request.api().known() {
                Some(ApiKey::ApiVersions) => Reaction::Respond(every_version(&request)),
                Some(ApiKey::Metadata) => Reaction::Respond(self.members(&request)),
                _ => (self.react)(request).await,
            };
            let written = match reaction {
                Reaction::Respond(response) => stream.write_all(&response.frame().to_bytes()).await,
                Reaction::Send(bytes) => stream.write_all(&bytes).await,
                Reaction::Nothing => Ok(()),
                Reaction::HangUp => return,
            };
            if written.is_err() {
                return;
            }
        }
    }

    /// The Metadata response that names the members of the cluster.
    fn members(&self, request: &Request) -> Response {
        let broker = |(id, address): (&i32, &SocketAddr)| {
            MetadataResponseBroker::default()
                .with_node_id((*id).into())
                .with_host(StrBytes::from_string(address.ip().to_string()))
                .with_port(i32::from(address.port()))
        };
        let brokers = self.members.lock().unwrap().iter().map(broker).collect();
        request
            .response(&MetadataResponse::default().with_brokers(brokers))
            .unwrap()
    }
}

/// The `ApiVersions` response of a broker that knows Metadata in every version.
fn every_version(request: &Request) -> Response {
    let metadata = ApiVersion::default()
        .with_api_key(ApiKey::Metadata as i16)
        .with_min_version(0)
        .with_max_version(13);
    request
        .response(&ApiVersionsResponse::default().with_api_keys(vec![metadata]))
        .unwrap()
}

/// A response to `request` that says nothing went wrong, in the form of a Heartbeat response.
pub fn calm(request: &Request) -> Response {
    request.response(&HeartbeatResponse::default()).unwrap()
}

/// A Heartbeat request of a client, with this correlation ID.
pub fn heartbeat(correlation_id: i32) -> Request {
    Request::new(4, correlation_id, Some("payments-api"), &HeartbeatRequest::default()).unwrap()
}

/// A Metadata request of a client, with this correlation ID.
pub fn metadata(correlation_id: i32) -> Request {
    Request::new(12, correlation_id, Some("payments-api"), &MetadataRequest::default()).unwrap()
}

/// The proxy in front of the backend of one test, as far as the backend sees it:
/// it counts the calls of all its connections, and forwards each request under the ID of its call.
#[derive(Default)]
pub struct Proxy {
    call_ids: CallIds,
}

impl Proxy {
    /// A proxy that forwarded no request yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// `request` as the next call of the proxy, which arrived on connection `connection`, a connection on `endpoint`.
    pub fn forwards(&self, connection: u64, endpoint: Endpoint, request: Request) -> BackendRequest {
        let call = self.call_ids.assign();
        BackendRequest::new(call, ConnectionNumber::new(connection), endpoint, request)
    }
}

/// The endpoint of broker `id`.
pub fn broker(id: u16) -> Endpoint {
    Endpoint::Broker(BrokerId::from(id))
}

/// The configuration of a backend in front of the cluster behind `bootstrap`, with short waits.
pub fn configuration(bootstrap: &FakeBroker) -> UpstreamBackendConfiguration {
    let upstream = Upstream::new(bootstrap.server());
    let mut configuration = UpstreamBackendConfiguration::new(upstream, ADVERTISED_HOST.parse().unwrap());
    configuration.socket_connection_setup_timeout = TimeSpan::from_secs(2);
    configuration.request_timeout = TimeSpan::from_secs(2);
    configuration.retry_backoff = TimeSpan::from_millis(10);
    configuration
}

/// A backend in front of the cluster behind `bootstrap`, with the [`configuration`] of the tests.
pub async fn backend(bootstrap: &FakeBroker) -> UpstreamBackend {
    UpstreamBackend::start(configuration(bootstrap)).await.unwrap()
}

/// The engine opened a listener for the endpoint of broker `id` on this port, and tells the backend.
pub async fn listening(backend: &UpstreamBackend, id: u16, port: u16) {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    soon(backend.listens(broker(id), address)).await.unwrap();
}

/// The brokers that a Metadata answer names, each with its address.
pub fn named(answer: &Response) -> Vec<(i32, String)> {
    let metadata: MetadataResponse = answer.body().unwrap();
    let address = |broker: &MetadataResponseBroker| (broker.node_id.0, format!("{}:{}", broker.host, broker.port));
    metadata.brokers.iter().map(address).collect()
}

/// The output of `future`, which has to come soon.
pub async fn soon<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(SOON, future)
        .await
        .expect("this was to happen within seconds")
}

/// The truth of an answer.
pub fn truth(answer: BackendAnswer) -> Truth {
    match answer {
        BackendAnswer::Truth(truth) => truth,
        other => panic!("the cluster answered, and the backend made of it: {other:?}"),
    }
}

/// The failure of an answer.
pub fn failure(answer: BackendAnswer) -> BackendFailure {
    match answer {
        BackendAnswer::Failed(failure) => failure,
        other => panic!("the backend has no answer, and it answered: {other:?}"),
    }
}

/// Starts every one of `calls` in their order, as an engine does that forwards the requests of one connection,
/// and returns them for whoever waits for the answers.
///
/// A call starts when it is polled for the first time.
pub async fn started<F: Future>(calls: Vec<F>) -> Vec<Pin<Box<F>>> {
    let mut calls: Vec<Pin<Box<F>>> = calls.into_iter().map(Box::pin).collect();
    poll_fn(|context| {
        for call in &mut calls {
            assert!(call.as_mut().poll(context).is_pending(), "no call ends at once");
        }
        Poll::Ready(())
    })
    .await;
    calls
}
