//! The backend of these tests: a cluster of three brokers, written by hand.

use std::collections::{BTreeMap, BTreeSet};
use std::future::{pending, poll_fn, ready};
use std::net::SocketAddr;
use std::sync::Mutex;
use std::task::{Poll, Waker};

use gaslightmq_backend::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::metadata_response::MetadataResponseBroker;
use gaslightmq_wire::messages::{
    FetchRequest, FetchResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
};
use gaslightmq_wire::protocol::{Encodable, StrBytes};
use gaslightmq_wire::{ApiKey as KnownApi, Request};

/// The client of every request of these tests.
pub const CLIENT: &str = "order-ingest";

/// The host under which clients reach the listeners of the proxy.
pub const ADVERTISED_HOST: &str = "gaslight.test";

/// A cluster of brokers 1, 2 and 3.
///
/// Broker 2 leads `orders.eu/3` and hosts the coordinator of the group `payments`.
/// A Fetch waits until a record arrived, and a Produce brings one.
/// Metadata names every broker whose endpoint has a listener, at the address of that listener.
///
/// It has no runtime and no channel.
/// Its state sits behind one lock, and whoever waits for the state to change leaves a waker there.
#[derive(Default)]
pub struct Cluster {
    state: Mutex<State>,
}

struct State {
    /// The endpoints that accept connections. The endpoint of a broker that is down is closed.
    endpoints: Endpoints,
    /// The port on which the engine listens for each broker, once it said so.
    listening: BTreeMap<BrokerId, u16>,
    unresponsive: BTreeSet<BrokerId>,
    /// The calls whose requests the cluster took, in the order it took them.
    taken: Vec<CallId>,
    /// How many records were produced.
    records: usize,
    /// The connections that the engine closed, in that order.
    closed: Vec<ConnectionNumber>,
    /// Whoever waits for the state to change.
    waiting: Vec<Waker>,
}

/// Every broker of the cluster is up, and nothing happened yet.
impl Default for State {
    fn default() -> Self {
        let mut endpoints = Endpoints::none().with(Endpoint::Bootstrap);
        endpoints.extend(BROKERS.into_iter().map(broker));
        Self {
            endpoints,
            listening: BTreeMap::new(),
            unresponsive: BTreeSet::new(),
            taken: Vec::new(),
            records: 0,
            closed: Vec::new(),
            waiting: Vec::new(),
        }
    }
}

const BROKERS: [u16; 3] = [1, 2, 3];

/// Whether the cluster has that broker, up or down.
fn has_broker(broker: BrokerId) -> bool {
    BROKERS.into_iter().any(|id| BrokerId::from(id) == broker)
}

/// The broker that leads a partition and hosts a coordinator.
const BUSY_BROKER: u16 = 2;

impl Topology for State {
    fn partitions_led_by(&self, broker: BrokerId) -> Vec<TopicPartition> {
        if broker == BrokerId::from(BUSY_BROKER) {
            vec!["orders.eu/3".parse().unwrap()]
        } else {
            vec![]
        }
    }

    fn coordinators_hosted_by(&self, broker: BrokerId) -> Vec<Coordinator> {
        if broker == BrokerId::from(BUSY_BROKER) {
            vec![GroupId::new("payments").unwrap().into()]
        } else {
            vec![]
        }
    }
}

impl Cluster {
    /// The calls whose requests the cluster took, in the order it took them.
    pub fn taken(&self) -> Vec<String> {
        self.read(|state| state.taken.iter().map(ToString::to_string).collect())
    }

    /// The connections that the engine closed, in that order.
    pub fn closed(&self) -> Vec<ConnectionNumber> {
        self.read(|state| state.closed.clone())
    }

    /// The endpoints that accept connections now, without waiting for a change.
    pub fn endpoints_now(&self) -> Endpoints {
        self.read(|state| state.endpoints.clone())
    }

    fn read<T>(&self, read: impl FnOnce(&State) -> T) -> T {
        read(&self.state.lock().unwrap())
    }

    /// Changes the state, and wakes whoever waits for a change.
    fn change<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        let mut state = self.state.lock().unwrap();
        let changed = change(&mut state);
        for waiting in state.waiting.drain(..) {
            waiting.wake();
        }
        changed
    }

    /// Waits until `ready` reads something from the state.
    async fn wait_for<T>(&self, mut ready: impl FnMut(&State) -> Option<T>) -> T {
        poll_fn(|context| {
            let mut state = self.state.lock().unwrap();
            if let Some(read) = ready(&state) {
                Poll::Ready(read)
            } else {
                state.waiting.push(context.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

impl Backend for Cluster {
    async fn call(&self, request: BackendRequest) -> BackendAnswer {
        // The cluster takes the request at the first poll, before it waits for anything.
        self.change(|state| state.taken.push(request.call()));

        if let Some(broker) = request.endpoint().broker() {
            // A broker that is down takes no request, and neither does one that the cluster does not have.
            let endpoint = request.endpoint();
            if !self.read(|state| state.endpoints.contains(endpoint)) {
                return BackendFailure::NoSuchEndpoint { endpoint }.into();
            }
            if self.read(|state| state.unresponsive.contains(&broker)) {
                return pending().await;
            }
        }

        match request.request().api().known() {
            // The cluster answers every Heartbeat with three bytes, which is no response header.
            Some(KnownApi::Heartbeat) => BackendAnswer::NotAResponse {
                reason: "response header is cut short at 3 bytes".to_owned(),
            },
            Some(KnownApi::Fetch) => {
                self.wait_for(|state| (state.records > 0).then_some(())).await;
                truth(request.request(), &FetchResponse::default())
            }
            Some(KnownApi::Produce) => match request.request().body::<ProduceRequest>() {
                Ok(produce) => {
                    self.change(|state| state.records += 1);
                    if produce.acks == 0 {
                        BackendAnswer::NoResponse
                    } else {
                        truth(request.request(), &ProduceResponse::default())
                    }
                }
                // A broker closes the connection of a client whose request it cannot read.
                Err(error) => BackendFailure::ConnectionLost {
                    reason: format!("the broker cannot read the request: {error}"),
                }
                .into(),
            },
            Some(KnownApi::Metadata) => {
                let brokers = self.read(|state| state.listening.iter().map(advertised).collect());
                truth(request.request(), &MetadataResponse::default().with_brokers(brokers))
            }
            _ => truth(request.request(), &MetadataResponse::default()),
        }
    }

    async fn close(&self, connection: ConnectionNumber) {
        self.change(|state| state.closed.push(connection));
    }

    async fn endpoints(&self, known: &Endpoints) -> Endpoints {
        self.wait_for(|state| Some(&state.endpoints).filter(|endpoints| *endpoints != known).cloned())
            .await
    }

    // This cluster decides at once, so the method returns a future that is ready.
    fn listens(
        &self,
        endpoint: Endpoint,
        address: SocketAddr,
    ) -> impl Future<Output = Result<(), ListenerRejection>> + Send {
        ready(self.listening(endpoint, address))
    }

    fn incident_support(&self) -> IncidentSupport {
        IncidentSupport::none().emulates([
            IncidentKind::BrokerGoesDown,
            IncidentKind::BrokerGoesUnresponsive,
            IncidentKind::BrokerComesBack,
        ])
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the incident starts at the first poll, and this cluster applies it in that one step; \
                  a backend behind a task or a socket waits here"
    )]
    async fn apply(&self, incident: Incident) -> Result<ExpandedIncident, IncidentRejection> {
        self.applied(incident)
    }
}

impl Cluster {
    fn listening(&self, endpoint: Endpoint, address: SocketAddr) -> Result<(), ListenerRejection> {
        let Some(broker) = endpoint.broker() else {
            // No answer names the bootstrap endpoint.
            return Ok(());
        };
        self.change(|state| {
            if !state.endpoints.contains(endpoint) {
                // The endpoint closed in the meantime, and its listener goes with it.
                return Ok(());
            }
            let taken = state
                .listening
                .iter()
                .find(|(other, port)| **other != broker && **port == address.port());
            if let Some((other, _)) = taken {
                return Err(ListenerRejection::Unadvertisable {
                    endpoint,
                    address,
                    reason: format!("broker {other} is advertised at {ADVERTISED_HOST}:{}", address.port()),
                });
            }
            state.listening.insert(broker, address.port());
            Ok(())
        })
    }

    fn applied(&self, incident: Incident) -> Result<ExpandedIncident, IncidentRejection> {
        self.incident_support().check(&Requirements::from(&incident))?;
        let (broker, down, unresponsive) = match &incident {
            Incident::BrokerGoesDown { broker, .. } => (*broker, true, false),
            Incident::BrokerGoesUnresponsive { broker } => (*broker, false, true),
            Incident::BrokerComesBack { broker } => (*broker, false, false),
            _ => unreachable!("the cluster supports no other incident"),
        };
        if !has_broker(broker) {
            return Err(IncidentRejection::Impossible {
                incident: incident.kind(),
                reason: format!("the cluster has no broker {broker}"),
            });
        }
        // The consequences come from the topology of this cluster, and they take effect in the same step.
        Ok(self.change(|state| {
            let expanded = incident.expand(state);
            state.unresponsive.remove(&broker);
            if down {
                state.endpoints.close(Endpoint::Broker(broker));
                // The listener of an endpoint is gone with the endpoint.
                state.listening.remove(&broker);
            } else {
                state.endpoints.open(Endpoint::Broker(broker));
            }
            if unresponsive {
                state.unresponsive.insert(broker);
            }
            expanded
        }))
    }
}

/// A broker as Metadata names it to clients: on the advertised host, at the port of its listener.
fn advertised((broker, port): (&BrokerId, &u16)) -> MetadataResponseBroker {
    MetadataResponseBroker::default()
        .with_node_id(i32::try_from(broker.value()).unwrap().into())
        .with_host(StrBytes::from_static_str(ADVERTISED_HOST))
        .with_port(i32::from(*port))
}

/// The truth that is `body`, as the response to `request`.
fn truth<B: Encodable>(request: &Request, body: &B) -> BackendAnswer {
    match request.response(body) {
        Ok(response) => Truth::new(response).into(),
        Err(error) => BackendFailure::ResponseNotBuilt {
            reason: error.to_string(),
        }
        .into(),
    }
}

// The requests that a pipeline forwards.

pub fn broker(id: u16) -> Endpoint {
    Endpoint::Broker(BrokerId::from(id))
}

/// A Metadata request under `call`, such as `#7.1`, from a connection on `endpoint`.
pub fn metadata(call: &str, endpoint: Endpoint) -> BackendRequest {
    let request = Request::new(12, correlation_id(call), Some(CLIENT), &MetadataRequest::default()).unwrap();
    BackendRequest::new(call.parse().unwrap(), endpoint, request)
}

/// A Fetch under `call`, from a connection on `endpoint`. It waits for a record.
pub fn fetch(call: &str, endpoint: Endpoint) -> BackendRequest {
    let request = Request::new(13, correlation_id(call), Some(CLIENT), &FetchRequest::default()).unwrap();
    BackendRequest::new(call.parse().unwrap(), endpoint, request)
}

/// A Produce under `call`, from a connection on `endpoint`. It brings a record.
pub fn produce(call: &str, endpoint: Endpoint, acks: i16) -> BackendRequest {
    let body = ProduceRequest::default().with_acks(acks);
    let request = Request::new(9, correlation_id(call), Some(CLIENT), &body).unwrap();
    BackendRequest::new(call.parse().unwrap(), endpoint, request)
}

/// The correlation ID that the client gave the request of `call`: a number of its own, and not the call number.
pub fn correlation_id(call: &str) -> i32 {
    let call: CallId = call.parse().unwrap();
    1000 + i32::try_from(call.number().value()).unwrap()
}

/// The response of an answer that is the truth.
pub fn truth_of(answer: BackendAnswer) -> Truth {
    match answer {
        BackendAnswer::Truth(truth) => truth,
        other => unreachable!("the cluster answered {other:?}"),
    }
}
