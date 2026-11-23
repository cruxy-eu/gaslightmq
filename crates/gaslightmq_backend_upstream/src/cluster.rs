//! What a backend knows about its cluster, and what it decides from that.

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;

use gaslightmq_backend::{BackendAnswer, BackendFailure, Endpoints, ListenerRejection};
use gaslightmq_environment::{HostName, ServerAddress};
use gaslightmq_field::Endpoint;
use gaslightmq_kernel::BrokerId;
use gaslightmq_overlay::{AdvertisedAddresses, BrokerAddress, Destination, Forwarding, Overlay, OverlayError};
use gaslightmq_wire::{Request, Response, WireError};

use crate::error::UpstreamBackendError;
use crate::in_flight::Reply;

/// Where a server of the real cluster takes connections: a host and a port.
///
/// It is the bootstrap server of the environment, or a broker as the cluster names it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ClusterAddress {
    host: String,
    port: u16,
}

impl ClusterAddress {
    /// The name or the IP address of the host, without brackets.
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) const fn port(&self) -> u16 {
        self.port
    }
}

impl From<&ServerAddress> for ClusterAddress {
    fn from(server: &ServerAddress) -> Self {
        Self {
            host: server.host().as_str().to_owned(),
            port: server.port(),
        }
    }
}

impl From<&BrokerAddress> for ClusterAddress {
    fn from(broker: &BrokerAddress) -> Self {
        Self {
            host: broker.host().to_owned(),
            port: broker.port(),
        }
    }
}

/// Writes the host and the port, such as `kafka-2.internal:9092`. An IPv6 address stands in brackets.
impl fmt::Display for ClusterAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// What a backend does with a request.
#[derive(Debug, Clone)]
pub(crate) enum Route {
    /// The overlay answers the request, and the cluster never sees it. This response is the truth of the call.
    Answer(Response),
    /// The request goes to the cluster.
    Forward {
        /// Where the connection of the request goes when it is not open yet: the first of these that takes it.
        to: Vec<ClusterAddress>,
        /// The request as the cluster gets it.
        request: Request,
        /// Whether the cluster answers it.
        reply: Reply,
    },
}

/// One real cluster, as its backend knows it.
///
/// | It knows | From |
/// |---|---|
/// | The brokers of the cluster, each with its real address | The answers of the cluster, through the overlay. |
/// | The endpoints that accept connections | The same answers: the bootstrap endpoint, and one per broker. |
/// | The address clients are told for each broker | The engine, which opens a listener for each endpoint and says where it [listens](Self::listens). |
///
/// The backend asks the cluster for its brokers, and tells this type the [answer](Self::named).
/// From then on the type decides [where a request goes](Self::route), and makes the [truth](Self::truth) of an answer.
/// It opens no connection, and no listener.
#[derive(Debug, Clone)]
pub(crate) struct Cluster {
    bootstrap: ClusterAddress,
    /// The host under which clients reach every listener of the proxy.
    advertised_host: HostName,
    overlay: Overlay,
    endpoints: Endpoints,
}

impl Cluster {
    /// The cluster behind `bootstrap`, of which nothing is known yet. No endpoint accepts connections.
    pub(crate) fn new(bootstrap: &ServerAddress, advertised_host: HostName) -> Self {
        Self {
            bootstrap: ClusterAddress::from(bootstrap),
            advertised_host,
            overlay: Overlay::new(AdvertisedAddresses::new()),
            endpoints: Endpoints::none(),
        }
    }

    /// The cluster named its brokers in `answer`, the response to a Metadata request of the backend itself.
    ///
    /// The brokers of that answer are the cluster from now on:
    ///
    /// - The endpoint of each of them opens, and so does the bootstrap endpoint.
    ///   A broker that had its endpoint keeps it, with the address of its listener.
    /// - A broker that is new has no listener yet, so clients are not told about it
    ///   until the engine says where its endpoint [listens](Self::listens).
    /// - The endpoint of a broker that the answer no longer names closes, and the address of its listener is forgotten.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the answer does not read,
    /// and when it names no broker with an address.
    /// An error changes nothing: the cluster stays as the last answer named it.
    pub(crate) fn named(&mut self, answer: &Response) -> Result<(), UpstreamBackendError> {
        // The overlay keeps every broker it once learned, and a broker that left would keep its address.
        // A new overlay knows the brokers of this answer, and no other.
        let mut overlay = Overlay::new(AdvertisedAddresses::new());
        // The overlay says that the response of the cluster cannot be read, and why.
        overlay.learn_from(answer).map_err(UpstreamBackendError::undiscovered)?;
        let brokers: BTreeSet<BrokerId> = overlay.real_brokers().map(|(broker, _)| broker).collect();
        if brokers.is_empty() {
            return Err(UpstreamBackendError::undiscovered(
                "its answer names no broker with an address",
            ));
        }

        for (broker, listener) in self.overlay.advertised().iter() {
            if brokers.contains(&broker) {
                // No two brokers shared this address before, so none do now.
                let _ = overlay.advertise(broker, listener.clone());
            }
        }
        let left: Vec<Endpoint> = self
            .broker_endpoints()
            .filter(|broker| !brokers.contains(broker))
            .map(Endpoint::Broker)
            .collect();
        for endpoint in left {
            self.endpoints.close(endpoint);
        }
        self.endpoints.open(Endpoint::Bootstrap);
        self.endpoints.extend(brokers.into_iter().map(Endpoint::Broker));
        self.overlay = overlay;
        Ok(())
    }

    /// The engine listens for `endpoint` on `address`, so clients are told that address from now on:
    /// the advertised host, and the port of the listener.
    ///
    /// The bootstrap endpoint has no address that an answer tells.
    /// An endpoint that accepts no connections takes none: its broker left, and the engine closes the listener.
    ///
    /// # Errors
    ///
    /// [`ListenerRejection::Unadvertisable`] when the port of `address` is zero,
    /// and when clients are told the same address for another broker.
    /// Nothing changes then.
    pub(crate) fn listens(&mut self, endpoint: Endpoint, address: SocketAddr) -> Result<(), ListenerRejection> {
        let Endpoint::Broker(broker) = endpoint else {
            return Ok(());
        };
        if !self.endpoints.contains(endpoint) {
            return Ok(());
        }
        let unadvertisable = |reason: String| ListenerRejection::Unadvertisable {
            endpoint,
            address,
            reason,
        };
        let advertised = BrokerAddress::new(self.advertised_host.as_str(), address.port())
            .map_err(|error| unadvertisable(error.to_string()))?;
        self.overlay
            .advertise(broker, advertised)
            .map_err(|error| unadvertisable(error.to_string()))
    }

    /// The brokers whose endpoints accept connections.
    fn broker_endpoints(&self) -> impl Iterator<Item = BrokerId> + '_ {
        self.endpoints.iter().filter_map(Endpoint::broker)
    }

    /// The endpoints that accept connections, and how often each one opened.
    pub(crate) const fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }

    /// Whether an answer of the cluster named a broker that has no endpoint.
    ///
    /// Clients are not told about such a broker. The backend asks the cluster for its brokers again,
    /// the endpoint of the broker opens, and the engine opens a listener for it.
    pub(crate) fn names_broker_without_endpoint(&self) -> bool {
        let mut named = self.overlay.real_brokers().map(|(broker, _)| Endpoint::Broker(broker));
        named.any(|endpoint| !self.endpoints.contains(endpoint))
    }

    /// The servers that know the whole cluster: the bootstrap server first, then every broker in the order of the IDs.
    ///
    /// The bootstrap endpoint stands for no broker, so its requests go to the first of these that takes a connection.
    /// The backend asks the same servers for the brokers of the cluster.
    pub(crate) fn any_broker(&self) -> Vec<ClusterAddress> {
        let brokers = self
            .overlay
            .real_brokers()
            .map(|(_, address)| ClusterAddress::from(address));
        let mut servers = vec![self.bootstrap.clone()];
        servers.extend(brokers.filter(|broker| *broker != self.bootstrap));
        servers
    }

    /// Decide what happens to `request`, which arrived on a connection to `endpoint`.
    ///
    /// # Errors
    ///
    /// - [`BackendFailure::NoSuchEndpoint`] when the endpoint accepts no connections,
    ///   or stands for a broker the cluster never named.
    /// - [`BackendFailure::ResponseNotBuilt`] when the overlay cannot read a request that it has to read,
    ///   or cannot write the request or the answer it makes of it.
    ///   The cluster gets nothing of such a request.
    pub(crate) fn route(&self, endpoint: Endpoint, request: &Request) -> Result<Route, BackendFailure> {
        if !self.endpoints.contains(endpoint) {
            return Err(BackendFailure::NoSuchEndpoint { endpoint });
        }
        let forwarding = self.overlay.forward(endpoint, request).map_err(|error| match error {
            OverlayError::UnknownBroker { .. } => BackendFailure::NoSuchEndpoint { endpoint },
            OverlayError::UnreadableRequest { error, .. } | OverlayError::UnwritableRequest { error, .. } => {
                BackendFailure::ResponseNotBuilt { error }
            }
            other => BackendFailure::ResponseNotBuilt {
                error: WireError::Encode {
                    what: "response",
                    reason: other.to_string(),
                },
            },
        })?;
        match forwarding {
            Forwarding::Answer(response) => Ok(Route::Answer(response)),
            Forwarding::Forward { to, request } => {
                let reply = Reply::to(&request).map_err(|error| BackendFailure::ResponseNotBuilt { error })?;
                let to = match to {
                    Destination::AnyBroker => self.any_broker(),
                    Destination::Broker { address, .. } => vec![ClusterAddress::from(&address)],
                };
                Ok(Route::Forward { to, request, reply })
            }
        }
    }

    /// The answer of the backend to `request`, which the cluster answered with `upstream`.
    ///
    /// | The overlay | The answer |
    /// |---|---|
    /// | Makes the truth of the response. | [`BackendAnswer::Truth`] |
    /// | Cannot read the response, or finds that it answers another request. | [`BackendAnswer::NotAResponse`]. The response may tell a real address, so it goes nowhere. |
    /// | Cannot write the truth. | [`BackendFailure::ResponseNotBuilt`] |
    pub(crate) fn truth(&mut self, request: &Request, upstream: Response) -> BackendAnswer {
        match self.overlay.answer(request, upstream) {
            Ok(truth) => truth.into(),
            Err(OverlayError::UnreadableResponse { error, .. }) => BackendAnswer::NotAResponse { error },
            Err(OverlayError::UnwritableResponse { error, .. }) => BackendFailure::ResponseNotBuilt { error }.into(),
            Err(other) => BackendAnswer::NotAResponse {
                error: WireError::Decode {
                    what: "response",
                    reason: other.to_string(),
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_backend::Truth;
    use gaslightmq_kernel::ErrorCode;
    use gaslightmq_wire::Frame;
    use gaslightmq_wire::messages::metadata_response::MetadataResponseBroker;
    use gaslightmq_wire::messages::produce_request::ProduceRequest;
    use gaslightmq_wire::messages::{FetchRequest, FetchResponse, HeartbeatRequest, MetadataRequest, MetadataResponse};
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    /// A cluster behind `kafka.staging:9092`, whose proxy clients reach under `gaslight.staging`.
    fn cluster() -> Cluster {
        Cluster::new(
            &"kafka.staging:9092".parse().unwrap(),
            "gaslight.staging".parse().unwrap(),
        )
    }

    fn metadata() -> Request {
        Request::new(12, 1, Some("gaslightmq"), &MetadataRequest::default()).unwrap()
    }

    /// A Metadata response that names each of these brokers at `kafka-<id>.internal:9092`.
    fn naming<const N: usize>(brokers: [i32; N]) -> Response {
        let broker = |id: i32| {
            MetadataResponseBroker::default()
                .with_node_id(id.into())
                .with_host(StrBytes::from_string(format!("kafka-{id}.internal")))
                .with_port(9092)
        };
        let body = MetadataResponse::default().with_brokers(brokers.map(broker).to_vec());
        metadata().response(&body).unwrap()
    }

    fn broker(id: u16) -> Endpoint {
        Endpoint::Broker(BrokerId::from(id))
    }

    fn open(cluster: &Cluster) -> Vec<Endpoint> {
        cluster.endpoints().iter().collect()
    }

    /// The engine opened a listener for the endpoint of broker `id`, on this port of every address.
    fn listening(cluster: &mut Cluster, id: u16, port: u16) -> Result<(), ListenerRejection> {
        cluster.listens(broker(id), SocketAddr::from(([0, 0, 0, 0], port)))
    }

    /// The brokers that a client is told about when the cluster names `brokers`, each with its address.
    fn told<const N: usize>(cluster: &mut Cluster, brokers: [i32; N]) -> Vec<(i32, String)> {
        let request = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default()).unwrap();
        let truth = truth_of(cluster.truth(&request, naming(brokers)));
        let told: MetadataResponse = truth.response().body().unwrap();
        told.brokers
            .iter()
            .map(|broker| (broker.node_id.0, format!("{}:{}", broker.host, broker.port)))
            .collect()
    }

    fn servers(addresses: &[ClusterAddress]) -> Vec<String> {
        addresses.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_cluster_that_named_nothing_has_no_endpoint() {
        let cluster = cluster();

        assert!(cluster.endpoints().is_empty());
        assert_eq!(servers(&cluster.any_broker()), ["kafka.staging:9092"]);
        let heartbeat = Request::new(4, 1, None, &HeartbeatRequest::default()).unwrap();
        assert_eq!(
            cluster.route(Endpoint::Bootstrap, &heartbeat).unwrap_err(),
            BackendFailure::NoSuchEndpoint {
                endpoint: Endpoint::Bootstrap
            }
        );
    }

    #[test]
    fn every_broker_the_cluster_names_gets_an_endpoint() {
        let mut cluster = cluster();

        cluster.named(&naming([2, 1])).unwrap();

        assert_eq!(open(&cluster), [Endpoint::Bootstrap, broker(1), broker(2)]);
        assert!(!cluster.names_broker_without_endpoint());
        assert_eq!(
            servers(&cluster.any_broker()),
            ["kafka.staging:9092", "kafka-1.internal:9092", "kafka-2.internal:9092"]
        );
    }

    #[test]
    fn clients_are_told_about_a_broker_once_its_endpoint_has_a_listener() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        assert_eq!(told(&mut cluster, [1, 2]), [], "no endpoint has a listener yet");

        // The engine opened the listener of broker 2 on a free port, and says which one it got.
        listening(&mut cluster, 2, 50_123).unwrap();

        assert_eq!(told(&mut cluster, [1, 2]), [(2, "gaslight.staging:50123".to_owned())]);
        listening(&mut cluster, 1, 50_124).unwrap();
        assert_eq!(
            told(&mut cluster, [1, 2]),
            [
                (1, "gaslight.staging:50124".to_owned()),
                (2, "gaslight.staging:50123".to_owned())
            ]
        );
    }

    #[test]
    fn the_bootstrap_endpoint_has_no_address_that_an_answer_tells() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();

        let listening = cluster.listens(Endpoint::Bootstrap, "0.0.0.0:9092".parse().unwrap());

        assert_eq!(listening, Ok(()));
        assert_eq!(told(&mut cluster, [1]), []);
    }

    #[test]
    fn the_same_brokers_again_change_no_endpoint_and_no_address() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();
        listening(&mut cluster, 2, 19_093).unwrap();
        let known = cluster.endpoints().clone();

        cluster.named(&naming([1, 2])).unwrap();

        assert_eq!(cluster.endpoints(), &known);
        assert_eq!(
            told(&mut cluster, [1, 2]),
            [
                (1, "gaslight.staging:19092".to_owned()),
                (2, "gaslight.staging:19093".to_owned())
            ]
        );
    }

    #[test]
    fn a_broker_that_joins_opens_an_endpoint_and_waits_for_its_listener() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();
        listening(&mut cluster, 2, 19_093).unwrap();
        let known = cluster.endpoints().clone();

        cluster.named(&naming([1, 2, 3])).unwrap();

        let endpoints = cluster.endpoints();
        assert_eq!(endpoints.opened_since(&known).collect::<Vec<_>>(), [broker(3)]);
        assert_eq!(endpoints.closed_since(&known).count(), 0);
        assert_eq!(told(&mut cluster, [1, 2, 3]).len(), 2);
        listening(&mut cluster, 3, 19_094).unwrap();
        assert_eq!(
            told(&mut cluster, [1, 2, 3])[2],
            (3, "gaslight.staging:19094".to_owned())
        );
    }

    #[test]
    fn a_broker_that_leaves_closes_its_endpoint_and_is_forgotten() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2, 3])).unwrap();
        listening(&mut cluster, 3, 19_094).unwrap();
        let known = cluster.endpoints().clone();

        cluster.named(&naming([1, 2])).unwrap();

        let endpoints = cluster.endpoints();
        assert_eq!(endpoints.closed_since(&known).collect::<Vec<_>>(), [broker(3)]);
        assert_eq!(endpoints.opened_since(&known).count(), 0);
        let heartbeat = Request::new(4, 1, None, &HeartbeatRequest::default()).unwrap();
        assert_eq!(
            cluster.route(broker(3), &heartbeat).unwrap_err(),
            BackendFailure::NoSuchEndpoint { endpoint: broker(3) }
        );
        // The engine has not closed the listener yet, and says where it listens. Nobody is sent there.
        assert_eq!(listening(&mut cluster, 3, 19_094), Ok(()));
        assert_eq!(told(&mut cluster, [1, 2, 3]), []);
    }

    #[test]
    fn a_broker_that_comes_back_opens_its_endpoint_anew_and_waits_for_a_new_listener() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 2, 19_093).unwrap();
        let known = cluster.endpoints().clone();

        cluster.named(&naming([1])).unwrap();
        cluster.named(&naming([1, 2])).unwrap();

        let endpoints = cluster.endpoints();
        assert_eq!(open(&cluster), known.iter().collect::<Vec<_>>());
        assert_eq!(endpoints.closed_since(&known).collect::<Vec<_>>(), [broker(2)]);
        assert_eq!(endpoints.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);
        // The listener of the endpoint that closed is gone, so nobody is sent to its address.
        assert_eq!(told(&mut cluster, [1, 2]), []);
        listening(&mut cluster, 2, 50_200).unwrap();
        assert_eq!(told(&mut cluster, [1, 2]), [(2, "gaslight.staging:50200".to_owned())]);
    }

    #[test]
    fn a_listener_that_opens_anew_moves_its_broker_to_the_new_address() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();

        listening(&mut cluster, 1, 19_192).unwrap();

        assert_eq!(told(&mut cluster, [1]), [(1, "gaslight.staging:19192".to_owned())]);
    }

    #[test]
    fn a_listener_that_clients_cannot_tell_from_another_is_rejected() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        cluster.listens(broker(1), "10.0.4.1:19092".parse().unwrap()).unwrap();

        // Clients see every listener under one host, so the same port of another address is the same address to them.
        let shared = cluster
            .listens(broker(2), "10.0.4.2:19092".parse().unwrap())
            .unwrap_err();
        let any_port = cluster.listens(broker(2), "10.0.4.2:0".parse().unwrap()).unwrap_err();

        assert_eq!(
            shared.to_string(),
            "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
             broker 2 cannot be advertised at gaslight.staging:19092: broker 1 is advertised there"
        );
        assert_eq!(
            any_port.to_string(),
            "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:0: \
             the port of a broker address cannot be zero"
        );
        assert_eq!(told(&mut cluster, [1, 2]), [(1, "gaslight.staging:19092".to_owned())]);
    }

    #[test]
    fn an_answer_that_does_not_read_changes_nothing() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();
        let known = cluster.endpoints().clone();

        let none = cluster.named(&naming([])).unwrap_err();
        assert_eq!(
            none.to_string(),
            "the cluster did not name its brokers: its answer names no broker with an address"
        );
        let cut = Response::decode(metadata().api(), Frame::new(&[0_u8, 0, 0, 1, 0, 9][..])).unwrap();
        let unreadable = cluster.named(&cut).unwrap_err();
        assert!(
            unreadable.to_string().starts_with(
                "the cluster did not name its brokers: the Metadata v12 response of the cluster cannot be read: "
            ),
            "{unreadable}"
        );

        assert_eq!(cluster.endpoints(), &known);
        assert_eq!(told(&mut cluster, [1, 2]), [(1, "gaslight.staging:19092".to_owned())]);
        for error in [none, unreadable] {
            assert!(std::error::Error::source(&error).is_none());
        }
    }

    #[test]
    fn a_request_goes_to_the_broker_of_its_endpoint() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        let heartbeat = Request::new(4, 7, Some("payments-api"), &HeartbeatRequest::default()).unwrap();

        let Route::Forward { to, request, reply } = cluster.route(broker(2), &heartbeat).unwrap() else {
            panic!("the cluster answers a Heartbeat");
        };

        assert_eq!(servers(&to), ["kafka-2.internal:9092"]);
        assert_eq!(request.frame(), heartbeat.frame());
        assert_eq!(reply, Reply::Comes);
    }

    #[test]
    fn a_bootstrap_request_goes_to_the_bootstrap_server_or_else_to_a_broker() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();

        let Route::Forward { to, .. } = cluster.route(Endpoint::Bootstrap, &metadata()).unwrap() else {
            panic!("the cluster answers a Metadata request");
        };

        assert_eq!(
            servers(&to),
            ["kafka.staging:9092", "kafka-1.internal:9092", "kafka-2.internal:9092"]
        );
    }

    #[test]
    fn a_produce_without_acks_has_no_reply() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();
        let produce = Request::new(9, 7, None, &ProduceRequest::default().with_acks(0)).unwrap();

        let Route::Forward { reply, .. } = cluster.route(broker(1), &produce).unwrap() else {
            panic!("the cluster takes a Produce");
        };

        assert_eq!(reply, Reply::Never);
    }

    #[test]
    fn the_overlay_answers_a_fetch_that_continues_a_session() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();
        let body = FetchRequest::default().with_session_id(41).with_session_epoch(3);
        let fetch = Request::new(12, 7, None, &body).unwrap();

        let Route::Answer(response) = cluster.route(broker(1), &fetch).unwrap() else {
            panic!("the overlay answers a Fetch that continues a session");
        };

        let answer: FetchResponse = response.body().unwrap();
        assert_eq!(answer.error_code, ErrorCode::FETCH_SESSION_ID_NOT_FOUND.value());
    }

    #[test]
    fn a_fetch_that_does_not_read_goes_nowhere() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();
        let whole = Request::new(12, 7, None, &FetchRequest::default()).unwrap();
        let cut = whole.frame().payload().slice(..whole.frame().payload().len() - 4);
        let fetch = Request::decode(Frame::new(cut)).unwrap();

        let failure = cluster.route(broker(1), &fetch).unwrap_err();

        assert!(matches!(failure, BackendFailure::ResponseNotBuilt { .. }));
    }

    #[test]
    fn the_truth_names_the_listeners_and_keeps_the_answer_of_the_cluster() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();
        listening(&mut cluster, 2, 19_093).unwrap();
        let request = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default()).unwrap();

        let BackendAnswer::Truth(truth) = cluster.truth(&request, naming([1, 2])) else {
            panic!("the overlay reads a Metadata response");
        };

        let told: MetadataResponse = truth.response().body().unwrap();
        let addresses: Vec<(&str, i32)> = told.brokers.iter().map(|b| (b.host.as_str(), b.port)).collect();
        assert_eq!(addresses, [("gaslight.staging", 19092), ("gaslight.staging", 19093)]);
        let upstream: MetadataResponse = truth.upstream().unwrap().body().unwrap();
        assert_eq!(upstream.brokers[0].host.as_str(), "kafka-1.internal");
    }

    #[test]
    fn an_answer_that_names_a_new_broker_leaves_it_out_and_says_so() {
        let mut cluster = cluster();
        cluster.named(&naming([1, 2])).unwrap();
        listening(&mut cluster, 1, 19_092).unwrap();
        listening(&mut cluster, 2, 19_093).unwrap();
        assert!(!cluster.names_broker_without_endpoint());

        assert_eq!(told(&mut cluster, [1, 2, 3]).len(), 2);
        assert!(cluster.names_broker_without_endpoint());

        // The backend asks again, and the broker gets its endpoint.
        cluster.named(&naming([1, 2, 3])).unwrap();
        assert!(!cluster.names_broker_without_endpoint());
        assert!(cluster.endpoints().contains(broker(3)));
    }

    #[test]
    fn an_answer_that_does_not_read_is_no_response() {
        let mut cluster = cluster();
        cluster.named(&naming([1])).unwrap();
        let request = Request::new(12, 1, None, &MetadataRequest::default()).unwrap();
        let cut = Response::decode(request.api(), Frame::new(&[0_u8, 0, 0, 1, 0, 9][..])).unwrap();

        assert!(matches!(
            cluster.truth(&request, cut),
            BackendAnswer::NotAResponse { .. }
        ));

        let of_another = Request::new(12, 2, None, &MetadataRequest::default()).unwrap();
        let BackendAnswer::NotAResponse { error } = cluster.truth(&of_another, naming([1])) else {
            panic!("the response answers request 1");
        };
        assert_eq!(
            error.to_string(),
            "cannot decode response: the Metadata v12 response 1 does not answer the Metadata v12 request 2"
        );
    }

    fn truth_of(answer: BackendAnswer) -> Truth {
        match answer {
            BackendAnswer::Truth(truth) => truth,
            other => panic!("the cluster answered, and the backend made of it: {other:?}"),
        }
    }

    #[test]
    fn an_address_stands_in_brackets_when_it_is_ipv6() {
        let name = ClusterAddress::from(&"kafka.staging:9092".parse::<ServerAddress>().unwrap());
        let ipv6 = ClusterAddress::from(&"[2001:db8::2]:9092".parse::<BrokerAddress>().unwrap());

        assert_eq!((name.host(), name.port()), ("kafka.staging", 9092));
        assert_eq!(name.to_string(), "kafka.staging:9092");
        assert_eq!(ipv6.host(), "2001:db8::2");
        assert_eq!(ipv6.to_string(), "[2001:db8::2]:9092");
    }
}
