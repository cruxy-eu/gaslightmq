use std::fmt;

use gaslightmq_backend::Truth;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::{
    ApiKey as KnownApi, DescribeClusterResponse, FetchResponse, FindCoordinatorResponse, MetadataResponse,
    ProduceResponse, ShareAcknowledgeResponse, ShareFetchResponse,
};
use gaslightmq_wire::protocol::{Decodable, Encodable};
use gaslightmq_wire::{Api, Request, Response, WireError};
use gaslightmq_wire_edit::prelude::*;

use crate::fetch_session::{SessionlessFetch, SessionlessFetchFlaw};
use crate::real_brokers::RealBrokers;
use crate::translation::AddressTranslation;
use crate::{AdvertisedAddresses, AdvertisedAddressesError, BrokerAddress, Destination, Forwarding};

/// What the Upstream backend lays over a real cluster, so that clients stay with GaslightMQ.
///
/// A backend has one overlay for its cluster, and asks it twice for every call:
///
/// | When | Method | The overlay |
/// |---|---|---|
/// | Before the cluster sees the request | [`forward`](Self::forward) | Says which real broker gets the request, and takes the fetch session out of a Fetch. |
/// | After the cluster answered | [`answer`](Self::answer) | Reads the real addresses of the brokers, and writes the advertised ones in their place. |
///
/// The overlay opens no connection and holds no socket.
/// It consumes a request or a response, and returns what the backend does next.
///
/// # What it knows
///
/// | It knows | From |
/// |---|---|
/// | The [address clients are told](AdvertisedAddresses) for each broker | The backend, which runs a listener per broker: [`new`](Self::new) and [`advertise`](Self::advertise). |
/// | The [real address](Self::real_address) of each broker | The answers of the cluster, as they pass: [`answer`](Self::answer) and [`learn_from`](Self::learn_from). |
///
/// A cluster can name a broker that the backend has no listener for yet, such as a broker that just joined.
/// Clients are not told about that broker until it is advertised, and [`unadvertised`](Self::unadvertised) names it.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_overlay::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::metadata_response::MetadataResponseBroker;
/// use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse};
/// use gaslightmq_wire::protocol::StrBytes;
///
/// // The backend asks the cluster which brokers it has, and the overlay reads the answer.
/// let mut overlay = Overlay::new(AdvertisedAddresses::new());
/// let broker = |id: i32, host: &'static str| {
///     MetadataResponseBroker::default().with_node_id(id.into()).with_host(StrBytes::from_static_str(host)).with_port(9092)
/// };
/// let cluster = MetadataResponse::default().with_brokers(vec![broker(1, "kafka-1.internal"), broker(2, "kafka-2.internal")]);
/// let discovery = Request::new(12, 1, Some("gaslightmq"), &MetadataRequest::default())?;
/// overlay.learn_from(&discovery.response(&cluster)?)?;
///
/// // The backend opens a listener for every broker that has none, and says where clients find it.
/// let unadvertised: Vec<BrokerId> = overlay.unadvertised().collect();
/// assert_eq!(unadvertised, [BrokerId::from(1), BrokerId::from(2)]);
/// overlay.advertise(BrokerId::from(1), "gaslight.staging:19092".parse()?)?;
/// overlay.advertise(BrokerId::from(2), "gaslight.staging:19093".parse()?)?;
///
/// // A client asks the listener of broker 2 for metadata. The request goes to the real broker 2.
/// let request = Request::new(12, 7, Some("order-ingest"), &MetadataRequest::default())?;
/// let Forwarding::Forward { to, request: forwarded } = overlay.forward(Endpoint::Broker(BrokerId::from(2)), &request)? else {
///     panic!("the cluster answers a Metadata request");
/// };
/// assert_eq!(to.to_string(), "broker 2 at kafka-2.internal:9092");
///
/// // The cluster tells its own addresses, and the client is told those of the listeners.
/// let truth = overlay.answer(&request, forwarded.response(&cluster)?)?;
/// let told: MetadataResponse = truth.response().body()?;
/// let addresses: Vec<(&str, i32)> = told.brokers.iter().map(|broker| (broker.host.as_str(), broker.port)).collect();
/// assert_eq!(addresses, [("gaslight.staging", 19092), ("gaslight.staging", 19093)]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overlay {
    advertised: AdvertisedAddresses,
    real: RealBrokers,
}

impl Overlay {
    /// An overlay that tells clients the addresses of `advertised`, and that knows no real broker yet.
    pub fn new(advertised: AdvertisedAddresses) -> Self {
        Self {
            advertised,
            real: RealBrokers::default(),
        }
    }

    /// Tell clients from now on that `broker` is at `address`: the address of the listener that stands for it.
    ///
    /// # Errors
    ///
    /// [`AdvertisedAddressesError::SharedAddress`] when another broker is advertised at `address`.
    pub fn advertise(&mut self, broker: BrokerId, address: BrokerAddress) -> Result<(), AdvertisedAddressesError> {
        self.advertised.advertise(broker, address)
    }

    /// The address clients are told for each broker.
    pub fn advertised(&self) -> &AdvertisedAddresses {
        &self.advertised
    }

    /// The address at which the real cluster reaches `broker`, or `None` when the cluster never named the broker.
    ///
    /// This is where the backend connects, and what a client must never be told.
    pub fn real_address(&self, broker: BrokerId) -> Option<&BrokerAddress> {
        self.real.address_of(broker)
    }

    /// Every broker that the real cluster named, with its real address, in the order of the broker IDs.
    pub fn real_brokers(&self) -> impl Iterator<Item = (BrokerId, &BrokerAddress)> {
        self.real.iter()
    }

    /// The brokers that the real cluster named and that are not advertised, in the order of the broker IDs.
    ///
    /// Clients are not told about these brokers.
    /// The backend opens a listener for each, and [advertises](Self::advertise) it.
    pub fn unadvertised(&self) -> impl Iterator<Item = BrokerId> {
        self.real
            .iter()
            .map(|(broker, _)| broker)
            .filter(|broker| self.advertised.address_of(*broker).is_none())
    }

    /// The support the Upstream backend declares for each kind of incident.
    ///
    /// The backend cannot change a real cluster, so it applies no incident.
    /// It supports the kinds that the overlay emulates, and the overlay emulates none yet.
    /// A scenario that needs an incident is therefore rejected when it loads, and not halfway through a run.
    pub fn incident_support() -> IncidentSupport {
        IncidentSupport::none()
    }

    /// Read the real addresses of the brokers that `response` names, a response of the real cluster.
    ///
    /// The backend calls it with the answers to its own requests, such as the Metadata request that discovers the brokers.
    /// The answer to a client's request goes to [`answer`](Self::answer), which reads it the same way.
    /// A response that tells no address teaches nothing.
    ///
    /// # Errors
    ///
    /// [`OverlayError::UnreadableResponse`] when the body of the response does not decode.
    pub fn learn_from(&mut self, response: &Response) -> Result<(), OverlayError> {
        self.overlaid(response).map(drop)
    }

    /// Decide what happens to `request` before the real cluster sees it.
    ///
    /// `endpoint` is the endpoint that the client of the request connected to.
    ///
    /// | The request | Is |
    /// |---|---|
    /// | A Fetch that asks for a fetch session | [Forwarded](Forwarding::Forward) without that wish, so the broker grants none. |
    /// | A Fetch that continues a fetch session | [Answered](Forwarding::Answer) with `FETCH_SESSION_ID_NOT_FOUND`, as a broker without that session does. |
    /// | Anything else, also of an API without messages | [Forwarded](Forwarding::Forward) byte for byte. |
    ///
    /// A request for the endpoint of a broker goes to that broker, and a request for the bootstrap endpoint to any.
    /// See [`Destination`].
    ///
    /// # Errors
    ///
    /// - [`OverlayError::UnknownBroker`] when the endpoint stands for a broker that the cluster never named.
    /// - [`OverlayError::UnreadableRequest`] when the body of a Fetch does not decode.
    ///   Nothing says then whether the request asks for a fetch session.
    /// - [`OverlayError::UnwritableRequest`] when a Fetch without its session does not encode.
    /// - [`OverlayError::UnansweredRequest`] when the answer to a Fetch that continues a session cannot be written.
    pub fn forward(&self, endpoint: Endpoint, request: &Request) -> Result<Forwarding, OverlayError> {
        let api = request.api();
        let to = self.destination(endpoint)?;
        if api.known() != Some(KnownApi::Fetch) {
            return Ok(Forwarding::Forward {
                to,
                request: request.clone(),
            });
        }
        let sessionless = SessionlessFetch::of(request).map_err(|flaw| match flaw {
            SessionlessFetchFlaw::UnreadableRequest(error) => OverlayError::UnreadableRequest { api, error },
            SessionlessFetchFlaw::UnwritableRequest(error) => OverlayError::UnwritableRequest { api, error },
        })?;
        match sessionless {
            SessionlessFetch::AsItArrived => Ok(Forwarding::Forward {
                to,
                request: request.clone(),
            }),
            SessionlessFetch::Rewritten(request) => Ok(Forwarding::Forward { to, request }),
            SessionlessFetch::ContinuesSession => {
                let unanswered = |error| OverlayError::UnansweredRequest { api, error };
                let no_session = Failures::whole_call(ErrorCode::FETCH_SESSION_ID_NOT_FOUND);
                EditableRequest::decode(request, &TopicNames::new())
                    .and_then(|asked| asked.error_response(&no_session))
                    .map(Forwarding::Answer)
                    .map_err(unanswered)
            }
        }
    }

    /// Turn `upstream`, the answer of the real cluster to `request`, into the [truth](Truth) of the call.
    ///
    /// The truth keeps `upstream` next to it when the overlay changed the answer, so the record of the call can show both.
    /// When the overlay changed nothing, the truth is `upstream`, byte for byte, and nothing stands next to it.
    ///
    /// `request` is the request of the client, or the one that [`forward`](Self::forward) returned for it.
    ///
    /// | The response | The overlay |
    /// |---|---|
    /// | Tells the address of a broker | Remembers it as the broker's [real address](Self::real_address), and writes the [advertised](AdvertisedAddresses) one. |
    /// | Tells the address of a broker that is not advertised | Leaves the broker out, or says that the coordinator is not available. |
    /// | Is a Fetch response that grants a fetch session | Says that the response is part of no session. |
    /// | Anything else, also of an API without messages | Changes nothing: the truth is the answer, byte for byte. |
    ///
    /// These responses tell addresses: Metadata, `FindCoordinator` and `DescribeCluster`,
    /// and Produce, Fetch, `ShareFetch` and `ShareAcknowledge` where they name the new leader of a partition that moved.
    ///
    /// # Errors
    ///
    /// - [`OverlayError::ResponseOfAnotherCall`] when `upstream` answers another request.
    /// - [`OverlayError::UnreadableResponse`] when the body of a response that the overlay reads does not decode.
    ///   Such a response may tell a real address, so it must not reach the client.
    /// - [`OverlayError::UnwritableResponse`] when the truth does not encode.
    pub fn answer(&mut self, request: &Request, upstream: Response) -> Result<Truth, OverlayError> {
        if upstream.api() != request.api() || upstream.correlation_id() != request.correlation_id() {
            return Err(OverlayError::ResponseOfAnotherCall {
                api: request.api(),
                correlation_id: request.correlation_id(),
                answered_api: upstream.api(),
                answered_correlation_id: upstream.correlation_id(),
            });
        }
        Ok(match self.overlaid(&upstream)? {
            Some(response) => Truth::new(response).with_upstream(upstream),
            None => Truth::new(upstream),
        })
    }

    /// Where a request of a client that connected to `endpoint` goes.
    fn destination(&self, endpoint: Endpoint) -> Result<Destination, OverlayError> {
        match endpoint {
            Endpoint::Bootstrap => Ok(Destination::AnyBroker),
            Endpoint::Broker(broker) => match self.real.address_of(broker) {
                Some(address) => Ok(Destination::Broker {
                    broker,
                    address: address.clone(),
                }),
                None => Err(OverlayError::UnknownBroker { broker }),
            },
        }
    }

    /// Read `upstream`, a response of the real cluster, and return what the overlay makes of it.
    /// `None` means that the overlay changes nothing.
    fn overlaid(&mut self, upstream: &Response) -> Result<Option<Response>, OverlayError> {
        let api = upstream.api();
        let mut translation = AddressTranslation::new(&self.advertised, &mut self.real);
        match api.known() {
            Some(KnownApi::Metadata) => rewritten(upstream, |body: &mut MetadataResponse| {
                translation.metadata(body);
                translation.changed()
            }),
            Some(KnownApi::FindCoordinator) => rewritten(upstream, |body: &mut FindCoordinatorResponse| {
                translation.find_coordinator(body, api.version);
                translation.changed()
            }),
            Some(KnownApi::DescribeCluster) => rewritten(upstream, |body: &mut DescribeClusterResponse| {
                translation.describe_cluster(body);
                translation.changed()
            }),
            Some(KnownApi::Fetch) if SessionlessFetch::applies_to(api.version) => {
                rewritten(upstream, |body: &mut FetchResponse| {
                    let granted = SessionlessFetch::deny_session(body);
                    translation.fetch(body);
                    granted || translation.changed()
                })
            }
            Some(KnownApi::Produce) if api.version >= FIRST_PRODUCE_VERSION_WITH_LEADERS => {
                rewritten(upstream, |body: &mut ProduceResponse| {
                    translation.produce(body);
                    translation.changed()
                })
            }
            Some(KnownApi::ShareFetch) => rewritten(upstream, |body: &mut ShareFetchResponse| {
                translation.share_fetch(body);
                translation.changed()
            }),
            Some(KnownApi::ShareAcknowledge) => rewritten(upstream, |body: &mut ShareAcknowledgeResponse| {
                translation.share_acknowledge(body);
                translation.changed()
            }),
            _ => Ok(None),
        }
    }
}

/// The first version of Produce whose response names the new leader of a partition that moved.
/// A response of an earlier version tells no address, so nobody reads it.
const FIRST_PRODUCE_VERSION_WITH_LEADERS: i16 = 10;

/// Decode the body of `upstream`, let `rewrite` change it, and encode it again when `rewrite` says it changed.
fn rewritten<B>(upstream: &Response, rewrite: impl FnOnce(&mut B) -> bool) -> Result<Option<Response>, OverlayError>
where
    B: Decodable + Encodable,
{
    let api = upstream.api();
    let mut body: B = upstream
        .body()
        .map_err(|error| OverlayError::UnreadableResponse { api, error })?;
    if !rewrite(&mut body) {
        return Ok(None);
    }
    Response::new(api, upstream.correlation_id(), &body)
        .map(Some)
        .map_err(|error| OverlayError::UnwritableResponse { api, error })
}

/// Why the overlay could not say what happens to a request or to a response.
///
/// An error changes nothing in the overlay, except for what it had already [learned](Overlay::real_address).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverlayError {
    /// The client connected to the endpoint of this broker, and the real cluster never named such a broker.
    /// Nothing says where the request goes.
    UnknownBroker { broker: BrokerId },
    /// The body of the request does not decode.
    UnreadableRequest { api: Api, error: WireError },
    /// The request without its fetch session does not encode.
    UnwritableRequest { api: Api, error: WireError },
    /// The overlay answers the request itself, and that answer cannot be written.
    UnansweredRequest { api: Api, error: EditableRequestError },
    /// The body of the real cluster's response does not decode.
    UnreadableResponse { api: Api, error: WireError },
    /// The truth does not encode.
    UnwritableResponse { api: Api, error: WireError },
    /// The response belongs to another request: its API or its correlation ID differs.
    ResponseOfAnotherCall {
        api: Api,
        correlation_id: i32,
        answered_api: Api,
        answered_correlation_id: i32,
    },
}

impl fmt::Display for OverlayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownBroker { broker } => {
                write!(
                    f,
                    "a request for broker {broker} has nowhere to go: the cluster named no such broker"
                )
            }
            Self::UnreadableRequest { api, error } => write!(f, "the {api} request cannot be read: {error}"),
            Self::UnwritableRequest { api, error } => {
                write!(
                    f,
                    "the {api} request cannot be written without its fetch session: {error}"
                )
            }
            Self::UnansweredRequest { api, error } => {
                write!(f, "the {api} request cannot be answered without the cluster: {error}")
            }
            Self::UnreadableResponse { api, error } => {
                write!(f, "the {api} response of the cluster cannot be read: {error}")
            }
            Self::UnwritableResponse { api, error } => {
                write!(f, "the truth of the {api} response cannot be written: {error}")
            }
            Self::ResponseOfAnotherCall {
                api,
                correlation_id,
                answered_api,
                answered_correlation_id,
            } => write!(
                f,
                "the {answered_api} response {answered_correlation_id} does not answer the {api} request {correlation_id}"
            ),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for OverlayError {}
