//! The overlay of the Upstream backend: what GaslightMQ lays over the answers of a real cluster.
//!
//! A proxy in front of a real cluster only works while clients talk to the proxy.
//! The cluster does not know the proxy: it tells every client where its brokers are, and a client goes there.
//! The overlay rewrites what passes between the two, so that clients stay with GaslightMQ
//! and the real cluster only sees requests it can answer in full.
//!
//! | Type | Is |
//! |---|---|
//! | [`Overlay`] | The overlay of one real cluster: it decides what happens to each request, and makes the truth of each answer. |
//! | [`AdvertisedAddresses`] | The address clients are told for each broker: that of the listener that stands for it. |
//! | [`BrokerAddress`] | The host and the port of a broker, real or advertised. |
//! | [`Forwarding`] | What the overlay decides about a request: forward it, as it is or rewritten, or answer it. |
//! | [`Destination`] | Which broker of the real cluster a request goes to. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! [`Overlay::answer`] returns the [`Truth`](gaslightmq_backend::Truth) of the backend contract:
//! the real cluster's answer seen through the overlay, with that answer next to it when the overlay changed it.
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_overlay::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::find_coordinator_response::Coordinator;
//! use gaslightmq_wire::messages::{FetchRequest, FindCoordinatorRequest, FindCoordinatorResponse};
//! use gaslightmq_wire::protocol::StrBytes;
//!
//! // The backend runs a listener for broker 2, and clients find it at `gaslight.staging:19093`.
//! let listener = "gaslight.staging:19093".parse().expect("an address");
//! let advertised = AdvertisedAddresses::new().with(BrokerId::from(2), listener).expect("the first address");
//! let mut overlay = Overlay::new(advertised);
//!
//! // A client asks where the coordinator of its group is, and the real cluster says: on broker 2.
//! let payments = || StrBytes::from_static_str("payments");
//! let asked = FindCoordinatorRequest::default().with_coordinator_keys(vec![payments()]);
//! let request = Request::new(4, 7, Some("payments-api"), &asked).expect("a request");
//! let Forwarding::Forward { to, request: forwarded } = overlay.forward(Endpoint::Bootstrap, &request)? else {
//!     panic!("the cluster answers a FindCoordinator request");
//! };
//! assert_eq!(to, Destination::AnyBroker);
//! let kafka_2 = StrBytes::from_static_str("kafka-2.internal");
//! let coordinator = Coordinator::default().with_key(payments()).with_node_id(2.into()).with_host(kafka_2).with_port(9092);
//! let found = FindCoordinatorResponse::default().with_coordinators(vec![coordinator]);
//! let upstream = forwarded.response(&found).expect("a response");
//!
//! // The truth sends the client to the listener, and the overlay knows where the real broker is.
//! let truth = overlay.answer(&request, upstream).expect("an answer to the request");
//! let told: FindCoordinatorResponse = truth.response().body().expect("a body that decodes");
//! assert_eq!((told.coordinators[0].host.as_str(), told.coordinators[0].port), ("gaslight.staging", 19093));
//! assert!(truth.upstream().is_some());
//! let kafka_2: BrokerAddress = "kafka-2.internal:9092".parse().expect("an address");
//! assert_eq!(overlay.real_address(BrokerId::from(2)), Some(&kafka_2));
//!
//! // The client then fetches from the listener of broker 2, and asks for a fetch session.
//! let with_session = FetchRequest::default().with_session_epoch(0);
//! let fetch = Request::new(12, 8, Some("payments-api"), &with_session).expect("a request");
//! let Forwarding::Forward { to, request: sessionless } = overlay.forward(Endpoint::Broker(BrokerId::from(2)), &fetch)? else {
//!     panic!("the cluster answers a full Fetch request");
//! };
//! assert_eq!(to.to_string(), "broker 2 at kafka-2.internal:9092");
//! assert_eq!(sessionless.body::<FetchRequest>().expect("a body that decodes").session_epoch, -1);
//! # Ok::<(), ForwardError>(())
//! ```
//!
//! # The overlay is part of the truth
//!
//! The overlay is no lie and no rule.
//! What it answers is the truth of the call: rules match on it, and explanations compare the delivered response against it.
//! It takes no part in the conflicts between lies, and no rule can tell it from the cluster.
//! The [`Truth`](gaslightmq_backend::Truth) keeps the real cluster's answer next to it when the overlay changed that answer,
//! so a call record can show both.
//!
//! # Address translation
//!
//! The Upstream backend runs one listener for each broker of the real cluster.
//! Wherever a response tells the address of a broker, the overlay writes the address of that broker's listener.
//! It is always on: a client that learns one real address talks to the cluster directly from then on.
//!
//! | Response | Tells the address of |
//! |---|---|
//! | Metadata | Every broker. |
//! | `DescribeCluster` | Every broker. A list of controllers passes: a controller is no broker, and has no listener. |
//! | `FindCoordinator` | The coordinator, or the coordinator of each key since version 4. |
//! | Produce since version 10, Fetch since version 16, `ShareFetch`, `ShareAcknowledge` | The new leader of a partition that moved. |
//!
//! The overlay changes the host and the port, and nothing else.
//! Broker IDs, racks, leaders and replicas stay as the cluster wrote them.
//!
//! The backend tells the overlay which address stands for which broker, as [`AdvertisedAddresses`].
//! A cluster can name a broker that has no listener yet.
//! The real address of such a broker never reaches a client:
//!
//! - A list of brokers or of leaders comes without the broker.
//!   A client that finds a leader without an address asks for metadata again, as it does while a broker starts.
//! - A coordinator on that broker reads as `COORDINATOR_NOT_AVAILABLE`, and the client asks again.
//!
//! [`Overlay::unadvertised`] names those brokers.
//! Once the backend [advertises](Overlay::advertise) one, the next answer has it.
//!
//! # Where a request goes
//!
//! The translation has a way back.
//! A client connects to an [endpoint](gaslightmq_field::Endpoint) of the proxy,
//! and the endpoint of a broker stands for that broker alone.
//! [`Overlay::forward`] names the [`Destination`] of a request: the broker of the endpoint, at its real address.
//! The overlay reads the real addresses from the answers of the cluster, before it translates them.
//! The bootstrap endpoint stands for no broker, so a request for it goes to any.
//!
//! # Sessionless Fetch
//!
//! A client with a fetch session only names the partitions that changed since its last Fetch.
//! Most of its Fetch requests would not carry the topics and partitions that selectors test.
//! The overlay therefore keeps fetch sessions away from the real cluster, in three places:
//!
//! | Message | The overlay |
//! |---|---|
//! | A full Fetch request, since version 7 | Forwards it with session ID 0 and epoch -1, which asks for no session. |
//! | A Fetch request that continues a session | Answers it with `FETCH_SESSION_ID_NOT_FOUND`. The real cluster never sees it. |
//! | A Fetch response | Makes it say session ID 0, whatever the broker granted. |
//!
//! A broker that is asked for no session creates none.
//! A client that is granted no session sends its next Fetch in full again.
//! A request that continues a session only names what changed, and no broker holds the session it continues,
//! so nothing of it can be forwarded.
//! The client reads the error as a session that the broker dropped, and starts over with a full request.
//!
//! A Fetch before version 7 knows no sessions, and passes byte for byte.
//! So does a request of version 7 or later that asks for no session.
//!
//! # What passes untouched
//!
//! Everything the tables above do not name passes byte for byte, in both directions.
//! That includes a request and a response of an API without messages in `gaslightmq_wire`:
//! the overlay forwards the frame, and does not read it.
//! It also includes the share session of a `ShareFetch`: only fetch sessions stay away from the cluster.
//!
//! # Incidents
//!
//! The Upstream backend cannot change a real cluster.
//! It emulates an incident inside its overlay instead, by rewriting answers for as long as the incident is in force.
//! This crate emulates no incident yet, and [`Overlay::incident_support`] says so:
//! it declares every kind of incident unsupported, so a scenario that needs one is rejected when it loads.
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`BrokerAddress`] | [`BrokerAddressError`] | The host is empty, too long or has whitespace, the port is zero, or a text is not a host and a port. |
//! | [`AdvertisedAddresses`] | [`AdvertisedAddressesError`] | Two brokers would share one address. |
//!
//! The two things the overlay does fail in different ways, so each has an error of its own.
//! A variant is what the backend decides on, and the rest is text in its `reason`.
//!
//! | What failed | Error | What is wrong |
//! |---|---|---|
//! | [`Overlay::forward`] | [`ForwardError`] | The request names a broker the cluster never named, or the overlay could not decide what happens to a request that it has to read. |
//! | [`Overlay::answer`], [`Overlay::learn_from`] | [`AnswerError`] | What the cluster sent is no answer, or the truth cannot be written. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! What the reader or the writer of a message said is part of the `reason`, once, after what the overlay was doing.
//!
//! An [`AnswerError`] never hides a real address.
//! When the overlay cannot read a response that tells addresses, it returns the error and no truth,
//! and the backend must not pass that response on.
//!
//! [`Forwarding`] and [`Destination`] hold any value, so they have no error.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_wire` for the codec, on `gaslightmq_backend` for the truth it returns,
//! on `gaslightmq_wire_edit` for the response in which a call failed,
//! on `gaslightmq_incident` for the support it declares, on `gaslightmq_field` for the endpoint of a connection,
//! and on `gaslightmq_kernel` for broker IDs and error codes.
//! It has no async code, no socket, no file, no clock and no randomness.
//! It reaches the Kafka codec, so it does not compile to WebAssembly, and no crate that the UI shares depends on it.

mod address;
mod advertised;
mod fetch_session;
mod forwarding;
mod overlay;
mod real_brokers;
mod translation;

pub use address::{BrokerAddress, BrokerAddressError};
pub use advertised::{AdvertisedAddresses, AdvertisedAddressesError};
pub use forwarding::{Destination, Forwarding};
pub use overlay::{AnswerError, ForwardError, Overlay};

/// Every item of this crate, with its error.
///
/// Import it whole wherever code lays an overlay over a cluster, next to the preludes of the crates it builds on.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_overlay::prelude::*;
///
/// let address: Result<BrokerAddress, BrokerAddressError> = "gaslight.staging:19092".parse();
/// let advertised = AdvertisedAddresses::new().with(BrokerId::from(1), address?).expect("the first address");
/// assert_eq!(Overlay::new(advertised).advertised().len(), 1);
/// # Ok::<(), BrokerAddressError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        AdvertisedAddresses, AdvertisedAddressesError, AnswerError, BrokerAddress, BrokerAddressError, Destination,
        ForwardError, Forwarding, Overlay,
    };
}
