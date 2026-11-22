//! The state of one session, and the client context that sessions share.
//!
//! A session is the runtime of one client connection.
//! Its pipeline owns the state of this crate, and decides with it what happens to each call.
//!
//! | Type | Is |
//! |---|---|
//! | [`Session`] | The state of one session: lifecycle, identity, key, counters, calls in flight and the stalled state. |
//! | [`Connection`] | What is known the moment a client connects: the connection number, the address and the endpoint. |
//! | [`Identity`] | The identity fields of a session, as far as they are known. |
//! | [`Identification`], [`IdentityChange`] | What the header of one request changed about the identity. |
//! | [`Lifecycle`] | Where a session is in its life: opened, identified or closed. |
//! | [`Closed`], [`ClosedBy`] | That a session closed, when, and who closed it: the client, the backend or a lie. |
//! | [`SessionKey`] | The host, the endpoint and the ordinal of the connection: the same in every run. |
//! | [`ConnectionOrdinal`] | Which connection from its host to its endpoint a connection is. |
//! | [`ConnectionOrdinals`] | The ordinals a proxy handed out, per host and endpoint. |
//! | [`ClientInstance`] | A `client.id` with a host: one client across its connections and its reconnects. |
//! | [`ClientContexts`] | The handle to every client context, which the engine shares with its sessions. |
//! | [`ClientContext`] | One session's share of the context of its client instance. |
//! | [`LearnedFields`] | What a client context knows at one moment: the values of the learned fields. |
//! | [`InFlightCalls`] | The calls in flight, and the order in which their requests and responses go on. |
//! | [`InFlightCall`], [`CallPhase`] | One call in flight, and how far it is. |
//! | [`Stalled`] | That a silence stopped a connection, and by which call. |
//!
//! ```
//! use gaslightmq_clock::{Duration, Instant};
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_session::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::{FetchRequest, JoinGroupRequest};
//!
//! // The engine owns the client contexts, and hands every session a handle to them.
//! let contexts = ClientContexts::new(Duration::from_secs(60));
//! let mut ordinals = ConnectionOrdinals::new();
//! let now = Instant::ZERO;
//! let mut open = |number: u64, broker: i32| {
//!     let address: ClientAddress = "10.0.4.27:51459".parse().expect("an address");
//!     let endpoint = Endpoint::Broker(BrokerId::try_from(broker).expect("a broker"));
//!     let connection = Connection::new(ConnectionNumber::new(number), address, endpoint);
//!     let ordinal = ordinals.assign(connection.host(), endpoint);
//!     Session::<()>::open(connection, ordinal, contexts.clone(), now)
//! };
//!
//! // A consumer joins its group on the connection to its coordinator.
//! let mut coordinator = open(7, 1);
//! let join = Request::new(5, 1, Some("analytics"), &JoinGroupRequest::default())?;
//! coordinator.identify(&join);
//! // The pipeline decodes the request, and tells the session what it learned.
//! coordinator.learn(GroupId::new("analytics-etl")?);
//!
//! // The consumer fetches on another connection. A Fetch never names a group.
//! let mut fetcher = open(8, 2);
//! let fetch = Request::new(12, 1, Some("analytics"), &FetchRequest::default())?;
//! fetcher.identify(&fetch);
//!
//! // The client context provides the group, so a selector on `group` matches the Fetch.
//! let fields = fetcher.fields().with(ApiKey::FETCH);
//! let group = fields.read(Field::Group).present().expect("the client named its group");
//! assert_eq!(group.to_string(), "analytics-etl");
//! assert_eq!(group.origin(), Origin::ClientContext);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Lifecycle and identity
//!
//! A session [opens](Session::open) when a client connects.
//! Only its [`Connection`] is known then.
//! The pipeline hands the session every request, and [`Session::identify`] reads the header.
//! The first request identifies the session, and an `ApiVersions` request names the client software.
//! The session returns what changed, as an [`Identification`].
//!
//! A session [closes](Session::close) once, and records who closed it.
//! It never opens again: a client that reconnects gets a new session.
//!
//! # The client context
//!
//! One Kafka client opens several connections, and most of them never name the group or the transaction.
//! The sessions of one [`ClientInstance`] therefore share a client context,
//! which remembers the learned fields that any of them carried.
//!
//! The engine owns the contexts as a [`ClientContexts`], and every session holds a clone of that handle.
//! A session joins its context when it is identified.
//! Its pipeline [reads](Session::learned) the learned fields for every call, and [writes](Session::learn) what a call carried.
//! The engine [expires](ClientContexts::expire) the contexts that no session has shared for a while.
//!
//! The handle has no lock, and none of its methods waits.
//! A context swaps in a new snapshot of its learned fields, and counts its sessions with atomic numbers.
//!
//! # Calls in flight
//!
//! [`InFlightCalls`] numbers the calls of a session with a [`CallNumber`](gaslightmq_call::CallNumber),
//! and keeps what the pipeline needs for each one.
//! It holds the rules of ordering on one connection:
//! requests reach the backend in the order they arrived, responses leave in that order,
//! and a silence stalls everything behind it.
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`ConnectionOrdinal`] | [`ConnectionOrdinalError`] | The number is zero, or the text is not `#` and digits. |
//! | [`SessionKey`] | [`SessionKeyError`] | The text is not a host, an endpoint and an ordinal. |
//! | [`Identity`] | [`IdentityError`] | A header names a `client.id` or client software that is not one. |
//! | [`InFlightCalls`] | [`InFlightCallsError`] | A call is not in flight, or it is in another phase than the pipeline thinks. |
//!
//! An error that holds another error names its own value first, and then says what the other error says.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_field` and `gaslightmq_kernel` for the fields and their values,
//! on `gaslightmq_call` for the number of a call,
//! on `gaslightmq_wire` for the request whose header it reads,
//! on `gaslightmq_trigger` for the counters of a session,
//! and on `gaslightmq_clock` for instants.
//! It has no async code, no IO, no system clock and no randomness.
//! The current time comes in as an [`Instant`](gaslightmq_clock::Instant) argument.
//!
//! The crate has no definition types, so it has no `serde` feature.

mod calls;
mod client;
mod connection;
mod identity;
mod key;
mod session;

pub use calls::{CallPhase, InFlightCall, InFlightCalls, InFlightCallsError, Stalled};
pub use client::{ClientContext, ClientContexts, ClientInstance, LearnedFields};
pub use connection::{Connection, ConnectionOrdinal, ConnectionOrdinalError, ConnectionOrdinals};
pub use identity::{Closed, ClosedBy, Identification, Identity, IdentityChange, IdentityError, Lifecycle};
pub use key::{SessionKey, SessionKeyError};
pub use session::Session;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in sessions,
/// next to the preludes of the kernel, the fields, the calls and the triggers.
/// No name in it is a name of the standard prelude or of those four, so the import shadows nothing.
///
/// ```
/// use gaslightmq_session::prelude::*;
///
/// let key: Result<SessionKey, SessionKeyError> = "10.0.4.27 broker 2 #1".parse();
/// assert_eq!(key?.ordinal(), ConnectionOrdinal::FIRST);
/// # Ok::<(), SessionKeyError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        CallPhase, ClientContext, ClientContexts, ClientInstance, Closed, ClosedBy, Connection, ConnectionOrdinal,
        ConnectionOrdinalError, ConnectionOrdinals, Identification, Identity, IdentityChange, IdentityError,
        InFlightCall, InFlightCalls, InFlightCallsError, LearnedFields, Lifecycle, Session, SessionKey,
        SessionKeyError, Stalled,
    };
}
