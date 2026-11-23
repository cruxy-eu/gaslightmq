//! The Upstream backend: connections to the real cluster, with the overlay around them.
//!
//! A proxy in front of a real Kafka cluster answers every call with what the cluster answers.
//! This crate is the part that talks to the cluster: it implements the
//! [backend contract](gaslightmq_backend::Backend) over sockets.
//! What it rewrites on the way is the business of the [overlay](gaslightmq_overlay::Overlay), which does no IO.
//!
//! | Type | Is |
//! |---|---|
//! | [`UpstreamBackend`] | The backend of one proxy: it discovers the brokers, forwards each request to the broker it names, and answers with the truth. |
//! | [`UpstreamBackendConfiguration`] | What a backend is configured with: the cluster, the host clients see, and its waits and limits. |
//! | [`ResponseSizeLimit`] | The largest response a backend takes from a broker. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! # One connection for each connection of a client
//!
//! A broker handles the requests of one connection one at a time.
//! A Fetch that waits for records and a `JoinGroup` that waits for its group hold their connection for as long.
//! The backend therefore opens a connection to the cluster for every connection of a client:
//! to the broker of the endpoint that the client connected to.
//!
//! - **Order.**
//!   The requests of one connection reach the broker in the order in which the engine starts their calls.
//!   The broker answers them in that order, so the backend pairs each response with the oldest request in flight,
//!   and the correlation ID of the client stays as it is.
//! - **Leaving.**
//!   When the engine says that a connection of a client ended, the backend closes the one to the cluster,
//!   and the cluster sees the client leave.
//! - **Losing a broker.**
//!   A connection to the cluster that ends is not opened again behind the client's back.
//!   Every call of that connection fails from then on, so the session ends, as it does for a client whose broker goes away.
//!
//! A request for the bootstrap endpoint goes to the bootstrap server of the environment,
//! and to a broker of the cluster when that server takes no connection.
//!
//! # Discovering the brokers
//!
//! Nobody lists the brokers of a real cluster.
//! The backend asks the cluster, on a connection of its own, before it [starts](UpstreamBackend::start).
//! It reports the bootstrap endpoint and one endpoint per broker as [`Endpoints`](gaslightmq_backend::Endpoints).
//!
//! It asks again from then on:
//! every [`metadata_max_age`](UpstreamBackendConfiguration::metadata_max_age),
//! when a call loses its connection or gets none, and when an answer names a broker that has no endpoint.
//!
//! | The cluster | The endpoints |
//! |---|---|
//! | Names a broker it did not name before. | The endpoint of the broker opens. |
//! | No longer names a broker. | The endpoint of the broker closes. |
//!
//! A question that fails changes nothing: the endpoints stay as the last answer left them.
//!
//! # Where clients are sent
//!
//! The backend names endpoints, and no address.
//! The engine opens a listener for each endpoint, on an address of the environment or on any free port,
//! and tells the backend where it [listens](gaslightmq_backend::Backend::listens).
//! From then on the overlay tells clients that address wherever the cluster tells its own:
//! the [`advertised_host`](UpstreamBackendConfiguration::advertised_host), and the port of the listener.
//!
//! | The endpoint of a broker | Clients |
//! |---|---|
//! | Has no listener yet. | Are not told about the broker: Metadata leaves it out, and they ask again. |
//! | Has a listener. | Are sent to the listener. |
//! | Closed, because the cluster no longer names the broker. | Are not told about the broker. The address of its listener is forgotten. |
//!
//! No client is ever told the real address of a broker.
//!
//! # How a call ends
//!
//! | What happens | The answer |
//! |---|---|
//! | The cluster answers. | The truth, with the answer of the cluster next to it when the overlay changed it. |
//! | The request is a Produce with `acks=0`. | No response, once the request is written. |
//! | The cluster sends what is no response to the request. | `NotAResponse` |
//! | The request arrived on an endpoint that accepts no connections. | `NoSuchEndpoint` |
//! | No connection to the broker opens within [`socket_connection_setup_timeout`](UpstreamBackendConfiguration::socket_connection_setup_timeout). | `Unreachable` |
//! | The connection to the broker ends. | `ConnectionLost` |
//! | The overlay cannot read a request that it has to read, or cannot write the truth. | `ResponseNotBuilt` |
//! | The broker never answers. | None. |
//!
//! The backend never gives up on a broker that takes its time.
//! A Fetch and a `JoinGroup` wait for as long as the client asked for,
//! and the client has a timeout of its own: when it gives up, the engine drops the call.
//! The backend therefore never answers `TimedOut`.
//!
//! A response that puts a connection out of step, such as one with the correlation ID of another request,
//! ends that connection: nothing says any more which response answers which request.
//!
//! # TLS and SASL
//!
//! The backend connects over plain TCP, and it does not log in.
//! An environment that asks for [TLS](gaslightmq_environment::Tls) or [SASL](gaslightmq_environment::Sasl)
//! does not [start](UpstreamBackend::start): the error says which of the two the backend cannot do yet.
//!
//! # Incidents
//!
//! The backend cannot change a real cluster, so it supports the incidents that the overlay emulates.
//! The overlay emulates none yet, and the backend rejects every incident.
//!
//! # Errors
//!
//! The crate has one error, and each value that checks itself has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`UpstreamBackend`] | [`UpstreamBackendError`] | The backend does not start: the environment asks for TLS or SASL, or the cluster does not name its brokers. |
//! | [`ResponseSizeLimit`] | [`ResponseSizeLimitError`] | The number of bytes is zero, or more than a response can hold. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! An error that holds another error writes it once.
//! What a socket or the runtime reports is part of the message, and of no field.
//!
//! A backend that runs returns no error of its own.
//! A call that fails ends with an answer of the contract,
//! a listener that clients cannot be sent to is rejected with the
//! [`ListenerRejection`](gaslightmq_backend::ListenerRejection) of the contract,
//! and a later question for the brokers that fails leaves the endpoints as they are.
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, it runs on Tokio, and it owns the sockets to the cluster.
//! Its waits are real time, on the timer of the runtime, whatever clock the instance runs on.
//! What it decides is plain code without a socket or a timer:
//! the configuration, what is known about the cluster, the question for the brokers,
//! and the pairing of responses with requests.
//!
//! It depends on `gaslightmq_backend` for the contract, on `gaslightmq_overlay` for what it lays over the cluster,
//! on `gaslightmq_environment` for the cluster and the host clients see, on `gaslightmq_wire` for frames,
//! on `gaslightmq_field` for endpoints and connections, on `gaslightmq_kernel` for broker IDs,
//! on `gaslightmq_incident` for the incidents it rejects, and on `gaslightmq_clock` for the time spans of a configuration.
//! It reaches the Kafka codec, so it does not compile to WebAssembly.

mod backend;
mod brokers;
mod cluster;
mod configuration;
mod connection;
mod discovery;
mod error;
mod in_flight;
mod socket;
mod watched;

pub use backend::UpstreamBackend;
pub use configuration::{ResponseSizeLimit, ResponseSizeLimitError, UpstreamBackendConfiguration};
pub use error::UpstreamBackendError;

/// Every item of this crate, with its error.
///
/// Import it whole wherever code starts a backend in front of a real cluster,
/// next to the preludes of the contract and of the environment.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_backend_upstream::prelude::*;
/// use gaslightmq_environment::prelude::*;
///
/// let upstream = Upstream::new("kafka:9092".parse().expect("a server address"));
/// let configuration = UpstreamBackendConfiguration::new(upstream, "localhost".parse().expect("a host name"));
/// assert_eq!(configuration.response_max_bytes, ResponseSizeLimit::default());
///
/// let limit: Result<ResponseSizeLimit, ResponseSizeLimitError> = ResponseSizeLimit::new(0);
/// assert_eq!(limit, Err(ResponseSizeLimitError::Zero));
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ResponseSizeLimit, ResponseSizeLimitError, UpstreamBackend, UpstreamBackendConfiguration, UpstreamBackendError,
    };
}
