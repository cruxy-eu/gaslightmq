//! The backend contract: the request and answer messages, and the one trait through which the engine reaches a backend.
//!
//! A backend answers a call after the pipeline of its session has decided what to do with it.
//! It is a real cluster, or a broker in the same process, and the engine cannot tell which.
//!
//! | Type | Is |
//! |---|---|
//! | [`Backend`] | The trait: answer a request, let go of a connection, report the endpoints, take the address of each listener, declare and apply incidents. |
//! | [`BackendRequest`] | The request message: a request under the ID of its call, with the endpoint its connection is on. |
//! | [`BackendAnswer`] | The answer message: the truth, no response, an answer that is no response, or a failure. |
//! | [`Truth`] | What the backend answered, and what the real cluster answered when an overlay changed it. |
//! | [`BackendFailure`] | Why a backend has no answer for a call. |
//! | [`Endpoints`] | The endpoints that accept connections now, and how often each one opened. |
//! | [`ListenerRejection`] | Why a backend does not send clients to the listener of an endpoint. |
//! | [`IncidentRejection`] | Why a backend did not apply or emulate an incident. |
//!
//! The [`prelude`] holds every type of the table.
//!
//! The crate holds no logic.
//! It is what both sides of the seam agree on: the engine on one side, and on the other side the backend
//! in front of a real cluster and the task that drives a broker.
//! A broker is a state machine, and it speaks the two messages without the trait.
//!
//! # A call
//!
//! The engine makes a [`BackendRequest`] from every request that a pipeline forwards, and awaits
//! [`Backend::call`].
//! The future is the call: it ends with the [`BackendAnswer`], and the engine drops it when nobody waits any more.
//!
//! ```
//! use std::net::SocketAddr;
//!
//! use futures::executor::block_on;
//! use gaslightmq_backend::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_incident::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
//!
//! /// A cluster of one broker that answers every request as a heartbeat, and leads nothing.
//! struct Calm;
//!
//! impl Backend for Calm {
//!     async fn call(&self, request: BackendRequest) -> BackendAnswer {
//!         match request.request().response(&HeartbeatResponse::default()) {
//!             Ok(response) => Truth::new(response).into(),
//!             Err(error) => BackendFailure::ResponseNotBuilt { reason: error.to_string() }.into(),
//!         }
//!     }
//!
//!     async fn close(&self, _connection: ConnectionNumber) {}
//!
//!     async fn endpoints(&self, known: &Endpoints) -> Endpoints {
//!         let bootstrap = Endpoints::none().with(Endpoint::Bootstrap);
//!         if *known == bootstrap {
//!             // The endpoints of this cluster never change, so there is nothing new to report.
//!             std::future::pending::<()>().await;
//!         }
//!         bootstrap
//!     }
//!
//!     async fn listens(&self, _endpoint: Endpoint, _address: SocketAddr) -> Result<(), ListenerRejection> {
//!         // No answer of this cluster names a broker, so no client is sent anywhere.
//!         Ok(())
//!     }
//!
//!     fn incident_support(&self) -> IncidentSupport {
//!         IncidentSupport::none().emulates([IncidentKind::Rebalance])
//!     }
//!
//!     async fn apply(&self, incident: Incident) -> Result<ExpandedIncident, IncidentRejection> {
//!         self.incident_support().check(&Requirements::from(&incident))?;
//!         // A backend adds the consequences from the topology of its own cluster, and applies the whole.
//!         Ok(incident.expand(self))
//!     }
//! }
//!
//! impl Topology for Calm {
//!     fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
//!         vec![]
//!     }
//!
//!     fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
//!         vec![]
//!     }
//! }
//!
//! // The pipeline of connection 7 forwards its first request, which arrived on the bootstrap endpoint.
//! let heartbeat = Request::new(4, 41, Some("order-ingest"), &HeartbeatRequest::default()).expect("a request");
//! let request = BackendRequest::new("#7.1".parse().expect("a call ID"), Endpoint::Bootstrap, heartbeat);
//!
//! // Any executor drives a backend. The engine has its runtime, and this example has the smallest one.
//! let BackendAnswer::Truth(truth) = block_on(Calm.call(request)) else {
//!     unreachable!("this cluster answers every request");
//! };
//! assert_eq!(truth.response().correlation_id(), 41);
//!
//! // A rule fires an incident, and the backend says what it had as consequences.
//! let rebalance = Incident::Rebalance { group: GroupId::new("payments").expect("a group ID") };
//! assert!(block_on(Calm.apply(rebalance))?.consequences().is_empty());
//!
//! let down = Incident::BrokerGoesDown { broker: BrokerId::from(1), failover: false };
//! let rejection = block_on(Calm.apply(down)).unwrap_err();
//! assert_eq!(rejection.to_string(), "the backend cannot apply or emulate: broker-goes-down");
//! # Ok::<(), IncidentRejection>(())
//! ```
//!
//! # What the engine does
//!
//! | The backend | The engine |
//! |---|---|
//! | Answers [`BackendAnswer::Truth`]. | Reports the response to the pipeline of the session, under the call of the request. |
//! | Answers [`BackendAnswer::NoResponse`]. | Reports nothing. The pipeline expects no answer for a request without a response. |
//! | Answers [`BackendAnswer::NotAResponse`]. | Reports to the pipeline that the answer does not read, as `Input::BackendAnswerUnreadable`. The proxy closes the connection when that answer would be the next to leave. |
//! | Answers [`BackendAnswer::Failed`]. | Reports the failure to the pipeline, as `Input::BackendFailed`. The session ends at once as closed by the backend, and the record of every call in flight says why. |
//! | Never answers. | Keeps the call in flight until the session ends, and then drops it. |
//! | Reports [`Endpoints`] in which an endpoint [closed](Endpoints::closed_since). | Closes the listener of that endpoint, and reports `Input::BackendClosed` to the pipeline of every session on it. |
//! | Reports [`Endpoints`] in which an endpoint [opened](Endpoints::opened_since). | Opens a listener for that endpoint, and tells the backend where it [listens](Backend::listens). |
//! | Rejects a listener with a [`ListenerRejection`]. | Closes that listener: no client is told its address. |
//!
//! The pipeline takes the failure as text, because it knows no backend.
//! The message of a [`BackendFailure`] is complete, so the engine passes it on as it is.
//!
//! A failure never becomes a response.
//! Kafka has no error code for a proxy that lost its cluster,
//! and a failure of GaslightMQ must never look like an answer of the cluster.
//! A client whose broker goes away loses its connection, so that is what a failure looks like.
//!
//! The engine also keeps the order of a connection:
//! it starts the calls of one connection in the order in which the pipeline forwarded them.
//! It tells the backend when a connection ended, with [`Backend::close`].
//!
//! # Errors
//!
//! No type of this crate checks a value, so none has an error of its own.
//! The contract has an error for each thing that the engine asks of a backend, because the engine does something else
//! with each one, and none of them can stand for another:
//!
//! | Error | The backend | The engine |
//! |---|---|---|
//! | [`BackendFailure`] | Has no answer for a [call](Backend::call), and none will come. | Ends the session, as closed by the backend. |
//! | [`ListenerRejection`] | Does not send clients to a [listener](Backend::listens). | Closes that listener. |
//! | [`IncidentRejection`] | Did not [apply](Backend::apply) or emulate an incident. | Records that the incident changed nothing. |
//!
//! Every backend returns these errors and no other, whatever stands behind it.
//! Each says everything in one message, and has no [`source`](std::error::Error::source).
//! What the cluster, the system or the writer of a response said is text, in a field `reason`.
//! [`BackendAnswer::NotAResponse`] keeps what does not read the same way.
//!
//! # No runtime
//!
//! The crate has no logic and no IO: no socket, no file, no clock and no randomness.
//! The trait returns futures, and nothing in the crate spawns, sleeps or polls one.
//! It is the only crate outside the IO edge with an async signature.
//! A backend waits on whatever its own crate brings, and the engine drives the futures on its runtime.
//!
//! The crate reaches the Kafka codec, so it does not compile to WebAssembly.

mod answer;
mod backend;
mod endpoints;
mod failure;
mod listener;
mod rejection;
mod request;

pub use answer::{BackendAnswer, Truth};
pub use backend::Backend;
pub use endpoints::Endpoints;
pub use failure::BackendFailure;
pub use listener::ListenerRejection;
pub use rejection::IncidentRejection;
pub use request::BackendRequest;

/// Every item of this crate.
///
/// Import it whole wherever code implements a backend or drives one, next to the preludes of the crates it builds on.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let endpoints = Endpoints::none().with(Endpoint::Bootstrap);
/// assert!(endpoints.contains(Endpoint::Bootstrap));
///
/// let failure = BackendFailure::NoSuchEndpoint { endpoint: "broker 2".parse()? };
/// assert_eq!(failure.to_string(), "the cluster takes no requests on endpoint `broker 2`");
/// # Ok::<(), EndpointError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Backend, BackendAnswer, BackendFailure, BackendRequest, Endpoints, IncidentRejection, ListenerRejection, Truth,
    };
}
