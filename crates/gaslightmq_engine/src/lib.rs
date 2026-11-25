//! The runtime of one proxy: one task per session, the armed rules, the clock and the endpoints.
//!
//! A proxy stands between Kafka clients and a backend, and decides for every call what the client gets to see.
//! What it decides is the business of the pipeline, which does no IO.
//! This crate is what runs it: it owns the tasks, the timers and the channels, and it calls the pipeline with what happened.
//!
//! | Type | Is |
//! |---|---|
//! | [`Engine`] | The runtime of one proxy, from the moment it starts until someone stops it. |
//! | [`EngineConfiguration`] | What an engine is configured with: its listeners, its seed, and its waits and limits. |
//! | [`ArmedRules`] | The armed rules of the proxy. Arming and disarming publishes a snapshot to every session. |
//! | [`RuleChanges`] | Every snapshot that the armed rules publish, for whoever follows what is armed. |
//! | [`OpenSession`], [`SessionEvent`] | A session that is open, and what happens to a session: it opens, says who its client is, and closes. |
//! | [`EngineEvent`] | What a session hands over: the record of a call, or an incident that a rule fired. |
//! | [`LiveEvents`], [`EveryEvent`] | The events as a listener receives them: one that watches and may miss the oldest, and one that keeps every event. |
//! | [`EventBacklog`] | How many events an engine keeps for a live listener that falls behind. |
//! | [`EngineStartError`], [`EndpointOpenError`] | Why an engine did not start, and why an endpoint has no listener. |
//! | [`ArmError`], [`DisarmError`], [`RetuneError`], [`LiveEventsError`] | Why a rule was not armed, disarmed or retuned, and why a live listener has no next event. |
//! | [`NotOpenError`] | No session is open on the connection that someone asked to close. |
//!
//! The [`prelude`] holds all of them.
//!
//! ```
//! use std::net::SocketAddr;
//!
//! use gaslightmq_backend::prelude::*;
//! use gaslightmq_clock::{Duration, TimeSpan};
//! use gaslightmq_clock_waitable::WallClock;
//! use gaslightmq_engine::prelude::*;
//! use gaslightmq_environment::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_incident::prelude::*;
//! use gaslightmq_kafka_client_api::{ProtocolConsumer, RawKafkaClient};
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_wire::messages::HeartbeatResponse;
//!
//! /// A cluster without brokers that answers every request as a heartbeat.
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
//!         // No answer of this cluster names a broker, so no client has to be sent anywhere.
//!         Ok(())
//!     }
//!
//!     fn incident_support(&self) -> IncidentSupport {
//!         IncidentSupport::none()
//!     }
//!
//!     async fn apply(&self, incident: Incident) -> Result<ExpandedIncident, IncidentRejection> {
//!         let unsupported = self.incident_support().check(&Requirements::from(&incident));
//!         Err(unsupported.expect_err("this cluster supports no incident").into())
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), EngineStartError> {
//! // The proxy listens where its environment says. Port 0 asks for any free port, and the engine names it.
//! let listeners = Listeners::new(
//!     "127.0.0.1:0".parse().expect("an address"),
//!     "127.0.0.1:19092-19191".parse().expect("a range of ports"),
//!     "localhost".parse().expect("a host"),
//! );
//! let configuration = EngineConfiguration::new(listeners.expect("listeners that do not meet"));
//! let engine = Engine::start(configuration, Calm, WallClock::new(), Lies::of_every_build()).await?;
//! let bootstrap = engine.address_of(Endpoint::Bootstrap).expect("the backend opened the endpoint");
//!
//! // Someone arms a rule: "answer every Heartbeat 20 ms late".
//! let late = Rule::new(
//!     "late-heartbeat".parse().expect("a rule name"),
//!     Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
//!     TimingLie::new().with_delay(TimeSpan::from_millis(20)),
//! );
//! let resolved = late.resolve(&SelectorSet::default(), &PresetSet::default());
//! let armed = engine.rules().arm(resolved.expect("a rule that names no selector and no preset"));
//! assert!(armed.is_ok(), "no rule of that name was armed before");
//!
//! // A member of a group sends a heartbeat through the proxy, and waits for the answer.
//! let mut events = engine.live_events();
//! let member = RawKafkaClient::new(bootstrap);
//! let group: GroupId = "analytics-etl".parse().expect("a group ID");
//! let answer = member.heartbeat(group.as_str()).await;
//! assert_eq!(answer, Ok(()));
//!
//! // The session of the member opened, and its first request said who its client is.
//! let opened = events.next().await.expect("the engine runs, and the listener kept up");
//! assert!(matches!(opened, EngineEvent::Session(SessionEvent::Opened(_))));
//! let identified = events.next().await.expect("the engine runs, and the listener kept up");
//! assert!(matches!(identified, EngineEvent::Session(SessionEvent::Identified { .. })));
//!
//! // The record of the call says what the client was told, and how long it waited for it.
//! let event = events.next().await.expect("the engine runs, and the listener kept up");
//! let EngineEvent::CallRecorded(record) = event else {
//!     panic!("the call has a record");
//! };
//! assert_eq!(record.verdict().to_string(), "+20 ms");
//! assert!(record.timing().client_waited() >= Some(Duration::from_millis(20)));
//!
//! engine.stop().await;
//! # Ok(())
//! # }
//! ```
//!
//! # A session is one task
//!
//! The engine opens a listener for every endpoint, and the listener runs every connection in a task of its own.
//! That task is the session.
//! It owns the transport of the connection and the pipeline, and nothing else touches either.
//!
//! The task waits for the next thing to happen, reports it to the pipeline as an input with the instant of the clock,
//! and performs every output that the pipeline returns, in order.
//!
//! | The task waits for | And reports |
//! |---|---|
//! | A snapshot of the armed rules | `SnapshotPublished` |
//! | An answer of the backend | `BackendAnswered`, `BackendAnswerUnreadable` or `BackendFailed`. Nothing for a request without a response. |
//! | A timer on the clock | `TimerFired` |
//! | A frame of the client | `ClientBytes` |
//! | The end of the connection | `ClientClosed` |
//! | The end of its endpoint | `BackendClosed`, when the backend is why the endpoint ended |
//!
//! When several things happened, the task takes them in the order of the table.
//! A new snapshot comes first, so the next request is evaluated against it,
//! and what is in flight goes on before the session takes another frame.
//!
//! | The pipeline asks to | The task |
//! |---|---|
//! | Forward a request | Starts the call on the backend, and goes on without the answer. The calls of one connection start in the order in which the pipeline forwarded them. |
//! | Send bytes | Writes them to the transport, and waits until the system took them. |
//! | Set a timer | Keeps it, and waits on the clock for the one that ends first. |
//! | Close | Closes the transport once the outputs are performed, so the client reads what was sent before. |
//! | Emit a call record | Hands it over as [`EngineEvent::CallRecorded`], and disarms a rule that the call used up. |
//! | Fire an incident | Hands it over as [`EngineEvent::IncidentFired`]. |
//!
//! The session ends when its transport closes, whoever closed it.
//! The task drops the calls that the backend did not answer, tells the backend that the connection ended
//! when it got a request of it, and ends. The pipeline and its state go with it.
//!
//! A client whose bytes are no frame sends nothing that the transport hands on.
//! The task reports the bytes of an empty frame instead, which are no request either,
//! so the pipeline ends the session as closed by the proxy.
//!
//! # Endpoints
//!
//! The backend says which endpoints its cluster has, and the [listeners](EngineConfiguration::listeners) of the
//! configuration say where each one listens.
//! The engine asks the backend when it [starts](Engine::start), and again whenever the answer changes.
//!
//! | The endpoint | The engine |
//! |---|---|
//! | Opened. | Opens a listener for it, and tells the backend the address that the listener got. |
//! | Closed. | Closes its listener, and every session on it ends as closed by the backend. |
//! | Closed and opened again. | Does both, in that order. |
//! | Is open, and gets another address. | Moves its listener: closes it, and opens it on the new address. |
//!
//! - **Addresses.**
//!   The bootstrap endpoint listens on the bootstrap address.
//!   The broker endpoints that are open get their addresses together, as the listeners assign them:
//!   a range gives its ports in the order of the broker IDs.
//!   A broker that joins or leaves therefore gives the brokers behind it another port, and their listeners move.
//!   The sessions on a listener that moves end as closed by the backend, because the cluster changed.
//! - **Any free port.**
//!   A listener on port 0 takes a port that the system picks.
//!   The backend hears the address that the listener got, and sends clients there.
//!   [`Engine::address_of`] names it too.
//! - **Closing comes first.**
//!   The engine waits until every session on a listener that closes has ended,
//!   so the address is free for the listener that opens next.
//! - **A listener that the backend rejects** closes again, and no client is told its address.
//! - **An endpoint that does not open** is reported: in the error of [`Engine::start`], and in the log from then on.
//!   Its address did not take a listener, the backend rejected the listener,
//!   or the listeners of the configuration have no address for its broker.
//!   An engine that runs tries an endpoint that has an address again, for as long as it has no listener:
//!   after the [`endpoint_retry_backoff`](EngineConfiguration::endpoint_retry_backoff),
//!   and with every report of the backend.
//! - **A change is followed to its end.**
//!   An engine that [stops](Engine::stop) while its listeners follow a change lets them finish,
//!   so every address is free once the stop returns.
//! - **A stop has an end.**
//!   It takes no longer than the [`stop_timeout`](EngineConfiguration::stop_timeout), whatever is in its way:
//!   a change that the listeners still follow, an endpoint that still closes, or a session that the backend
//!   does not let go of. The engine lets go of what is left then.
//!
//! The listeners of one engine number their connections together.
//! Its sessions count their calls together too: one proxy is one environment,
//! so the first request that arrives is call 1, and a higher call ID is a later call on whichever connection.
//!
//! # Rules
//!
//! [`ArmedRules`] holds the armed rules.
//! Every change publishes a new snapshot, and every session hands it to its pipeline before its next request.
//! A call in flight finishes on the snapshot it started with.
//!
//! It disarms a rule whose lifetime ended: on the clock for a duration,
//! and after the call that spent the last match or firing of a budget.
//!
//! # Events
//!
//! The record of every call and every incident that a rule fired leave the engine in one place.
//! A session hands an event over and goes on: it never waits for a listener.
//!
//! | The listener | Asks for | When it falls behind |
//! |---|---|---|
//! | Watches what happens now: a traffic view, a UI. | [`Engine::live_events`] | Misses the oldest events, and learns how many. The engine keeps a [backlog](EngineConfiguration::event_backlog) for it. |
//! | Keeps what happened: a recorder. | [`Engine::every_event`] | Misses nothing. The engine keeps every event until the listener took it. |
//!
//! A listener that misses nothing costs memory, and no time.
//! What it did not take yet stays in the engine, without a limit,
//! so a recorder that stops taking events ends with a process that has no memory left.
//! It never costs a client anything: a slow disk does not change when an answer leaves.
//!
//! # Time
//!
//! An engine has one [`WaitableClock`](gaslightmq_clock_waitable::WaitableClock), which whoever starts it hands over.
//! Every instant that a pipeline sees comes from it, and every wait is a wait on it:
//! a delay, the lifetime of a rule, the time a client context stays after its last session,
//! and the time before an endpoint without a listener is tried again.
//! With a [`SteppedClock`](gaslightmq_clock_waitable::SteppedClock) none of them takes real time.
//!
//! Three waits are real time on every clock, because they belong to the sockets and to stopping:
//! the [wait after a failed accept](EngineConfiguration::accept_retry_backoff),
//! the [wait for a client](EngineConfiguration::close_timeout) when the proxy closes a connection,
//! and the [time a stop may take](EngineConfiguration::stop_timeout).
//!
//! # What is not here yet
//!
//! The scenario runner, the journal, the traffic window, the index, the recorder and the workspace arrive
//! with their crates. Until then:
//!
//! - Nobody applies an incident. The engine hands it over, and whoever listens passes it to the backend.
//! - The armed rules are a live set alone: the rule that was armed last wins, and every die rolls from the
//!   [seed](EngineConfiguration::seed) of the configuration.
//! - The record of a call does not keep what the real cluster answered next to a truth that an overlay changed.
//!   The pipeline takes the truth alone.
//! - A proxy that stops ends its sessions without a word to their pipelines, which have no input for it.
//!   A call that is in flight then gets no record.
//!
//! # Errors
//!
//! Each thing that fails in its own way has an error of its own, so nobody handles a failure that cannot come.
//!
//! | What fails | Error | What is wrong |
//! |---|---|---|
//! | [`Engine::start`] | [`EngineStartError`] | The listeners do not hold the brokers, or an endpoint did not open. |
//! | Opening an endpoint, when the engine starts and when its backend opens one later | [`EndpointOpenError`] | The address did not take a listener, or the backend rejected the listener. |
//! | [`ArmedRules::arm`] | [`ArmError`] | The rule is armed already. |
//! | [`ArmedRules::disarm`] | [`DisarmError`] | The rule is not armed. |
//! | [`LiveEvents::next`] | [`LiveEventsError`] | The listener missed events, or the engine stopped. |
//! | [`EventBacklog`] | [`EventBacklogError`] | The number of events is zero, or more than an engine can keep. |
//!
//! A variant is something its caller decides on, and the rest is text.
//! The errors speak of endpoints, rules and events, and of nothing that the engine is built with.
//! What a listener or the backend said is text, in a field `reason`.
//! The error of a value object is kept as it is, in a field `error`.
//! A variant writes either one once, after what it says itself.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! A listener of [every event](EveryEvent) has no error: its events end when the engine stops.
//!
//! What goes wrong inside a session is no error of the engine.
//! It is part of the record of a call, or it ends the session, as the pipeline decides.
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it runs on Tokio.
//! It owns tasks, timers and channels, and it opens no socket itself: the listeners do, and the backend does.
//! [`Engine::start`] starts tasks, so it runs inside the runtime.
//! The configuration, the error and the timers of a session are plain code that never waits.

mod configuration;
mod endpoints;
mod engine;
mod error;
mod events;
mod open_sessions;
mod rules;
mod session;
mod timers;

pub use configuration::{EngineConfiguration, EventBacklog, EventBacklogError};
pub use engine::Engine;
pub use error::{
    ArmError, DisarmError, EndpointOpenError, EngineStartError, LiveEventsError, NotOpenError, RetuneError,
};
pub use events::{EngineEvent, EveryEvent, LiveEvents};
pub use open_sessions::{OpenSession, SessionEvent};
pub use rules::{ArmedRules, RuleChanges};

/// Every item of this crate.
///
/// Import it whole wherever code starts an engine, next to the preludes of the crates an engine is made of.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let backlog: Result<EventBacklog, EventBacklogError> = EventBacklog::new(64);
/// assert_eq!(backlog?.to_string(), "64 events");
/// assert_eq!(LiveEventsError::EngineStopped.to_string(), "the engine stopped");
/// # Ok::<(), EventBacklogError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ArmError, ArmedRules, DisarmError, EndpointOpenError, Engine, EngineConfiguration, EngineEvent,
        EngineStartError, EventBacklog, EventBacklogError, EveryEvent, LiveEvents, LiveEventsError, NotOpenError,
        OpenSession, RetuneError, RuleChanges, SessionEvent,
    };
}
