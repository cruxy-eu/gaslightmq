//! The request pipeline as a state machine: one call from the moment its request arrives
//! to the moment the client receives an answer, or nothing.
//!
//! A proxy runs every client connection as a session, and a session is a transport and a pipeline.
//! The transport reads and writes bytes. The pipeline decides what happens to each call.
//! This crate holds the pipeline, and it does no IO: the engine tells it what happened, and performs what it returns.
//!
//! | Type | Is |
//! |---|---|
//! | [`Pipeline`] | The state machine of one connection. It holds the state of its session. |
//! | [`Input`] | What happened: bytes of the client, an answer or a failure of the backend, a timer, a new snapshot, a close. |
//! | [`Output`] | What should happen: forward, send, set a timer, close, emit a call record, fire an incident. |
//! | [`Outputs`] | What one input leads to, in the order the engine performs it. |
//! | [`TimerId`] | Which wait a timer ends: the call that waits, and the stage it waits at. |
//! | [`SharedBySessions`] | What every session of one proxy shares, as the engine passes it in. |
//! | [`KnownTopics`] | The topics that the cluster named with their ID, shared by the sessions of a proxy. |
//!
//! The [`prelude`] holds all of them.
//!
//! ```
//! use std::sync::Arc;
//!
//! use gaslightmq_clock::{Clock, Duration, Instant, VirtualClock};
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_lie_refusal::prelude::*;
//! use gaslightmq_pipeline::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_session::prelude::*;
//! use gaslightmq_trigger::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
//! use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
//! use gaslightmq_wire::messages::{ProduceRequest, ProduceResponse};
//! use gaslightmq_wire::protocol::StrBytes;
//!
//! // The engine owns what the sessions of a proxy share, and registers the kinds of lies of its build.
//! let shared = SharedBySessions::new(
//!     ClientContexts::new(Duration::from_secs(60)),
//!     KnownTopics::new(),
//!     Arc::new(Lies::of_every_build().with(Refusal).expect("a kind that is new to the set")),
//! );
//!
//! // Someone armed a rule: "refuse partition 3 of orders.eu".
//! let clock = VirtualClock::new();
//! let partition_3 = Conditions::new()
//!     .with(Condition::parse(Field::Topic, "orders.eu").expect("a condition"))
//!     .with(Condition::parse(Field::Partition, "3").expect("a condition"));
//! let name = "Leader flap".parse().expect("a rule name");
//! let flap = Rule::new(name, partition_3, RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
//! let resolved = flap.resolve(&SelectorSet::default(), &PresetSet::default()).expect("a rule that resolves");
//! let snapshot = Arc::new(Snapshot::new([resolved.arm(Seed::new(42), clock.now())]).expect("one rule"));
//!
//! // A client connects, and the engine opens the pipeline of its connection.
//! let client = "10.0.4.27:51459".parse().expect("a client address");
//! let connection = Connection::new(ConnectionNumber::new(7), client, Endpoint::Bootstrap);
//! let mut pipeline = Pipeline::open(connection, ConnectionOrdinal::FIRST, shared, snapshot, clock.now());
//!
//! // The client produces to two partitions of `orders.eu`. The engine reports the bytes.
//! let orders = || StrBytes::from_static_str("orders.eu").into();
//! let partition = |index| PartitionProduceData::default().with_index(index);
//! let topic = TopicProduceData::default().with_name(orders()).with_partition_data(vec![partition(3), partition(4)]);
//! let body = ProduceRequest::default().with_acks(-1).with_topic_data(vec![topic]);
//! let produce = Request::new(9, 41, Some("order-ingest"), &body)?;
//! let outputs = pipeline.handle(Input::ClientBytes(&produce.frame().to_bytes()), clock.now());
//!
//! // The pipeline hands out what is left of the request: partition 4 alone.
//! let [Output::Forward { call, request }] = outputs.as_slice() else {
//!     panic!("the rest of the request goes to the backend");
//! };
//! let forwarded: ProduceRequest = request.body().expect("a Produce request");
//! assert_eq!(forwarded.topic_data[0].partition_data.len(), 1);
//!
//! // The backend appends the records of partition 4, and the engine reports its answer under the same call.
//! let appended = PartitionProduceResponse::default().with_index(4).with_base_offset(120);
//! let answered = TopicProduceResponse::default().with_name(orders()).with_partition_responses(vec![appended]);
//! let response = request.response(&ProduceResponse::default().with_responses(vec![answered]))?;
//! let outputs = pipeline.handle(Input::BackendAnswered { call: *call, response }, clock.now());
//!
//! // The client gets one response for both partitions, and the record of the call says what happened and why.
//! let [Output::Send(_), Output::Emit(record)] = outputs.as_slice() else {
//!     panic!("the answer leaves, and its record follows");
//! };
//! assert_eq!(record.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
//! assert_eq!(record.delivered().to_string(), "ok; NOT_LEADER_OR_FOLLOWER on orders.eu/3");
//! # Ok::<(), gaslightmq_wire::EncodeError>(())
//! ```
//!
//! # The engine drives, and the pipeline decides
//!
//! The session task of the engine waits for the next thing to happen,
//! reports it with [`Pipeline::handle`] and the current instant, and performs every [`Output`] in order.
//! The same interface serves a test, which writes its inputs by hand and owns a virtual clock,
//! and a replay, which feeds the requests and the truths of a recording.
//!
//! The pipeline never waits.
//! While one call waits for a timer or for the backend, it takes the next request of the connection.
//! Requests still reach the backend in the order they arrived, and answers leave in that order:
//! [`InFlightCalls`](gaslightmq_session::InFlightCalls) of the session holds both lines.
//!
//! # The journey of one call
//!
//! | Stage | The pipeline |
//! |---|---|
//! | 1 · Receive | Cuts the bytes of the client into requests. |
//! | 2 · Decode | Decodes the request once, with the [topics it knows by ID](KnownTopics). Selectors and lies read that. |
//! | 3 · Identify | Identifies the session, and tells the client context the group or the transaction that the request names. |
//! | 4 · Match, 5 · Trigger | Evaluates the request pass on the [snapshot](gaslightmq_rule::Snapshot) it received last. |
//! | 6 · Resolve | Combines the lies that fired, and hands out the incidents as [`Output::Fire`]. |
//! | 7 · Before forwarding | Deals with the lies that act now: a silence, a hang-up, a delay, refusals. |
//! | 8 · Backend | Hands out what is left of the request as [`Output::Forward`], and waits for [`Input::BackendAnswered`]. |
//! | 9 · After the response | Reads the truth, evaluates the response pass, and deals with the lies that act now. |
//! | 10 · Send | Hands out the bytes of the answer as [`Output::Send`], in the order of the requests. |
//! | 11 · Emit | Hands out the [record](gaslightmq_call::Call) of the call as [`Output::Emit`]. |
//!
//! A call uses one snapshot from start to finish.
//! [`Input::SnapshotPublished`] only changes the rules of the calls that arrive after it.
//!
//! # Lies
//!
//! The pipeline names no kind of lie.
//! `gaslightmq_combine` says which lies apply at each stage, and the [`Lies`](gaslightmq_lie_api::Lies) of the build
//! say what each of them asks for.
//!
//! | The lie | The pipeline |
//! |---|---|
//! | Holds the call back (timing) | Does not ask it. It waits for the delays of the stage together, as an [`Output::SetTimer`], and the held request or answer leaves on [`Input::TimerFired`]. |
//! | Drops the call (silence) | Sends nothing, stalls the session, and emits the record at once. A request that arrives behind it is parked: no rule sees it, the session takes no identity from it, and the client context learns nothing from it. |
//! | Closes the connection (hang-up) | Closes once the call is at the head of its line, with [`Output::Close`]. Every call in flight ends without an answer. |
//! | Forwards another request (a partial refusal) | Hands out that request, and asks the lie again after the response, so it makes the response fit. |
//! | Sends another response (a refusal, a rewrite) | Sends that response instead. Before forwarding, the backend never sees the request. |
//!
//! After the response, the pipeline asks rewrites first, then the refusal of the whole call, then the refusals of parts,
//! and last the lies that forwarded another request.
//! A refusal therefore writes over what a rewrite left,
//! and a partition that was refused before forwarding keeps its own refusal under a refusal of the whole call.
//!
//! The jitter of a delay rolls on the die of its rule, for the match that fired it,
//! so the same seed gives the same waits. `Pipeline` rolls it; no global randomness takes part.
//!
//! # What cannot be told
//!
//! A mistake of GaslightMQ never looks like a lie.
//!
//! - A lie that fails changes nothing, and only that lie: its kind is not in the build, or its response has no place for an error.
//!   Its line in the explanation says that it was [not told](gaslightmq_call::LieFate::untold) and why,
//!   the verdict does not list it, and its firing goes back to its budget.
//!   What combining decided stays: a lie that it shadowed stays shadowed.
//! - A silence or a hang-up before forwarding is asked first, before anything acts on the call.
//!   When it fails, it ends nothing and takes nothing from the other lies: they combine again without it.
//!   The lies that it would have shadowed or pre-empted apply, and the rules that wait for the truth get their pass.
//! - A lie that withheld a part of the request did act, also when it then fails to put its error into the response.
//!   It stays told, and the [failure](gaslightmq_call::Explanation::failure) of the explanation says what the response lacks.
//! - A request whose body does not decode goes through untouched. No rule sees it, and the failure says so.
//! - A truth that nobody can read goes to the client as the backend sent it.
//!   It has no outcome, so the response pass does not run, and the record shows an
//!   [unread response](gaslightmq_call::Delivered::UnreadResponse): a response that nobody read is no success.
//! - A lie acts before forwarding when its request goes on, and after the response when its answer leaves.
//!   A call that ends with its session before that never heard the lie: it is not told, and its firing goes back.
//!   What a lie did before stays: a refused partition never reached the backend.
//! - A Produce with `acks=0` has no response. Nothing is sent for it, the response pass does not run,
//!   and a lie that acts after the response is not told.
//!   A Produce whose body does not decode still says its `acks` at the start of that body.
//!
//! Sometimes the proxy has no way to keep the answers of a connection in order.
//! It then closes the connection itself, as a broker does, and the session records that the proxy closed it:
//!
//! - The bytes of the client are no request.
//! - A Produce ends before its `acks`, so nothing says whether a response comes.
//! - The backend answers with something that is no response to the request, so the client must not get it.
//!   The connection closes when that answer would be at the head of the line.
//!   The answer is a response that does not fit the request, or [`Input::BackendAnswerUnreadable`].
//!
//! The backend ends a session too.
//! [`Input::BackendFailed`] says that it has no answer for a call, and [`Input::BackendClosed`] that the broker of
//! the endpoint went away.
//! The session then ends at once, the session records that the backend closed it,
//! and the record of every call in flight says why.
//!
//! Two things are not written yet, and the record says so, once, instead of claiming them:
//! the throttle of an answer, and the answer of a call whose error only stands in an entry that is no part.
//! A lie that only throttles is not told, and its own line says why.
//! A lie that delays and throttles is told, and the failure of the explanation says that the throttle is missing.
//!
//! A call always gets its record.
//! A moment that does not follow the one before it is missing from the timing, and the failure says which.
//!
//! # Errors
//!
//! [`Pipeline::handle`] never fails, so the crate has no error type.
//! What goes wrong while a call is handled is part of the record of that call.
//! An input that names a call or a timer that the pipeline does not wait for changes nothing:
//! the engine cannot avoid every race, such as an answer for a call whose session just closed.
//! Only [`Input::BackendFailed`] ends the session whichever call of the connection it names.
//!
//! # What the engine does
//!
//! - It numbers connections, and counts the [ordinal](gaslightmq_session::ConnectionOrdinals) of each one.
//! - It turns an [`Output::SetTimer`] into a wait on its clock. It need not cancel a timer: a stale one changes nothing.
//! - It [expires](gaslightmq_session::ClientContexts::expire) the client contexts that no session shares any more.
//! - It disarms a rule whose lifetime ended. The armed rules of a snapshot say so, and a call record names the rules that applied.
//! - It passes the incidents of [`Output::Fire`] to the scenario runner, which hands them to the backend.
//!
//! # No IO
//!
//! The crate has no async code, no socket, no file, no system clock, no global randomness and no blocking lock.
//! The current time comes in as an [`Instant`](gaslightmq_clock::Instant) argument.
//! What sessions share sits behind handles that never wait: atomic numbers, and values that are swapped as a whole.
//! It reaches the Kafka codec, so it does not compile to WebAssembly, and no crate that the UI shares depends on it.

mod body_fields;
mod call_fields;
mod explain;
mod in_flight;
mod io;
mod pipeline;
mod rules;
mod shared;
mod timer;
mod untold;

pub use io::{Input, Output, Outputs};
pub use pipeline::Pipeline;
pub use shared::{KnownTopics, SharedBySessions};
pub use timer::TimerId;

/// Every item of this crate.
///
/// Import it whole wherever code drives a pipeline, next to the preludes of the crates it builds on.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_pipeline::prelude::*;
///
/// let timer = TimerId::new("#7.3".parse()?, Stage::BeforeForwarding);
/// assert_eq!(timer.to_string(), "#7.3 before forwarding");
/// # Ok::<(), gaslightmq_call::CallIdError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Input, KnownTopics, Output, Outputs, Pipeline, SharedBySessions, TimerId};
}
