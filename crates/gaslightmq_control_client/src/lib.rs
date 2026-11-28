//! A client of the control plane, for a native target and for WebAssembly in a browser.
//!
//! The UI, the CLI and tests reach a process through the control plane.
//! This crate sends the operations of [`gaslightmq_control_api`] and follows its live stream,
//! over HTTP and a WebSocket.
//!
//! | Type | Is |
//! |---|---|
//! | [`ControlClient`] | A client of one control plane. It sends operations and opens live streams. |
//! | [`LiveStream`] | The presence of the client, the snapshot of the process, then every change, and the calls it follows. |
//! | [`ControlClientConfiguration`] | Where the control plane is, which surface the client is, and how long it waits. |
//! | [`ControlPlaneAddress`], [`ControlPlaneAddressError`] | Where a control plane listens. |
//! | [`SendError`] | Why an operation got no answer of its own. |
//! | [`StreamError`] | Why a live stream did not open, or ended. |
//! | [`ControlledEnvironment`] | One environment that runs, in the words of whoever works with it: [`Rules`], [`EnvironmentRule`], [`Scenarios`], [`ActiveScenario`], [`Recordings`], [`Recording`], [`FollowedCalls`] and the journal. |
//! | [`Definitions`] | The definitions of the workspace, to create some. |
//! | [`Preparation`], [`Prepared`], [`PrepareError`] | What an instance starts with, declared before it starts, what that did, and why it did not. |
//! | [`RuleToArm`], [`ScenarioToActivate`], [`SelectorToMatch`], [`ScenarioText`] | What a caller hands over: a definition as its text or its value, or a name. |
//! | [`DefinitionText`], [`ReadDefinitionError`] | The text of a definition, as a file holds it and as [`rule!`], [`scenario!`] and [`selector!`] write it, and why it did not read. |
//! | [`ArmError`], [`DisarmError`], [`FireError`], [`FiringsError`], [`ActivateError`], [`StepError`], [`DeactivateError`], [`DefineError`], [`RecordingStartError`], [`RecordingStopError`], [`QueryError`], [`JournalError`], [`FollowError`], [`WaitForCallsError`], [`EnvironmentStateError`], [`CurrentRecordingError`] | Why each call of an environment did not do what it asked. |
//!
//! # An operation gets its answer or its error
//!
//! A caller sends an [`Operation`](gaslightmq_control_api::Operation), and gets back what the control plane
//! answered: the answer of the operation, or its error, each typed by the operation.
//! The error of the operation is an answer too, so the call returns it as its value.
//! A [`SendError`] says that no answer came, that the control plane did not take the request,
//! or that the client cannot read what came.
//!
//! Every request says [where it comes from](gaslightmq_control_api::OperationRequest).
//! A client is one surface, which its configuration names, and [`ControlClient::send`] sends from there.
//! [`ControlClient::send_from`] names the source of one request: another surface, or the cause of an undo or a redo.
//!
//! ```no_run
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_client::{ControlClient, SendError};
//!
//! # async fn arm(client: ControlClient) -> Result<(), SendError> {
//! let arm = ArmRule {
//!     environment: "orders-sim".parse().expect("an environment name"),
//!     rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
//!     expects: None,
//! };
//! match client.send(arm).await? {
//!     // The rule is armed, and the answer holds the revision that it has in the environment from here on.
//!     Ok(Revised { .. }) => {}
//!     // The environment does not run: start it, and arm the rule again.
//!     Err(RuleCommandError::NotRunning { environment }) => {
//!         let _ = client.send(StartEnvironment { environment }).await?;
//!     }
//!     Err(other) => eprintln!("{other}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # One environment, in the words of a test
//!
//! [`ControlClient::environment`] gives the words that a test speaks, and any other client that works with one
//! environment: arm a rule, activate a scenario, start, stop and query a recording, follow calls and wait for them,
//! and read the journal. Each call has an error of its own, named after it.
//!
//! A definition is written as a file of the workspace holds it, with [`rule!`], [`scenario!`] and [`selector!`],
//! or as the YAML of a file. Nothing needs a name: a rule has the name of what it says, a step the name of its place,
//! and a scenario that a test activates without a name is `scenario`.
//!
//! ```no_run
//! use gaslightmq_control_client::{ControlClient, rule, selector};
//!
//! # async fn refuse_once(client: ControlClient) -> anyhow::Result<()> {
//! let orders = client.environment("orders-sim".parse()?);
//! let mut produces = orders.follow(selector! { api: Produce }).await?;
//! let recording = orders.recordings().start().await?;
//! let refused = orders
//!     .rules()
//!     .arm(rule! { selector: { api: Produce }, action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER }, trigger: { first: 1 } })
//!     .await?;
//! // … the client under test produces, and tries again …
//! produces.wait_for(2).await?;
//! recording.stop().await?;
//! let recorded = recording.query(selector! { api: Produce }).await?;
//! assert!(recorded.calls[0].explanation().applied(&refused));
//! # Ok(())
//! # }
//! ```
//!
//! # The live stream
//!
//! A [`LiveStream`] starts with the [snapshot](gaslightmq_control_api::StreamMessage::Snapshot) of the process,
//! and every message after it is one change, or one call that the stream follows.
//! A client that falls behind gets a new snapshot, and goes on from there.
//!
//! The [journal](gaslightmq_control_api::QueryJournal) of an environment is no part of the snapshot.
//! A client reads it with an operation, and every entry after that comes as a
//! [message](gaslightmq_control_api::StreamMessage::JournalEntry) of the stream.
//! A client that got a new snapshot asks for the entries after the last one it holds.
//!
//! A client that follows the stream is there, and the control plane knows it as a
//! [presence](LiveStream::presence). A request that names the presence adds holds that end with it,
//! so a pad never stays held for a client that is gone.
//! The control plane keeps the presence of a stream that ended for a grace period,
//! and a client that [comes back as it](ControlClient::live_stream_as) in that time keeps its holds.
//!
//! ```no_run
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_api::selector::{Conditions, SelectorRef};
//! use gaslightmq_control_client::{ControlClient, StreamError};
//!
//! # async fn follow(client: ControlClient) -> Result<(), StreamError> {
//! let mut stream = client.live_stream().await?;
//! stream
//!     .follow(Subscription::Calls {
//!         environment: "orders-sim".parse().expect("an environment name"),
//!         selector: SelectorRef::Inline(Conditions::new()),
//!     })
//!     .await;
//! loop {
//!     match stream.next().await? {
//!         StreamMessage::Snapshot(state) => println!("{} environments run", state.environments.len()),
//!         StreamMessage::Call { call, .. } => println!("{call:?}"),
//!         _ => {}
//!     }
//! }
//! # }
//! ```
//!
//! # Transports
//!
//! The crate has one feature per transport, and each takes effect on its own target alone.
//! A build with every feature therefore has the transport of the target it builds for.
//!
//! | Feature | Target | Transport |
//! |---|---|---|
//! | `native`, on by default | Every target but `wasm32` | HTTP and WebSocket on Tokio. The runtime has its timers on. |
//! | `wasm` | `wasm32` | The browser's own fetch and WebSocket, and its timers. |
//!
//! The client speaks plain HTTP and plain WebSocket: an address is an `http` URL.

#[cfg(not(any(
    all(feature = "native", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32"),
)))]
compile_error!("the client needs a transport: the feature `native` on a native target, or `wasm` on wasm32");

mod address;
mod client;
mod configuration;
mod environment;
mod error;
mod exchange;
mod prepare;
mod stream;
mod transport;
mod written;

pub use address::{ControlPlaneAddress, ControlPlaneAddressError};
pub use client::ControlClient;
pub use configuration::ControlClientConfiguration;
pub use environment::{
    ActiveScenario, ControlledEnvironment, Definitions, EnvironmentRule, FollowedCalls, Recording, Recordings, Rules,
    ScenarioText, Scenarios,
};
pub use error::{
    ActivateError, ArmError, CurrentRecordingError, DeactivateError, DefineError, DisarmError, EnvironmentStateError,
    FireError, FiringsError, FollowError, JournalError, PrepareError, QueryError, RecordingStartError,
    RecordingStopError, SendError, StepError, StreamError, WaitForCallsError,
};
pub use gaslightmq_definition_text::{DefinitionText, ReadDefinitionError, definition_text};
pub use prepare::{Preparation, Prepared};
pub use written::{RuleToArm, ScenarioToActivate, SelectorToMatch};

/// The kinds of definition that the macros of this crate write, for the macros alone.
#[doc(hidden)]
pub mod __kinds {
    pub use gaslightmq_control_api::rule::Rule;
    pub use gaslightmq_control_api::scenario::Scenario;
    pub use gaslightmq_control_api::selector::SelectorRef;
}
pub use stream::LiveStream;
