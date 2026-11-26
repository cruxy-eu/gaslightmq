//! The control plane as types: the operations a client asks, their answers and errors, and the live stream.
//!
//! The UI, the CLI and tests reach a process through the control plane, and nothing else.
//! This crate holds what travels between them and the process: each operation, what it answers with,
//! why it fails, and each message of the live stream.
//! It does no IO and holds no behaviour of the engine: a server answers the operations, and a client sends them.
//!
//! | Type | Is |
//! |---|---|
//! | [`Operation`] | One thing a client asks, with its answer and its error. |
//! | [`AnyOperation`] | Any operation, as one value. |
//! | [`OperationRequest`] | One request: an operation, and where it comes from. |
//! | [`RequestSource`], [`RequestSurface`], [`RequestCause`] | Where a request comes from: the surface that sends it, whether it takes back or repeats an earlier change, and the presence of its sender. |
//! | [`RequestError`] | Why a request got neither the answer nor the error of its operation. |
//! | [`EditableDefinition`] | A kind of definition that the control plane lists, gets, creates, updates and deletes. |
//! | [`ListDefinitions`], [`GetDefinition`], [`CreateDefinition`], [`UpdateDefinition`], [`DeleteDefinition`] | The operations on the definitions of the workspace, for each kind. |
//! | [`AnyDefinition`], [`DefinitionRef`], [`Definitions`] | A definition of any kind, its name with its kind, and every definition by kind. |
//! | [`DefinitionStatus`] | A definition, with its revision beside it, and whether a write of it would lose a comment. |
//! | [`Revision`], [`RunId`] | Which state of something someone saw, and the run of the process that gave the revision. |
//! | [`Expects`], [`Revised`] | What a change expects of what it changes, and what it answers with: the revision it left. |
//! | [`ListEnvironments`], [`GetEnvironment`], [`StartEnvironment`], [`StopEnvironment`] | The environments of the workspace, and running one. |
//! | [`EnvironmentStatus`] | An environment, with its revision, and whether it runs. |
//! | [`ListEnvironmentRules`], [`RuleStatus`] | The rules of an environment, each with its state and its revision there. |
//! | [`CreateLiveRule`], [`UpdateLiveRule`], [`DeleteLiveRule`], [`KeepRule`] | The live rules of an environment, and keeping one in the workspace. |
//! | [`ArmRule`], [`DisarmRule`], [`FireRule`] | Commands on a rule of an environment. |
//! | [`SendIntent`], [`IntentEffect`] | What someone did at a pad, and what that did to the rule of the pad. |
//! | [`ListSessions`], [`CloseSession`], [`SessionStatus`], [`SessionIdentity`], [`SessionCloser`] | The sessions of an environment. |
//! | [`GetBackend`], [`BackendStatus`], [`BackendKind`] | The backend of an environment: its kind, its endpoints and the incidents it can do. |
//! | [`ParseSentence`], [`ParsedSentence`], [`WordCompletion`], [`WordKind`], [`UnreadWord`] | A ⌘K sentence that is still being typed, and the rule it gives. |
//! | [`QueryJournal`] | The journal of an environment: what changed there, and where each change came from. |
//! | [`ReloadWorkspace`], [`Reloaded`], [`DefinitionChange`] | Reading the files of the workspace again, what the reload took in, and what it changed in one definition. |
//! | [`StreamMessage`], [`ProcessState`], [`RunningEnvironment`] | The live stream, and the state it starts from. |
//! | [`Stats`], [`RuleStats`] | Calls per API, and how far each rule got. |
//! | [`Subscription`] | Which calls a client of the stream follows. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! ```
//! use gaslightmq_control_api::prelude::*;
//!
//! // Someone presses a pad, and the UI arms the rule of the pad in the environment it shows.
//! let arm = ArmRule {
//!     environment: "orders-sim".parse().expect("an environment name"),
//!     rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
//!     expects: None,
//! };
//! let request = serde_json::to_value(OperationRequest::new(RequestSurface::Pad, arm)).expect("a request has a form");
//! assert_eq!(
//!     request,
//!     serde_json::json!({
//!         "from": { "surface": "pad" },
//!         "operation": "arm-rule",
//!         "environment": "orders-sim",
//!         "rule": "delay-1s-on-order-producers",
//!     }),
//! );
//!
//! // The environment does not run, and the UI offers to start it.
//! let refusal = serde_json::json!({ "error": "not-running", "environment": "orders-sim" });
//! let error: <ArmRule as Operation>::Error = serde_json::from_value(refusal).expect("an error of the operation");
//! assert!(matches!(error, RuleCommandError::NotRunning { .. }));
//! assert_eq!(error.to_string(), "environment \"orders-sim\" does not run");
//! ```
//!
//! # The definition types travel as they are
//!
//! A definition has one form: in a file, in a request and in the UI.
//! The control plane carries the definition types of their own crates, and this crate re-exports those crates,
//! one module per domain, so the UI names every type it shares with the engine through this crate:
//! [`selector`], [`rule`], [`lie`], [`trigger`], [`incident`], [`surface`], [`environment`], [`call`] and
//! [`journal`], and the vocabulary they are made of, [`kernel`], [`field`] and [`clock`].
//! A definition only holds valid values, so the UI checks a value in a form with the engine's own types.
//!
//! A definition travels as its file holds it, in a request as in an answer.
//!
//! # Where a request comes from
//!
//! An operation travels in an [`OperationRequest`], which says where it comes from: the [surface](RequestSurface) that
//! sends it, and the [cause](RequestCause) when it takes back or repeats an earlier change of its sender.
//! The control plane has no users, so a [`RequestSource`] is what the sender says about itself.
//! A request names one of six surfaces. The process names the surface of a change that no request made,
//! so no request can claim one of those.
//!
//! The journal of an environment records the surface and the cause with every change that a request makes:
//! a [`RequestSource`] becomes the [`ChangeSource`](journal::ChangeSource) of the entry.
//! An entry can also name a surface that no request names: `step`, `lifetime` or `file`.
//!
//! A client that follows the live stream is a presence, whose ID is a [`PresenceId`](surface::PresenceId):
//! the stream names it [first](StreamMessage::Present). A request of that client says which presence it is,
//! and a hold that it adds ends with the presence. A request of `gmq` or of a test names none.
//!
//! # Revisions
//!
//! Several clients change one process, and a revision keeps one of them from overwriting what it has not seen.
//! Whatever a request changes has a [`Revision`]: a definition, and a rule in an environment.
//! One counter of the process gives all of them, so a revision never comes back within a run.
//!
//! - **Whoever reads something gets its revision**, beside what it belongs to: in a [`DefinitionStatus`],
//!   a [`RuleStatus`], an [`EnvironmentStatus`] and a [`StreamMessage::DefinitionChanged`].
//! - **The snapshot names the run**: see [`ProcessState::run`]. A revision means something in the run that gave it.
//! - **A change answers with the revision it left**: a [`Revised`].
//! - **A change can say what it [`Expects`]**: the run that its sender read from, and the revision that it saw.
//!   A change that expects another revision changes nothing, and its error is `changed`, with the revision that the
//!   thing has now. A change that expects another run changes nothing, and its error is `other-run`, with the run
//!   of the process.
//! - **Without it, the last change wins**, and a create expects nothing.
//!
//! Live state travels beside a definition, and never inside it: a definition holds what a person writes.
//!
//! # The journal
//!
//! Each environment has a journal of its own: what changed there, next to what happened on the wire.
//! An entry is a [`JournalEntry`](journal::JournalEntry), with the number that the environment gave it,
//! the time, the revision that the change left, and the surface and the cause that the change came from.
//! The control plane carries the entries as their own crate has them, which this crate re-exports as [`journal`].
//!
//! - **A client reads the journal with [`QueryJournal`]**: the entries that the environment keeps in memory,
//!   or those after the entry that the client read last.
//! - **Every new entry is a [`StreamMessage::JournalEntry`]**, which reaches every client of the live stream.
//! - **The journal only grows.** An undo is one more entry, with the cause `undo`, next to the change it took back.
//! - **The revision of an entry is the [`Revision`] of every answer**, so a client finds the entry of its own
//!   change by the revision that the change answered with.
//!
//! # Names
//!
//! Every operation names a definition by its name, and an operation on something live names its environment too.
//! Nothing renames a definition: an update changes everything but the name.
//!
//! # Errors
//!
//! Each operation fails in its own ways, and its error holds only those.
//! Operations that fail in the same ways share one error.
//!
//! | Operation | Error | What is wrong |
//! |---|---|---|
//! | [`ListDefinitions`], [`ListEnvironments`] | [`ListDefinitionsError`] | A store could not read. |
//! | [`GetDefinition`], [`GetEnvironment`] | [`GetDefinitionError`] | No definition has the name, or a store could not read. |
//! | [`CreateDefinition`] | [`CreateDefinitionError`] | The name is taken, the definition does not fit the workspace, someone changed its file since the process read it, or a store failed. |
//! | [`UpdateDefinition`] | [`UpdateDefinitionError`] | No definition has the name, it changed since, the definition does not fit the workspace, an armed live rule would not fit it, someone changed its file since the process read it, or a store failed. |
//! | [`DeleteDefinition`] | [`DeleteDefinitionError`] | No definition has the name, it changed since, other definitions or an armed live rule use it, someone changed its file since the process read it, or a store failed. |
//! | [`StartEnvironment`] | [`StartEnvironmentError`] | No environment has the name, it runs already, or it did not come up. |
//! | [`StopEnvironment`], [`ListSessions`], [`GetBackend`], [`QueryJournal`] | [`EnvironmentNotRunning`] | The environment does not run. |
//! | [`ListEnvironmentRules`], [`ParseSentence`] | [`EnvironmentReadError`] | The environment does not run, or a store could not read. |
//! | [`CreateLiveRule`] | [`CreateLiveRuleError`] | The environment does not run, a rule has the name, the rule does not fit the workspace, or a store could not read. |
//! | [`UpdateLiveRule`] | [`UpdateLiveRuleError`] | The environment does not run, no live rule has the name, it changed since, the rule does not fit the workspace, or a store could not read. |
//! | [`DeleteLiveRule`] | [`DeleteLiveRuleError`] | The environment does not run, no live rule has the name, or it changed since. |
//! | [`KeepRule`] | [`KeepRuleError`] | The environment does not run, no live rule has the name, it changed since, another rule has the name, the rule does not fit the workspace, someone changed the file where it would go, or a store failed. |
//! | [`ArmRule`], [`DisarmRule`] | [`RuleCommandError`] | The environment does not run, no rule has the name, it changed since, the rule cannot do this from where it stands, or a store could not read. |
//! | [`FireRule`] | [`FireRuleError`] | The environment does not run, no rule has the name, the rule cannot fire from where it stands, or a store could not read. |
//! | [`SendIntent`] | [`IntentError`] | The environment does not run, the layout does not exist, the environment cannot act on the intent, or a store could not read. |
//! | [`CloseSession`] | [`CloseSessionError`] | The environment does not run, or no session is open on the connection. |
//! | [`ReloadWorkspace`] | [`ReloadWorkspaceError`] | A rule of the files has the name of a live rule, an armed live rule would not fit the files, the files do not fit each other, or a file does not read. |
//!
//! A change that says what it [`Expects`] fails with the variant `Changed` when the thing has another revision,
//! and with `OtherRun` when the revision is of another run. Either way it changed nothing.
//!
//! A change that writes a file fails with `ChangedOutside` when someone edited that file since the process read it.
//! No revision is involved: the process does not hold what the file holds now, until someone
//! [reloads](ReloadWorkspace). A read-only workspace refuses every such change with `StoreFailed`, and the
//! [state of the process](ProcessState::read_only) says beforehand that it is read-only.
//!
//! A request can also fail before its operation runs, in the same way for every operation:
//! the process does not serve the operation yet, reads no operation in the request,
//! or does not read where the request comes from.
//! That is a [`RequestError`].
//!
//! The server meets the errors of the crates that do the work, such as the workspace, and turns them into these.
//! What they said is text, in a field `reason`.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # The `serde` forms
//!
//! Every type of this crate travels, so every one has a `serde` form, and the crate has no feature for it.
//! The definition types it carries have the forms of their own crates.
//!
//! | Type | Form |
//! |---|---|
//! | [`OperationRequest`] | The map of its [`AnyOperation`], with a `from` key next to its keys: `{ "from": { "surface": "cli" }, "operation": "arm-rule", … }`. |
//! | [`AnyOperation`] | The map of its operation, with an `operation` key that names it: `arm-rule`, `create-selector`. |
//! | An operation | A map of its fields. A change that expects nothing leaves `expects` out. |
//! | [`Revision`], [`RunId`] | The number, and the 32 hexadecimal digits in lower case as text. |
//! | [`Expects`], [`Revised`], [`DefinitionStatus`] | A map of its fields: `{ "run": "4be0…47dd", "revision": 7 }`, `{ "revision": 8 }` and `{ "definition": { … }, "revision": 7 }`. A definition whose write would lose a comment has `"loses-comment": true` next to its revision. |
//! | An error | A map of its fields, with an `error` key that names the variant: `{ "error": "not-found", "name": { "rule": "x" } }`. [`EnvironmentNotRunning`] is the map of its field. |
//! | [`DefinitionRef`], [`AnyDefinition`] | A map with one entry: the kind, and the name or the definition. |
//! | [`DefinitionChange`] | A map with one entry: `created`, `updated` or `deleted`, and the [`DefinitionRef`]. |
//! | [`StreamMessage`] | The map of its fields, with a `message` key that names it: `rule-changed`. |
//! | [`Subscription`] | The map of its fields, with a `follow` key: `calls` or `nothing`. |
//! | [`BackendKind`], [`SessionCloser`], [`WordKind`], [`RequestSurface`], [`RequestCause`] | The name in kebab case. |
//! | Every other type | A map of its fields. A field that is `None` or empty is left out. |
//!
//! # What the control plane does not hold yet
//!
//! Scenarios travel as definitions: the state of a process holds them, and a change of one is a message of the
//! live stream. No operation creates, changes, activates or steps a scenario yet.
//! Recordings and capture arrive with the crates of their domains,
//! and the World with the broker. Their operations and messages arrive with them,
//! and so do the entries of the journal for steps and recordings.
//!
//! # No IO
//!
//! The crate depends on the crates of the definitions it carries, and on nothing that touches the network,
//! a file or a clock. It does not depend on the Kafka codec, and it compiles to WebAssembly.

pub use gaslightmq_call as call;
pub use gaslightmq_clock as clock;
pub use gaslightmq_environment as environment;
pub use gaslightmq_field as field;
pub use gaslightmq_incident as incident;
pub use gaslightmq_journal as journal;
pub use gaslightmq_kernel as kernel;
pub use gaslightmq_lie as lie;
pub use gaslightmq_rule as rule;
pub use gaslightmq_scenario as scenario;
pub use gaslightmq_selector as selector;
pub use gaslightmq_surface as surface;
pub use gaslightmq_trigger as trigger;

mod backend;
mod definitions;
mod environments;
mod journal_query;
mod operation;
mod pads;
mod request;
mod revision;
mod rules;
mod sentence;
mod sessions;
mod stats;
mod stream;
mod workspace;

pub use backend::{BackendKind, BackendStatus, GetBackend};
pub use definitions::{
    AnyDefinition, CreateDefinition, CreateDefinitionError, DefinitionRef, DefinitionStatus, Definitions,
    DeleteDefinition, DeleteDefinitionError, EditableDefinition, GetDefinition, GetDefinitionError, ListDefinitions,
    ListDefinitionsError, UpdateDefinition, UpdateDefinitionError,
};
pub use environments::{
    EnvironmentNotRunning, EnvironmentReadError, EnvironmentStatus, GetEnvironment, ListEnvironments, StartEnvironment,
    StartEnvironmentError, StopEnvironment,
};
pub use journal_query::QueryJournal;
pub use operation::{AnyOperation, Operation, RequestError};
pub use pads::{IntentEffect, IntentError, SendIntent};
pub use request::{OperationRequest, RequestCause, RequestSource, RequestSurface};
pub use revision::{Expects, Revised, Revision, RunId, RunIdError};
pub use rules::{
    ArmRule, CreateLiveRule, CreateLiveRuleError, DeleteLiveRule, DeleteLiveRuleError, DisarmRule, FireRule,
    FireRuleError, KeepRule, KeepRuleError, ListEnvironmentRules, RuleCommandError, RuleHolder, RuleStatus,
    UpdateLiveRule, UpdateLiveRuleError,
};
pub use sentence::{ParseSentence, ParsedSentence, UnreadWord, WordCompletion, WordKind};
pub use sessions::{CloseSession, CloseSessionError, ListSessions, SessionCloser, SessionIdentity, SessionStatus};
pub use stats::{RuleStats, Stats};
pub use stream::{ProcessState, RunningEnvironment, StreamMessage, Subscription};
pub use workspace::{DefinitionChange, ReloadWorkspace, ReloadWorkspaceError, Reloaded};

/// Every type of this crate, with its error.
///
/// Import it whole wherever code speaks to the control plane, next to the preludes of the domains it re-exports.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_control_api::environment::EnvironmentNameError;
/// use gaslightmq_control_api::prelude::*;
///
/// let stop = StopEnvironment { environment: "orders-sim".parse()? };
/// assert_eq!(AnyOperation::from(stop.clone()), AnyOperation::StopEnvironment(stop));
/// # Ok::<(), EnvironmentNameError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        AnyDefinition, AnyOperation, ArmRule, BackendKind, BackendStatus, CloseSession, CloseSessionError,
        CreateDefinition, CreateDefinitionError, CreateLiveRule, CreateLiveRuleError, DefinitionChange, DefinitionRef,
        DefinitionStatus, Definitions, DeleteDefinition, DeleteDefinitionError, DeleteLiveRule, DeleteLiveRuleError,
        DisarmRule, EditableDefinition, EnvironmentNotRunning, EnvironmentReadError, EnvironmentStatus, Expects,
        FireRule, FireRuleError, GetBackend, GetDefinition, GetDefinitionError, GetEnvironment, IntentEffect,
        IntentError, KeepRule, KeepRuleError, ListDefinitions, ListDefinitionsError, ListEnvironmentRules,
        ListEnvironments, ListSessions, Operation, OperationRequest, ParseSentence, ParsedSentence, ProcessState,
        QueryJournal, ReloadWorkspace, ReloadWorkspaceError, Reloaded, RequestCause, RequestError, RequestSource,
        RequestSurface, Revised, Revision, RuleCommandError, RuleHolder, RuleStats, RuleStatus, RunId, RunIdError,
        RunningEnvironment, SendIntent, SessionCloser, SessionIdentity, SessionStatus, StartEnvironment,
        StartEnvironmentError, Stats, StopEnvironment, StreamMessage, Subscription, UnreadWord, UpdateDefinition,
        UpdateDefinitionError, UpdateLiveRule, UpdateLiveRuleError, WordCompletion, WordKind,
    };
}
