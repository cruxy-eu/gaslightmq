//! One process with several environments: a proxy for each, and one workspace for all of them.
//!
//! An [environment](gaslightmq_environment::Environment) describes the instances that run together.
//! A process starts one environment or several, and the host is what runs them side by side.
//!
//! | Type | Is |
//! |---|---|
//! | [`Host`] | The environments of one process, and the workspace they share. |
//! | [`HostConfiguration`] | What holds for every environment of a host: how it reads a ⌘K sentence. |
//! | [`EnvironmentConfiguration`] | What a host starts one environment with: the configuration of its proxy and of its backend. |
//! | [`EnvironmentStartError`], [`NotRunningError`] | Why an environment did not start, and that an environment does not run. |
//! | [`RunId`], [`RunIdError`] | The ID of the run of a host: of one life of the process. |
//! | [`Expects`] | What a change expects of what it changes: the run that its sender read from, and the revision that it saw. |
//! | [`UpdateDefinitionError`], [`DeleteDefinitionError`] | Why a definition of the workspace was not updated or deleted. |
//! | [`ArmRuleError`], [`DisarmRuleError`], [`FireRuleError`], [`ReadSentenceError`] | Why a rule was not armed, disarmed or fired, and why a sentence was not read. |
//! | [`EnvironmentConfigurationError`] | Why an environment has no configuration that a host can start. |
//! | [`EnvironmentRule`] | A rule of an environment, with its state and its revision there: a rule of the workspace, or a live rule. |
//! | [`IntentEffect`] | What an intent of a pad did: the rule it acted on, whether it made or changed it, and the hold it added. |
//! | [`HostChange`], [`HostChanges`] | One change of the host, whoever made it, and the changes for one follower. |
//! | [`ChangeBacklog`], [`CallBacklog`] | How many changes, and how many calls, a host keeps for a follower that falls behind. |
//! | [`CallSubscription`] | The calls of one environment that one selector matches, as its traffic matches them. |
//! | [`CreateDefinitionError`] | Why a definition was not created in the workspace. |
//! | [`Reloaded`], [`ReloadWorkspaceError`], [`Unsaved`] | What a reload of the workspace took in from its files, why a reload changed nothing, and what it does with changes that nobody saved. |
//! | [`CreateLiveRuleError`], [`UpdateLiveRuleError`], [`DeleteLiveRuleError`], [`KeepRuleError`] | Why a live rule was not made, changed, removed or kept. |
//! | [`EnvironmentRulesError`], [`IntentError`], [`CloseSessionError`], [`HostChangesError`], [`FollowCallsError`] | Why the rules of an environment were not read, a pad was not acted on, a session was not closed, a follower has no next change, and the calls of an environment were not followed. |
//!
//! The [`prelude`] holds all of them.
//!
//! ```no_run
//! use gaslightmq_environment::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_host::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_workspace::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), EnvironmentStartError> {
//! let host = Host::new(
//!     HostConfiguration::default(),
//!     MemoryContext::new(),
//!     Lies::of_every_build(),
//!     RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd),
//! );
//!
//! // A proxy in front of the staging cluster, as `environments/staging-proxy.yaml` describes it.
//! let listen = Listeners::new(
//!     "0.0.0.0:9092".parse().expect("a listen address"),
//!     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
//!     "gaslight.staging".parse().expect("a host name"),
//! );
//! let staging = Environment::new(
//!     "staging-proxy".parse().expect("an environment name"),
//!     Instances::ProxyInFrontOfCluster {
//!         proxy: ProxyDefinition::new(listen.expect("a bootstrap port outside the ports of the brokers")),
//!         upstream: Upstream::new("kafka-1.staging:9092".parse().expect("a server address")),
//!     },
//! );
//! let configuration = EnvironmentConfiguration::new(&staging).expect("a proxy in front of a real cluster");
//!
//! // The proxy accepts connections once the start returns.
//! host.start(configuration).await?;
//! let bootstrap = host.address_of(staging.name(), Endpoint::Bootstrap).expect("the environment runs");
//! assert_eq!(bootstrap, Some("0.0.0.0:9092".parse().expect("a socket address")));
//!
//! host.stop(staging.name()).await.expect("the environment runs");
//! # Ok(())
//! # }
//! ```
//!
//! # Several environments in one process
//!
//! - **Each environment is separate.**
//!   It has its own proxy: its own listeners, sessions, armed rules and events.
//!   Arming a rule in one environment does not arm it in another.
//! - **Environments share the workspace.**
//!   The host holds one [`Workspace`](gaslightmq_workspace::Workspace) over the context it is handed, and names no
//!   store: whoever puts the process together picks the stores in that context.
//!   A rule resolves against the selectors and the presets of the workspace in whichever environment it is armed.
//! - **Listeners must not overlap.**
//!   The host refuses to start an environment whose listeners name a port that an environment that runs names on
//!   the same address. A listener on port 0 takes any free port, and meets nothing.
//! - **One start or stop at a time.**
//!   The host checks the listeners and opens them as one step, so two starts never claim one port.
//!   What the environments that run hand over, and their rules, do not wait for a start.
//! - **Whatever someone changes has a revision.**
//!   One counter of the process gives the [`Revision`](gaslightmq_workspace::Revision) of every definition and of
//!   every rule in an environment. A change answers with the revision it left, and it can say what it [`Expects`]:
//!   a change that expects another revision, or a revision of another [run](RunId), changes nothing.
//! - **Every change has a surface.**
//!   Whoever asks the host for a change says where it comes from: a
//!   [`ChangeSource`](gaslightmq_journal::ChangeSource), as the last argument of the call.
//!   The host records the change in the [journal](Host::journal) of the environment, with that source and the
//!   revision it left, and a change of a definition in the journal of every environment that runs.
//!   The host names the surface of a change that nobody asked for.
//!
//! # Presences
//!
//! A presence is one client of a control surface that is there, such as a tab of the UI.
//! Whoever lets clients connect [opens](Host::open_presence) a presence for a client that came,
//! and [ends](Host::end_presence) it once the client is gone.
//! A hold of a pad belongs to the presence that its press [came from](Host::send_intent_from), and ends with it:
//! a client that is gone never sends its release.
//! A presence is no part of where a change comes from: the journal records a surface, and not who was there.
//! The host knows no connection, and waits for no client that may come back: when a presence ends is up to whoever
//! opened it. An intent that comes from [no presence](Host::send_intent) adds a hold that ends by its release or by a
//! disarm alone.
//!
//! The host does not care where an environment comes from.
//! [`EnvironmentConfiguration::new`] takes an environment of the workspace as well as one that the flags of a
//! command describe, and whoever starts it sets what a flag, a variable or a file says first.
//!
//! # A proxy in front of a real cluster
//!
//! Each environment runs as an engine of `gaslightmq_engine`, in front of the backend of `gaslightmq_backend_upstream`,
//! on the wall clock of `gaslightmq_clock_waitable`.
//! The lies that every proxy tells are the lies of the build, which whoever makes the host hands over once.
//!
//! # What is not here yet
//!
//! - **Brokers.** An environment with a broker, behind a proxy or alone, has no configuration yet:
//!   see [`EnvironmentConfigurationError::UnsupportedBroker`]. The broker and its task arrive with the crate `broker`.
//! - **What a process configures.** No workspace setting reaches an environment by itself:
//!   whoever starts one sets its seed and its other properties in its [`EnvironmentConfiguration`].
//! - **The live set, scenarios and recordings** arrive with their crates.
//!   Until then the armed rules of an environment are its live set, and no journal has an entry of a scenario,
//!   of a step or of a recording.
//! - **Steps.** No change has the surface `step` until a scenario runs.
//! - **The control plane** stands in front of the host, in the crate `gaslightmq_control_server`.
//!
//! # Errors
//!
//! Each thing that fails in its own way has an error of its own, so nobody handles a failure that cannot come.
//!
//! | What fails | Error | What is wrong |
//! |---|---|---|
//! | [`EnvironmentConfiguration::new`] | [`EnvironmentConfigurationError`] | The environment runs a broker. |
//! | [`Host::start`] | [`EnvironmentStartError`] | The environment runs already, its listeners meet those of another, or its backend or its proxy did not start. |
//! | [`Host::stop`], and every question about an environment that runs | [`NotRunningError`] | No environment of that name runs. |
//! | [`Host::update`] | [`UpdateDefinitionError`] | The change expects another run, an armed live rule would not resolve with it, or the workspace did not take the update. |
//! | [`Host::delete`] | [`DeleteDefinitionError`] | The change expects another run, an armed live rule uses the definition, or the workspace did not take the delete. |
//! | [`Host::arm`] | [`ArmRuleError`] | The environment does not run, the workspace could not be read or is not valid, the rule does not resolve, it is armed already, or it changed since. |
//! | [`Host::disarm`] | [`DisarmRuleError`] | The environment does not run, the rule is not armed, or it changed since. |
//! | [`Host::fire`] | [`FireRuleError`] | The environment does not run, the rule is not armed or fires no more, its incident needs a call, or the backend rejects it. |
//! | [`Host::read_sentence`] | [`ReadSentenceError`] | The environment does not run, or the workspace could not be read. |
//! | [`Host::rules_of`], [`Host::rule_of`] | [`EnvironmentRulesError`] | The environment does not run, or the rules could not be read. |
//! | [`Host::create_live_rule`], [`Host::create_live_rule_armed`] | [`CreateLiveRuleError`] | The environment does not run, a rule has the name, or the rule does not resolve. |
//! | [`Host::update_live_rule`] | [`UpdateLiveRuleError`] | The environment does not run, it has no live rule of the name, the rule does not resolve, or it changed since. |
//! | [`Host::delete_live_rule`] | [`DeleteLiveRuleError`] | The environment does not run, it has no live rule of the name, or it changed since. |
//! | [`Host::create`] | [`CreateDefinitionError`] | A live rule of an environment that runs has the name of the rule, or the workspace did not take the definition. |
//! | [`Host::reload`] | [`ReloadWorkspaceError`] | A rule of the files has the name of a live rule, an armed rule would not resolve against the files, the workspace holds changes that nobody saved, or the workspace did not take the reload. |
//! | [`Host::save`] | [`SaveError`](gaslightmq_workspace::SaveError) | Someone changed a file that the save would write, or a store could not write. |
//! | [`Host::keep`] | [`KeepRuleError`] | The environment does not run, it has no live rule of the name, it changed since, a live rule of another environment has the name, or the workspace did not take it. |
//! | [`Host::send_intent`], [`Host::send_intent_from`] | [`IntentError`] | The environment does not run, the layout does not exist, a store could not read, or the host cannot act on the intent: a hold of a presence that is not open is one such intent. An intent that finds the state it wants changes nothing, and is no error. |
//! | [`Host::close_session`] | [`CloseSessionError`] | The environment does not run, or no session is open on the connection. |
//! | [`Host::follow_calls`] | [`FollowCallsError`] | The environment does not run, the selectors could not be read, or the selector does not resolve. |
//! | [`CallBacklog::new`] | [`CallBacklogError`] | The backlog is zero, or larger than the largest. |
//! | [`HostChanges::next`] | [`HostChangesError`] | The follower fell behind, or the host is gone. |
//! | [`ChangeBacklog::new`] | [`ChangeBacklogError`] | The backlog is zero, or larger than the largest. |
//! | [`RunId::new`] | [`RunIdError`] | The text is no run ID: it has another length than 32, or holds what is no hexadecimal digit in lower case. |
//!
//! A change that names what it [`Expects`] fails with `Changed` when the thing has another revision,
//! and with `OtherRun` when the revision is of another run. Either way it changed nothing.
//!
//! A variant is something its caller decides on, and the rest is text.
//! The error of the backend, of the proxy and of a rule is kept as it is, in a field `error`,
//! and what a store said is text, in a field `reason`.
//! A variant writes either one once, after what it says itself.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it runs on Tokio.
//! It starts one task of its own for each environment, which watches it, on the runtime of whoever starts the
//! environment. Each engine and each backend runs its tasks there too.
//! The configuration and the errors are plain values that never wait.

mod calls;
mod changes;
mod configuration;
mod error;
mod holds;
mod host;
mod journaling;
mod reloaded;
mod revisions;
mod rules;
mod run;

pub use calls::CallSubscription;
pub use changes::{HostChange, HostChanges, HostChangesError};
pub use configuration::{
    CallBacklog, CallBacklogError, ChangeBacklog, ChangeBacklogError, EnvironmentConfiguration,
    EnvironmentConfigurationError, HostConfiguration,
};
pub use error::{
    ArmRuleError, CloseSessionError, CreateDefinitionError, CreateLiveRuleError, DeleteDefinitionError,
    DeleteLiveRuleError, DisarmRuleError, EnvironmentRulesError, EnvironmentStartError, FireRuleError,
    FollowCallsError, IntentError, KeepRuleError, NotRunningError, ReadSentenceError, ReloadWorkspaceError, RuleHolder,
    UpdateDefinitionError, UpdateLiveRuleError,
};
pub use host::Host;
pub use reloaded::{Reloaded, Unsaved};
pub use rules::{EnvironmentRule, IntentEffect};
pub use run::{Expects, RunId, RunIdError};

/// Every item of this crate.
///
/// Import it whole wherever code runs a host, next to the preludes of the crates a host is made of.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = NotRunningError {
///     name: "staging-proxy".parse().expect("an environment name"),
/// };
/// assert_eq!(error.to_string(), "environment \"staging-proxy\" does not run");
/// assert_eq!(HostConfiguration::default().quick_inject.completion_limit, 5);
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ArmRuleError, CallBacklog, CallBacklogError, CallSubscription, ChangeBacklog, ChangeBacklogError,
        CloseSessionError, CreateDefinitionError, CreateLiveRuleError, DeleteDefinitionError, DeleteLiveRuleError,
        DisarmRuleError, EnvironmentConfiguration, EnvironmentConfigurationError, EnvironmentRule,
        EnvironmentRulesError, EnvironmentStartError, Expects, FireRuleError, FollowCallsError, Host, HostChange,
        HostChanges, HostChangesError, HostConfiguration, IntentEffect, IntentError, KeepRuleError, NotRunningError,
        ReadSentenceError, ReloadWorkspaceError, Reloaded, RuleHolder, RunId, RunIdError, Unsaved,
        UpdateDefinitionError, UpdateLiveRuleError,
    };
}
