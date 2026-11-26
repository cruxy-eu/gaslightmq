//! Why a host could not do what it was asked to do: one error for each thing that fails in its own way.

use gaslightmq_backend::IncidentRejection;
use gaslightmq_backend_upstream::UpstreamBackendError;
use gaslightmq_engine::EngineStartError;
use gaslightmq_environment::{EnvironmentName, ListenAddress};
use gaslightmq_field::ConnectionNumber;
use gaslightmq_rule::{RuleError, RuleName};
use gaslightmq_scenario::ScenarioName;
use gaslightmq_selector::SelectorSetError;
use gaslightmq_surface::LayoutName;
use gaslightmq_workspace::{CreateError, DeleteError, Revision, UpdateError};

use crate::run::RunId;

/// Why a host did not [start](crate::Host::start) an environment.
///
/// Each variant is a thing that whoever starts the environment does something else about:
///
/// | Variant | What to do about it |
/// |---|---|
/// | [`AlreadyRunning`](Self::AlreadyRunning) | Nothing, or stop the environment first to start it with another configuration. |
/// | [`SharedListener`](Self::SharedListener) | Pick other ports, or stop the environment that listens there. |
/// | [`BackendNotStarted`](Self::BackendNotStarted) | Look at the cluster: its `error` says whether it did not answer, or asks for what a proxy cannot do yet. |
/// | [`ProxyNotStarted`](Self::ProxyNotStarted) | Look at the listeners: its `error` says which endpoint did not open, and why. |
///
/// A host that did not start an environment runs nothing of it.
///
/// ```
/// use gaslightmq_backend_upstream::prelude::*;
/// use gaslightmq_host::prelude::*;
///
/// let error = EnvironmentStartError::BackendNotStarted {
///     name: "staging-proxy".parse().expect("an environment name"),
///     error: UpstreamBackendError::ClusterUndiscovered {
///         reason: "kafka-1.staging:9092 did not answer within 30s".to_owned(),
///     },
/// };
/// assert_eq!(
///     error.to_string(),
///     "environment \"staging-proxy\" did not start: \
///      the cluster did not name its brokers: kafka-1.staging:9092 did not answer within 30s"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentStartError {
    /// An environment of this name runs already.
    #[error("environment {:?} runs already", .name.as_str())]
    AlreadyRunning { name: EnvironmentName },
    /// A listener of the environment would listen on the same port of the same address as one of an environment
    /// that runs.
    #[error(
        "environment {:?} would listen on {address}, where environment {:?} listens",
        .name.as_str(),
        .running.as_str()
    )]
    SharedListener {
        name: EnvironmentName,
        running: EnvironmentName,
        address: ListenAddress,
    },
    /// The backend in front of the real cluster did not start. Its error says why.
    #[error("environment {:?} did not start: {error}", .name.as_str())]
    BackendNotStarted {
        name: EnvironmentName,
        error: UpstreamBackendError,
    },
    /// The proxy did not start: an endpoint of the cluster has no listener. Its error says which, and why.
    #[error("environment {:?} did not start: {error}", .name.as_str())]
    ProxyNotStarted {
        name: EnvironmentName,
        error: EngineStartError,
    },
}

/// The environment that a host was asked about does not run.
///
/// It is the one way in which a question about a running environment fails:
/// where it listens, what it hands over, and which rules it has armed.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = NotRunningError {
///     name: "staging-proxy".parse().expect("an environment name"),
/// };
/// assert_eq!(error.to_string(), "environment \"staging-proxy\" does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("environment {:?} does not run", .name.as_str())]
pub struct NotRunningError {
    /// The name that no running environment has.
    pub name: EnvironmentName,
}

/// Why a host did not [update](crate::Host::update) a definition of the workspace.
///
/// An update that fails changed nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let name: PresetName = "blackhole".parse()?;
/// let error = UpdateDefinitionError::NotUpdated { error: UpdateError::NotFound { name: name.into() } };
/// assert_eq!(error.to_string(), "preset \"blackhole\" does not exist");
/// # Ok::<(), PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpdateDefinitionError {
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
    /// The workspace did not take the update. Its error says why, and names the definition:
    /// no definition has the name, it has another revision than the change expects, it does not fit the workspace,
    /// or a store failed.
    #[error("{error}")]
    NotUpdated { error: UpdateError },
}

/// Why a host did not [delete](crate::Host::delete) a definition of the workspace.
///
/// A delete that fails changed nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse()?;
/// let error = DeleteDefinitionError::OtherRun { run };
/// assert_eq!(
///     error.to_string(),
///     "the change expects a revision of another run than 4be0643f1d98573b97cdca98a65347dd, \
///      which is the run of the process"
/// );
/// # Ok::<(), RunIdError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeleteDefinitionError {
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
    /// The workspace did not take the delete. Its error says why, and names the definition:
    /// no definition has the name, it has another revision than the change expects, other definitions use it,
    /// or a store failed.
    #[error("{error}")]
    NotDeleted { error: DeleteError },
}

/// Why a host did not [arm](crate::Host::arm) a rule in an environment.
///
/// | Variant | What is wrong |
/// |---|---|
/// | [`NotRunning`](Self::NotRunning) | No environment of that name runs. |
/// | [`StoreFailed`](Self::StoreFailed) | The workspace could not read the selectors or the presets that the rule may name. |
/// | [`WorkspaceInvalid`](Self::WorkspaceInvalid) | The selectors or the presets of the workspace do not hold together, so no rule resolves against them. |
/// | [`Unresolved`](Self::Unresolved) | The rule does not resolve against the workspace: it names a selector or a preset that does not exist, or it could never fire. |
/// | [`AlreadyArmed`](Self::AlreadyArmed) | A rule of that name is armed in the environment. |
/// | [`Changed`](Self::Changed) | The rule has another revision in the environment than the change expects. |
/// | [`OtherRun`](Self::OtherRun) | The change expects a revision of another run. |
///
/// An error that leaves a rule disarmed changes nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = ArmRuleError::AlreadyArmed {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     rule: "leader-flap".parse().expect("a rule name"),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"leader-flap\" is armed already in environment \"staging-proxy\""
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArmRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// A store of the workspace could not be read. `reason` says which store, and what it said.
    #[error("rule {:?} is not armed: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The selectors or the presets of the workspace do not hold together, as after someone edited a file.
    /// `reason` says what does not hold.
    #[error("rule {:?} is not armed, because the workspace is not valid: {reason}", .rule.as_str())]
    WorkspaceInvalid { rule: RuleName, reason: String },
    /// The rule does not resolve against the selectors and the presets of the workspace. Its error names the rule.
    #[error("{error}")]
    Unresolved { error: RuleError },
    /// A rule of this name is armed in the environment.
    /// Arming it again would reset it, and whoever wants that disarms it first.
    #[error("rule {:?} is armed already in environment {:?}", .rule.as_str(), .environment.as_str())]
    AlreadyArmed {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule has another revision in the environment than the change expects: someone changed its definition
    /// or its state there since. `revision` is the one it has now.
    #[error(
        "rule {:?} changed in environment {:?} since: it has revision {revision} now",
        .rule.as_str(),
        .environment.as_str()
    )]
    Changed {
        environment: EnvironmentName,
        rule: RuleName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why a host did not [disarm](crate::Host::disarm) a rule in an environment.
///
/// A rule that was not disarmed is as it was.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = DisarmRuleError::NotArmed {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     rule: "leader-flap".parse().expect("a rule name"),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"leader-flap\" is not armed in environment \"staging-proxy\""
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DisarmRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No rule of this name is armed in the environment: nobody armed it, someone disarmed it,
    /// or its lifetime ended.
    #[error("rule {:?} is not armed in environment {:?}", .rule.as_str(), .environment.as_str())]
    NotArmed {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule has another revision in the environment than the change expects: someone changed its definition
    /// or its state there since. `revision` is the one it has now.
    #[error(
        "rule {:?} changed in environment {:?} since: it has revision {revision} now",
        .rule.as_str(),
        .environment.as_str()
    )]
    Changed {
        environment: EnvironmentName,
        rule: RuleName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why a host did not [fire a rule by hand](crate::Host::fire).
///
/// | Variant | What is wrong |
/// |---|---|
/// | [`NotRunning`](Self::NotRunning) | No environment of that name runs. |
/// | [`NotArmed`](Self::NotArmed) | No rule of that name is armed in the environment. |
/// | [`NotFired`](Self::NotFired) | The lifetime of the rule has ended, or its budget has no firing free. |
/// | [`NeedsCall`](Self::NeedsCall) | The incident of the rule takes its subject from a call, and a firing by hand has none. |
/// | [`IncidentRejected`](Self::IncidentRejected) | The backend cannot apply the incident. |
///
/// A rule that did not fire spent nothing of its budget.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = FireRuleError::NotArmed {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     rule: "leader-flap".parse().expect("a rule name"),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"leader-flap\" is not armed in environment \"staging-proxy\""
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FireRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No rule of this name is armed in the environment, so it has nothing to fire.
    #[error("rule {:?} is not armed in environment {:?}", .rule.as_str(), .environment.as_str())]
    NotArmed {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule is armed, and fires no more: its lifetime has ended, or its budget has no firing free.
    #[error("rule {:?} fires no more in environment {:?}", .rule.as_str(), .environment.as_str())]
    NotFired {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The incident of the rule takes its subject from the call it fires on, and a firing by hand has no call.
    /// Its error names the rule.
    #[error("{error}")]
    NeedsCall { error: RuleError },
    /// The backend of the environment cannot apply the incident that the rule fired. Its error says why.
    #[error("rule {:?} did not fire: {error}", .rule.as_str())]
    IncidentRejected { rule: RuleName, error: IncidentRejection },
}

/// Why a host could not [read a sentence](crate::Host::read_sentence).
///
/// A sentence that means nothing is no error: what it could not read is part of the sentence.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = ReadSentenceError::StoreFailed {
///     reason: "the preset store could not be read: disk full".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the names of the workspace could not be read: the preset store could not be read: disk full"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadSentenceError {
    /// No environment of this name runs, so no traffic says which names a sentence may use.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// A store of the workspace could not be read, so the host does not know the names a sentence may use.
    /// `reason` says which store, and what it said.
    #[error("the names of the workspace could not be read: {reason}")]
    StoreFailed { reason: String },
}

/// Why a host did not list the [rules of an environment](crate::Host::rules_of).
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = EnvironmentRulesError::StoreFailed {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     reason: "the rule store could not be read: disk full".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the rules of environment \"staging-proxy\" were not read: the rule store could not be read: disk full"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentRulesError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The store of the rules of the workspace could not be read. `reason` says what it said.
    #[error("the rules of environment {:?} were not read: {reason}", .environment.as_str())]
    StoreFailed {
        environment: EnvironmentName,
        reason: String,
    },
}

/// Why a host did not [add a live rule](crate::Host::create_live_rule) to an environment,
/// disarmed or [armed](crate::Host::create_live_rule_armed).
///
/// A rule that was not added changed nothing: it is not armed either.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CreateLiveRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// Another rule has the name, and `holder` says which: a rule of the workspace when it is `None`,
    /// a live rule of the environment or a rule of that name that is armed there, or a rule that a scenario owns.
    /// Rule names are unique among all rules, so the new one cannot have it.
    #[error("{}", live_rule_taken(.environment, .rule, .holder.as_ref()))]
    NameTaken {
        environment: EnvironmentName,
        rule: RuleName,
        holder: Option<RuleHolder>,
    },
    /// A store of the workspace could not be read. `reason` says which store, and what it said.
    #[error("rule {:?} was not added: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The selectors or the presets of the workspace do not hold together, so no rule resolves against them.
    #[error("rule {:?} was not added, because the workspace is not valid: {reason}", .rule.as_str())]
    WorkspaceInvalid { rule: RuleName, reason: String },
    /// The rule does not resolve against the workspace. Its error names the rule.
    #[error("{error}")]
    Unresolved { error: RuleError },
}

/// Why a host did not [replace a live rule](crate::Host::update_live_rule) of an environment.
///
/// A rule that was not replaced changed nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpdateLiveRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name. A rule of the workspace changes in the workspace.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// A store of the workspace could not be read. `reason` says which store, and what it said.
    #[error("rule {:?} was not replaced: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The selectors or the presets of the workspace do not hold together, so no rule resolves against them.
    #[error("rule {:?} was not replaced, because the workspace is not valid: {reason}", .rule.as_str())]
    WorkspaceInvalid { rule: RuleName, reason: String },
    /// The rule does not resolve against the workspace. Its error names the rule.
    #[error("{error}")]
    Unresolved { error: RuleError },
    /// The live rule has another revision than the change expects: someone changed its definition or its state
    /// since. `revision` is the one it has now.
    #[error(
        "rule {:?} changed in environment {:?} since: it has revision {revision} now",
        .rule.as_str(),
        .environment.as_str()
    )]
    Changed {
        environment: EnvironmentName,
        rule: RuleName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why a host did not [remove a live rule](crate::Host::delete_live_rule) from an environment.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeleteLiveRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name. A rule of the workspace is deleted in the workspace.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The live rule has another revision than the change expects: someone changed its definition or its state
    /// since. `revision` is the one it has now.
    #[error(
        "rule {:?} changed in environment {:?} since: it has revision {revision} now",
        .rule.as_str(),
        .environment.as_str()
    )]
    Changed {
        environment: EnvironmentName,
        rule: RuleName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why a host did not [keep a live rule](crate::Host::keep) in the workspace.
///
/// A keep that failed changed nothing: the live rule is still there.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeepRuleError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// Another rule has the name: a live rule of another environment that runs, or a rule that a scenario owns.
    /// A rule of the workspace is in every environment, so it never stands next to a live rule of its name,
    /// and rule names are unique among all rules.
    #[error("rule {:?} was not kept: {}", .rule.as_str(), .holder.holds())]
    NameTaken { rule: RuleName, holder: RuleHolder },
    /// The workspace did not take the rule. Its error says why: a rule of the workspace has the name,
    /// the rule does not fit the workspace, or a store failed.
    #[error("rule {:?} was not kept: {error}", .rule.as_str())]
    NotKept { rule: RuleName, error: CreateError },
    /// The live rule has another revision than the change expects: someone changed its definition or its state
    /// since. `revision` is the one it has now.
    #[error(
        "rule {:?} changed in environment {:?} since: it has revision {revision} now",
        .rule.as_str(),
        .environment.as_str()
    )]
    Changed {
        environment: EnvironmentName,
        rule: RuleName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the host, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// What holds a rule of a name outside the workspace, so that no other rule can have that name.
///
/// Rule names are unique among all rules: those of the workspace, the live rules of every environment that runs,
/// and the rules that scenarios own.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let holder = RuleHolder::Scenario("leader-election".parse().expect("a scenario name"));
/// assert_eq!(holder.holds(), "scenario \"leader-election\" owns a rule of that name");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleHolder {
    /// A live rule of this environment has the name.
    Environment(EnvironmentName),
    /// A rule that this scenario of the workspace owns has the name.
    Scenario(ScenarioName),
}

impl RuleHolder {
    /// What the holder holds, as a sentence: `environment "canary-proxy" has a live rule of that name`.
    pub fn holds(&self) -> String {
        match self {
            RuleHolder::Environment(environment) => {
                format!("environment {:?} has a live rule of that name", environment.as_str())
            }
            RuleHolder::Scenario(scenario) => format!("scenario {:?} owns a rule of that name", scenario.as_str()),
        }
    }
}

/// The message of a live rule whose name is taken: by the workspace, by the environment, or by a scenario.
fn live_rule_taken(environment: &EnvironmentName, rule: &RuleName, holder: Option<&RuleHolder>) -> String {
    let not_added = format!(
        "rule {:?} was not added to environment {:?}",
        rule.as_str(),
        environment.as_str()
    );
    match holder {
        None => format!("{not_added}: the workspace has a rule of that name"),
        Some(RuleHolder::Environment(_)) => {
            format!(
                "environment {:?} has a rule {:?} already",
                environment.as_str(),
                rule.as_str()
            )
        }
        Some(scenario @ RuleHolder::Scenario(_)) => format!("{not_added}: {}", scenario.holds()),
    }
}

/// Why a host did not [create](crate::Host::create) a definition in the workspace.
///
/// A create that failed changed nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = CreateDefinitionError::NameTaken {
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     holder: RuleHolder::Environment("orders-sim".parse().expect("an environment name")),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" was not created: environment \"orders-sim\" has a live rule of that name",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CreateDefinitionError {
    /// Another rule has the name of the rule: a live rule of an environment that runs, or a rule that a scenario
    /// owns. A rule of the workspace is in every environment, so it never stands next to a live rule of its name,
    /// and rule names are unique among all rules.
    #[error("rule {:?} was not created: {}", .rule.as_str(), .holder.holds())]
    NameTaken { rule: RuleName, holder: RuleHolder },
    /// The workspace did not take the definition. Its error says why.
    #[error("{error}")]
    NotCreated { error: CreateError },
}

/// Why a host did not act on what someone did at a pad: see [`send_intent`](crate::Host::send_intent).
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = IntentError::UnknownLayout { layout: "order-desk".parse().expect("a layout name") };
/// assert_eq!(error.to_string(), "layout \"order-desk\" does not exist");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntentError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The pad belongs to a layout that the workspace does not have.
    #[error("layout {:?} does not exist", .layout.as_str())]
    UnknownLayout { layout: LayoutName },
    /// A store of the workspace could not be read. `reason` says which store, and what it said.
    #[error("the intent was not acted on: {reason}")]
    StoreFailed { reason: String },
    /// The host cannot act on the intent: the pad lies outside its layout, its rule cannot be made or armed,
    /// it has no rule to turn, the turn cannot change the rule, or the presence that holds the pad is not open.
    /// `reason` says which.
    #[error("the intent was refused: {reason}")]
    Refused { reason: String },
}

/// Why a host did not [close a session](crate::Host::close_session) of an environment.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_host::prelude::*;
///
/// let error = CloseSessionError::NotOpen {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     connection: ConnectionNumber::new(7),
/// };
/// assert_eq!(error.to_string(), "environment \"staging-proxy\" has no open session on connection #7");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CloseSessionError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No session of the environment is open on the connection: it closed already, or it never opened.
    #[error("environment {:?} has no open session on connection {connection}", .environment.as_str())]
    NotOpen {
        environment: EnvironmentName,
        connection: ConnectionNumber,
    },
}

/// Why a host did not [follow the calls](crate::Host::follow_calls) of an environment.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = FollowCallsError::NotRunning { environment: "staging-proxy".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"staging-proxy\" does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FollowCallsError {
    /// No environment of this name runs.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The store of the selectors could not be read. `reason` says what it said.
    #[error("the calls were not followed: {reason}")]
    StoreFailed { reason: String },
    /// The selector does not resolve against the selectors of the workspace: it names one that does not exist,
    /// or the selectors include each other in a circle. Its error says which.
    #[error("{error}")]
    Unresolved { error: SelectorSetError },
}
