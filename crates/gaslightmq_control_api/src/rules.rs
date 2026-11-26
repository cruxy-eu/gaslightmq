//! The rules of an environment: their state, live rules, and arming, disarming, firing and keeping one.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Rule, RuleName, RuleState};
use gaslightmq_scenario::ScenarioName;
use serde::{Deserialize, Serialize};

use crate::{EnvironmentReadError, Expects, Operation, Revised, Revision, RunId};

/// A rule of an environment, with its state and its revision there.
///
/// An environment holds every rule of the workspace, and its own live rules.
/// A workspace rule has a state in each environment: arming it in one does not arm it in another.
/// A live rule belongs to one environment, and no workspace rule has its name.
///
/// A rule in an environment has one revision, for its definition and its state together: the number of the last
/// change of either. Someone who arms a rule of the workspace after another client changed its definition
/// therefore arms nothing they have not seen.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleStatus {
    /// The definition of the rule, as a file holds it.
    pub rule: Rule,
    /// Whether the rule is a live rule: one that a pad or ⌘K made, and that only this environment holds.
    pub live: bool,
    /// Whether the rule is a draft, armed or disarmed in this environment.
    pub state: RuleState,
    /// The revision of the rule in this environment: the later of the last change of its definition,
    /// and the last change of its state here.
    pub revision: Revision,
}

/// List the rules of an environment, each with its state there: the rules of the workspace, and the live rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListEnvironmentRules {
    /// The environment.
    pub environment: EnvironmentName,
}

impl Operation for ListEnvironmentRules {
    type Answer = Vec<RuleStatus>;
    type Error = EnvironmentReadError;
}

/// Add a live rule to an environment, disarmed, or armed when the operation says so.
///
/// A live rule stays in the memory of its environment, and never in the workspace, until someone
/// [keeps](KeepRule) it. Its name is a name that no rule of the workspace and no other live rule has.
///
/// One gesture is one operation, so a ⌘K sentence creates its live rule armed.
/// The operation applies as a whole: a rule that does not arm is no live rule either.
///
/// With `serde`, a rule that is created disarmed leaves `armed` out.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::{PresetName, Rule};
/// use gaslightmq_control_api::selector::SelectorName;
///
/// // Enter in ⌘K: the rule of the sentence is a live rule of the environment shown, and it is armed.
/// let rule = Rule::new(
///     "delay-1s-on-order-producers".parse().expect("a rule name"),
///     "order-producers".parse::<SelectorName>().expect("a selector name"),
///     "delay-1s".parse::<PresetName>().expect("a preset name"),
/// );
/// let create = CreateLiveRule { environment: "orders-sim".parse().expect("an environment name"), rule, armed: true };
/// let request = OperationRequest::new(RequestSurface::QuickInject, create);
/// let request = serde_json::to_value(request).expect("a request has a form");
/// assert_eq!(
///     request,
///     serde_json::json!({
///         "from": { "surface": "quick-inject" },
///         "operation": "create-live-rule",
///         "environment": "orders-sim",
///         "rule": { "name": "delay-1s-on-order-producers", "selector": "order-producers", "action": "delay-1s" },
///         "armed": true,
///     }),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The rule, with its name.
    pub rule: Rule,
    /// Whether the rule is armed from the moment it is there.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub armed: bool,
}

impl Operation for CreateLiveRule {
    type Answer = Revised;
    type Error = CreateLiveRuleError;
}

/// Replace the live rule that has the name of this one.
///
/// Everything but the name changes. Changing a rule that is armed resets none of its counters.
/// With `serde`, an update that expects nothing leaves `expects` out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The rule as it is to be, with the name of the one it replaces.
    pub rule: Rule,
    /// What the update [expects](Expects) of the live rule it replaces, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for UpdateLiveRule {
    type Answer = Revised;
    type Error = UpdateLiveRuleError;
}

/// Remove a live rule from its environment, armed or not.
///
/// The delete answers with the revision of the delete, which nothing has afterwards.
/// With `serde`, a delete that expects nothing leaves `expects` out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the live rule.
    pub rule: RuleName,
    /// What the delete [expects](Expects) of the live rule it removes, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for DeleteLiveRule {
    type Answer = Revised;
    type Error = DeleteLiveRuleError;
}

/// Save a live rule as a rule of the workspace.
///
/// The rule keeps its name, and it stays in the state it has.
/// From then on it is a rule of the workspace, which every environment holds.
///
/// A keep expects the live rule as its sender saw it: a turn of an encoder by someone else in between is a change,
/// so the workspace never gets a rule that nobody looked at.
/// The keep is one change, and it answers with the revision that the rule has from then on,
/// in the workspace and in the environment.
/// With `serde`, a keep that expects nothing leaves `expects` out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeepRule {
    /// The environment that holds the live rule.
    pub environment: EnvironmentName,
    /// The name of the live rule.
    pub rule: RuleName,
    /// What the keep [expects](Expects) of the live rule it saves, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for KeepRule {
    type Answer = Revised;
    type Error = KeepRuleError;
}

/// Arm a rule in an environment.
///
/// Arming resets the counters, the budget and the time window of the rule.
/// A rule that is armed is not armed again: disarm it first.
///
/// An arm expects the rule in its environment: its definition and its state together.
/// It answers with the revision that the rule has there from then on.
/// With `serde`, an arm that expects nothing leaves `expects` out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the rule: a rule of the workspace, or a live rule of the environment.
    pub rule: RuleName,
    /// What the arm [expects](Expects) of the rule in the environment, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for ArmRule {
    type Answer = Revised;
    type Error = RuleCommandError;
}

/// Disarm a rule that is armed in an environment.
///
/// A disarm expects the rule in its environment: its definition and its state together.
/// It answers with the revision that the rule has there from then on.
/// With `serde`, a disarm that expects nothing leaves `expects` out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisarmRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the rule.
    pub rule: RuleName,
    /// What the disarm [expects](Expects) of the rule in the environment, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for DisarmRule {
    type Answer = Revised;
    type Error = RuleCommandError;
}

/// Fire a rule that is armed in an environment by hand: skip its trigger once.
///
/// An incident fires at once. A lie applies to the next call that the rule matches.
/// A firing is what happens, and no change of what someone set: it expects nothing, and it leaves no revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FireRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the rule.
    pub rule: RuleName,
}

impl Operation for FireRule {
    type Answer = ();
    type Error = FireRuleError;
}

/// Why an environment did not [add a live rule](CreateLiveRule).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = CreateLiveRuleError::NameTaken {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
///     holder: Some(RuleHolder::Environment("orders-sim".parse().expect("an environment name"))),
/// };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" has a rule \"delay-1s-on-order-producers\" already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CreateLiveRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// Another rule has the name, and `holder` says which: a rule of the workspace when it is `None`,
    /// a live rule of the environment or a rule of that name that is armed there, or a rule that a scenario owns.
    /// Rule names are unique among all rules, so the new one cannot have it.
    #[error("{}", live_rule_taken(.environment, .rule, .holder.as_ref()))]
    NameTaken {
        environment: EnvironmentName,
        rule: RuleName,
        /// What holds a rule of the name outside the workspace, or `None` when the workspace has a rule of the name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        holder: Option<RuleHolder>,
    },
    /// The rule does not fit the workspace: it names a selector or a preset that does not exist, or it could never
    /// work. `reason` says what is wrong.
    #[error("rule {:?} does not fit the workspace: {reason}", .rule.as_str())]
    DoesNotFit { rule: RuleName, reason: String },
    /// A store of the workspace could not read what the rule is checked against. `reason` says which store, and what
    /// it said.
    #[error("rule {:?} was not checked: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
}

/// Why an environment did not [replace a live rule](UpdateLiveRule).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum UpdateLiveRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name. A rule of the workspace changes in the workspace.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule does not fit the workspace: it names a selector or a preset that does not exist, or it could never
    /// work. `reason` says what is wrong.
    #[error("rule {:?} does not fit the workspace: {reason}", .rule.as_str())]
    DoesNotFit { rule: RuleName, reason: String },
    /// A store of the workspace could not read what the rule is checked against. `reason` says which store, and what
    /// it said.
    #[error("rule {:?} was not checked: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The change expects another revision than the rule has in the environment: someone changed its definition
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
    /// The change expects a revision of another run than the run of the process, which is `run`:
    /// the process started again since the sender read.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why an environment did not [remove a live rule](DeleteLiveRule).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeleteLiveRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name. A rule of the workspace is deleted in the workspace.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The change expects another revision than the rule has in the environment: someone changed its definition
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
    /// The change expects a revision of another run than the run of the process, which is `run`:
    /// the process started again since the sender read.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why an environment did not [keep a live rule](KeepRule).
///
/// A keep that fails changed nothing: the live rule is still there.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = KeepRuleError::NameTaken {
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     holder: Some(RuleHolder::Environment("canary-proxy".parse().expect("an environment name"))),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" was not kept: environment \"canary-proxy\" has a live rule of that name",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum KeepRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No live rule of the environment has the name.
    #[error("environment {:?} has no live rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// Another rule has the name: a rule of the workspace, a live rule of another environment, or a rule that a
    /// scenario owns. A rule of the workspace is in every environment, so it never stands next to a live rule of its
    /// name: whoever wants the rule in the workspace deletes that live rule first.
    #[error("rule {:?} was not kept: {}", .rule.as_str(), taken_by(.holder.as_ref()))]
    NameTaken {
        rule: RuleName,
        /// What holds a rule of the name outside the workspace, or `None` when the workspace has a rule of the name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        holder: Option<RuleHolder>,
    },
    /// The rule does not fit the workspace any more. `reason` says what is wrong.
    #[error("rule {:?} does not fit the workspace: {reason}", .rule.as_str())]
    DoesNotFit { rule: RuleName, reason: String },
    /// A store could not keep the rule. `reason` is what the store said.
    #[error("rule {:?} was not kept: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The change expects another revision than the rule has in the environment: someone changed its definition
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
    /// The change expects a revision of another run than the run of the process, which is `run`:
    /// the process started again since the sender read.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why an environment did not [arm](ArmRule) or [disarm](DisarmRule) a rule.
///
/// With `serde`, a rule that changed since is
/// `{ "error": "changed", "environment": "orders-sim", "rule": "slow-order-producers", "revision": 9 }`,
/// and a change that expects a revision of another run is
/// `{ "error": "other-run", "run": "4be0643f1d98573b97cdca98a65347dd" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = RuleCommandError::Refused {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     reason: "an armed rule is not armed again".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" in environment \"orders-sim\": an armed rule is not armed again",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RuleCommandError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// Neither the workspace nor the environment has a rule of that name.
    #[error("environment {:?} has no rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule cannot do this from where it stands: it is armed already, it is not armed,
    /// or its selector or its preset does not exist. `reason` says which.
    #[error("rule {:?} in environment {:?}: {reason}", .rule.as_str(), .environment.as_str())]
    Refused {
        environment: EnvironmentName,
        rule: RuleName,
        reason: String,
    },
    /// A store of the workspace could not read the rule, or what it names. `reason` says which store, and what it said.
    #[error("rule {:?} was not read: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
    /// The change expects another revision than the rule has in the environment: someone changed its definition
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
    /// The change expects a revision of another run than the run of the process, which is `run`:
    /// the process started again since the sender read.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
}

/// Why an environment did not [fire](FireRule) a rule by hand.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = FireRuleError::Refused {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     reason: "it is not armed".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" in environment \"orders-sim\": it is not armed",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FireRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// Neither the workspace nor the environment has a rule of that name.
    #[error("environment {:?} has no rule {:?}", .environment.as_str(), .rule.as_str())]
    NotFound {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule cannot fire from where it stands: it is not armed, it fires no more,
    /// or its incident needs a call to fire. `reason` says which.
    #[error("rule {:?} in environment {:?}: {reason}", .rule.as_str(), .environment.as_str())]
    Refused {
        environment: EnvironmentName,
        rule: RuleName,
        reason: String,
    },
    /// A store of the workspace could not read the rule. `reason` says which store, and what it said.
    #[error("rule {:?} was not read: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
}

/// What holds a rule of a name outside the workspace, so that no other rule can have that name.
///
/// Rule names are unique among all rules: those of the workspace, the live rules of every environment that runs,
/// and the rules that scenarios own.
///
/// With `serde`, it is a map with one entry: `{ "environment": "canary-proxy" }` or
/// `{ "scenario": "leader-election" }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuleHolder {
    /// A live rule of this environment has the name.
    Environment(EnvironmentName),
    /// A rule that this scenario of the workspace owns has the name.
    Scenario(ScenarioName),
}

/// Who has the name of a rule: a live rule of an environment, a rule of a scenario, or the workspace.
pub(crate) fn taken_by(holder: Option<&RuleHolder>) -> String {
    match holder {
        Some(RuleHolder::Environment(environment)) => {
            format!("environment {:?} has a live rule of that name", environment.as_str())
        }
        Some(RuleHolder::Scenario(scenario)) => format!("scenario {:?} owns a rule of that name", scenario.as_str()),
        None => "the workspace has a rule of that name".to_owned(),
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
        Some(RuleHolder::Environment(_)) => {
            format!(
                "environment {:?} has a rule {:?} already",
                environment.as_str(),
                rule.as_str()
            )
        }
        other => format!("{not_added}: {}", taken_by(other)),
    }
}
