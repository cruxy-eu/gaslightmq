//! The rules of an environment: their state, live rules, and arming, disarming, firing and keeping one.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Rule, RuleName, RuleState};
use serde::{Deserialize, Serialize};

use crate::{EnvironmentReadError, Operation};

/// A rule of an environment, and its state there.
///
/// An environment holds every rule of the workspace, and its own live rules.
/// A workspace rule has a state in each environment: arming it in one does not arm it in another.
/// A live rule belongs to one environment, and no workspace rule has its name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleStatus {
    /// The definition of the rule, without its UUID.
    pub rule: Rule,
    /// Whether the rule is a live rule: one that a pad or ⌘K made, and that only this environment holds.
    pub live: bool,
    /// Whether the rule is a draft, armed or disarmed in this environment.
    pub state: RuleState,
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

/// Add a live rule to an environment, disarmed.
///
/// A live rule stays in the memory of its environment, and never in the workspace, until someone
/// [keeps](KeepRule) it. Its name is a name that no rule of the workspace and no other live rule has.
/// The environment gives it its UUID, whatever UUID the request holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The rule, with its name.
    pub rule: Rule,
}

impl Operation for CreateLiveRule {
    type Answer = ();
    type Error = CreateLiveRuleError;
}

/// Replace the live rule that has the name of this one.
///
/// Everything but the name changes. Changing a rule that is armed resets none of its counters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The rule as it is to be, with the name of the one it replaces.
    pub rule: Rule,
}

impl Operation for UpdateLiveRule {
    type Answer = ();
    type Error = UpdateLiveRuleError;
}

/// Remove a live rule from its environment, armed or not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteLiveRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the live rule.
    pub rule: RuleName,
}

impl Operation for DeleteLiveRule {
    type Answer = ();
    type Error = DeleteLiveRuleError;
}

/// Save a live rule as a rule of the workspace.
///
/// The rule keeps its name and its UUID, and it stays in the state it has.
/// From then on it is a rule of the workspace, which every environment holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeepRule {
    /// The environment that holds the live rule.
    pub environment: EnvironmentName,
    /// The name of the live rule.
    pub rule: RuleName,
}

impl Operation for KeepRule {
    type Answer = ();
    type Error = KeepRuleError;
}

/// Arm a rule in an environment.
///
/// Arming resets the counters, the budget and the time window of the rule.
/// A rule that is armed is not armed again: disarm it first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the rule: a rule of the workspace, or a live rule of the environment.
    pub rule: RuleName,
}

impl Operation for ArmRule {
    type Answer = ();
    type Error = RuleCommandError;
}

/// Disarm a rule that is armed in an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisarmRule {
    /// The environment.
    pub environment: EnvironmentName,
    /// The name of the rule.
    pub rule: RuleName,
}

impl Operation for DisarmRule {
    type Answer = ();
    type Error = RuleCommandError;
}

/// Fire a rule that is armed in an environment by hand: skip its trigger once.
///
/// An incident fires at once. A lie applies to the next call that the rule matches.
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
    type Error = RuleCommandError;
}

/// Why an environment did not [add a live rule](CreateLiveRule).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = CreateLiveRuleError::NameTaken {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
/// };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" has a rule \"delay-1s-on-order-producers\" already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CreateLiveRuleError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// A rule of the workspace or a live rule of the environment has the name already.
    #[error("environment {:?} has a rule {:?} already", .environment.as_str(), .rule.as_str())]
    NameTaken {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The rule does not fit the workspace: it names a selector or a preset that does not exist, or it could never
    /// work. `reason` says what is wrong.
    #[error("rule {:?} does not fit the workspace: {reason}", .rule.as_str())]
    DoesNotFit { rule: RuleName, reason: String },
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
}

/// Why an environment did not [keep a live rule](KeepRule).
///
/// A keep that fails changed nothing: the live rule is still there.
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
    /// Someone created a rule with that name in the workspace since the live rule was made.
    #[error("the workspace has a rule {:?} already", .rule.as_str())]
    NameTaken { rule: RuleName },
    /// The rule does not fit the workspace any more. `reason` says what is wrong.
    #[error("rule {:?} does not fit the workspace: {reason}", .rule.as_str())]
    DoesNotFit { rule: RuleName, reason: String },
    /// A store could not keep the rule. `reason` is what the store said.
    #[error("rule {:?} was not kept: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
}

/// Why an environment did not [arm](ArmRule), [disarm](DisarmRule) or [fire](FireRule) a rule.
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
    /// its selector or its preset does not exist, or its incident needs a call to fire. `reason` says which.
    #[error("rule {:?} in environment {:?}: {reason}", .rule.as_str(), .environment.as_str())]
    Refused {
        environment: EnvironmentName,
        rule: RuleName,
        reason: String,
    },
    /// A store of the workspace could not read the rule, or what it names. `reason` says which store, and what it said.
    #[error("rule {:?} was not read: {reason}", .rule.as_str())]
    StoreFailed { rule: RuleName, reason: String },
}
