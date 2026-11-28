//! The scenario of an environment: activating, deactivating and advancing it, and where it stands.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_incident::IncidentKind;
use gaslightmq_scenario::{ScenarioName, ScenarioPosition};
use serde::{Deserialize, Serialize};

use crate::{Expects, Operation, Revised, Revision, RunId};

/// The scenario of an environment: where the active one stands, and the revision of the scenario there.
///
/// At most one scenario is active in an environment. The revision is the one of the last change of the scenario
/// there: the start of the environment, an activation, a deactivation, or a step that advanced. A change of the
/// scenario [expects](Expects) it.
///
/// With `serde`, a scenario that is not active leaves `active` out:
/// `{ "active": { "scenario": "leader-churn", "step": "warm-up" }, "revision": 12 }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioStatus {
    /// The scenario that is active, and the step that runs. `None` while no scenario is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<ScenarioPosition>,
    /// The revision of the scenario of the environment.
    pub revision: Revision,
}

/// Activate a scenario of the workspace in an environment.
///
/// It arms the rules of the scenario, and those of its first step when it is a Sequence, below the rules that someone
/// armed by hand. The backend of the environment says first whether it supports the incidents that the scenario
/// fires. At most one scenario is active: whoever wants another deactivates this one first.
///
/// An activation expects the scenario of the environment as its sender saw it, and its inverse is a
/// [deactivation](DeactivateScenario). It answers with the revision of the scenario of the environment.
/// With `serde`, an activation that expects nothing leaves `expects` out.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let activate = ActivateScenario {
///     environment: "orders-sim".parse().expect("an environment name"),
///     scenario: "leader-election".parse().expect("a scenario name"),
///     expects: None,
/// };
/// let request = serde_json::to_value(OperationRequest::new(RequestSurface::Test, activate)).expect("a form");
/// assert_eq!(
///     request,
///     serde_json::json!({
///         "from": { "surface": "test" },
///         "operation": "activate-scenario",
///         "environment": "orders-sim",
///         "scenario": "leader-election",
///     }),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateScenario {
    /// The environment.
    pub environment: EnvironmentName,
    /// The scenario of the workspace.
    pub scenario: ScenarioName,
    /// What the activation [expects](Expects) of the scenario of the environment, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for ActivateScenario {
    type Answer = Revised;
    type Error = ActivateScenarioError;
}

/// Deactivate the scenario of an environment: every rule that it armed disarms, and the rules that someone armed by
/// hand stay.
///
/// Its inverse is the [activation](ActivateScenario) of the scenario, which starts it again at its first step.
/// It answers with the revision of the scenario of the environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeactivateScenario {
    /// The environment.
    pub environment: EnvironmentName,
    /// What the deactivation [expects](Expects) of the scenario of the environment, or `None` when the last change
    /// wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for DeactivateScenario {
    type Answer = Revised;
    type Error = DeactivateScenarioError;
}

/// End the step of the scenario of an environment, whatever its condition says, and start the next one.
///
/// A step expects the scenario as its sender saw it: two clients that both step from the same step advance the
/// scenario once, and the second one gets [`StepScenarioError::Changed`]. A step has no inverse.
/// It answers with the revision of the scenario of the environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepScenario {
    /// The environment.
    pub environment: EnvironmentName,
    /// What the step [expects](Expects) of the scenario of the environment, or `None` when the last change wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expects: Option<Expects>,
}

impl Operation for StepScenario {
    type Answer = Revised;
    type Error = StepScenarioError;
}

/// Why an environment did not [activate a scenario](ActivateScenario). An activation that fails changed nothing.
///
/// With `serde`, a backend that cannot run the scenario is
/// `{ "error": "unsupported", "environment": "staging-proxy", "scenario": "leader-election", "incidents": ["leader-moves"] }`.
///
/// ```
/// use gaslightmq_control_api::incident::IncidentKind;
/// use gaslightmq_control_api::prelude::*;
///
/// let error = ActivateScenarioError::Unsupported {
///     environment: "staging-proxy".parse().expect("an environment name"),
///     scenario: "leader-election".parse().expect("a scenario name"),
///     incidents: vec![IncidentKind::LeaderMoves],
/// };
/// assert_eq!(
///     error.to_string(),
///     "scenario \"leader-election\" does not run in environment \"staging-proxy\": \
///      its backend can neither apply nor emulate leader-moves"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ActivateScenarioError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The workspace has no scenario of the name.
    #[error("scenario {:?} does not exist", .scenario.as_str())]
    NotFound { scenario: ScenarioName },
    /// The scenario does not resolve against the workspace: a rule that it names is not there, a rule or the
    /// selector of a step does not resolve, or the selectors and the presets do not hold together.
    /// `reason` says what is wrong.
    #[error("scenario {:?} does not fit the workspace: {reason}", .scenario.as_str())]
    DoesNotFit { scenario: ScenarioName, reason: String },
    /// A store could not read its definitions. `reason` is what the store said.
    #[error("scenario {:?} was not activated: {reason}", .scenario.as_str())]
    StoreFailed { scenario: ScenarioName, reason: String },
    /// The change expects another revision than the scenario of the environment has: someone activated,
    /// deactivated or advanced it since. `revision` is the one it has now.
    #[error("the scenario of environment {:?} changed since: it has revision {revision} now", .environment.as_str())]
    Changed {
        environment: EnvironmentName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the process, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
    /// A scenario is active in the environment, and at most one is. Deactivate it first.
    #[error("environment {:?} runs scenario {:?} already", .environment.as_str(), .scenario.as_str())]
    AlreadyActive {
        environment: EnvironmentName,
        scenario: ScenarioName,
    },
    /// The backend of the environment can neither apply nor emulate these incidents, which the rules of the scenario
    /// fire or have as consequences, in the order of their kinds.
    #[error(
        "scenario {:?} does not run in environment {:?}: its backend can neither apply nor emulate {}",
        .scenario.as_str(),
        .environment.as_str(),
        listed(.incidents)
    )]
    Unsupported {
        environment: EnvironmentName,
        scenario: ScenarioName,
        incidents: Vec<IncidentKind>,
    },
}

/// Why an environment did not [deactivate its scenario](DeactivateScenario). It changed nothing.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = DeactivateScenarioError::NotActive { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" runs no scenario");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeactivateScenarioError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The change expects another revision than the scenario of the environment has. `revision` is the one it has now.
    #[error("the scenario of environment {:?} changed since: it has revision {revision} now", .environment.as_str())]
    Changed {
        environment: EnvironmentName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the process, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
    /// No scenario is active in the environment.
    #[error("environment {:?} runs no scenario", .environment.as_str())]
    NotActive { environment: EnvironmentName },
}

/// Why an environment did not [advance its scenario](StepScenario). It changed nothing.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StepScenarioError::NoStep {
///     environment: "orders-sim".parse().expect("an environment name"),
///     scenario: "steady-jitter".parse().expect("a scenario name"),
/// };
/// assert_eq!(error.to_string(), "scenario \"steady-jitter\" runs no step in environment \"orders-sim\" that could end");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StepScenarioError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The change expects another revision than the scenario of the environment has: someone, or the condition of
    /// the step, advanced it since. `revision` is the one it has now.
    #[error("the scenario of environment {:?} changed since: it has revision {revision} now", .environment.as_str())]
    Changed {
        environment: EnvironmentName,
        revision: Revision,
    },
    /// The change expects a revision of another run than the run of the process, which is `run`.
    #[error("the change expects a revision of another run than {run}, which is the run of the process")]
    OtherRun { run: RunId },
    /// No scenario is active in the environment.
    #[error("environment {:?} runs no scenario", .environment.as_str())]
    NotActive { environment: EnvironmentName },
    /// The scenario runs no step: it is no Sequence, or it ran its last step already.
    #[error(
        "scenario {:?} runs no step in environment {:?} that could end",
        .scenario.as_str(),
        .environment.as_str()
    )]
    NoStep {
        environment: EnvironmentName,
        scenario: ScenarioName,
    },
}

/// The kinds as a message lists them: `leader-moves, coordinator-moves`.
fn listed(incidents: &[IncidentKind]) -> String {
    let kinds: Vec<String> = incidents.iter().map(ToString::to_string).collect();
    kinds.join(", ")
}
