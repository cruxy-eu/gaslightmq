//! The capture of an environment: starting and stopping it, and saving what it captured as a scenario.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Rule, RuleName};
use gaslightmq_scenario::{ScenarioName, ScenarioTitle, Step};
use serde::{Deserialize, Serialize};

use crate::rules::taken_by;
use crate::{DefinitionRef, Operation, Revised, RuleHolder};

/// Start a capture of an environment: from here on, it marks every arm and disarm of the live set, with its time.
///
/// What the active scenario arms and disarms is no part of it. A start has no inverse and no revision. The journal
/// of the environment records it as `capture-started`, so every client learns of it from the live stream.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let start = StartCapture { environment: "orders-sim".parse().expect("an environment name") };
/// let request = serde_json::to_value(OperationRequest::new(RequestSurface::Pad, start)).expect("a form");
/// assert_eq!(
///     request,
///     serde_json::json!({ "from": { "surface": "pad" }, "operation": "start-capture", "environment": "orders-sim" }),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartCapture {
    /// The environment, which runs one capture at a time.
    pub environment: EnvironmentName,
}

impl Operation for StartCapture {
    type Answer = ();
    type Error = StartCaptureError;
}

/// Stop the capture of an environment. It answers with what the capture captured: the steps of a Sequence, and the
/// live rules that they arm, as they were armed.
///
/// The process keeps what the capture captured until a capture of the environment starts again, so whoever wants it
/// [saves it as a scenario](SaveCapture). A stop has no inverse and no revision. The journal of the environment
/// records it as `capture-stopped`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopCapture {
    /// The environment.
    pub environment: EnvironmentName,
}

impl Operation for StopCapture {
    type Answer = CapturedSteps;
    type Error = StopCaptureError;
}

/// Save what the last capture of an environment captured as a Sequence scenario of the workspace, with the live
/// rules that it uses.
///
/// The scenario takes the seed that the environment arms a rule by hand with. Every live rule that a step arms goes
/// on as a rule of the workspace, as it was armed: a live rule of the environment is kept, and a rule that went is
/// created again. It answers with the revision of the scenario. A save has no inverse.
///
/// With `serde`, a save without a title leaves `title` out:
/// `{ "operation": "save-capture", "environment": "orders-sim", "scenario": "slow-then-refused" }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveCapture {
    /// The environment whose last capture is saved.
    pub environment: EnvironmentName,
    /// The name of the scenario, which no scenario of the workspace has.
    pub scenario: ScenarioName,
    /// What the scenario shows, in free text, or `None` for a scenario without a title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<ScenarioTitle>,
}

impl Operation for SaveCapture {
    type Answer = Revised;
    type Error = SaveCaptureError;
}

/// What a capture captured: the steps of a Sequence, and the live rules that they arm, as they were armed.
///
/// The first step is the live set as the capture found it, and every other step the live set after one change,
/// its rules with the most recently armed first. Each ends after the time until the next change, and the last one
/// after the time until the stop. A step names a rule of the workspace, or one of `rules`.
///
/// With `serde`, it is a map: `{ "steps": [{ "name": "step-1", "until": { "after": "2s" } }], "rules": [ … ] }`,
/// which leaves `rules` out when no step arms a live rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedSteps {
    /// The steps, in the order they ran.
    pub steps: Vec<Step>,
    /// The live rules that the steps arm, each as it was armed, in the order in which they first armed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,
}

/// Why an environment did not [start a capture](StartCapture).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StartCaptureError::AlreadyCapturing { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" captures already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StartCaptureError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The environment captures already. Whoever wants another capture stops this one first.
    #[error("environment {:?} captures already", .environment.as_str())]
    AlreadyCapturing { environment: EnvironmentName },
}

/// Why an environment did not [stop its capture](StopCapture).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StopCaptureError::NotCapturing { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" captures nothing");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StopCaptureError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The environment captures nothing: nobody started a capture, or someone stopped it.
    #[error("environment {:?} captures nothing", .environment.as_str())]
    NotCapturing { environment: EnvironmentName },
}

/// Why the control plane did not [save a capture](SaveCapture) as a scenario.
///
/// The save checks the name of the scenario before it keeps a rule. A live rule that it kept before it failed in
/// another way stays a rule of the workspace.
///
/// With `serde`, a live rule that changed since the capture armed it is
/// `{ "error": "rule-changed", "environment": "orders-sim", "rule": "slow-produce" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = SaveCaptureError::NothingCaptured { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" holds no capture to save");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SaveCaptureError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No capture of the environment stopped since it started, or since a capture started again.
    #[error("environment {:?} holds no capture to save", .environment.as_str())]
    NothingCaptured { environment: EnvironmentName },
    /// A live rule that the capture armed changed since, or went: the workspace would not get the rule that the
    /// capture armed. Whoever wants the capture saved turns the rule back, or deletes it, and saves again.
    #[error(
        "live rule {:?} of environment {:?} changed since the capture armed it",
        .rule.as_str(),
        .environment.as_str()
    )]
    RuleChanged {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// A definition has the name of the scenario or of a rule of the capture: a scenario or a rule of the
    /// workspace, a live rule of another environment, or a rule that a scenario owns.
    #[error("{name} was not saved: {}", taken(.name, .holder.as_ref()))]
    NameTaken {
        name: DefinitionRef,
        /// What holds a rule of the name outside the workspace, or `None` when the workspace has the name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        holder: Option<RuleHolder>,
    },
    /// The scenario does not fit the workspace: a rule of the workspace that a step arms went since the capture.
    /// `reason` says everything that would be wrong.
    #[error("{name} does not fit the workspace: {reason}")]
    DoesNotFit { name: DefinitionRef, reason: String },
    /// Someone changed the file where a definition would go since the process read it, and the process wrote
    /// nothing of it. A [reload](crate::ReloadWorkspace) takes the edit in. `reason` says which file changed, and how.
    #[error("{name} was not saved, because {reason}")]
    ChangedOutside { name: DefinitionRef, reason: String },
    /// A store could not read its definitions, or could not keep a definition. `reason` is what the store said.
    #[error("{name} was not saved: {reason}")]
    StoreFailed { name: DefinitionRef, reason: String },
}

/// Who has the name: the workspace, or what holds a rule outside it.
fn taken(name: &DefinitionRef, holder: Option<&RuleHolder>) -> String {
    match (name, holder) {
        (DefinitionRef::Rule(_), holder) => taken_by(holder),
        (_, _) => "the workspace has a definition of that name".to_owned(),
    }
}
