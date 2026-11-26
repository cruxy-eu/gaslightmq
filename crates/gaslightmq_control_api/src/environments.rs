//! The environments of the workspace: which of them run, and starting and stopping one.

use gaslightmq_environment::{Environment, EnvironmentName};
use serde::{Deserialize, Serialize};

use crate::{GetDefinitionError, ListDefinitionsError, Operation, Revision};

/// An environment of the workspace, with its revision beside it, whether a write of it would lose a comment, and
/// whether the process runs it.
///
/// The environment shows its [secret references](gaslightmq_environment::SecretRef), and never a secret.
/// With `serde`, an environment whose write loses nothing leaves `loses-comment` out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentStatus {
    /// The definition of the environment, as its file holds it.
    pub environment: Environment,
    /// The revision of the definition: the number of the change that last created or updated it.
    pub revision: Revision,
    /// Whether a write of the environment would lose a comment that someone wrote inside it, in its file.
    #[serde(rename = "loses-comment", default, skip_serializing_if = "std::ops::Not::not")]
    pub loses_comment: bool,
    /// Whether the process runs the environment now.
    pub running: bool,
}

/// List the environments of the workspace, each with whether it runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListEnvironments {}

impl Operation for ListEnvironments {
    type Answer = Vec<EnvironmentStatus>;
    type Error = ListDefinitionsError;
}

/// Get one environment of the workspace, with whether it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetEnvironment {
    /// The name of the environment.
    pub environment: EnvironmentName,
}

impl Operation for GetEnvironment {
    type Answer = EnvironmentStatus;
    type Error = GetDefinitionError;
}

/// Start an environment of the workspace: its listeners, its proxy and its broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartEnvironment {
    /// The name of the environment.
    pub environment: EnvironmentName,
}

impl Operation for StartEnvironment {
    type Answer = ();
    type Error = StartEnvironmentError;
}

/// Stop an environment that runs.
///
/// Its sessions end, and so does everything live in it: armed rules, live rules and counters.
/// An environment that starts again starts with nothing armed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopEnvironment {
    /// The name of the environment.
    pub environment: EnvironmentName,
}

impl Operation for StopEnvironment {
    type Answer = ();
    type Error = EnvironmentNotRunning;
}

/// Say whether every environment that the process was started with runs: the health of the process.
///
/// Whoever starts the process names the environments that it starts with, such as the one of the flags of
/// `gmq proxy`. The process is healthy while each of them runs. An environment that someone started later counts for
/// nothing, and one of the start that someone stopped since makes the process unhealthy, until someone starts it
/// again. A process that started with no environment is healthy.
///
/// It answers with every environment of the start, each with whether it runs, and fails with the same list when one
/// of them does not run. The check reads what the process holds in memory, so it fails in no other way.
///
/// With `serde`, it is `{ "operation": "check-health" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let request = OperationRequest::new(RequestSurface::Cli, CheckHealth {});
/// assert_eq!(
///     serde_json::to_value(request).expect("a request has a form"),
///     serde_json::json!({ "from": { "surface": "cli" }, "operation": "check-health" }),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckHealth {}

impl Operation for CheckHealth {
    type Answer = Health;
    type Error = Unhealthy;
}

/// The environments that the process was started with, each with whether it runs, in the order of their start.
///
/// The answer of a [check](CheckHealth) holds it when every one of them runs, and [`Unhealthy`] holds it otherwise.
///
/// With `serde`, it is a map of its field: `{ "environments": [{ "environment": "staging-proxy", "running": true }] }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let health = Health {
///     environments: vec![EnvironmentHealth { environment: "staging-proxy".parse().expect("an environment name"), running: true }],
/// };
/// assert!(health.is_healthy());
/// assert_eq!(
///     serde_json::to_value(&health).expect("an answer has a form"),
///     serde_json::json!({ "environments": [{ "environment": "staging-proxy", "running": true }] }),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    /// Every environment of the start, in the order of their start.
    pub environments: Vec<EnvironmentHealth>,
}

impl Health {
    /// Whether every environment of the start runs.
    pub fn is_healthy(&self) -> bool {
        self.environments.iter().all(|environment| environment.running)
    }

    /// The environments of the start that do not run.
    pub fn not_running(&self) -> impl Iterator<Item = &EnvironmentName> {
        self.environments
            .iter()
            .filter(|environment| !environment.running)
            .map(|environment| &environment.environment)
    }
}

/// One environment that the process was started with, and whether it runs now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentHealth {
    /// The name of the environment.
    pub environment: EnvironmentName,
    /// Whether it runs now.
    pub running: bool,
}

/// An environment that the process was started with does not run: the process is not healthy.
///
/// It holds every environment of the start, as the answer of a healthy process does, so a reader of either reads one
/// form.
///
/// With `serde`, it is the form of its [`Health`]: `{ "environments": [{ "environment": "orders-sim", "running": false }] }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let health = Health {
///     environments: vec![
///         EnvironmentHealth { environment: "staging-proxy".parse().expect("an environment name"), running: true },
///         EnvironmentHealth { environment: "orders-sim".parse().expect("an environment name"), running: false },
///     ],
/// };
/// let error = Unhealthy { health };
/// assert_eq!(error.to_string(), "the process started with environment \"orders-sim\", which does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("the process started with {}", stopped(.health))]
#[serde(transparent)]
pub struct Unhealthy {
    /// Every environment of the start, with whether it runs. One of them at least does not.
    pub health: Health,
}

/// The environments of the start that do not run, as the rest of a sentence:
/// `environments "orders-sim" and "payments", which do not run`.
fn stopped(health: &Health) -> String {
    let names: Vec<String> = health
        .not_running()
        .map(|name| format!("{:?}", name.as_str()))
        .collect();
    match names.as_slice() {
        [] => "every environment running".to_owned(),
        [one] => format!("environment {one}, which does not run"),
        [first @ .., last] => format!("environments {} and {last}, which do not run", first.join(", ")),
    }
}

/// Why the control plane did not [start](StartEnvironment) an environment.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StartEnvironmentError::Failed {
///     environment: "orders-sim".parse().expect("an environment name"),
///     reason: "0.0.0.0:29092 is taken by environment \"staging-proxy\"".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "environment \"orders-sim\" did not start: 0.0.0.0:29092 is taken by environment \"staging-proxy\"",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StartEnvironmentError {
    /// No environment of the workspace has the name.
    #[error("environment {:?} does not exist", .environment.as_str())]
    NotFound { environment: EnvironmentName },
    /// The environment runs already.
    #[error("environment {:?} runs already", .environment.as_str())]
    Running { environment: EnvironmentName },
    /// The environment did not come up: a listener it claims is taken, or its backend did not start.
    /// `reason` says what stood in the way.
    #[error("environment {:?} did not start: {reason}", .environment.as_str())]
    Failed {
        environment: EnvironmentName,
        reason: String,
    },
}

/// The environment that a command names does not run, so nothing live in it can be asked or changed.
///
/// Every command on something live names its environment.
/// A name that no environment has is a name of no environment that runs, so it gets this error too.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = EnvironmentNotRunning { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("environment {:?} does not run", .environment.as_str())]
#[serde(deny_unknown_fields)]
pub struct EnvironmentNotRunning {
    /// The name that the command gave.
    pub environment: EnvironmentName,
}

/// Why an environment did not answer a question that reads the workspace too:
/// [the rules of the environment](crate::ListEnvironmentRules), or [a sentence](crate::ParseSentence).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = EnvironmentReadError::StoreFailed {
///     environment: "orders-sim".parse().expect("an environment name"),
///     reason: "the rule store could not be read: permission denied".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "environment \"orders-sim\" did not answer: the rule store could not be read: permission denied",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EnvironmentReadError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// A store of the workspace could not read its definitions. `reason` says which store, and what it said.
    #[error("environment {:?} did not answer: {reason}", .environment.as_str())]
    StoreFailed {
        environment: EnvironmentName,
        reason: String,
    },
}
