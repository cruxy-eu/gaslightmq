//! The environments of the workspace: which of them run, and starting and stopping one.

use gaslightmq_environment::{Environment, EnvironmentName};
use serde::{Deserialize, Serialize};

use crate::{GetDefinitionError, ListDefinitionsError, Operation};

/// An environment of the workspace, and whether the process runs it.
///
/// The environment shows its [secret references](gaslightmq_environment::SecretRef), and never a secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentStatus {
    /// The definition of the environment, without its UUID.
    pub environment: Environment,
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
