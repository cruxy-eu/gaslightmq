//! Why GaslightMQ did not start in a test.

use std::path::PathBuf;

use gaslightmq_control_client::PrepareError;
use gaslightmq_control_server::ControlServerError;
use gaslightmq_host::{CreateDefinitionError, EnvironmentStartError};

/// Why GaslightMQ did not start in the process of a test: see
/// [`GaslightMqConfiguration::start`](crate::GaslightMqConfiguration::start).
///
/// Nothing of it runs then: whatever had started stopped again, and its ports are free.
///
/// ```
/// use gaslightmq_test::GaslightMqStartError;
///
/// let error = GaslightMqStartError::NoRandomness { reason: "no entropy source".to_owned() };
/// assert_eq!(error.to_string(), "the system gave no randomness for the ID of the run: no entropy source");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GaslightMqStartError {
    /// The test named no run, and the system gave no randomness to make one from. `reason` is what the system said.
    #[error("the system gave no randomness for the ID of the run: {reason}")]
    NoRandomness { reason: String },
    /// The workspace did not take the environment of the test: it holds one of the name, or a store failed.
    #[error("{error}")]
    EnvironmentNotCreated { error: CreateDefinitionError },
    /// The proxy did not start: it did not reach the cluster, or a broker of the cluster got no listener.
    #[error("{error}")]
    ProxyNotStarted { error: EnvironmentStartError },
    /// The control plane did not start: it cannot listen where its configuration says.
    #[error("{error}")]
    ControlPlaneNotStarted { error: ControlServerError },
    /// The files of the [workspace](crate::GaslightMqConfiguration::with_workspace) of the test do not read.
    /// `reason` names each file that does not, and the key that failed.
    #[error("the workspace in {} does not read: {reason}", .directory.display())]
    WorkspaceUnreadable { directory: PathBuf, reason: String },
    /// The workspace of the test did not take a definition of the files: it does not fit the others.
    #[error("{error}")]
    WorkspaceNotTaken { error: CreateDefinitionError },
    /// The proxy ran, and did not do what the configuration declared: a rule did not arm, a scenario did not activate,
    /// or the recording from the start did not start.
    #[error("{error}")]
    NotPrepared { error: PrepareError },
}
