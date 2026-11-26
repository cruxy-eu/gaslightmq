//! Why a command of `gmq` failed.

use std::path::PathBuf;

use gaslightmq_control_api::{ReloadWorkspaceError, Unhealthy};
use gaslightmq_control_client::SendError;
use gaslightmq_control_server::ControlServerError;
use gaslightmq_environment::{EnvironmentName, ListenersError};
use gaslightmq_host::EnvironmentStartError;
use gaslightmq_workspace_fs::OpenError;

/// Why `gmq proxy` did not start, or did not wait for its stop.
///
/// Nothing of it runs then: whatever had started stopped again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProxyError {
    /// The bootstrap listener of the flags takes a port of the brokers.
    #[error("{error}")]
    ListenersMeet { error: ListenersError },
    /// The files of the workspace do not read. `error` holds every file that does not, which the command prints one
    /// per line.
    #[error("{}: {error}", .directory.display())]
    WorkspaceUnreadable { directory: PathBuf, error: OpenError },
    /// The workspace holds an environment with the name of the proxy of the flags, which the control plane would take
    /// for it.
    #[error(
        "the workspace holds an environment named {:?}: name the proxy of the flags another way with --name",
        .name.as_str()
    )]
    NameTaken { name: EnvironmentName },
    /// The environments of the workspace could not be read. `reason` is what the store said.
    #[error("the environments of the workspace could not be read: {reason}")]
    WorkspaceNotRead { reason: String },
    /// The system gave no randomness for the ID of the run. `reason` is what the system said.
    #[error("the system gave no randomness for the ID of the run: {reason}")]
    NoRandomness { reason: String },
    /// The proxy did not start: it did not reach the cluster, a listener is taken, or a broker of the cluster got no
    /// listener.
    #[error("{error}")]
    ProxyNotStarted { error: EnvironmentStartError },
    /// The control plane did not start: it cannot listen where the flags say.
    #[error("{error}")]
    ControlPlaneNotStarted { error: ControlServerError },
    /// The process cannot hear the signal that stops it. `reason` is what the system said.
    #[error("the process cannot wait for the signal that stops it: {reason}")]
    NoStopSignal { reason: String },
}

/// Why `gmq reload` reloaded nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReloadError {
    /// No control plane answered the reload: nothing listens at the address, or it did not take the request.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The control plane refused the reload, and the process holds what it held. `error` says why: the files do not
    /// read or do not fit each other, or a live rule stands in the way.
    #[error("{error}")]
    Refused { error: ReloadWorkspaceError },
}

/// Why `gmq health` found the process not healthy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HealthError {
    /// No control plane answered the check: nothing listens at the address, or it did not take the request.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// An environment that the process was started with does not run. `error` names each environment of the start,
    /// with whether it runs, which the command prints.
    #[error("{error}")]
    Unhealthy { error: Unhealthy },
}
