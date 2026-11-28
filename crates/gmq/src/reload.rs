//! `gmq reload`: a process that runs reads the files of its workspace again.

use gaslightmq_control_api::{ReloadWorkspace, Reloaded, RequestSurface};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, OperationError};

use crate::cli::ReloadFlags;
use crate::error::ReloadError;

/// Asks the control plane that the flags name to reload the workspace, as `gmq` itself, and to let go of the edits
/// that wait for a save when the flags say so.
///
/// # Errors
///
/// [`ReloadError`] when no control plane answered, or it refused the reload.
pub(crate) async fn reload(flags: &ReloadFlags) -> Result<Reloaded, ReloadError> {
    let mut configuration = ControlClientConfiguration::new(flags.control.address());
    configuration.surface = RequestSurface::Cli;
    let client = ControlClient::new(configuration);
    client
        .send(ReloadWorkspace {
            discard_unsaved: flags.discard_unsaved,
        })
        .await
        .map_err(|error| match error {
            OperationError::Unanswered { error } => ReloadError::Unanswered { error },
            OperationError::Refused { error } => ReloadError::Refused { error },
        })
}
