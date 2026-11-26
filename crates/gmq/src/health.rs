//! `gmq health`: whether every environment that a process was started with runs.

use gaslightmq_control_api::{CheckHealth, Health, RequestSurface};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration};

use crate::cli::ControlFlags;
use crate::error::HealthError;

/// Asks the control plane that the flags name whether every environment that its process was started with runs,
/// as `gmq` itself.
///
/// # Errors
///
/// [`HealthError`] when no control plane answered, or an environment of the start does not run.
pub(crate) async fn health(flags: &ControlFlags) -> Result<Health, HealthError> {
    let mut configuration = ControlClientConfiguration::new(flags.address());
    configuration.surface = RequestSurface::Cli;
    let client = ControlClient::new(configuration);
    client
        .send(CheckHealth {})
        .await
        .map_err(|error| HealthError::Unanswered { error })?
        .map_err(|error| HealthError::Unhealthy { error })
}
