//! Why GaslightMQ did not start in a container.

use std::path::PathBuf;

use gaslightmq_control_client::PrepareError;

use crate::image::{GaslightMqImage, GaslightMqImageError};

/// Why GaslightMQ did not start in a container next to a test: see
/// [`GaslightMqContainerConfiguration::start`](crate::GaslightMqContainerConfiguration::start).
///
/// Nothing of it runs then: a container that had started is gone again.
///
/// ```
/// use gaslightmq_testcontainers::{GaslightMqContainerStartError, GaslightMqImage};
///
/// let error = GaslightMqContainerStartError::ImageMissing {
///     image: GaslightMqImage::version("latest")?,
///     reason: "manifest unknown".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the image ghcr.io/cruxy-eu/gaslightmq:latest is not on the machine, and could not be pulled: manifest unknown"
/// );
/// # Ok::<(), gaslightmq_testcontainers::GaslightMqImageError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GaslightMqContainerStartError {
    /// The environment variable [`GASLIGHTMQ_IMAGE`](GaslightMqImage::VARIABLE) names no image. `error` says why.
    #[error("environment variable {}: {error}", GaslightMqImage::VARIABLE)]
    InvalidImage { error: GaslightMqImageError },
    /// The machine does not hold the image, and could not pull it. `reason` is what the registry or the machine said.
    #[error("the image {image} is not on the machine, and could not be pulled: {reason}")]
    ImageMissing { image: GaslightMqImage, reason: String },
    /// The workspace of the test could not be read, so nothing was copied into a container. `reason` says why.
    #[error("the workspace in {} cannot be read: {reason}", .directory.display())]
    WorkspaceUnreadable { directory: PathBuf, reason: String },
    /// The container never ran: the machine runs no containers, or it refused this one, so whoever runs the test
    /// sets the machine up. `reason` says why.
    #[error("the container of GaslightMQ did not start: {reason}")]
    ContainerNotStarted { reason: String },
    /// The container ran, and GaslightMQ in it never said that it is ready within the
    /// [start timeout](crate::GaslightMqContainerConfiguration::start_timeout): the process ended, it did not get
    /// where its brokers are published, or its environment does not run, so the configuration or the cluster stands
    /// in the way. `reason` says which, and `log` holds the last lines that the process wrote, when it wrote any.
    #[error("GaslightMQ did not get ready in its container: {reason}{}", end_of(.log))]
    NotReady { reason: String, log: String },
    /// The process got ready, and did not do what the configuration declared: a rule did not arm, a scenario did not
    /// activate, or the recording from the start did not start. The container is gone again.
    #[error("{error}")]
    NotPrepared { error: PrepareError },
}

/// What the message of a process that did not get ready adds for `log`: nothing when the process wrote nothing.
fn end_of(log: &str) -> String {
    if log.is_empty() {
        String::new()
    } else {
        format!("\nthe end of what it wrote:\n{log}")
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_process_that_did_not_get_ready_shows_what_it_wrote_when_it_wrote_anything() {
        let silent = GaslightMqContainerStartError::NotReady {
            reason: "it did not say that it is ready within 60000 ms".to_owned(),
            log: String::new(),
        };
        let logged = GaslightMqContainerStartError::NotReady {
            reason: "the process ended".to_owned(),
            log: "the proxy did not reach the cluster".to_owned(),
        };

        assert_eq!(
            silent.to_string(),
            "GaslightMQ did not get ready in its container: it did not say that it is ready within 60000 ms"
        );
        assert_eq!(
            logged.to_string(),
            "GaslightMQ did not get ready in its container: the process ended\n\
             the end of what it wrote:\nthe proxy did not reach the cluster"
        );
        assert!(logged.source().is_none());
    }
}
