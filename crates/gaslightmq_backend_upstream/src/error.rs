//! The one error of the Upstream backend.

use std::fmt;

use gaslightmq_environment::SaslMechanism;

/// Why an [`UpstreamBackend`](crate::UpstreamBackend) does not start.
///
/// A backend starts once it knows the brokers of its cluster.
/// Every message is complete, so whoever starts a proxy shows one line.
///
/// A backend that runs returns no error of its own.
/// A call that fails ends with an answer of the [contract](gaslightmq_backend::BackendAnswer),
/// a listener that clients cannot be sent to is [rejected](gaslightmq_backend::ListenerRejection) as the contract says,
/// and a later question for the brokers that fails changes nothing.
///
/// ```
/// use gaslightmq_backend_upstream::prelude::*;
///
/// let error = UpstreamBackendError::ClusterUndiscovered {
///     reason: "kafka-1.staging:9092 did not answer within 30s".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the cluster did not name its brokers: kafka-1.staging:9092 did not answer within 30s"
/// );
/// ```
///
/// The message is complete, so no variant has a source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum UpstreamBackendError {
    /// The environment asks for encrypted connections to the cluster, and the backend cannot make them yet.
    #[error("the environment asks for TLS to its cluster, and a proxy cannot connect to a cluster over TLS yet")]
    UnsupportedTls,
    /// The environment asks the backend to log in to the cluster, and the backend cannot do that yet.
    #[error("the environment asks for a {mechanism} login to its cluster, and a proxy cannot log in to a cluster yet")]
    UnsupportedSasl { mechanism: SaslMechanism },
    /// The cluster did not say which brokers it has.
    /// `reason` names the server that was asked, and says what stood in the way:
    /// no server took a connection, the server did not answer in time, or its answer names no broker.
    #[error("the cluster did not name its brokers: {reason}")]
    ClusterUndiscovered { reason: String },
}

impl UpstreamBackendError {
    /// The cluster did not say which brokers it has, for `reason`.
    pub(crate) fn undiscovered(reason: impl fmt::Display) -> Self {
        Self::ClusterUndiscovered {
            reason: reason.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn every_error_says_everything_in_one_message() {
        let errors = [
            (
                UpstreamBackendError::UnsupportedTls,
                "the environment asks for TLS to its cluster, and a proxy cannot connect to a cluster over TLS yet",
            ),
            (
                UpstreamBackendError::UnsupportedSasl {
                    mechanism: SaslMechanism::Plain,
                },
                "the environment asks for a PLAIN login to its cluster, and a proxy cannot log in to a cluster yet",
            ),
            (
                UpstreamBackendError::undiscovered("kafka-1.staging:9092 did not answer within 30s"),
                "the cluster did not name its brokers: kafka-1.staging:9092 did not answer within 30s",
            ),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none());
        }
    }
}
