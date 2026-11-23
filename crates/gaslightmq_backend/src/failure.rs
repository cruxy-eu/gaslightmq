//! Why a backend has no answer for a call.

use std::time::Duration;

use gaslightmq_field::Endpoint;
use gaslightmq_wire::WireError;

/// Why a backend has no answer for a call.
///
/// A backend that fails a call gave up on it: no answer comes later.
/// A backend that only waits, such as one in front of a broker that is unresponsive, fails nothing.
/// Its call stays open until the client gives up, and the engine drops it.
///
/// The message says everything in one line, so the engine can put it into the record of the call as it is.
///
/// ```
/// use std::time::Duration;
///
/// use gaslightmq_backend::prelude::*;
///
/// let failure = BackendFailure::TimedOut { after: Duration::from_secs(30) };
/// assert_eq!(failure.to_string(), "the cluster did not answer within 30s");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BackendFailure {
    /// The backend has no connection to the cluster, and cannot make one.
    /// `reason` says what stood in the way, such as a refused connection.
    #[error("the backend cannot reach the cluster: {reason}")]
    Unreachable { reason: String },
    /// The connection to the cluster ended before the answer arrived.
    /// `reason` says how it ended.
    #[error("the connection to the cluster ended before the answer arrived: {reason}")]
    ConnectionLost { reason: String },
    /// The backend waited `after` for the answer, and gave up.
    #[error("the cluster did not answer within {after:?}")]
    TimedOut { after: Duration },
    /// The backend answers the request itself, and could not build its response.
    /// The cluster sent nothing that is wrong.
    #[error("the backend could not build its response: {error}")]
    ResponseNotBuilt { error: WireError },
    /// The request arrived on an endpoint that the cluster does not have, or whose broker is down.
    #[error("the cluster takes no requests on endpoint `{endpoint}`")]
    NoSuchEndpoint { endpoint: Endpoint },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_failure_says_everything_in_one_message() {
        let cases = [
            (
                BackendFailure::Unreachable {
                    reason: "kafka-1.staging:9092 refused the connection".to_owned(),
                },
                "the backend cannot reach the cluster: kafka-1.staging:9092 refused the connection",
            ),
            (
                BackendFailure::ConnectionLost {
                    reason: "the broker closed it".to_owned(),
                },
                "the connection to the cluster ended before the answer arrived: the broker closed it",
            ),
            (
                BackendFailure::TimedOut {
                    after: Duration::from_millis(1500),
                },
                "the cluster did not answer within 1.5s",
            ),
            (
                BackendFailure::ResponseNotBuilt {
                    error: WireError::UnknownApi { api_key: 9999 },
                },
                "the backend could not build its response: unknown api key 9999",
            ),
            (
                BackendFailure::NoSuchEndpoint {
                    endpoint: Endpoint::Bootstrap,
                },
                "the cluster takes no requests on endpoint `bootstrap`",
            ),
        ];

        for (failure, message) in cases {
            assert_eq!(failure.to_string(), message);
            assert!(failure.source().is_none(), "{failure}");
        }
    }
}
