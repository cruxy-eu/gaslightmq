use std::fmt;

use gaslightmq_wire::{ResponseError, WireError};

/// Why a call of a producer, a consumer or an admin failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// The broker answered with an error code. `error` is the error Kafka gives that code.
    Broker { error: ResponseError },
    /// A request got no answer within the time it had.
    RequestTimedOut,
    /// Retriable failures kept happening until the time of the call ran out.
    /// `error` is the last of them.
    CallTimedOut { error: Box<ClientError> },
    /// Connecting failed or the connection dropped.
    ConnectionFailed(String),
    /// The broker sent something we can't make sense of.
    Protocol(String),
    /// No version of the API is spoken by both client and broker.
    UnsupportedVersion { api_key: i16 },
    /// The partition has no known leader.
    NoLeader { topic: String, partition: i32 },
}

impl ClientError {
    pub(crate) fn broker(code: i16) -> Option<Self> {
        ResponseError::try_from_code(code).map(|error| Self::Broker { error })
    }

    /// Whether the same call can succeed when it is tried again.
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Broker { error } => error.is_retriable(),
            Self::RequestTimedOut | Self::ConnectionFailed(_) | Self::NoLeader { .. } => true,
            Self::CallTimedOut { .. } | Self::Protocol(_) | Self::UnsupportedVersion { .. } => false,
        }
    }

    /// The closest Kafka error code, for a result that has no other place for a failure.
    pub fn to_response_error(&self) -> ResponseError {
        match self {
            Self::Broker { error } => *error,
            Self::RequestTimedOut | Self::CallTimedOut { .. } => ResponseError::RequestTimedOut,
            Self::ConnectionFailed(_) => ResponseError::NetworkException,
            Self::NoLeader { .. } => ResponseError::LeaderNotAvailable,
            Self::UnsupportedVersion { .. } => ResponseError::UnsupportedVersion,
            Self::Protocol(_) => ResponseError::UnknownServerError,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker { error } => write!(f, "broker error: {error}"),
            Self::RequestTimedOut => f.write_str("request timed out"),
            Self::CallTimedOut { error } => write!(f, "the call ran out of time, last error: {error}"),
            Self::ConnectionFailed(reason) => write!(f, "connection error: {reason}"),
            Self::Protocol(reason) => write!(f, "protocol error: {reason}"),
            Self::UnsupportedVersion { api_key } => write!(f, "no common version for api key {api_key}"),
            Self::NoLeader { topic, partition } => write!(f, "no leader for {topic}-{partition}"),
        }
    }
}

/// The message is complete, so the error has no source.
impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::ConnectionFailed(error.to_string())
    }
}

impl From<WireError> for ClientError {
    fn from(error: WireError) -> Self {
        Self::Protocol(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_call_that_ran_out_of_time_names_its_last_error_once() {
        let error = ClientError::CallTimedOut {
            error: Box::new(ClientError::RequestTimedOut),
        };

        assert_eq!(
            error.to_string(),
            "the call ran out of time, last error: request timed out"
        );
        assert!(error.source().is_none());
        assert!(!error.is_retriable());
    }

    #[test]
    fn an_error_of_the_broker_names_its_cause_once() {
        let error = ClientError::Broker {
            error: ResponseError::NotLeaderOrFollower,
        };

        assert_eq!(
            error.to_string(),
            format!("broker error: {}", ResponseError::NotLeaderOrFollower)
        );
        assert!(error.source().is_none());
        assert_eq!(
            ClientError::broker(ResponseError::NotLeaderOrFollower.code()),
            Some(error)
        );
        assert_eq!(ClientError::broker(0), None);
    }

    #[test]
    fn a_failure_maps_to_the_closest_kafka_error() {
        let timed_out = ClientError::CallTimedOut {
            error: Box::new(ClientError::Broker {
                error: ResponseError::NotLeaderOrFollower,
            }),
        };

        assert_eq!(timed_out.to_response_error(), ResponseError::RequestTimedOut);
        assert_eq!(
            ClientError::Broker {
                error: ResponseError::NotLeaderOrFollower
            }
            .to_response_error(),
            ResponseError::NotLeaderOrFollower
        );
        assert_eq!(
            ClientError::ConnectionFailed("reset".into()).to_response_error(),
            ResponseError::NetworkException
        );
    }
}
