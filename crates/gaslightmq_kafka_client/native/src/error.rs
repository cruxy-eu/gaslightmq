use std::fmt;

use gaslightmq_wire::{ResponseError, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// The broker answered with an error code. `error` is the error Kafka gives that code.
    Broker { error: ResponseError },
    /// A request got no answer within the request timeout.
    Timeout,
    /// Retriable failures kept happening until the delivery timeout ran out.
    /// `error` is the last of them.
    DeliveryTimeout { error: Box<ClientError> },
    /// Connecting failed or the connection dropped.
    ConnectionFailed(String),
    /// The broker sent something we can't make sense of.
    Protocol(String),
    /// No version of the API is supported by both client and broker.
    UnsupportedVersion { api_key: i16 },
    /// The partition has no known leader.
    NoLeader { topic: String, partition: i32 },
}

impl ClientError {
    pub(crate) fn broker(code: i16) -> Option<Self> {
        ResponseError::try_from_code(code).map(|error| Self::Broker { error })
    }

    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Broker { error } => error.is_retriable(),
            Self::Timeout | Self::ConnectionFailed(_) | Self::NoLeader { .. } => true,
            Self::DeliveryTimeout { .. } | Self::Protocol(_) | Self::UnsupportedVersion { .. } => false,
        }
    }

    /// The closest Kafka error code, for facade results.
    pub fn to_response_error(&self) -> ResponseError {
        match self {
            Self::Broker { error } => *error,
            Self::Timeout | Self::DeliveryTimeout { .. } => ResponseError::RequestTimedOut,
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
            Self::Timeout => f.write_str("request timed out"),
            Self::DeliveryTimeout { error } => write!(f, "delivery timed out, last error: {error}"),
            Self::ConnectionFailed(e) => write!(f, "connection error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::UnsupportedVersion { api_key } => write!(f, "no common version for api key {api_key}"),
            Self::NoLeader { topic, partition } => write!(f, "no leader for {topic}-{partition}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::ConnectionFailed(e.to_string())
    }
}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        Self::Protocol(e.to_string())
    }
}
