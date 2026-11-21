use std::fmt;

use gmq_wire::{ResponseError, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// The broker answered with an error code.
    Broker(ResponseError),
    /// A request got no answer within the request timeout.
    Timeout,
    /// Retriable failures kept happening until the delivery timeout ran out.
    DeliveryTimeout(Box<ClientError>),
    /// Connecting failed or the connection dropped.
    Io(String),
    /// The broker sent something we can't make sense of.
    Protocol(String),
    /// No version of the API is supported by both client and broker.
    UnsupportedVersion { api_key: i16 },
    /// The partition has no known leader.
    NoLeader { topic: String, partition: i32 },
}

impl ClientError {
    pub(crate) fn broker(code: i16) -> Option<Self> {
        ResponseError::try_from_code(code).map(Self::Broker)
    }

    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Broker(e) => e.is_retriable(),
            Self::Timeout | Self::Io(_) | Self::NoLeader { .. } => true,
            Self::DeliveryTimeout(_) | Self::Protocol(_) | Self::UnsupportedVersion { .. } => false,
        }
    }

    /// The closest Kafka error code, for facade results.
    pub fn to_response_error(&self) -> ResponseError {
        match self {
            Self::Broker(e) => *e,
            Self::Timeout | Self::DeliveryTimeout(_) => ResponseError::RequestTimedOut,
            Self::Io(_) => ResponseError::NetworkException,
            Self::NoLeader { .. } => ResponseError::LeaderNotAvailable,
            Self::UnsupportedVersion { .. } => ResponseError::UnsupportedVersion,
            Self::Protocol(_) => ResponseError::UnknownServerError,
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker(e) => write!(f, "broker error: {e}"),
            Self::Timeout => f.write_str("request timed out"),
            Self::DeliveryTimeout(last) => write!(f, "delivery timed out, last error: {last}"),
            Self::Io(e) => write!(f, "connection error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::UnsupportedVersion { api_key } => write!(f, "no common version for api key {api_key}"),
            Self::NoLeader { topic, partition } => write!(f, "no leader for {topic}-{partition}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<WireError> for ClientError {
    fn from(e: WireError) -> Self {
        Self::Protocol(e.to_string())
    }
}
