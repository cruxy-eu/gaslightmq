//! Why a call of a client failed.

use std::fmt;

use gaslightmq_kernel::{ApiKey, TopicName};
use gaslightmq_wire::{Partition, ResponseError};

/// Why a call of a Kafka client failed.
///
/// Every call of every client returns it: the methods of [`Producer`](crate::Producer),
/// [`Consumer`](crate::Consumer), [`Admin`](crate::Admin) and [`ProtocolConsumer`](crate::ProtocolConsumer),
/// the traits named after an API, and the [`RawKafkaClient`](crate::RawKafkaClient).
///
/// # The broker, or the client
///
/// [`Refused`](Self::Refused) is the only variant that holds an error code, and the broker wrote that code.
/// Every other variant is a failure of the client or of its connection, and holds no error code.
/// A test of a proxy that lies about error codes therefore never takes a failure of its client
/// for an answer of the broker.
///
/// | Variant | The broker answered | A request went out |
/// |---|---|---|
/// | [`Refused`](Self::Refused) | Yes, with an error code. | Yes. |
/// | [`UnreadableAnswer`](Self::UnreadableAnswer) | Yes, with what the client cannot read. | Yes. |
/// | [`NoAnswer`](Self::NoAnswer) | Not in time. | Yes. |
/// | [`ConnectionLost`](Self::ConnectionLost) | No. | It may have. |
/// | [`Unreachable`](Self::Unreachable) | No. | No. |
/// | [`UnwritableRequest`](Self::UnwritableRequest) | No. | No. |
/// | [`NoCommonVersion`](Self::NoCommonVersion) | No. | No. |
/// | [`NoLeader`](Self::NoLeader) | No. | No. |
/// | [`OutOfTime`](Self::OutOfTime) | As its last failure says. | As its last failure says. |
///
/// A client that sends one request and tries nothing again never returns `NoLeader` or `OutOfTime`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError {
    /// The broker answered with an error code. `error` is the error Kafka gives that code.
    #[error("the broker refused the request: {error}")]
    Refused { error: ResponseError },
    /// The broker answered with something the client cannot read as the answer to its request:
    /// bytes that are no response, the response to another request, or a response without what was asked for.
    #[error("the answer of the broker is unreadable: {reason}")]
    UnreadableAnswer { reason: String },
    /// A request got no answer within the time it had.
    #[error("no answer of the broker in time")]
    NoAnswer,
    /// The connection ended before the answer arrived. The broker may have taken the request.
    #[error("the connection to the broker was lost: {reason}")]
    ConnectionLost { reason: String },
    /// No connection to the broker opened, so no request went out.
    #[error("no connection to the broker: {reason}")]
    Unreachable { reason: String },
    /// What the call asks for cannot be written as a request, so no request went out.
    #[error("the request cannot be written: {reason}")]
    UnwritableRequest { reason: String },
    /// The client and the broker speak no version of the API in common, so no request went out.
    #[error("the client and the broker share no version of the {api_key} API")]
    NoCommonVersion { api_key: ApiKey },
    /// No broker leads the partition, so the client has no broker to send the request to.
    #[error("no broker leads {topic}/{partition}")]
    NoLeader { topic: TopicName, partition: Partition },
    /// Failures that may pass kept happening until the time of the call ran out.
    /// `error` is the last of them.
    #[error("the call ran out of time, and failed last with: {error}")]
    OutOfTime { error: Box<ClientError> },
}

impl ClientError {
    /// The refusal that the error code of a response stands for. `None` when the code is no error.
    pub fn refusal(error_code: i16) -> Option<Self> {
        ResponseError::try_from_code(error_code).map(|error| Self::Refused { error })
    }

    /// An answer that is unreadable for `reason`.
    pub fn unreadable_answer(reason: impl fmt::Display) -> Self {
        Self::UnreadableAnswer {
            reason: reason.to_string(),
        }
    }

    /// A connection that was lost for `reason`.
    pub fn connection_lost(reason: impl fmt::Display) -> Self {
        Self::ConnectionLost {
            reason: reason.to_string(),
        }
    }

    /// A broker that no connection opened to, for `reason`.
    pub fn unreachable(reason: impl fmt::Display) -> Self {
        Self::Unreachable {
            reason: reason.to_string(),
        }
    }

    /// A request that cannot be written for `reason`.
    pub fn unwritable_request(reason: impl fmt::Display) -> Self {
        Self::UnwritableRequest {
            reason: reason.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn an_error_code_is_a_refusal_and_no_error_code_is_none() {
        assert_eq!(
            ClientError::refusal(ResponseError::NotLeaderOrFollower.code()),
            Some(ClientError::Refused {
                error: ResponseError::NotLeaderOrFollower
            })
        );
        assert_eq!(ClientError::refusal(0), None);
    }

    #[test]
    fn a_refusal_names_the_error_of_the_broker_once() {
        let error = ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower,
        };

        assert_eq!(
            error.to_string(),
            format!("the broker refused the request: {}", ResponseError::NotLeaderOrFollower)
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn a_call_that_ran_out_of_time_names_its_last_failure_once() {
        let error = ClientError::OutOfTime {
            error: Box::new(ClientError::NoAnswer),
        };

        assert_eq!(
            error.to_string(),
            "the call ran out of time, and failed last with: no answer of the broker in time"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn what_the_system_said_is_the_reason() {
        let closed = ClientError::connection_lost("the broker closed the connection");

        assert_eq!(
            closed,
            ClientError::ConnectionLost {
                reason: "the broker closed the connection".into()
            }
        );
        assert_eq!(
            closed.to_string(),
            "the connection to the broker was lost: the broker closed the connection"
        );
        assert_eq!(
            ClientError::unreachable("connection refused").to_string(),
            "no connection to the broker: connection refused"
        );
        assert_eq!(
            ClientError::unreadable_answer("frame of zero bytes").to_string(),
            "the answer of the broker is unreadable: frame of zero bytes"
        );
        assert_eq!(
            ClientError::unwritable_request("no version 99").to_string(),
            "the request cannot be written: no version 99"
        );
    }

    #[test]
    fn a_partition_without_a_leader_is_named() {
        let error = ClientError::NoLeader {
            topic: TopicName::new("orders.eu").unwrap(),
            partition: Partition::new(3),
        };

        assert_eq!(error.to_string(), "no broker leads orders.eu/3");
    }

    #[test]
    fn an_api_without_a_common_version_is_named() {
        let named = ClientError::NoCommonVersion {
            api_key: ApiKey::METADATA,
        };
        let unnamed = ClientError::NoCommonVersion {
            api_key: ApiKey::new(4711),
        };

        assert_eq!(
            named.to_string(),
            "the client and the broker share no version of the Metadata API"
        );
        assert_eq!(
            unnamed.to_string(),
            "the client and the broker share no version of the 4711 API"
        );
    }
}
