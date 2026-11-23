use std::fmt;

use gaslightmq_wire::Api;

/// Why surgery on a request or on a response failed.
///
/// It is the one error of this crate: every operation of [`EditableRequest`](crate::EditableRequest) returns it,
/// and so does [`TopicNames::learn_from`](crate::TopicNames::learn_from).
///
/// A variant about a body that does not decode or encode keeps what the codec said as text, in `reason`,
/// and its message writes that text once.
///
/// ```
/// use gaslightmq_wire::Api;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let error = SurgeryError::NothingFailed { api: Api::from_wire(0, 9) };
/// assert_eq!(error.to_string(), "nothing that the Produce v9 request asks failed");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SurgeryError {
    /// There are no messages for the API of the request, so its body has no known shape.
    UnknownApi { api: Api },
    /// The body of the request does not decode.
    UnreadableRequest { api: Api, reason: String },
    /// What is left of the request does not encode.
    UnwritableRequest { api: Api, reason: String },
    /// The body of the response does not decode.
    UnreadableResponse { api: Api, reason: String },
    /// The response does not encode.
    UnwritableResponse { api: Api, reason: String },
    /// The response belongs to another request: its API or its correlation ID differs.
    ResponseOfAnotherCall {
        api: Api,
        correlation_id: i32,
        answered_api: Api,
        answered_correlation_id: i32,
    },
    /// The failures name nothing that the request asks, so there is no error to answer with.
    NothingFailed { api: Api },
}

impl SurgeryError {
    /// The body of a request of `api` does not decode, and the codec said `reason`.
    pub(crate) fn unreadable_request(api: Api, reason: &impl fmt::Display) -> Self {
        Self::UnreadableRequest {
            api,
            reason: reason.to_string(),
        }
    }

    /// What is left of a request of `api` does not encode, and the codec said `reason`.
    pub(crate) fn unwritable_request(api: Api, reason: &impl fmt::Display) -> Self {
        Self::UnwritableRequest {
            api,
            reason: reason.to_string(),
        }
    }

    /// The body of a response of `api` does not decode, and the codec said `reason`.
    pub(crate) fn unreadable_response(api: Api, reason: &impl fmt::Display) -> Self {
        Self::UnreadableResponse {
            api,
            reason: reason.to_string(),
        }
    }

    /// A response of `api` does not encode, and the codec said `reason`.
    pub(crate) fn unwritable_response(api: Api, reason: &impl fmt::Display) -> Self {
        Self::UnwritableResponse {
            api,
            reason: reason.to_string(),
        }
    }
}

impl fmt::Display for SurgeryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownApi { api } => write!(f, "a request of {api} cannot be edited: its messages are unknown"),
            Self::UnreadableRequest { api, reason } => write!(f, "the {api} request cannot be read: {reason}"),
            Self::UnwritableRequest { api, reason } => {
                write!(f, "what is left of the {api} request cannot be written: {reason}")
            }
            Self::UnreadableResponse { api, reason } => write!(f, "the {api} response cannot be read: {reason}"),
            Self::UnwritableResponse { api, reason } => write!(f, "the {api} response cannot be written: {reason}"),
            Self::ResponseOfAnotherCall {
                api,
                correlation_id,
                answered_api,
                answered_correlation_id,
            } => write!(
                f,
                "the {answered_api} response {answered_correlation_id} does not answer the {api} request {correlation_id}"
            ),
            Self::NothingFailed { api } => write!(f, "nothing that the {api} request asks failed"),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for SurgeryError {}
