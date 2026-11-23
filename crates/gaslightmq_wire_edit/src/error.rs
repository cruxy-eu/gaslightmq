use std::fmt;

use gaslightmq_wire::Api;

/// Why surgery on a request or on a response failed.
///
/// It is the one error of this crate: every operation of [`EditableRequest`](crate::EditableRequest) returns it,
/// and so does [`TopicNames::learn_from`](crate::TopicNames::learn_from).
///
/// Whoever asks for surgery does the same whatever stood in the way: it goes on without the surgery, and says why.
/// So the error has no variants, and what is wrong is its message.
/// The message names the request or the response and its API first.
/// When a body does not decode or encode, it then writes what the codec said, once.
///
/// ```
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let request = Request::new(4, 7, None, &HeartbeatRequest::default()).expect("a request");
/// let asked = EditableRequest::decode(&request, &TopicNames::new())?;
///
/// let error = asked.error_response(&Failures::none()).expect_err("nothing failed");
/// assert_eq!(error.to_string(), "nothing that the Heartbeat v4 request asks failed");
/// # Ok::<(), SurgeryError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct SurgeryError {
    /// What is wrong, in one message.
    reason: String,
}

impl SurgeryError {
    /// There are no messages for `api`, so the body of its request has no known shape.
    pub(crate) fn unknown_api(api: Api) -> Self {
        Self::of(format_args!(
            "a request of {api} cannot be edited: its messages are unknown"
        ))
    }

    /// The body of a request of `api` does not decode, and the codec said `reason`.
    pub(crate) fn unreadable_request(api: Api, reason: &impl fmt::Display) -> Self {
        Self::of(format_args!("the {api} request cannot be read: {reason}"))
    }

    /// What is left of a request of `api` does not encode, and the codec said `reason`.
    pub(crate) fn unwritable_request(api: Api, reason: &impl fmt::Display) -> Self {
        Self::of(format_args!(
            "what is left of the {api} request cannot be written: {reason}"
        ))
    }

    /// The body of a response of `api` does not decode, and the codec said `reason`.
    pub(crate) fn unreadable_response(api: Api, reason: &impl fmt::Display) -> Self {
        Self::of(format_args!("the {api} response cannot be read: {reason}"))
    }

    /// A response of `api` does not encode, and the codec said `reason`.
    pub(crate) fn unwritable_response(api: Api, reason: &impl fmt::Display) -> Self {
        Self::of(format_args!("the {api} response cannot be written: {reason}"))
    }

    /// The response belongs to another request than `request`: its API or its correlation ID differs.
    pub(crate) fn response_of_another_call(request: (Api, i32), response: (Api, i32)) -> Self {
        let (api, correlation_id) = request;
        let (answered_api, answered_correlation_id) = response;
        Self::of(format_args!(
            "the {answered_api} response {answered_correlation_id} does not answer the {api} request {correlation_id}"
        ))
    }

    /// The failures name nothing that the request of `api` asks, so there is no error to answer with.
    pub(crate) fn nothing_failed(api: Api) -> Self {
        Self::of(format_args!("nothing that the {api} request asks failed"))
    }

    fn of(reason: fmt::Arguments<'_>) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}
