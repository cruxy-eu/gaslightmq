//! What a session cannot do, or cannot keep.

use gaslightmq_call::CallNumber;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::CallPhase;

/// Why a session cannot keep what a request says about its client, or cannot do what the pipeline asked for a call.
///
/// It is the one error of what a session does. The value objects of this crate have errors of their own.
///
/// | The variants | Come from | Say |
/// |---|---|---|
/// | `InvalidClientId`, `UnreadableClientSoftware`, `InvalidClientSoftware` | The [errors](crate::Identification::errors) of an identification | A header says something about the client that the session cannot keep. The rest of the header still counts. |
/// | `CallNotInFlight`, `RequestNotHeld`, `ResponseNotAwaited` | [`InFlightCalls`](crate::InFlightCalls) | The pipeline lost track of a call. None of them is a failure of a client. |
///
/// A variant about a value that is wrong keeps the error of that value, and writes it after the name of the field.
///
/// ```
/// use gaslightmq_call::CallNumber;
/// use gaslightmq_session::prelude::*;
///
/// let error = SessionError::CallNotInFlight { call: CallNumber::FIRST };
/// assert_eq!(error.to_string(), "call 1 is not in flight");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// The `client.id` in the header is not a client ID. The session stays unidentified.
    #[error("{}: {error}", Field::ClientId)]
    InvalidClientId { error: ClientIdError },
    /// The body of the `ApiVersions` request, which names the client software, does not decode.
    /// `reason` says what is wrong with it.
    #[error("{}: {reason}", Field::ClientSoftware)]
    UnreadableClientSoftware { reason: String },
    /// The name and the version in the `ApiVersions` request are not client software.
    #[error("{}: {error}", Field::ClientSoftware)]
    InvalidClientSoftware { error: ClientSoftwareError },
    /// No call with that number is in flight: it never arrived, it left, or it ended with its session.
    #[error("call {call} is not in flight")]
    CallNotInFlight { call: CallNumber },
    /// The request of the call cannot be let go, because the pipeline does not hold it any more.
    #[error("the request of call {call} is not held: the call is in the phase {phase}")]
    RequestNotHeld { call: CallNumber, phase: CallPhase },
    /// The response of the call cannot be let go, because the call does not await one.
    #[error("call {call} does not await its response: the call is in the phase {phase}")]
    ResponseNotAwaited { call: CallNumber, phase: CallPhase },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn each_error_of_a_call_names_the_call_and_says_what_is_wrong() {
        let call = CallNumber::new(7).unwrap();
        let cases = [
            (SessionError::CallNotInFlight { call }, "call 7 is not in flight"),
            (
                SessionError::RequestNotHeld {
                    call,
                    phase: CallPhase::AwaitingResponse,
                },
                "the request of call 7 is not held: the call is in the phase awaiting response",
            ),
            (
                SessionError::ResponseNotAwaited {
                    call,
                    phase: CallPhase::RequestHeld,
                },
                "call 7 does not await its response: the call is in the phase request held",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn each_error_of_an_identity_names_its_field_first() {
        let client_id = SessionError::InvalidClientId {
            error: ClientId::new("a".repeat(40_000)).unwrap_err(),
        };
        let unreadable = SessionError::UnreadableClientSoftware {
            reason: "cannot decode request body: unexpected end".to_owned(),
        };

        assert!(client_id.to_string().starts_with("client.id: "), "{client_id}");
        assert_eq!(
            unreadable.to_string(),
            "client.software: cannot decode request body: unexpected end"
        );
        assert!(client_id.source().is_none());
        assert!(unreadable.source().is_none());
    }
}
