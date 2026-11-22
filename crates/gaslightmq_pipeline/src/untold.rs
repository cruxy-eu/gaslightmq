//! Why the pipeline could not tell a lie, and what else went wrong while it handled a call.

use std::fmt;

use gaslightmq_call::{CallError, CallTimingError};
use gaslightmq_lie_api::{Instruction, LieError};
use gaslightmq_session::IdentityError;
use gaslightmq_wire::{Api, WireError};
use gaslightmq_wire_edit::{EditableRequestError, TopicNamesError};

use crate::explain::UnexplainedRule;

/// Why a throttle does not reach the client. The line of a lie and the failure of a call say it in the same words.
const THROTTLE_UNWRITTEN: &str = "the throttle of an answer cannot be written yet";

/// Why the pipeline did not tell a lie that applies to a call.
///
/// A lie that is not told changes nothing, and its firing goes back to its budget.
/// The message goes into the explanation of the call, on the line of the rule that fired the lie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UntoldLie {
    /// The behaviour of the lie failed.
    Failed { error: LieError },
    /// The rule fired no lie, so there is nothing to ask.
    NoLie,
    /// The lie asked for something that the pipeline cannot do where it asked.
    NotFollowed { instruction: Instruction },
    /// Another lie answered the call before this one was asked.
    AnsweredBefore { by: String },
    /// The lie asked to forward a request that is not the request of this call.
    ForwardsAnotherCall { api: Api, correlation_id: i32 },
    /// What the lie left of the request does not decode.
    UnreadableRemainder { error: EditableRequestError },
    /// The lie asked to send a response that does not answer the request of this call.
    AnswersAnotherCall { api: Api, correlation_id: i32 },
    /// The call has no response: a Produce with `acks=0`.
    NoResponse,
    /// The call ended before the stage of the lie came.
    CallEnded,
    /// The lie only throttles, and no surgery writes the throttle of an answer yet.
    ThrottleUnwritten,
}

impl fmt::Display for UntoldLie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed { error } => fmt::Display::fmt(error, f),
            Self::NoLie => f.write_str("the rule fired no lie"),
            Self::NotFollowed { instruction } => {
                write!(f, "the lie asked to {}, which it cannot ask here", instruction.phrase())
            }
            Self::AnsweredBefore { by } => write!(f, "the lie of {by:?} answered the call before"),
            Self::ForwardsAnotherCall { api, correlation_id } => {
                write!(
                    f,
                    "the lie asked to forward the {api} request {correlation_id}, which is another call"
                )
            }
            Self::UnreadableRemainder { error } => write!(f, "what the lie left of the request is unusable: {error}"),
            Self::AnswersAnotherCall { api, correlation_id } => {
                write!(
                    f,
                    "the lie asked to send the {api} response {correlation_id}, which answers another call"
                )
            }
            Self::NoResponse => f.write_str("the call has no response: a Produce with acks=0 expects none"),
            Self::CallEnded => f.write_str("the call ended before the lie could be told"),
            Self::ThrottleUnwritten => write!(f, "the lie only throttles, and {THROTTLE_UNWRITTEN}"),
        }
    }
}

/// The message says everything, also the error it holds, so the error has no source.
impl std::error::Error for UntoldLie {}

impl From<LieError> for UntoldLie {
    fn from(error: LieError) -> Self {
        Self::Failed { error }
    }
}

/// What went wrong inside the pipeline while it handled a call, apart from a lie it could not tell.
///
/// The call goes on as far as it can without what failed, and its explanation keeps the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StageFailure {
    /// The header of the request names the client in a way the session cannot keep.
    Unidentified { error: IdentityError },
    /// The body of the request does not decode, so no rule saw the call.
    UnreadableRequest { error: EditableRequestError },
    /// The fields of the body do not decode.
    UnreadableRequestFields { api: Api, error: WireError },
    /// The response of the backend cannot carry the API and the correlation ID of the client's request.
    UnrestoredResponse { api: Api, error: WireError },
    /// The response of the backend cannot be read, so the rules saw no truth.
    UnreadableTruth { error: EditableRequestError },
    /// The topics that the response of the backend names with their ID cannot be read.
    UnreadableTopicNames { error: TopicNamesError },
    /// The member ID that the response of the backend gives the client cannot be read.
    UnreadableMemberId { api: Api, error: WireError },
    /// The response that the client received cannot be read, so the record shows no answer.
    UnreadableDelivery { error: EditableRequestError },
    /// The answer should carry a throttle, and no surgery writes one yet.
    ThrottleUnwritten { millis: u64 },
    /// A lie changed the request that the backend got, and failed to make the response fit the client's request.
    /// The lie did act, so it stays told.
    UnmergedLie { rule: String, error: UntoldLie },
    /// A Produce whose body does not decode does not say its `acks`, so nothing says whether a response comes.
    UnknownAcks { error: EditableRequestError },
    /// A moment of the call does not follow the one before it, so the timing of the record lacks it.
    Mistimed { error: CallTimingError },
    /// The fields of the call do not make a record, so the record only names its connection.
    Unrecorded { error: CallError },
    /// The explanation of the call contradicts itself.
    Unexplained { error: UnexplainedRule },
}

impl fmt::Display for StageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unidentified { error } => write!(f, "the client is not identified: {error}"),
            Self::UnreadableRequest { error } => write!(f, "no rule saw the call: {error}"),
            Self::UnreadableRequestFields { api, error } => {
                write!(f, "the fields of the {api} request cannot be read: {error}")
            }
            Self::UnrestoredResponse { api, error } => {
                write!(
                    f,
                    "the response of the backend does not read as a {api} response: {error}"
                )
            }
            Self::UnreadableTruth { error } => write!(f, "the rules saw no truth: {error}"),
            Self::UnreadableTopicNames { error } => write!(f, "no topic was learned: {error}"),
            Self::UnreadableMemberId { api, error } => {
                write!(f, "no member ID was learned from the {api} response: {error}")
            }
            Self::UnreadableDelivery { error } => write!(f, "the record shows no answer: {error}"),
            Self::ThrottleUnwritten { millis } => {
                write!(f, "the answer carries no throttle of {millis} ms: {THROTTLE_UNWRITTEN}")
            }
            Self::UnmergedLie { rule, error } => {
                write!(
                    f,
                    "the lie of {rule:?} changed the request, and could not make the response fit it: {error}"
                )
            }
            Self::UnknownAcks { error } => {
                write!(
                    f,
                    "the Produce request does not say whether it expects a response: {error}"
                )
            }
            Self::Mistimed { error } => write!(f, "a moment of the call is missing: {error}"),
            Self::Unrecorded { error } => write!(f, "the record keeps no field of the call: {error}"),
            Self::Unexplained { error } => write!(f, "the explanation is incomplete: {error}"),
        }
    }
}

/// The message says everything, also the error it holds, so the error has no source.
impl std::error::Error for StageFailure {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_lie::LieKind;

    use super::*;

    #[test]
    fn an_untold_lie_says_everything_in_one_message() {
        let not_registered = LieError::NotRegistered { kind: LieKind::Refusal };
        let cases = [
            (
                UntoldLie::from(not_registered),
                "no behaviour is registered for refusal",
            ),
            (UntoldLie::NoLie, "the rule fired no lie"),
            (
                UntoldLie::NotFollowed {
                    instruction: Instruction::Hold,
                },
                "the lie asked to hold the call back, which it cannot ask here",
            ),
            (
                UntoldLie::AnsweredBefore {
                    by: "Leader flap".to_owned(),
                },
                "the lie of \"Leader flap\" answered the call before",
            ),
            (
                UntoldLie::ForwardsAnotherCall {
                    api: Api::from_wire(1, 13),
                    correlation_id: 8,
                },
                "the lie asked to forward the Fetch v13 request 8, which is another call",
            ),
            (
                UntoldLie::AnswersAnotherCall {
                    api: Api::from_wire(1, 13),
                    correlation_id: 8,
                },
                "the lie asked to send the Fetch v13 response 8, which answers another call",
            ),
            (
                UntoldLie::NoResponse,
                "the call has no response: a Produce with acks=0 expects none",
            ),
            (UntoldLie::CallEnded, "the call ended before the lie could be told"),
            (
                UntoldLie::ThrottleUnwritten,
                "the lie only throttles, and the throttle of an answer cannot be written yet",
            ),
        ];

        for (untold, message) in cases {
            assert_eq!(untold.to_string(), message);
            assert!(untold.source().is_none(), "{untold}");
        }
    }

    #[test]
    fn a_stage_failure_names_what_failed_and_writes_the_cause_once() {
        let unknown = EditableRequestError::UnknownApi {
            api: Api::from_wire(9999, 0),
        };
        let wrong_api = WireError::WrongApi {
            expected: 0,
            actual: 12,
        };
        let cases = [
            (
                StageFailure::UnreadableRequest { error: unknown.clone() },
                "no rule saw the call: a request of api 9999 v0 cannot be edited: its messages are unknown",
            ),
            (
                StageFailure::UnreadableRequestFields {
                    api: Api::from_wire(0, 9),
                    error: wrong_api.clone(),
                },
                "the fields of the Produce v9 request cannot be read: body of api key 12 read as api key 0",
            ),
            (
                StageFailure::UnrestoredResponse {
                    api: Api::from_wire(0, 9),
                    error: wrong_api,
                },
                "the response of the backend does not read as a Produce v9 response: body of api key 12 read as api key 0",
            ),
            (
                StageFailure::UnreadableTruth { error: unknown.clone() },
                "the rules saw no truth: a request of api 9999 v0 cannot be edited: its messages are unknown",
            ),
            (
                StageFailure::UnreadableDelivery { error: unknown },
                "the record shows no answer: a request of api 9999 v0 cannot be edited: its messages are unknown",
            ),
            (
                StageFailure::ThrottleUnwritten { millis: 500 },
                "the answer carries no throttle of 500 ms: the throttle of an answer cannot be written yet",
            ),
            (
                StageFailure::UnmergedLie {
                    rule: "Leader flap".to_owned(),
                    error: UntoldLie::NoLie,
                },
                "the lie of \"Leader flap\" changed the request, and could not make the response fit it: \
                 the rule fired no lie",
            ),
            (
                StageFailure::Mistimed {
                    error: CallTimingError::AnsweredBeforeForwarding,
                },
                "a moment of the call is missing: a backend does not answer before the request leaves for it",
            ),
            (
                StageFailure::Unrecorded {
                    error: CallError::TruthPending,
                },
                "the record keeps no field of the call: a call record is of a call that ended, \
                 and its response fields still wait for the backend",
            ),
            (
                StageFailure::UnreadableMemberId {
                    api: Api::from_wire(11, 9),
                    error: WireError::WrongApi {
                        expected: 11,
                        actual: 12,
                    },
                },
                "no member ID was learned from the JoinGroup v9 response: body of api key 12 read as api key 11",
            ),
            (
                StageFailure::Unexplained {
                    error: UnexplainedRule::of("a lie that fired applied or lost somewhere, and this one did neither"),
                },
                "the explanation is incomplete: a rule has no line: \
                 a lie that fired applied or lost somewhere, and this one did neither",
            ),
        ];

        for (failure, message) in cases {
            assert_eq!(failure.to_string(), message);
            assert!(failure.source().is_none(), "{failure}");
        }
    }
}
