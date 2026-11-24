//! Why the pipeline could not tell a lie, and what else went wrong while it handled a call.

use gaslightmq_call::{CallError, CallId, CallTimingError};
use gaslightmq_lie_api::{Instruction, LieError};
use gaslightmq_session::SessionError;
use gaslightmq_wire::{Api, DecodeError};
use gaslightmq_wire_edit::SurgeryError;

use crate::explain::UnexplainedRule;

/// Why a throttle does not reach the client. The line of a lie and the failure of a call say it in the same words.
const THROTTLE_UNWRITTEN: &str = "the throttle of an answer cannot be written yet";

/// Why the pipeline did not tell a lie that applies to a call.
///
/// A lie that is not told changes nothing, and its firing goes back to its budget.
/// The message goes into the explanation of the call, on the line of the rule that fired the lie.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum UntoldLie {
    /// The behaviour of the lie failed.
    #[error("{error}")]
    Failed { error: LieError },
    /// The rule fired no lie, so there is nothing to ask.
    #[error("the rule fired no lie")]
    NoLie,
    /// The lie asked for something that the pipeline cannot do where it asked.
    #[error("the lie asked to {}, which it cannot ask here", .instruction.phrase())]
    NotFollowed { instruction: Instruction },
    /// Another lie answered the call before this one was asked.
    #[error("the lie of {by:?} answered the call before")]
    AnsweredBefore { by: String },
    /// The lie asked to forward a request that is not the request of this call.
    #[error("the lie asked to forward the {api} request {correlation_id}, which is another call")]
    ForwardsAnotherCall { api: Api, correlation_id: i32 },
    /// What the lie left of the request does not decode.
    #[error("what the lie left of the request is unusable: {error}")]
    UnreadableRemainder { error: SurgeryError },
    /// The lie asked to send a response that does not answer the request of this call.
    #[error("the lie asked to send the {api} response {correlation_id}, which answers another call")]
    AnswersAnotherCall { api: Api, correlation_id: i32 },
    /// The call has no response: a Produce with `acks=0`.
    #[error("the call has no response: a Produce with acks=0 expects none")]
    NoResponse,
    /// The call ended before the stage of the lie came.
    #[error("the call ended before the lie could be told")]
    CallEnded,
    /// The lie only throttles, and no surgery writes the throttle of an answer yet.
    #[error("the lie only throttles, and {THROTTLE_UNWRITTEN}")]
    ThrottleUnwritten,
}

impl From<LieError> for UntoldLie {
    fn from(error: LieError) -> Self {
        Self::Failed { error }
    }
}

/// What went wrong inside the pipeline while it handled a call, apart from a lie it could not tell.
///
/// The call goes on as far as it can without what failed, and its explanation keeps the message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StageFailure {
    /// The header of the request names the client in a way the session cannot keep.
    #[error("the client is not identified: {error}")]
    Unidentified { error: SessionError },
    /// The body of the request does not decode, so no rule saw the call.
    #[error("no rule saw the call: {error}")]
    UnreadableRequest { error: SurgeryError },
    /// The fields of the body do not decode.
    #[error("the fields of the {api} request cannot be read: {error}")]
    UnreadableRequestFields { api: Api, error: DecodeError },
    /// The response of the backend cannot carry the API and the correlation ID of the client's request.
    #[error("the response of the backend does not read as a {api} response: {error}")]
    UnrestoredResponse { api: Api, error: DecodeError },
    /// The response of the backend cannot be read, so the rules saw no truth.
    #[error("the rules saw no truth: {error}")]
    UnreadableTruth { error: SurgeryError },
    /// The topics that the response of the backend names with their ID cannot be read.
    #[error("no topic was learned: {error}")]
    UnreadableTopicNames { error: SurgeryError },
    /// The member ID that the response of the backend gives the client cannot be read.
    #[error("no member ID was learned from the {api} response: {error}")]
    UnreadableMemberId { api: Api, error: DecodeError },
    /// The response that the client received cannot be read, so the record shows no answer.
    #[error("the record shows no answer: {error}")]
    UnreadableDelivery { error: SurgeryError },
    /// The answer should carry a throttle, and no surgery writes one yet.
    #[error("the answer carries no throttle of {millis} ms: {THROTTLE_UNWRITTEN}")]
    ThrottleUnwritten { millis: u64 },
    /// A lie changed the request that the backend got, and failed to make the response fit the client's request.
    /// The lie did act, so it stays told.
    #[error("the lie of {rule:?} changed the request, and could not make the response fit it: {error}")]
    UnmergedLie { rule: String, error: UntoldLie },
    /// A Produce whose body does not decode does not say its `acks`, so nothing says whether a response comes.
    #[error("the Produce request does not say whether it expects a response: {error}")]
    UnknownAcks { error: SurgeryError },
    /// A moment of the call does not follow the one before it, so the timing of the record lacks it.
    #[error("a moment of the call is missing: {error}")]
    Mistimed { error: CallTimingError },
    /// The fields of the call do not make a record, so the record only names its connection.
    #[error("the record keeps no field of the call: {error}")]
    Unrecorded { error: CallError },
    /// The explanation of the call contradicts itself.
    #[error("the explanation is incomplete: {error}")]
    Unexplained { error: UnexplainedRule },
    /// The backend answered the call with something that it could not hand over as a response.
    #[error("the backend answered with what is no response: {reason}")]
    UnreadableAnswer { reason: String },
    /// The backend had no answer for `call`, so the session ended.
    #[error("the backend has no answer for call {call}: {reason}")]
    BackendFailed { call: CallId, reason: String },
    /// The backend closed the session.
    #[error("the backend closed the session: {reason}")]
    BackendClosed { reason: String },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_lie::LieKind;
    use gaslightmq_wire::Request;
    use gaslightmq_wire::messages::{HeartbeatRequest, ProduceRequest};

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
    fn a_stage_failure_writes_what_the_surgery_or_the_codec_said_once() {
        let unknown = SurgeryError::UnknownApi {
            api: Api::from_wire(9999, 0),
        };
        // A Heartbeat is no Produce.
        let heartbeat = Request::new(4, 7, None, &HeartbeatRequest::default()).unwrap();
        let of_another_api = heartbeat.body::<ProduceRequest>().unwrap_err();
        let cases = [
            (
                StageFailure::UnreadableRequest { error: unknown.clone() },
                "no rule saw the call: a request of api 9999 v0 cannot be edited: its messages are unknown",
            ),
            (
                StageFailure::UnreadableRequestFields {
                    api: Api::from_wire(0, 9),
                    error: of_another_api.clone(),
                },
                "the fields of the Produce v9 request cannot be read: \
                 cannot decode request body: it is of api key 12, and it was read as api key 0",
            ),
            (
                StageFailure::UnrestoredResponse {
                    api: Api::from_wire(0, 9),
                    error: of_another_api.clone(),
                },
                "the response of the backend does not read as a Produce v9 response: \
                 cannot decode request body: it is of api key 12, and it was read as api key 0",
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
                StageFailure::UnreadableMemberId {
                    api: Api::from_wire(11, 9),
                    error: of_another_api,
                },
                "no member ID was learned from the JoinGroup v9 response: \
                 cannot decode request body: it is of api key 12, and it was read as api key 0",
            ),
        ];

        for (failure, message) in cases {
            assert_eq!(failure.to_string(), message);
            assert!(failure.source().is_none(), "{failure}");
        }
    }

    #[test]
    fn a_stage_failure_names_what_failed_and_writes_the_cause_once() {
        let cases = [
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
                StageFailure::UnreadableAnswer {
                    reason: "cannot decode response header: it is cut short at 3 bytes".to_owned(),
                },
                "the backend answered with what is no response: \
                 cannot decode response header: it is cut short at 3 bytes",
            ),
            (
                StageFailure::BackendFailed {
                    call: "#7.3".parse().unwrap(),
                    reason: "the connection to the cluster ended before the answer arrived".to_owned(),
                },
                "the backend has no answer for call #7.3: the connection to the cluster ended before the answer arrived",
            ),
            (
                StageFailure::BackendClosed {
                    reason: "endpoint `broker 2` accepts no connections any more".to_owned(),
                },
                "the backend closed the session: endpoint `broker 2` accepts no connections any more",
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
