use std::collections::BTreeSet;
use std::fmt;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::{Api, Response};
use gaslightmq_wire_edit::prelude::*;

/// A request with what one refusal refuses of it: the whole call, or some of its parts, with one error.
///
/// It does the work of a refusal at both stages of a call.
///
/// | Method | Stage | Gives |
/// |---|---|---|
/// | [`before_forwarding`](Self::before_forwarding) | Before forwarding | The request without the refused parts, or the answer when nothing is left to forward. |
/// | [`after_response`](Self::after_response) | After the response | The response in which everything the refusal targets failed. |
///
/// A refused request refuses exactly its target.
/// [`new`](Self::new) only builds one when the request names every part of the target,
/// and when the response has a place for the error.
/// A refusal therefore never fails more than its rule matched, and never less.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_lie_refusal::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
/// use gaslightmq_wire::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
/// use gaslightmq_wire::messages::{ListOffsetsRequest, ListOffsetsResponse};
/// use gaslightmq_wire::protocol::StrBytes;
/// use gaslightmq_wire_edit::prelude::*;
///
/// // A client asks for the end of two partitions.
/// let orders = || StrBytes::from_static_str("orders.eu").into();
/// let partition = |index| ListOffsetsPartition::default().with_partition_index(index);
/// let topic = ListOffsetsTopic::default().with_name(orders()).with_partitions(vec![partition(3), partition(4)]);
/// let request = Request::new(7, 42, Some("order-ingest"), &ListOffsetsRequest::default().with_topics(vec![topic]))?;
/// let asked = EditableRequest::decode(&request, &TopicNames::new())?;
///
/// // A rule refuses `orders.eu/3`.
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// let target = Target::of_parts([three.clone()]).expect("one part");
/// let refused = RefusedRequest::new(asked.clone(), &target, ErrorCode::NOT_LEADER_OR_FOLLOWER)?;
///
/// // Before forwarding, the backend only gets the other partition.
/// let Effect::Forward(forwarded) = refused.before_forwarding()? else {
///     panic!("one partition is left to forward");
/// };
/// assert_eq!(EditableRequest::decode(&forwarded, &TopicNames::new())?.parts(), [four.clone()]);
///
/// // The backend answers for that one, and the refusal joins its answer.
/// let end = ListOffsetsPartitionResponse::default().with_partition_index(4).with_offset(120);
/// let answered = ListOffsetsTopicResponse::default().with_name(orders()).with_partitions(vec![end]);
/// let truth = forwarded.response(&ListOffsetsResponse::default().with_topics(vec![answered]))?;
///
/// let delivered = refused.after_response(&truth)?;
/// assert_eq!(
///     asked.parts_of(&delivered)?,
///     [PartFields::new(three).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER), PartFields::new(four).succeeded()],
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Topics by ID
///
/// A refused request takes the request as an [`EditableRequest`], which names its parts.
/// Whoever decodes it gives it the [`TopicNames`] it knows,
/// so a Fetch that names `orders.eu` by its ID has `orders.eu/3` as a part.
/// A topic with an unknown ID is no part, and a refusal that targets it [cannot be told](RefusedRequestError::PartsNotInRequest).
#[derive(Debug, Clone)]
pub struct RefusedRequest {
    asked: EditableRequest,
    failures: Failures,
}

impl RefusedRequest {
    /// `asked`, of which a refusal with `error` refuses `target`.
    ///
    /// `asked` is the request as it stands at the stage the refusal acts at:
    /// before forwarding it is what the lies before this one left of the request,
    /// and after the response it is the request as the client sent it.
    ///
    /// # Errors
    ///
    /// - [`RefusedRequestError::PartsNotInRequest`] when `target` names a part that `asked` does not name.
    ///   A request of an API without parts names none, so it is only refused as a [whole call](Target::Call).
    /// - [`RefusedRequestError::NoPlaceForError`] when `target` is the whole call,
    ///   and the response to `asked` has no place for an error code.
    /// - [`RefusedRequestError::SurgeryFailed`] when the response to `asked` does not encode.
    pub fn new(asked: EditableRequest, target: &Target, error: ErrorCode) -> Result<Self, RefusedRequestError> {
        let refused = Self {
            asked,
            failures: Failures::of_target(target, error),
        };
        match target {
            Target::Call => refused.check_place_for_error(error)?,
            Target::Parts(parts) => refused.check_names(parts)?,
        }
        Ok(refused)
    }

    /// The request, with its parts.
    pub fn asked(&self) -> &EditableRequest {
        &self.asked
    }

    /// The API of the request.
    pub fn api(&self) -> Api {
        self.asked.api()
    }

    /// What the client is told failed: the whole call, or each refused part, with the error of the refusal.
    pub fn failures(&self) -> &Failures {
        &self.failures
    }

    /// Whether the refusal refuses the whole call, and not some parts of it.
    pub fn refuses_whole_call(&self) -> bool {
        self.failures.of_call().is_some()
    }

    /// What the refusal asks for before the backend sees the request.
    ///
    /// | The refusal refuses | It asks to |
    /// |---|---|
    /// | The whole call | [Answer](Effect::Answer) with the response in which the call failed. The backend never sees the request. |
    /// | Some parts, and the request asks for more | [Forward](Effect::Forward) the request without those parts. |
    /// | Some parts, and the request asks for nothing else | [Answer](Effect::Answer) with the response of those parts. The backend never sees the request. |
    ///
    /// After a forward, the response of the backend lacks the refused parts,
    /// and [`after_response`](Self::after_response) adds them.
    ///
    /// The refusal that removes the last part of a request is the one that answers.
    /// Its answer holds its own parts only.
    /// The refusals that forwarded before it add theirs with [`after_response`](Self::after_response), as after any other response.
    ///
    /// # Errors
    ///
    /// [`RefusedRequestError::SurgeryFailed`] when the request to forward or the response does not encode.
    pub fn before_forwarding(&self) -> Result<Effect, RefusedRequestError> {
        if self.refuses_whole_call() {
            return self.answer();
        }
        match self.asked.without(self.failures.parts().map(|(part, _)| part))? {
            Remainder::Rest(rest) => Ok(Effect::Forward(rest)),
            Remainder::Nothing => self.answer(),
            // The request names every refused part, so one of them always leaves it.
            Remainder::Whole => Ok(Effect::Nothing),
        }
    }

    /// `response` with the refusal in it: everything the refusal targets failed, with its error.
    ///
    /// | The refusal refuses | The result |
    /// |---|---|
    /// | The whole call | The response in which the call failed. Nothing of `response` is left. |
    /// | Some parts | `response` with the error on each of those parts. Everything else stays as `response` has it. |
    ///
    /// A refused part gets the error whatever `response` says about it:
    /// the error replaces what the backend answered for the part,
    /// and the part is added when the backend never saw it.
    /// The parts come in the order of the request.
    ///
    /// The same method therefore serves a refusal after the response,
    /// and a refusal that [forwarded](Self::before_forwarding) the rest of its request.
    /// It also gives the same response when it runs again on its own result.
    ///
    /// The last refusal to run decides a part.
    /// A refusal of the whole call writes over every part,
    /// so a refusal that keeps a part of its own runs after it.
    ///
    /// # Errors
    ///
    /// [`RefusedRequestError::SurgeryFailed`] when `response` answers another request,
    /// when its body does not decode, or when the result does not encode.
    pub fn after_response(&self, response: &Response) -> Result<Response, RefusedRequestError> {
        Ok(self.asked.merge(response, &self.failures)?)
    }

    /// Asks to answer with the response that nobody asks a backend for.
    fn answer(&self) -> Result<Effect, RefusedRequestError> {
        Ok(Effect::Answer(self.asked.error_response(&self.failures)?))
    }

    /// Checks that the request names every part of `parts`.
    fn check_names(&self, parts: &PartSet) -> Result<(), RefusedRequestError> {
        let named: BTreeSet<&Part> = self.asked.parts().iter().collect();
        let missing = parts.iter().filter(|part| !named.contains(part)).cloned();
        // A set always has a part, so there is one when the request lacks a part.
        match PartSet::new(missing) {
            Ok(parts) => Err(RefusedRequestError::PartsNotInRequest { api: self.api(), parts }),
            Err(_) => Ok(()),
        }
    }

    /// Checks that the response in which the whole call failed says so.
    ///
    /// A response says its error when it reads differently with another error.
    /// A response that reads the same whatever the error has no place for one.
    fn check_place_for_error(&self, error: ErrorCode) -> Result<(), RefusedRequestError> {
        let other = if error == ErrorCode::UNKNOWN_SERVER_ERROR {
            ErrorCode::REQUEST_TIMED_OUT
        } else {
            ErrorCode::UNKNOWN_SERVER_ERROR
        };
        let failed = self.asked.error_response(&self.failures)?;
        let failed_otherwise = self.asked.error_response(&Failures::whole_call(other))?;
        if failed.frame() == failed_otherwise.frame() {
            Err(RefusedRequestError::NoPlaceForError { api: self.api() })
        } else {
            Ok(())
        }
    }
}

/// Why a request cannot be refused as a refusal asks.
///
/// A refusal that cannot be told changes nothing about its call.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie_refusal::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
/// use gaslightmq_wire_edit::prelude::*;
///
/// // A Heartbeat names no partition, so no partition of it can be refused.
/// let request = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default())?;
/// let asked = EditableRequest::decode(&request, &TopicNames::new())?;
/// let target = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
///
/// let error = RefusedRequest::new(asked, &target, ErrorCode::NOT_LEADER_OR_FOLLOWER).unwrap_err();
/// assert_eq!(error.to_string(), "the Heartbeat v4 request does not name orders.eu/3");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusedRequestError {
    /// The refusal targets parts that the request does not name.
    ///
    /// A request of an API without parts names none.
    /// A request that names a topic by an unknown ID does not name the parts of that topic either.
    /// The refusal is not told on the other parts of its target, and never on the whole call instead.
    PartsNotInRequest { api: Api, parts: PartSet },
    /// The refusal targets the whole call, and the response has no place for an error code.
    ///
    /// The request asks for nothing by name, and its API has no error code in the response itself at this version.
    /// A Metadata request for every topic is one, up to version 12.
    /// A response without an error would tell the client that the call succeeded with nothing in it.
    NoPlaceForError { api: Api },
    /// The request to forward or the response could not be made.
    SurgeryFailed { error: EditableRequestError },
}

impl fmt::Display for RefusedRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartsNotInRequest { api, parts } => {
                let parts: Vec<String> = parts.iter().map(ToString::to_string).collect();
                write!(f, "the {api} request does not name {}", parts.join(", "))
            }
            Self::NoPlaceForError { api } => {
                write!(f, "the response to this {api} request has no place for an error code")
            }
            Self::SurgeryFailed { error } => write!(f, "the refusal cannot be told: {error}"),
        }
    }
}

/// The message says everything, also the error it holds, so the error has no source.
impl std::error::Error for RefusedRequestError {}

/// Surgery on the request or on the response failed.
impl From<EditableRequestError> for RefusedRequestError {
    fn from(error: EditableRequestError) -> Self {
        Self::SurgeryFailed { error }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_wire::WireError;

    use super::*;

    #[test]
    fn a_refused_request_error_says_everything_in_one_message() {
        let produce = Api::from_wire(0, 9);
        let parts = PartSet::new(["orders.eu/4".parse().unwrap(), "orders.eu/3".parse().unwrap()]).unwrap();
        let cases = [
            (
                RefusedRequestError::PartsNotInRequest { api: produce, parts },
                "the Produce v9 request does not name orders.eu/3, orders.eu/4",
            ),
            (
                RefusedRequestError::NoPlaceForError {
                    api: Api::from_wire(3, 9),
                },
                "the response to this Metadata v9 request has no place for an error code",
            ),
            (
                RefusedRequestError::SurgeryFailed {
                    error: EditableRequestError::UnwritableResponse {
                        api: produce,
                        error: WireError::UnknownApi { api_key: 0 },
                    },
                },
                "the refusal cannot be told: the Produce v9 response cannot be written: unknown api key 0",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }

    #[test]
    fn an_error_of_surgery_becomes_an_error_of_the_refused_request() {
        let error = EditableRequestError::NothingFailed {
            api: Api::from_wire(0, 9),
        };

        assert_eq!(
            RefusedRequestError::from(error.clone()),
            RefusedRequestError::SurgeryFailed { error }
        );
    }
}
