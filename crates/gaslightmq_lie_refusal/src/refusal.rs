use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::Response;
use gaslightmq_wire_edit::prelude::*;

use crate::RefusedRequest;

/// The behaviour of a refusal: the client is told that the request failed.
///
/// It tells every [`RefusalLie`], on a request of every API that `gaslightmq_wire` has messages for.
/// The pipeline hands it the request decoded, so it decodes none itself.
/// The engine registers it next to the kinds of every build.
///
/// | Asked | The refusal refuses | It asks to |
/// |---|---|---|
/// | [Before forwarding](Lie::before_forwarding) | The whole call | [Answer](Effect::Answer) with the response in which the call failed. |
/// | | Some parts of several | [Forward](Effect::Forward) the request without those parts. |
/// | | Every part that is left | [Answer](Effect::Answer) with the response of those parts. |
/// | [After the response](Lie::after_response) | The whole call | [Answer](Effect::Answer) with the response in which the call failed. |
/// | | Some parts | [Answer](Effect::Answer) with the response and the error on each of those parts. |
///
/// The method says the stage, and the behaviour does not read it from the definition.
/// [`RefusedRequest`] does the work, and says what each row means.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_lie_refusal::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
/// use gaslightmq_wire_edit::prelude::*;
///
/// let lies = Lies::of_every_build().with(Refusal).expect("a kind that is new to the set");
///
/// // A rule refuses a Heartbeat, so the consumer believes that its group rebalances.
/// let heartbeat = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default()).expect("a request");
/// let request = EditableRequest::decode(&heartbeat, &TopicNames::new()).expect("a request that decodes");
/// let rebalance = LieDefinition::from(RefusalLie::new(ErrorCode::REBALANCE_IN_PROGRESS)).on(Target::Call);
///
/// // The backend never sees the Heartbeat.
/// let Effect::Answer(response) = lies.before_forwarding(&rebalance, &request)? else {
///     panic!("a refusal of the whole call answers it");
/// };
/// let body: HeartbeatResponse = response.body().expect("a body that decodes");
/// assert_eq!(ErrorCode::from_wire(body.error_code), Some(ErrorCode::REBALANCE_IN_PROGRESS));
/// # Ok::<(), LieError>(())
/// ```
///
/// # A refusal that cannot be told
///
/// A refusal refuses exactly its target, or it fails and changes nothing.
/// The behaviour returns the [`LieError`] of the contract, as [`RefusedRequest`] does, and no other error.
///
/// | What is wrong | The error |
/// |---|---|
/// | The lie is of another kind. | [`LieError::OtherKind`] |
/// | The response does not decode or answers another request, or the request to forward or the response does not encode. | [`LieError::CallNotRewritten`] |
/// | The refusal targets parts that the request does not name, as on an API without parts. | [`LieError::PartsNotInRequest`] |
/// | The refusal targets the whole call, and the response has no place for an error code. | [`LieError::NoPlaceForError`] |
///
/// # Topics by ID
///
/// The contract hands a behaviour the request as the pipeline decoded it, with the topics it knows by ID.
/// A Fetch that names `orders.eu` by its ID therefore has `orders.eu/3` as a part, and a refusal of that part is told.
/// A topic with an unknown ID is no part, and a refusal that targets it [cannot be told](LieError::PartsNotInRequest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Refusal;

impl Refusal {
    /// `request` with what `lie` refuses of it.
    fn refused(lie: &TargetedLie, request: &EditableRequest) -> Result<RefusedRequest, LieError> {
        let LieDefinition::Refusal(refusal) = lie.lie() else {
            return Err(LieError::OtherKind {
                tells: LieKind::Refusal,
                lie: lie.lie().kind(),
            });
        };
        RefusedRequest::new(request.clone(), lie.target(), refusal.error)
    }
}

impl Lie for Refusal {
    fn kind(&self) -> LieKind {
        LieKind::Refusal
    }

    /// Asks to answer the call without the backend, or to forward what the refusal leaves of the request.
    ///
    /// See [`RefusedRequest::before_forwarding`].
    fn before_forwarding(&self, lie: &TargetedLie, request: &EditableRequest) -> Result<Effect, LieError> {
        Self::refused(lie, request)?.before_forwarding()
    }

    /// Asks to answer with the response in which everything the refusal targets failed.
    ///
    /// The pipeline asks it of a refusal after the response, and again of a refusal that forwarded the rest of its request.
    /// Both get the same answer: the error stands on every refused part, also on the ones the backend never saw.
    /// See [`RefusedRequest::after_response`].
    fn after_response(
        &self,
        lie: &TargetedLie,
        request: &EditableRequest,
        response: &Response,
    ) -> Result<Effect, LieError> {
        Self::refused(lie, request)?
            .after_response(response)
            .map(Effect::Answer)
    }
}
