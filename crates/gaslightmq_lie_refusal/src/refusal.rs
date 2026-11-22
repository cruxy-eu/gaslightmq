use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::{Api, Response};
use gaslightmq_wire_edit::prelude::*;

use crate::{RefusedRequest, RefusedRequestError};

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
/// let lies = Lies::of_every_build().with(Refusal)?;
///
/// // A rule refuses a Heartbeat, so the consumer believes that its group rebalances.
/// let heartbeat = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default())?;
/// let request = EditableRequest::decode(&heartbeat, &TopicNames::new())?;
/// let rebalance = LieDefinition::from(RefusalLie::new(ErrorCode::REBALANCE_IN_PROGRESS)).on(Target::Call);
///
/// // The backend never sees the Heartbeat.
/// let Effect::Answer(response) = lies.before_forwarding(&rebalance, &request)? else {
///     panic!("a refusal of the whole call answers it");
/// };
/// let body: HeartbeatResponse = response.body()?;
/// assert_eq!(ErrorCode::from_wire(body.error_code), Some(ErrorCode::REBALANCE_IN_PROGRESS));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # A refusal that cannot be told
///
/// A refusal refuses exactly its target, or it fails and changes nothing.
/// The behaviour returns the [`LieError`] of the contract, which keeps the reason of the [`RefusedRequestError`].
///
/// | What is wrong | The error |
/// |---|---|
/// | The lie is of another kind. | [`LieError::OtherKind`] |
/// | The response does not decode. | [`LieError::Unreadable`] |
/// | The request to forward or the response does not encode. | [`LieError::Unwritable`] |
/// | The refusal targets parts that the request does not name, as on an API without parts. | [`LieError::PartsNotInRequest`] |
/// | The refusal targets the whole call, and the response has no place for an error code. | [`LieError::NoPlaceForError`] |
/// | The response answers another request. | [`LieError::ResponseOfAnotherCall`] |
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
            .map_err(|error| Self::untold(request.api(), error))
    }

    /// Why the refusal of a request of `api` could not be told, in the words of the contract.
    fn untold(api: Api, error: RefusedRequestError) -> LieError {
        let kind = LieKind::Refusal;
        match error {
            RefusedRequestError::PartsNotInRequest { api, parts } => LieError::PartsNotInRequest { kind, api, parts },
            RefusedRequestError::NoPlaceForError { api } => LieError::NoPlaceForError { kind, api },
            RefusedRequestError::SurgeryFailed { error } => match error {
                EditableRequestError::UnreadableRequest { error, .. }
                | EditableRequestError::UnreadableResponse { error, .. } => LieError::Unreadable { kind, error },
                EditableRequestError::UnwritableRequest { error, .. }
                | EditableRequestError::UnwritableResponse { error, .. } => LieError::Unwritable { kind, error },
                EditableRequestError::ResponseOfAnotherCall {
                    api,
                    correlation_id,
                    answered_api,
                    answered_correlation_id,
                } => LieError::ResponseOfAnotherCall {
                    kind,
                    api,
                    correlation_id,
                    answered_api,
                    answered_correlation_id,
                },
                // A decoded request has messages, and a refused request rules out that nothing failed.
                // Surgery has no other failure today.
                _ => LieError::ApiNotSupported { kind, api },
            },
        }
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
        Self::refused(lie, request)?
            .before_forwarding()
            .map_err(|error| Self::untold(request.api(), error))
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
            .map_err(|error| Self::untold(request.api(), error))
    }
}
