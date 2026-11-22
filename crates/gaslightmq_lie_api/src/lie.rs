use std::fmt;

use gaslightmq_field::PartSet;
use gaslightmq_lie::prelude::*;
use gaslightmq_wire::{Api, Request, Response, WireError};

use crate::{Effect, Instruction};

/// The behaviour of one kind of lie: what a lie of that kind asks the pipeline for.
///
/// A definition says what the client is told, and it is data.
/// An implementation of this trait does the telling for every definition of its [kind](Self::kind).
/// It reads the request or the response, and it returns an [`Effect`].
/// It keeps no state between calls, so one value serves every session.
///
/// # When the pipeline asks
///
/// The pipeline first combines the lies that fired on a call.
/// It then asks every lie that applies, once at the stage the lie acts at,
/// with the target the lie holds there.
///
/// | The pipeline asks | When | With |
/// |---|---|---|
/// | [`before_forwarding`](Self::before_forwarding) | The lie acts before forwarding. | The request as the lies before it left it. |
/// | [`after_response`](Self::after_response) | The lie acts after the response. | The request of the client, and the response as the lies before it left it. |
/// | [`after_response`](Self::after_response), again | The lie asked to [forward](Effect::Forward) another request. | The same, so that the lie makes the response fit the request of the client. |
///
/// A lie of the response pass acts after the response, whatever stage its definition names.
/// The method says the stage, and a behaviour does not read it from the definition.
///
/// # What a kind implements
///
/// A kind implements the method of every stage it acts at.
/// The other method stays as it is here, and fails with [`LieError::NoBehaviourAtStage`]:
/// a rewrite has no behaviour before forwarding, and a silence has none after the response.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
///
/// /// A kind that answers every Heartbeat itself.
/// struct Refusal;
///
/// impl Lie for Refusal {
///     fn kind(&self) -> LieKind {
///         LieKind::Refusal
///     }
///
///     fn before_forwarding(&self, lie: &TargetedLie, request: &Request) -> Result<Effect, LieError> {
///         let LieDefinition::Refusal(refusal) = lie.lie() else {
///             return Err(LieError::OtherKind { tells: self.kind(), lie: lie.lie().kind() });
///         };
///         let body = HeartbeatResponse::default().with_error_code(refusal.error.value());
///         let response = request.response(&body).map_err(|error| LieError::Unwritable { kind: self.kind(), error })?;
///         Ok(Effect::Answer(response))
///     }
/// }
///
/// let request = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default())?;
/// let lie = LieDefinition::from(RefusalLie::new(ErrorCode::REBALANCE_IN_PROGRESS)).on(Target::Call);
///
/// // Before forwarding, the backend never sees the Heartbeat.
/// let Effect::Answer(response) = Refusal.before_forwarding(&lie, &request)? else {
///     panic!("the refusal answers the call itself");
/// };
/// assert_eq!(response.body::<HeartbeatResponse>()?.error_code, 27);
///
/// // This kind has no behaviour after the response.
/// let error = Refusal.after_response(&lie, &request, &response).unwrap_err();
/// assert_eq!(error.to_string(), "refusal has no behaviour after the response");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Shared between sessions
///
/// The engine registers one value per kind, and every session asks it.
/// A behaviour is therefore `Send` and `Sync`.
pub trait Lie: Send + Sync {
    /// The kind of lie this tells.
    fn kind(&self) -> LieKind;

    /// What `lie` asks for before the backend sees `request`.
    ///
    /// `lie` holds the target the lie applies to at this stage, which can be less than its selector matched.
    /// `request` is the request as the lies that were asked before this one left it.
    ///
    /// # Errors
    ///
    /// [`LieError`] when the lie cannot be told.
    /// The pipeline then forwards the call untouched, and the explanation of the call says why.
    #[expect(
        unused_variables,
        reason = "a kind that acts at this stage reads the same parameters, under the same names"
    )]
    fn before_forwarding(&self, lie: &TargetedLie, request: &Request) -> Result<Effect, LieError> {
        Err(LieError::NoBehaviourAtStage {
            kind: self.kind(),
            stage: Stage::BeforeForwarding,
        })
    }

    /// What `lie` asks for once the backend answered.
    ///
    /// `lie` holds the target the lie applies to at this stage.
    /// `request` is the request as the client sent it.
    /// `response` is the response as the lies that were asked before this one left it:
    /// the truth, or what a lie [answered](Effect::Answer) instead.
    ///
    /// # Errors
    ///
    /// [`LieError`] when the lie cannot be told.
    /// The pipeline then sends the response untouched, and the explanation of the call says why.
    #[expect(
        unused_variables,
        reason = "a kind that acts at this stage reads the same parameters, under the same names"
    )]
    fn after_response(&self, lie: &TargetedLie, request: &Request, response: &Response) -> Result<Effect, LieError> {
        Err(LieError::NoBehaviourAtStage {
            kind: self.kind(),
            stage: Stage::AfterResponse,
        })
    }
}

/// Why a lie could not be told.
///
/// A lie that fails changes nothing about its call.
/// The pipeline passes the call on untouched and writes the error into the explanation,
/// so that a mistake of GaslightMQ never looks like a lie.
///
/// Every variant names the kind of lie, so the message stands on its own in an explanation.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Api;
///
/// let error = LieError::ApiNotSupported { kind: LieKind::Refusal, api: Api::from_wire(9999, 4) };
/// assert_eq!(error.to_string(), "refusal cannot be told on api 9999 v4");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LieError {
    /// No behaviour is registered for the kind of the lie.
    /// The build left the kind out, or nobody registered it.
    NotRegistered { kind: LieKind },
    /// The behaviour tells one kind of lie, and it was asked to tell a lie of another kind.
    OtherKind { tells: LieKind, lie: LieKind },
    /// The kind has no behaviour at the stage it was asked at.
    NoBehaviourAtStage { kind: LieKind, stage: Stage },
    /// The lie asked for something that its class or the stage does not allow.
    /// See [`Instruction::fits`] and [`Instruction::allowed_at`].
    InstructionNotAllowed {
        kind: LieKind,
        stage: Stage,
        instruction: Instruction,
    },
    /// The kind has no way to tell its lie on calls of this API.
    ApiNotSupported { kind: LieKind, api: Api },
    /// The lie could not read the request or the response.
    Unreadable { kind: LieKind, error: WireError },
    /// The lie could not write the request or the response it asks for.
    Unwritable { kind: LieKind, error: WireError },
    /// The lie targets parts that the request does not name.
    ///
    /// A request of an API without parts names none.
    /// The lie is not told on the other parts of its target, and never on the whole call instead:
    /// it changes exactly its target, or nothing.
    PartsNotInRequest { kind: LieKind, api: Api, parts: PartSet },
    /// The lie tells the client an error, and the response to the request has no place for an error code.
    ///
    /// The request asks for nothing by name, and its API has no error code in the response itself at this version.
    /// A Metadata request for every topic is one, on most versions.
    NoPlaceForError { kind: LieKind, api: Api },
    /// The response the lie was given answers another request than the one it was given:
    /// its API or its correlation ID differs.
    ResponseOfAnotherCall {
        kind: LieKind,
        api: Api,
        correlation_id: i32,
        answered_api: Api,
        answered_correlation_id: i32,
    },
}

impl fmt::Display for LieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRegistered { kind } => write!(f, "no behaviour is registered for {kind}"),
            Self::OtherKind { tells, lie } => write!(f, "the behaviour of {tells} cannot tell a {lie} lie"),
            Self::NoBehaviourAtStage { kind, stage } => write!(f, "{kind} has no behaviour {}", stage.phrase()),
            Self::InstructionNotAllowed {
                kind,
                stage,
                instruction,
            } => write!(
                f,
                "a {kind} lie cannot ask to {} {}",
                instruction.phrase(),
                stage.phrase()
            ),
            Self::ApiNotSupported { kind, api } => write!(f, "{kind} cannot be told on {api}"),
            Self::Unreadable { kind, error } => write!(f, "{kind} cannot read the call: {error}"),
            Self::Unwritable { kind, error } => write!(f, "{kind} cannot rewrite the call: {error}"),
            Self::PartsNotInRequest { kind, api, parts } => {
                let parts: Vec<String> = parts.iter().map(ToString::to_string).collect();
                write!(
                    f,
                    "{kind} targets parts that the {api} request does not name: {}",
                    parts.join(", ")
                )
            }
            Self::NoPlaceForError { kind, api } => write!(
                f,
                "{kind} cannot be told on this {api} request: its response has no place for an error code"
            ),
            Self::ResponseOfAnotherCall {
                kind,
                api,
                correlation_id,
                answered_api,
                answered_correlation_id,
            } => write!(
                f,
                "{kind} was given the {answered_api} response {answered_correlation_id}, \
                 which does not answer the {api} request {correlation_id}"
            ),
        }
    }
}

/// The message says everything, also the error it holds, so the error has no source.
impl std::error::Error for LieError {}

/// The definition of `lie`, when `lie` is of the kind that a behaviour tells.
///
/// # Errors
///
/// [`LieError::OtherKind`] when `lie` is of another kind than `tells`.
pub(crate) fn definition_of(tells: LieKind, lie: &TargetedLie) -> Result<&LieDefinition, LieError> {
    let definition = lie.lie();
    if definition.kind() == tells {
        Ok(definition)
    } else {
        Err(LieError::OtherKind {
            tells,
            lie: definition.kind(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};

    use super::*;

    /// A kind that implements neither stage.
    struct Unfinished;

    impl Lie for Unfinished {
        fn kind(&self) -> LieKind {
            LieKind::Topology
        }
    }

    fn heartbeat() -> Request {
        Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
    }

    #[test]
    fn a_kind_has_no_behaviour_at_a_stage_it_does_not_implement() {
        let request = heartbeat();
        let response = request.response(&HeartbeatResponse::default()).unwrap();
        let lie = LieDefinition::Silence.on(Target::Call);

        assert_eq!(
            Unfinished.before_forwarding(&lie, &request).unwrap_err(),
            LieError::NoBehaviourAtStage {
                kind: LieKind::Topology,
                stage: Stage::BeforeForwarding
            }
        );
        assert_eq!(
            Unfinished.after_response(&lie, &request, &response).unwrap_err(),
            LieError::NoBehaviourAtStage {
                kind: LieKind::Topology,
                stage: Stage::AfterResponse
            }
        );
    }

    #[test]
    fn a_behaviour_only_takes_the_definition_of_its_kind() {
        let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(Target::Call);

        assert_eq!(definition_of(LieKind::Refusal, &refusal), Ok(refusal.lie()));
        assert_eq!(
            definition_of(LieKind::Timing, &refusal),
            Err(LieError::OtherKind {
                tells: LieKind::Timing,
                lie: LieKind::Refusal
            })
        );
    }

    #[test]
    fn a_lie_error_says_everything_in_one_message() {
        let wire = WireError::WrongApi {
            expected: 0,
            actual: 12,
        };
        let cases = [
            (
                LieError::NotRegistered { kind: LieKind::HangUp },
                "no behaviour is registered for hang-up",
            ),
            (
                LieError::OtherKind {
                    tells: LieKind::Timing,
                    lie: LieKind::Refusal,
                },
                "the behaviour of timing cannot tell a refusal lie",
            ),
            (
                LieError::NoBehaviourAtStage {
                    kind: LieKind::Silence,
                    stage: Stage::AfterResponse,
                },
                "silence has no behaviour after the response",
            ),
            (
                LieError::InstructionNotAllowed {
                    kind: LieKind::Refusal,
                    stage: Stage::BeforeForwarding,
                    instruction: Instruction::Close,
                },
                "a refusal lie cannot ask to close the connection before forwarding",
            ),
            (
                LieError::InstructionNotAllowed {
                    kind: LieKind::Topology,
                    stage: Stage::AfterResponse,
                    instruction: Instruction::Forward,
                },
                "a topology lie cannot ask to forward another request after the response",
            ),
            (
                LieError::ApiNotSupported {
                    kind: LieKind::Refusal,
                    api: Api::from_wire(9999, 4),
                },
                "refusal cannot be told on api 9999 v4",
            ),
            (
                LieError::Unreadable {
                    kind: LieKind::Refusal,
                    error: wire.clone(),
                },
                "refusal cannot read the call: body of api key 12 read as api key 0",
            ),
            (
                LieError::Unwritable {
                    kind: LieKind::Data,
                    error: wire,
                },
                "data cannot rewrite the call: body of api key 12 read as api key 0",
            ),
            (
                LieError::PartsNotInRequest {
                    kind: LieKind::Refusal,
                    api: Api::from_wire(0, 9),
                    parts: PartSet::new(["orders.eu/4".parse().unwrap(), "orders.eu/3".parse().unwrap()]).unwrap(),
                },
                "refusal targets parts that the Produce v9 request does not name: orders.eu/3, orders.eu/4",
            ),
            (
                LieError::NoPlaceForError {
                    kind: LieKind::Refusal,
                    api: Api::from_wire(3, 9),
                },
                "refusal cannot be told on this Metadata v9 request: its response has no place for an error code",
            ),
            (
                LieError::ResponseOfAnotherCall {
                    kind: LieKind::Refusal,
                    api: Api::from_wire(0, 9),
                    correlation_id: 7,
                    answered_api: Api::from_wire(1, 11),
                    answered_correlation_id: 8,
                },
                "refusal was given the Fetch v11 response 8, which does not answer the Produce v9 request 7",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }
}
