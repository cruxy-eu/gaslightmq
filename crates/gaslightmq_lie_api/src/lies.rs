use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_lie::prelude::*;
use gaslightmq_wire::{Request, Response};

use crate::{Effect, HangUp, Lie, LieError, Silence, Timing};

/// The lies a pipeline can tell: one behaviour per kind.
///
/// The engine registers the kinds that its build holds, and hands the set to every pipeline.
/// The pipeline asks the set, and the set asks the behaviour of the lie's kind.
/// The pipeline therefore names no kind, and a new kind is one more [`with`](Self::with).
///
/// A build can leave a kind out.
/// [`tells`](Self::tells) says so before a rule of that kind is armed,
/// and asking for a lie of that kind fails with [`LieError::NotRegistered`].
///
/// The set also holds every behaviour to its word:
/// it only returns an effect that the stage and the class of the lie allow.
/// See [`before_forwarding`](Self::before_forwarding).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
///
/// // Every build tells timing, silence and hang-up. This one leaves the refusal out.
/// let lies = Lies::of_every_build();
/// assert!(lies.tells(LieKind::HangUp));
/// assert!(!lies.tells(LieKind::Refusal));
///
/// // The pipeline asks without naming the kind.
/// let request = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default())?;
/// let hang_up = LieDefinition::from(HangUpLie::new()).on(Target::Call);
/// let effect = lies.before_forwarding(&hang_up, &request)?;
/// assert_eq!(effect.instruction(), Instruction::Close);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Default)]
pub struct Lies {
    by_kind: BTreeMap<LieKind, Box<dyn Lie>>,
}

impl Lies {
    /// The set that tells no lie.
    /// A pipeline with it only forwards.
    pub const fn new() -> Self {
        Self {
            by_kind: BTreeMap::new(),
        }
    }

    /// The lies that every build tells: [`Timing`], [`Silence`] and [`HangUp`].
    ///
    /// A build can leave out a kind that lives in a crate of its own.
    /// These three live here, so the engine starts from this set and adds the kinds of its build.
    pub fn of_every_build() -> Self {
        let mut lies = Self::new();
        lies.insert(Box::new(Timing));
        lies.insert(Box::new(Silence));
        lies.insert(Box::new(HangUp));
        lies
    }

    /// The same set with `lie` in it.
    ///
    /// # Errors
    ///
    /// [`LiesError::KindTwice`] when the set already tells the kind of `lie`.
    pub fn with(mut self, lie: impl Lie + 'static) -> Result<Self, LiesError> {
        let kind = lie.kind();
        if self.tells(kind) {
            return Err(LiesError::KindTwice { kind });
        }
        self.insert(Box::new(lie));
        Ok(self)
    }

    /// Whether the set tells lies of `kind`.
    pub fn tells(&self, kind: LieKind) -> bool {
        self.by_kind.contains_key(&kind)
    }

    /// The kinds the set tells, in the order of [`LieKind::ALL`].
    pub fn kinds(&self) -> impl Iterator<Item = LieKind> {
        self.by_kind.keys().copied()
    }

    /// The behaviour of `kind`, or `None` when the set does not tell that kind.
    pub fn of(&self, kind: LieKind) -> Option<&dyn Lie> {
        self.by_kind.get(&kind).map(Box::as_ref)
    }

    /// What `lie` asks for before the backend sees `request`.
    /// See [`Lie::before_forwarding`].
    ///
    /// # Errors
    ///
    /// - [`LieError::NotRegistered`] when the set does not tell the kind of `lie`.
    /// - [`LieError::InstructionNotAllowed`] when the behaviour asks for something that the class of `lie` does not allow.
    ///   Lies combine by their class before the pipeline asks, so such an effect would break what combining decided.
    /// - Every error of the behaviour itself.
    pub fn before_forwarding(&self, lie: &TargetedLie, request: &Request) -> Result<Effect, LieError> {
        let effect = self.behaviour_of(lie)?.before_forwarding(lie, request)?;
        allowed(lie, Stage::BeforeForwarding, effect)
    }

    /// What `lie` asks for once the backend answered.
    /// See [`Lie::after_response`].
    ///
    /// # Errors
    ///
    /// - [`LieError::NotRegistered`] when the set does not tell the kind of `lie`.
    /// - [`LieError::InstructionNotAllowed`] when the behaviour asks for something that the class of `lie` does not allow,
    ///   or asks to forward another request, which the backend already answered.
    /// - Every error of the behaviour itself.
    pub fn after_response(
        &self,
        lie: &TargetedLie,
        request: &Request,
        response: &Response,
    ) -> Result<Effect, LieError> {
        let effect = self.behaviour_of(lie)?.after_response(lie, request, response)?;
        allowed(lie, Stage::AfterResponse, effect)
    }

    /// Puts `lie` in the set, in the place of its kind.
    fn insert(&mut self, lie: Box<dyn Lie>) {
        self.by_kind.insert(lie.kind(), lie);
    }

    /// The behaviour that tells `lie`.
    fn behaviour_of(&self, lie: &TargetedLie) -> Result<&dyn Lie, LieError> {
        let kind = lie.lie().kind();
        self.of(kind).ok_or(LieError::NotRegistered { kind })
    }
}

/// `effect`, when a lie of the class of `lie` can ask for it at `stage`.
fn allowed(lie: &TargetedLie, stage: Stage, effect: Effect) -> Result<Effect, LieError> {
    let instruction = effect.instruction();
    if instruction.allowed_at(stage) && instruction.fits(lie.lie().class()) {
        Ok(effect)
    } else {
        Err(LieError::InstructionNotAllowed {
            kind: lie.lie().kind(),
            stage,
            instruction,
        })
    }
}

/// Writes the kinds the set tells.
impl fmt::Debug for Lies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.kinds()).finish()
    }
}

/// Why behaviours are not a set of [`Lies`].
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
///
/// let error = Lies::of_every_build().with(Silence).unwrap_err();
/// assert_eq!(error, LiesError::KindTwice { kind: LieKind::Silence });
/// assert_eq!(error.to_string(), "silence is registered twice");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LiesError {
    /// Two behaviours tell the same kind of lie, so nothing says which one the pipeline asks.
    KindTwice { kind: LieKind },
}

impl fmt::Display for LiesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KindTwice { kind } => write!(f, "{kind} is registered twice"),
        }
    }
}

impl std::error::Error for LiesError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_clock::TimeSpan;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};

    use super::*;
    use crate::Instruction;

    /// A behaviour of `kind` that asks for the same effect at both stages, whatever the lie.
    struct Asks {
        kind: LieKind,
        effect: Effect,
    }

    impl Lie for Asks {
        fn kind(&self) -> LieKind {
            self.kind
        }

        fn before_forwarding(&self, _: &TargetedLie, _: &Request) -> Result<Effect, LieError> {
            Ok(self.effect.clone())
        }

        fn after_response(&self, _: &TargetedLie, _: &Request, _: &Response) -> Result<Effect, LieError> {
            Ok(self.effect.clone())
        }
    }

    fn heartbeat() -> Request {
        Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
    }

    fn truth(request: &Request) -> Response {
        request.response(&HeartbeatResponse::default()).unwrap()
    }

    fn refusal() -> TargetedLie {
        LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(Target::Call)
    }

    #[test]
    fn a_new_set_tells_no_lie() {
        let lies = Lies::new();

        assert_eq!(lies.kinds().count(), 0);
        assert!(LieKind::ALL.into_iter().all(|kind| !lies.tells(kind)));
        assert!(lies.of(LieKind::Timing).is_none());
        assert_eq!(format!("{lies:?}"), "{}");
        assert_eq!(format!("{:?}", Lies::default()), "{}");
    }

    #[test]
    fn the_set_of_every_build_tells_timing_silence_and_hang_up() {
        let lies = Lies::of_every_build();

        assert_eq!(
            lies.kinds().collect::<Vec<_>>(),
            [LieKind::Timing, LieKind::Silence, LieKind::HangUp]
        );
        for kind in LieKind::ALL {
            assert_eq!(lies.tells(kind), kind.targets_whole_call(), "{kind}");
            assert_eq!(lies.of(kind).map(Lie::kind), lies.tells(kind).then_some(kind));
        }
        assert_eq!(format!("{lies:?}"), "{Timing, Silence, HangUp}");
    }

    #[test]
    fn a_kind_registers_once() {
        let lies = Lies::new().with(Timing).unwrap().with(HangUp).unwrap();
        assert_eq!(lies.kinds().collect::<Vec<_>>(), [LieKind::Timing, LieKind::HangUp]);

        let error = lies.with(Timing).unwrap_err();
        assert_eq!(error, LiesError::KindTwice { kind: LieKind::Timing });
        assert_eq!(error.to_string(), "timing is registered twice");
        assert!(error.source().is_none());
    }

    #[test]
    fn a_new_kind_plugs_in_next_to_the_others() {
        let request = heartbeat();
        let answer = truth(&request);
        let lies = Lies::of_every_build()
            .with(Asks {
                kind: LieKind::Refusal,
                effect: Effect::Answer(answer),
            })
            .unwrap();

        assert!(lies.tells(LieKind::Refusal));
        let effect = lies.before_forwarding(&refusal(), &request).unwrap();
        assert_eq!(effect.instruction(), Instruction::Answer);
    }

    #[test]
    fn the_set_asks_the_behaviour_of_the_kind_of_the_lie() {
        let request = heartbeat();
        let response = truth(&request);
        let lies = Lies::of_every_build();
        let delay = TimingLie::new().with_delay(TimeSpan::from_millis(750));
        let cases: [(LieDefinition, Instruction); 3] = [
            (delay.into(), Instruction::Hold),
            (LieDefinition::Silence, Instruction::Drop),
            (HangUpLie::new().into(), Instruction::Close),
        ];

        for (lie, instruction) in cases {
            let lie = lie.on(Target::Call);
            let before = lies.before_forwarding(&lie, &request).unwrap();
            assert_eq!(before.instruction(), instruction, "{lie}");
            if lie.lie().kind() != LieKind::Silence {
                let after = lies.after_response(&lie, &request, &response).unwrap();
                assert_eq!(after.instruction(), instruction, "{lie}");
            }
        }
    }

    #[test]
    fn a_lie_of_a_kind_the_set_leaves_out_is_not_told() {
        let request = heartbeat();
        let response = truth(&request);
        let lies = Lies::of_every_build();
        let error = LieError::NotRegistered { kind: LieKind::Refusal };

        assert_eq!(lies.before_forwarding(&refusal(), &request).unwrap_err(), error);
        assert_eq!(lies.after_response(&refusal(), &request, &response).unwrap_err(), error);
    }

    #[test]
    fn the_set_passes_on_the_error_of_the_behaviour() {
        let request = heartbeat();
        let response = truth(&request);
        let silence = LieDefinition::Silence.on(Target::Call);

        assert_eq!(
            Lies::of_every_build()
                .after_response(&silence, &request, &response)
                .unwrap_err(),
            LieError::NoBehaviourAtStage {
                kind: LieKind::Silence,
                stage: Stage::AfterResponse
            }
        );
    }

    #[test]
    fn a_lie_does_not_ask_outside_its_class() {
        let request = heartbeat();
        let response = truth(&request);
        let lies = Lies::new()
            .with(Asks {
                kind: LieKind::Refusal,
                effect: Effect::Close,
            })
            .unwrap();
        let not_allowed = |stage| LieError::InstructionNotAllowed {
            kind: LieKind::Refusal,
            stage,
            instruction: Instruction::Close,
        };

        assert_eq!(
            lies.before_forwarding(&refusal(), &request).unwrap_err(),
            not_allowed(Stage::BeforeForwarding)
        );
        assert_eq!(
            lies.after_response(&refusal(), &request, &response).unwrap_err(),
            not_allowed(Stage::AfterResponse)
        );
    }

    #[test]
    fn a_terminal_lie_does_not_ask_for_nothing() {
        let request = heartbeat();
        let lies = Lies::new()
            .with(Asks {
                kind: LieKind::HangUp,
                effect: Effect::Nothing,
            })
            .unwrap();
        let hang_up = LieDefinition::from(HangUpLie::new()).on(Target::Call);

        assert_eq!(
            lies.before_forwarding(&hang_up, &request).unwrap_err(),
            LieError::InstructionNotAllowed {
                kind: LieKind::HangUp,
                stage: Stage::BeforeForwarding,
                instruction: Instruction::Nothing,
            }
        );
    }

    #[test]
    fn no_lie_forwards_another_request_after_the_response() {
        let request = heartbeat();
        let response = truth(&request);
        let lies = Lies::new()
            .with(Asks {
                kind: LieKind::Refusal,
                effect: Effect::Forward(request.clone()),
            })
            .unwrap();

        let before = lies.before_forwarding(&refusal(), &request).unwrap();
        assert_eq!(before.instruction(), Instruction::Forward);
        assert_eq!(
            lies.after_response(&refusal(), &request, &response).unwrap_err(),
            LieError::InstructionNotAllowed {
                kind: LieKind::Refusal,
                stage: Stage::AfterResponse,
                instruction: Instruction::Forward,
            }
        );
    }

    #[test]
    fn one_set_serves_every_session() {
        fn shared<T: Send + Sync + 'static>(_: &T) {}

        shared(&Lies::of_every_build());
    }
}
