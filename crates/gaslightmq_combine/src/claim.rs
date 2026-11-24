use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

/// What a lie claims of a call: everything that combining needs to know about it.
///
/// A claim is a lie reduced to its [`CombinationClass`], with what that class combines on.
/// It leaves out what the client is told, such as the error code of a refusal.
///
/// | Claim | Claims | Combines |
/// |---|---|---|
/// | [`Modifier`](Self::Modifier) | The one answer of the call, at its stage. | Modifiers stack. |
/// | [`Rewrite`](Self::Rewrite) | One aspect of its target, after the response. | Two rewrites of the same aspect of the same target conflict. |
/// | [`Refusal`](Self::Refusal) | Everything about its target. | Two refusals of the same target conflict. A refusal pre-empts the rewrites of its target. |
/// | [`Terminal`](Self::Terminal) | The whole call. | It conflicts with every other terminal lie and with every refusal. It pre-empts modifiers and rewrites. |
///
/// A [`TargetedLie`] converts into its claim.
/// A kind of lie without a definition yet still has a claim, so its rules of combination hold today:
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let partition = Target::of_parts(["orders.eu/3".parse().expect("a part")]).expect("one part");
///
/// let refusal = TargetedLie::new(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER), partition.clone());
/// let claim = Claim::try_from(&refusal)?;
/// assert_eq!(claim.class(), CombinationClass::Refusal);
/// assert_eq!(claim.stage(), Stage::BeforeForwarding);
/// assert_eq!(claim.target(), &partition);
///
/// // A stale high watermark, which no definition writes yet.
/// let stale = Claim::Rewrite { aspect: Aspect::HighWatermark, target: partition };
/// assert_eq!(stale.stage(), Stage::AfterResponse);
/// assert_eq!(stale.aspect(), Some(Aspect::HighWatermark));
/// # Ok::<(), ClaimError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Claim {
    /// A timing lie: the call waits, or the answer tells the client to back off.
    /// It holds its own stage.
    Modifier(TimingLie),
    /// A rewrite of one aspect of its target.
    /// A rewrite always acts after the response, because it rewrites the truth.
    Rewrite { aspect: Aspect, target: Target },
    /// A refusal of its target at a stage.
    Refusal { stage: Stage, target: Target },
    /// A silence or a hang-up, which ends the whole call at a stage.
    Terminal { stage: Stage },
}

impl Claim {
    /// How the lie combines with the other lies on the call.
    pub const fn class(&self) -> CombinationClass {
        match self {
            Self::Modifier(_) => CombinationClass::Modifier,
            Self::Rewrite { .. } => CombinationClass::Rewrite,
            Self::Refusal { .. } => CombinationClass::Refusal,
            Self::Terminal { .. } => CombinationClass::Terminal,
        }
    }

    /// The stage the lie names.
    ///
    /// A lie of the response pass acts after the response, whatever stage it names.
    /// See [`RequestPass::response_pass`](crate::RequestPass::response_pass).
    pub const fn stage(&self) -> Stage {
        match self {
            Self::Modifier(timing) => timing.stage(),
            Self::Rewrite { .. } => Stage::AfterResponse,
            Self::Refusal { stage, .. } | Self::Terminal { stage } => *stage,
        }
    }

    /// The part of the call the lie claims.
    /// A modifier and a terminal lie claim the whole call.
    pub const fn target(&self) -> &Target {
        match self {
            Self::Modifier(_) | Self::Terminal { .. } => &Target::Call,
            Self::Rewrite { target, .. } | Self::Refusal { target, .. } => target,
        }
    }

    /// The aspect the lie rewrites, or `None` when it is not a rewrite.
    pub const fn aspect(&self) -> Option<Aspect> {
        match self {
            Self::Rewrite { aspect, .. } => Some(*aspect),
            Self::Modifier(_) | Self::Refusal { .. } | Self::Terminal { .. } => None,
        }
    }
}

/// The claim of a timing lie.
impl From<TimingLie> for Claim {
    fn from(timing: TimingLie) -> Self {
        Self::Modifier(timing)
    }
}

/// The claim of a lie on its target.
///
/// # Errors
///
/// Only for a definition that breaks its own word.
/// No definition of today's `gaslightmq_lie` does.
///
/// - [`ClaimError::NoAspect`] when the lie is a rewrite, and its definition names no aspect.
/// - [`ClaimError::NoTiming`] when the lie is a modifier, and its definition is not a timing lie.
impl TryFrom<&TargetedLie> for Claim {
    type Error = ClaimError;

    fn try_from(lie: &TargetedLie) -> Result<Self, Self::Error> {
        let definition = lie.lie();
        let kind = definition.kind();
        let target = || lie.target().clone();

        match definition.class() {
            CombinationClass::Modifier => match definition {
                LieDefinition::Timing(timing) => Ok(Self::Modifier(*timing)),
                _ => Err(ClaimError::NoTiming { kind }),
            },
            CombinationClass::Rewrite => match definition.aspect() {
                Some(aspect) => Ok(Self::Rewrite {
                    aspect,
                    target: target(),
                }),
                None => Err(ClaimError::NoAspect { kind }),
            },
            CombinationClass::Refusal => Ok(Self::Refusal {
                stage: definition.stage(),
                target: target(),
            }),
            CombinationClass::Terminal => Ok(Self::Terminal {
                stage: definition.stage(),
            }),
        }
    }
}

/// Why a lie cannot become a [`Claim`].
///
/// No definition of today's `gaslightmq_lie` causes either error.
/// They stand for a definition that breaks its own word,
/// so that a mistake in a new kind of lie is an error here and never a lie that combines wrongly.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let error = ClaimError::NoAspect { kind: LieKind::Progress };
/// assert_eq!(error.to_string(), "a progress lie rewrites, and its definition names no aspect");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClaimError {
    /// The lie is a rewrite, and its definition names no aspect.
    /// Rewrites conflict per aspect, so nothing says what this one conflicts with.
    #[error("a {kind} lie rewrites, and its definition names no aspect")]
    NoAspect { kind: LieKind },
    /// The lie is a modifier, and its definition is not a timing lie.
    /// Nothing says how long the call waits.
    #[error("a {kind} lie is a modifier, and its definition holds no timing")]
    NoTiming { kind: LieKind },
}

/// A lie that fired on a call: who it is, where it stands in the priority order, and what it claims.
///
/// The identity is whatever the caller tells its lies apart with, such as the ID of the rule that fired.
/// Combining never looks inside it.
/// The [`Fate`](crate::Fate) of the lie carries it back,
/// and so does every [`Loss`](crate::Loss) and [`StageEffect`](crate::StageEffect) that names the lie.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // The rule "blackhole" fired, and it is the first rule of the chain.
/// let silence = LieDefinition::Silence.on(Target::Call);
/// let blackhole = FiredLie::new("blackhole", Priority::HIGHEST, Claim::try_from(&silence)?);
///
/// assert_eq!(blackhole.id(), &"blackhole");
/// assert_eq!(blackhole.claim().class(), CombinationClass::Terminal);
/// # Ok::<(), ClaimError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FiredLie<Id> {
    id: Id,
    priority: Priority,
    claim: Claim,
}

impl<Id> FiredLie<Id> {
    /// The lie `id` at `priority`, with what it claims of the call.
    pub fn new(id: Id, priority: Priority, claim: impl Into<Claim>) -> Self {
        Self {
            id,
            priority,
            claim: claim.into(),
        }
    }

    /// Who the lie is.
    pub const fn id(&self) -> &Id {
        &self.id
    }

    /// Where the lie stands in the priority order of the call.
    pub const fn priority(&self) -> Priority {
        self.priority
    }

    /// What the lie claims of the call.
    pub const fn claim(&self) -> &Claim {
        &self.claim
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_kernel::prelude::*;

    use super::*;

    const BEFORE: Stage = Stage::BeforeForwarding;
    const AFTER: Stage = Stage::AfterResponse;

    fn partition() -> Target {
        Target::of_parts(["orders.eu/3".parse().unwrap()]).unwrap()
    }

    fn claim(lie: impl Into<LieDefinition>, target: Target) -> Claim {
        Claim::try_from(&lie.into().on(target)).unwrap()
    }

    #[test]
    fn a_refusal_claims_its_target_at_its_stage() {
        let flap = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);

        assert_eq!(
            claim(flap, partition()),
            Claim::Refusal {
                stage: BEFORE,
                target: partition()
            }
        );
        assert_eq!(
            claim(flap.at(AFTER), Target::Call),
            Claim::Refusal {
                stage: AFTER,
                target: Target::Call
            }
        );
    }

    #[test]
    fn a_timing_lie_claims_the_whole_call_as_a_modifier() {
        let delay = TimingLie::new().with_delay(TimeSpan::from_millis(750)).at(BEFORE);
        let claimed = claim(delay, partition());

        assert_eq!(claimed, Claim::Modifier(delay));
        assert_eq!(claimed, Claim::from(delay));
        assert_eq!(claimed.target(), &Target::Call);
        assert_eq!(claimed.stage(), BEFORE);
    }

    #[test]
    fn a_silence_and_a_hang_up_claim_the_whole_call_as_terminal_lies() {
        assert_eq!(
            claim(LieDefinition::Silence, partition()),
            Claim::Terminal { stage: BEFORE }
        );
        assert_eq!(claim(HangUpLie::new(), partition()), Claim::Terminal { stage: BEFORE });
        assert_eq!(
            claim(HangUpLie::new().at(AFTER), partition()),
            Claim::Terminal { stage: AFTER }
        );
        assert_eq!(Claim::Terminal { stage: AFTER }.target(), &Target::Call);
    }

    #[test]
    fn a_rewrite_claims_one_aspect_of_its_target_after_the_response() {
        for aspect in Aspect::ALL {
            let rewrite = Claim::Rewrite {
                aspect,
                target: partition(),
            };

            assert_eq!(rewrite.class(), CombinationClass::Rewrite);
            assert_eq!(rewrite.stage(), AFTER);
            assert_eq!(rewrite.aspect(), Some(aspect));
            assert_eq!(rewrite.target(), &partition());
        }
    }

    #[test]
    fn a_claim_has_the_class_of_the_kind_of_its_lie() {
        let lies: [LieDefinition; 4] = [
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER).into(),
            TimingLie::new().into(),
            LieDefinition::Silence,
            HangUpLie::new().into(),
        ];

        for lie in lies {
            let claimed = claim(lie.clone(), partition());

            assert_eq!(claimed.class(), lie.class(), "{lie}");
            assert_eq!(claimed.stage(), lie.stage(), "{lie}");
            assert_eq!(claimed.aspect(), None, "{lie}");
        }
    }

    #[test]
    fn a_claim_error_says_what_is_wrong() {
        let cases = [
            (
                ClaimError::NoAspect {
                    kind: LieKind::Progress,
                },
                "a progress lie rewrites, and its definition names no aspect",
            ),
            (
                ClaimError::NoTiming { kind: LieKind::Timing },
                "a timing lie is a modifier, and its definition holds no timing",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_fired_lie_holds_what_it_was_given() {
        let lie = FiredLie::new(7_u32, Priority::new(2), TimingLie::new());

        assert_eq!(lie.id(), &7);
        assert_eq!(lie.priority(), Priority::new(2));
        assert_eq!(lie.claim(), &Claim::Modifier(TimingLie::new()));
    }
}
