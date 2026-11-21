use std::fmt;

use gaslightmq_field::prelude::*;

use crate::{LieDefinition, LieKind};

/// A lie with the target it applies to: one line of a verdict.
///
/// A definition says what the client is told.
/// The rule's selector says about which part of the call, anew on every call it matches.
/// A targeted lie holds both, so it is what combines with the other lies on a call,
/// and what a verdict lists once it applied.
///
/// Timing, silence and hang-up always target the whole call.
/// [`new`](Self::new) gives such a lie the whole call, whatever the selector matched.
///
/// A targeted lie always changes something: its target is the whole call, or at least one part.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // A rule on `orders.*` matched one partition of a Fetch.
/// let matched = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
///
/// let refusal = TargetedLie::new(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER), matched.clone())?;
/// assert_eq!(refusal.target(), &matched);
/// assert_eq!(refusal.to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
///
/// // A call has one answer, so a delay holds all of it back.
/// let delay = TargetedLie::new(TimingLie::new().with_delay(TimeSpan::from_millis(750)), matched)?;
/// assert_eq!(delay.target(), &Target::Call);
/// assert_eq!(delay.to_string(), "+750 ms");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetedLie {
    pub(crate) lie: LieDefinition,
    pub(crate) target: Target,
}

impl TargetedLie {
    /// The lie on what a selector matched.
    ///
    /// A lie of a kind that [targets the whole call](crate::LieKind::targets_whole_call)
    /// gets [`Target::Call`] instead of `target`.
    /// That is no mistake to report: `target` is what the selector matched, and not yet what the lie changes.
    /// A rule on `orders.*` with a delay is a valid rule, and its delay holds the whole call back.
    ///
    /// Reading a targeted lie is stricter.
    /// With the `serde` feature, a map is a line of a verdict, so its `target` key says what the lie changed.
    /// A delay with parts there is a line that nothing writes,
    /// and reading rejects it with [`TargetedLieError::PartsNotAllowed`].
    ///
    /// # Errors
    ///
    /// [`TargetedLieError::EmptyTarget`] when `target` is a set of parts without a part, whatever the kind of the lie.
    /// A selector that matches no part does not match the call, so there is no lie to tell.
    /// [`Target::of_parts`] never yields such a target.
    pub fn new(lie: impl Into<LieDefinition>, target: Target) -> Result<Self, TargetedLieError> {
        if !target.is_call() && target.parts().next().is_none() {
            return Err(TargetedLieError::EmptyTarget);
        }
        let lie = lie.into();
        let target = if lie.kind().targets_whole_call() {
            Target::Call
        } else {
            target
        };
        Ok(Self { lie, target })
    }

    /// What the client is told.
    pub const fn lie(&self) -> &LieDefinition {
        &self.lie
    }

    /// The part of the call the lie changes.
    pub const fn target(&self) -> &Target {
        &self.target
    }
}

/// Writes the lie, and `on` with the parts unless the target is the whole call:
/// `NOT_LEADER_OR_FOLLOWER on orders.eu/3`, `+750 ms`.
impl fmt::Display for TargetedLie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.target.is_call() {
            write!(f, "{}", self.lie)
        } else {
            write!(f, "{} on {}", self.lie, self.target)
        }
    }
}

/// Why a lie and a target are not a [`TargetedLie`].
///
/// ```
/// use std::collections::BTreeSet;
///
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // `Target::of_parts` never yields this target, and whoever builds it by hand can.
/// let error = LieDefinition::Silence.on(Target::Parts(BTreeSet::new())).unwrap_err();
/// assert_eq!(error, TargetedLieError::EmptyTarget);
/// assert_eq!(error.to_string(), "a target needs at least one part");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TargetedLieError {
    /// The target is a set of parts without a part, so the lie would change nothing.
    EmptyTarget,
    /// The target is a set of parts, and a lie of this kind always targets the whole call.
    /// Only reading returns it, with the `serde` feature.
    /// [`TargetedLie::new`] gives such a lie the whole call instead.
    PartsNotAllowed { kind: LieKind },
}

impl fmt::Display for TargetedLieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyTarget => f.write_str("a target needs at least one part"),
            Self::PartsNotAllowed { kind } => write!(f, "{kind} always targets the whole call"),
        }
    }
}

impl std::error::Error for TargetedLieError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use gaslightmq_clock::TimeSpan;
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::{HangUpLie, RefusalLie, Stage, TimingLie};

    fn parts(texts: &[&str]) -> Target {
        Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
    }

    fn refusal() -> RefusalLie {
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)
    }

    fn delay() -> TimingLie {
        TimingLie::new().with_delay(TimeSpan::from_millis(750))
    }

    fn on(lie: impl Into<LieDefinition>, target: Target) -> TargetedLie {
        TargetedLie::new(lie, target).unwrap()
    }

    #[test]
    fn a_refusal_keeps_the_parts_the_selector_matched() {
        let target = parts(&["orders.eu/3", "orders.eu/4"]);
        let lie = on(refusal(), target.clone());

        assert_eq!(lie.lie(), &LieDefinition::from(refusal()));
        assert_eq!(lie.target(), &target);
    }

    #[test]
    fn a_refusal_of_a_request_without_parts_targets_the_whole_call() {
        let lie = on(RefusalLie::new(ErrorCode::COORDINATOR_NOT_AVAILABLE), Target::Call);

        assert_eq!(lie.target(), &Target::Call);
    }

    #[test]
    fn timing_silence_and_hang_up_target_the_whole_call_whatever_the_selector_matched() {
        let matched = parts(&["orders.eu/3"]);
        let lies: [LieDefinition; 3] = [delay().into(), LieDefinition::Silence, HangUpLie::new().into()];

        for lie in lies {
            assert_eq!(on(lie.clone(), matched.clone()).target(), &Target::Call, "{lie}");
            assert_eq!(on(lie.clone(), Target::Call).target(), &Target::Call);
        }
    }

    #[test]
    fn a_definition_binds_to_a_target() {
        let target = parts(&["orders.eu/3"]);

        assert_eq!(
            LieDefinition::from(refusal()).on(target.clone()),
            Ok(on(refusal(), target.clone()))
        );
        assert_eq!(
            LieDefinition::Silence.on(target),
            Ok(on(LieDefinition::Silence, Target::Call))
        );
    }

    #[test]
    fn no_lie_binds_to_a_target_without_parts() {
        // `Target::of_parts` never yields this target, and the variant is open to whoever builds it by hand.
        let empty = Target::Parts(BTreeSet::new());
        let lies: [LieDefinition; 4] = [
            refusal().into(),
            delay().into(),
            LieDefinition::Silence,
            HangUpLie::new().into(),
        ];

        for lie in lies {
            assert_eq!(
                TargetedLie::new(lie.clone(), empty.clone()),
                Err(TargetedLieError::EmptyTarget),
                "{lie}"
            );
            assert_eq!(
                lie.clone().on(empty.clone()),
                Err(TargetedLieError::EmptyTarget),
                "{lie}"
            );
        }
    }

    #[test]
    fn a_targeted_lie_error_says_what_is_wrong() {
        let cases = [
            (TargetedLieError::EmptyTarget, "a target needs at least one part"),
            (
                TargetedLieError::PartsNotAllowed { kind: LieKind::Timing },
                "timing always targets the whole call",
            ),
            (
                TargetedLieError::PartsNotAllowed { kind: LieKind::HangUp },
                "hang-up always targets the whole call",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_targeted_lie_writes_a_line_of_a_verdict() {
        let one = parts(&["orders.eu/3"]);
        let two = parts(&["orders.eu/4", "orders.eu/3"]);
        let after = Stage::AfterResponse;
        let cases = [
            (on(refusal(), one.clone()), "NOT_LEADER_OR_FOLLOWER on orders.eu/3"),
            (on(refusal(), two), "NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.eu/4"),
            (
                on(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(after), one.clone()),
                "REQUEST_TIMED_OUT after the response on orders.eu/3",
            ),
            (
                on(RefusalLie::new(ErrorCode::COORDINATOR_NOT_AVAILABLE), Target::Call),
                "COORDINATOR_NOT_AVAILABLE",
            ),
            (on(delay(), one.clone()), "+750 ms"),
            (on(LieDefinition::Silence, one.clone()), "silence"),
            (on(HangUpLie::new().at(after), one), "hang-up after the response"),
        ];

        for (lie, text) in cases {
            assert_eq!(lie.to_string(), text);
        }
    }
}
