use std::fmt;

use gaslightmq_field::prelude::*;

use crate::LieDefinition;

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
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // A rule on `orders.*` matched one partition of a Fetch.
/// let matched = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
///
/// let refusal = TargetedLie::new(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER), matched.clone());
/// assert_eq!(refusal.target(), &matched);
/// assert_eq!(refusal.to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
///
/// // A call has one answer, so a delay holds all of it back.
/// let delay = TargetedLie::new(TimingLie::new().with_delay(TimeSpan::from_millis(750)), matched);
/// assert_eq!(delay.target(), &Target::Call);
/// assert_eq!(delay.to_string(), "+750 ms");
/// # Ok::<(), FieldError>(())
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
    pub fn new(lie: impl Into<LieDefinition>, target: Target) -> Self {
        let lie = lie.into();
        let target = if lie.kind().targets_whole_call() {
            Target::Call
        } else {
            target
        };
        Self { lie, target }
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

#[cfg(test)]
mod tests {
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

    #[test]
    fn a_refusal_keeps_the_parts_the_selector_matched() {
        let target = parts(&["orders.eu/3", "orders.eu/4"]);
        let lie = TargetedLie::new(refusal(), target.clone());

        assert_eq!(lie.lie(), &LieDefinition::from(refusal()));
        assert_eq!(lie.target(), &target);
    }

    #[test]
    fn a_refusal_of_a_request_without_parts_targets_the_whole_call() {
        let lie = TargetedLie::new(RefusalLie::new(ErrorCode::COORDINATOR_NOT_AVAILABLE), Target::Call);

        assert_eq!(lie.target(), &Target::Call);
    }

    #[test]
    fn timing_silence_and_hang_up_target_the_whole_call_whatever_the_selector_matched() {
        let matched = parts(&["orders.eu/3"]);
        let lies: [LieDefinition; 3] = [delay().into(), LieDefinition::Silence, HangUpLie::new().into()];

        for lie in lies {
            assert_eq!(
                TargetedLie::new(lie.clone(), matched.clone()).target(),
                &Target::Call,
                "{lie}"
            );
            assert_eq!(TargetedLie::new(lie.clone(), Target::Call).target(), &Target::Call);
        }
    }

    #[test]
    fn a_definition_binds_to_a_target() {
        let target = parts(&["orders.eu/3"]);

        assert_eq!(
            LieDefinition::from(refusal()).on(target.clone()),
            TargetedLie::new(refusal(), target.clone())
        );
        assert_eq!(
            LieDefinition::Silence.on(target),
            TargetedLie::new(LieDefinition::Silence, Target::Call)
        );
    }

    #[test]
    fn a_targeted_lie_writes_a_line_of_a_verdict() {
        let one = parts(&["orders.eu/3"]);
        let two = parts(&["orders.eu/4", "orders.eu/3"]);
        let after = Stage::AfterResponse;
        let cases = [
            (
                TargetedLie::new(refusal(), one.clone()),
                "NOT_LEADER_OR_FOLLOWER on orders.eu/3",
            ),
            (
                TargetedLie::new(refusal(), two),
                "NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.eu/4",
            ),
            (
                TargetedLie::new(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(after), one.clone()),
                "REQUEST_TIMED_OUT after the response on orders.eu/3",
            ),
            (
                TargetedLie::new(RefusalLie::new(ErrorCode::COORDINATOR_NOT_AVAILABLE), Target::Call),
                "COORDINATOR_NOT_AVAILABLE",
            ),
            (TargetedLie::new(delay(), one.clone()), "+750 ms"),
            (TargetedLie::new(LieDefinition::Silence, one.clone()), "silence"),
            (
                TargetedLie::new(HangUpLie::new().at(after), one),
                "hang-up after the response",
            ),
        ];

        for (lie, text) in cases {
            assert_eq!(lie.to_string(), text);
        }
    }
}
