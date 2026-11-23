//! The verdict of a call: pass, or the lies that applied.

use std::fmt;

use gaslightmq_lie::TargetedLie;

/// The outcome of evaluating one call: pass, or the lies that applied.
///
/// - **Pass**: no rule fired, or no rule that fired changed anything.
/// - **Lied**: one or more lies applied. The verdict lists each one on the target it applied to.
///
/// Incidents that a call fired are part of its [`Explanation`](crate::Explanation), and not of its verdict:
/// they change the world, and not this call.
///
/// A verdict follows from the explanation, and [`Explanation::verdict`](crate::Explanation::verdict) gives it.
/// A test or a replay that compares two calls builds the one it expects here.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_millis(750)));
/// let flap = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// let partition = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
///
/// let verdict = Verdict::lied([delay.on(Target::Call), flap.on(partition)]);
/// assert!(!verdict.is_pass());
/// assert_eq!(verdict.to_string(), "+750 ms; NOT_LEADER_OR_FOLLOWER on orders.eu/3");
///
/// assert_eq!(Verdict::pass().to_string(), "pass");
/// assert_eq!(Verdict::lied([]), Verdict::pass());
/// # Ok::<(), PartError>(())
/// ```
///
/// With the `serde` feature, a verdict serializes as the list of its lies, and a pass as the empty list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Verdict {
    lies: Vec<TargetedLie>,
}

impl Verdict {
    /// No lie applied: the client got the truth, when it got it.
    pub const fn pass() -> Verdict {
        Verdict { lies: Vec::new() }
    }

    /// These lies applied, each on the target it applied to. Without a lie, the verdict is a pass.
    pub fn lied(lies: impl IntoIterator<Item = TargetedLie>) -> Verdict {
        Verdict {
            lies: lies.into_iter().collect(),
        }
    }

    /// Whether no lie applied.
    pub fn is_pass(&self) -> bool {
        self.lies.is_empty()
    }

    /// The lies that applied, each on the target it applied to. A pass has none.
    pub fn lies(&self) -> &[TargetedLie] {
        &self.lies
    }
}

/// Writes `pass`, or the lies with `; ` between them: `+750 ms; NOT_LEADER_OR_FOLLOWER on orders.eu/3`.
impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_pass() {
            return f.write_str("pass");
        }
        for (index, lie) in self.lies.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            fmt::Display::fmt(lie, f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_field::Target;
    use gaslightmq_lie::LieDefinition;

    use super::*;

    #[test]
    fn a_verdict_without_a_lie_is_a_pass() {
        assert!(Verdict::pass().is_pass());
        assert_eq!(Verdict::default(), Verdict::pass());
        assert_eq!(Verdict::lied([]), Verdict::pass());
        assert_eq!(Verdict::pass().lies(), []);
    }

    #[test]
    fn a_verdict_keeps_its_lies_in_order() {
        let silence = LieDefinition::Silence.on(Target::Call);
        let verdict = Verdict::lied([silence.clone()]);

        assert!(!verdict.is_pass());
        assert_eq!(verdict.lies(), [silence]);
        assert_eq!(verdict.to_string(), "silence");
    }
}
