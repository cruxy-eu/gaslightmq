use std::fmt;
use std::str::FromStr;

use crate::RuleError;

/// The state of a rule: whether it is being written, evaluated, or kept.
///
/// | State | Means |
/// |---|---|
/// | `Draft` | Being written. Never evaluated. |
/// | `Armed` | Evaluated for every matching call. |
/// | `Disarmed` | Kept, but not evaluated. |
///
/// A state is live state.
/// It is not part of the [definition](crate::Rule), and no file stores it: a workspace rule loads disarmed.
///
/// The methods are the changes a rule can take, and every other change is an error.
///
/// ```text
/// Draft ── arm ──► Armed ── disarm ──► Disarmed
///   │                ▲                    │
///   └── save ──► Disarmed ───── arm ──────┘
/// ```
///
/// A lifetime that ends disarms the rule like anyone else does.
/// Who changed the state is for the journal to say.
///
/// ```
/// use gaslightmq_rule::prelude::*;
///
/// let state = RuleState::Draft.arm()?;
/// assert_eq!(state, RuleState::Armed);
/// assert_eq!(state.disarm()?.arm()?, RuleState::Armed);
///
/// // Arming resets a rule, so arming an armed rule is not a change: disarm it first.
/// assert!(RuleState::Armed.arm().is_err());
/// # Ok::<(), RuleError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuleState {
    /// Being written. Never evaluated.
    Draft,
    /// Evaluated for every matching call.
    Armed,
    /// Kept, but not evaluated.
    Disarmed,
}

impl RuleState {
    /// Every state, in the order a rule first reaches them.
    pub const ALL: [RuleState; 3] = [RuleState::Draft, RuleState::Armed, RuleState::Disarmed];

    /// The name of the state in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            RuleState::Draft => "draft",
            RuleState::Armed => "armed",
            RuleState::Disarmed => "disarmed",
        }
    }

    /// Whether calls evaluate the rule.
    pub const fn is_armed(self) -> bool {
        matches!(self, RuleState::Armed)
    }

    /// Arms a draft or a disarmed rule.
    ///
    /// # Errors
    ///
    /// [`RuleError::InvalidStateChange`] when the rule is armed already.
    pub fn arm(self) -> Result<RuleState, RuleError> {
        match self {
            RuleState::Draft | RuleState::Disarmed => Ok(RuleState::Armed),
            RuleState::Armed => Err(self.cannot("arm")),
        }
    }

    /// Disarms an armed rule.
    ///
    /// # Errors
    ///
    /// [`RuleError::InvalidStateChange`] when the rule is not armed.
    pub fn disarm(self) -> Result<RuleState, RuleError> {
        match self {
            RuleState::Armed => Ok(RuleState::Disarmed),
            RuleState::Draft | RuleState::Disarmed => Err(self.cannot("disarm")),
        }
    }

    /// Saves a draft, which keeps it as a disarmed rule.
    ///
    /// # Errors
    ///
    /// [`RuleError::InvalidStateChange`] when the rule is not a draft.
    pub fn save(self) -> Result<RuleState, RuleError> {
        match self {
            RuleState::Draft => Ok(RuleState::Disarmed),
            RuleState::Armed | RuleState::Disarmed => Err(self.cannot("save")),
        }
    }

    fn cannot(self, change: &'static str) -> RuleError {
        RuleError::InvalidStateChange { from: self, change }
    }
}

impl fmt::Display for RuleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name of a state.
impl FromStr for RuleState {
    type Err = RuleError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let state = RuleState::ALL.into_iter().find(|state| state.name() == text);
        state.ok_or_else(|| RuleError::UnknownState { name: text.to_owned() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cannot(from: RuleState, change: &'static str) -> Result<RuleState, RuleError> {
        Err(RuleError::InvalidStateChange { from, change })
    }

    #[test]
    fn a_draft_arms_or_saves() {
        assert_eq!(RuleState::Draft.arm(), Ok(RuleState::Armed));
        assert_eq!(RuleState::Draft.save(), Ok(RuleState::Disarmed));
        assert_eq!(RuleState::Draft.disarm(), cannot(RuleState::Draft, "disarm"));
    }

    #[test]
    fn an_armed_rule_only_disarms() {
        assert_eq!(RuleState::Armed.disarm(), Ok(RuleState::Disarmed));
        assert_eq!(RuleState::Armed.arm(), cannot(RuleState::Armed, "arm"));
        assert_eq!(RuleState::Armed.save(), cannot(RuleState::Armed, "save"));
    }

    #[test]
    fn a_disarmed_rule_only_arms() {
        assert_eq!(RuleState::Disarmed.arm(), Ok(RuleState::Armed));
        assert_eq!(RuleState::Disarmed.disarm(), cannot(RuleState::Disarmed, "disarm"));
        assert_eq!(RuleState::Disarmed.save(), cannot(RuleState::Disarmed, "save"));
    }

    #[test]
    fn only_an_armed_rule_is_evaluated() {
        assert_eq!(
            RuleState::ALL.map(RuleState::is_armed),
            [false, true, false],
            "{:?}",
            RuleState::ALL
        );
    }

    #[test]
    fn a_state_has_a_name_that_reads_back() {
        assert_eq!(RuleState::ALL.map(RuleState::name), ["draft", "armed", "disarmed"]);
        for state in RuleState::ALL {
            assert_eq!(state.to_string(), state.name());
            assert_eq!(state.name().parse(), Ok(state));
        }
        assert_eq!(format!("{:>9}|", RuleState::Armed), "    armed|");
        assert_eq!(
            "Armed".parse::<RuleState>(),
            Err(RuleError::UnknownState { name: "Armed".into() })
        );
    }
}
