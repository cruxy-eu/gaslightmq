use std::fmt;

use gaslightmq_incident::IncidentError;
use gaslightmq_lie::LieKind;
use gaslightmq_selector::SelectorSetError;

use crate::{PresetName, RuleName, RuleState};

/// Why a rule cannot do what it was asked: resolve, fire by hand, or change its state.
///
/// It is the one error of what a rule does. The value objects of this crate have errors of their own.
///
/// | The variants | Come from |
/// |---|---|
/// | `UnknownSelector`, `UnknownPreset`, `LieBeforeTruth`, `ManualWithoutSubject` | [`Rule::resolve`](crate::Rule::resolve) |
/// | `NoCallForIncident` | [`ArmedRule::fire_by_hand`](crate::ArmedRule::fire_by_hand) |
/// | `InvalidChange` | [`RuleState::arm`], [`RuleState::disarm`] and [`RuleState::save`] |
///
/// A variant about one rule names the rule.
/// A variant that holds the error of a selector set or of an incident writes that error into its own message, once,
/// so no variant has a [`source`](std::error::Error::source).
///
/// ```
/// use gaslightmq_rule::prelude::*;
///
/// let error = RuleState::Armed.arm().unwrap_err();
/// assert_eq!(error, RuleError::InvalidChange { from: RuleState::Armed, change: "arm" });
/// assert_eq!(error.to_string(), "a rule that is armed cannot arm");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuleError {
    /// The rule names a selector that does not exist.
    UnknownSelector { rule: RuleName, error: SelectorSetError },
    /// The rule names a preset that does not exist.
    UnknownPreset { rule: RuleName, preset: PresetName },
    /// The rule tests a response field, and its lie acts before forwarding, when the truth is not known yet.
    LieBeforeTruth { rule: RuleName, lie: LieKind },
    /// The rule only fires by hand, and its incident takes a subject from the call.
    /// A hand firing has no call, so the rule could never fire.
    ManualWithoutSubject { rule: RuleName, error: IncidentError },
    /// The rule was fired by hand, and its incident takes a subject from the call. A hand firing has no call.
    NoCallForIncident { rule: RuleName, error: IncidentError },
    /// A rule in this state cannot take this change, where `change` is `arm`, `disarm` or `save`.
    InvalidChange { from: RuleState, change: &'static str },
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSelector { rule, error } => write!(f, "rule {:?}: {error}", rule.as_str()),
            Self::UnknownPreset { rule, preset } => {
                write!(f, "rule {:?}: no preset is named {:?}", rule.as_str(), preset.as_str())
            }
            Self::LieBeforeTruth { rule, lie } => write!(
                f,
                "rule {:?} tests a response field, and its {lie} acts before forwarding, \
                 when the truth is not known yet. Give the lie `stage: after`",
                rule.as_str()
            ),
            Self::ManualWithoutSubject { rule, error } => write!(
                f,
                "rule {:?} can never fire, because its trigger is manual and a hand firing has no call: {error}",
                rule.as_str()
            ),
            Self::NoCallForIncident { rule, error } => write!(
                f,
                "rule {:?} cannot fire by hand, because there is no call: {error}",
                rule.as_str()
            ),
            Self::InvalidChange { from, change } => write!(f, "a rule that is {from} cannot {change}"),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for RuleError {}
