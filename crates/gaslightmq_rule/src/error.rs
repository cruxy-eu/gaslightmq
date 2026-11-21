use std::fmt;

use gaslightmq_incident::IncidentError;
use gaslightmq_lie::LieKind;
use gaslightmq_selector::SelectorError;

use crate::{PresetName, RuleName, RuleState};

/// Why a value cannot become a part of a rule, why a rule does not resolve, or why it cannot do what was asked.
///
/// ```
/// use gaslightmq_rule::prelude::*;
///
/// let error = "".parse::<RuleName>().unwrap_err();
/// assert_eq!(error.to_string(), "rule name \"\" is empty");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuleError {
    /// A name breaks the rule for names, where `what` is `rule name` or `preset name`.
    InvalidName {
        what: &'static str,
        input: String,
        reason: &'static str,
    },
    /// Two rules have the same name.
    DuplicateRule { name: RuleName },
    /// Two presets have the same name.
    DuplicatePreset { name: PresetName },
    /// No rule has this name.
    UnknownRule { name: RuleName },
    /// The selector of a rule does not resolve: the rule names a selector that does not exist.
    UnknownSelector { rule: RuleName, error: SelectorError },
    /// A rule names a preset that does not exist.
    UnknownPreset { rule: RuleName, preset: PresetName },
    /// A rule tests a response field, and its lie acts before forwarding, when the truth is not known yet.
    LieBeforeTruth { rule: RuleName, lie: LieKind },
    /// A rule cannot fire its incident by hand, because the incident takes a subject from the call and there is none.
    NoCallForIncident { rule: RuleName, error: IncidentError },
    /// A rule in this state cannot take this change, where `change` is `arm`, `disarm` or `save`.
    InvalidStateChange { from: RuleState, change: &'static str },
    /// No state has this name.
    UnknownState { name: String },
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { what, input, reason } => write!(f, "{what} {input:?} {reason}"),
            Self::DuplicateRule { name } => write!(f, "two rules are named {:?}", name.as_str()),
            Self::DuplicatePreset { name } => write!(f, "two presets are named {:?}", name.as_str()),
            Self::UnknownRule { name } => write!(f, "no rule is named {:?}", name.as_str()),
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
            Self::NoCallForIncident { rule, error } => write!(
                f,
                "rule {:?} cannot fire by hand, because there is no call: {error}",
                rule.as_str()
            ),
            Self::InvalidStateChange { from, change } => write!(f, "a rule that is {from} cannot {change}"),
            Self::UnknownState { name } => write!(f, "no state is called {name:?}"),
        }
    }
}

impl std::error::Error for RuleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnknownSelector { error, .. } => Some(error),
            Self::NoCallForIncident { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_incident::IncidentKind;

    use super::*;

    fn rule(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    fn unknown_selector() -> SelectorError {
        SelectorError::UnknownSelector {
            name: "Order producers".parse().unwrap(),
        }
    }

    fn missing_partition() -> IncidentError {
        IncidentError::MissingSubject {
            incident: IncidentKind::LeaderMoves,
            subject: "partition",
        }
    }

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                RuleError::InvalidName {
                    what: "preset name",
                    input: " x".into(),
                    reason: "starts or ends with white space",
                },
                "preset name \" x\" starts or ends with white space",
            ),
            (
                RuleError::DuplicateRule { name: rule("Flap") },
                "two rules are named \"Flap\"",
            ),
            (
                RuleError::DuplicatePreset {
                    name: "Delay 1 s".parse().unwrap(),
                },
                "two presets are named \"Delay 1 s\"",
            ),
            (
                RuleError::UnknownRule { name: rule("Flap") },
                "no rule is named \"Flap\"",
            ),
            (
                RuleError::UnknownSelector {
                    rule: rule("Flap"),
                    error: unknown_selector(),
                },
                "rule \"Flap\": no selector is named \"Order producers\"",
            ),
            (
                RuleError::UnknownPreset {
                    rule: rule("Flap"),
                    preset: "Leader flap".parse().unwrap(),
                },
                "rule \"Flap\": no preset is named \"Leader flap\"",
            ),
            (
                RuleError::LieBeforeTruth {
                    rule: rule("Flap"),
                    lie: LieKind::Refusal,
                },
                "rule \"Flap\" tests a response field, and its refusal acts before forwarding, \
                 when the truth is not known yet. Give the lie `stage: after`",
            ),
            (
                RuleError::NoCallForIncident {
                    rule: rule("Election"),
                    error: missing_partition(),
                },
                "rule \"Election\" cannot fire by hand, because there is no call: \
                 leader-moves needs a partition, and none is named",
            ),
            (
                RuleError::InvalidStateChange {
                    from: RuleState::Armed,
                    change: "arm",
                },
                "a rule that is armed cannot arm",
            ),
            (
                RuleError::UnknownState { name: "Armed".into() },
                "no state is called \"Armed\"",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn the_error_of_another_crate_is_the_source() {
        let selector = RuleError::UnknownSelector {
            rule: rule("Flap"),
            error: unknown_selector(),
        };
        let incident = RuleError::NoCallForIncident {
            rule: rule("Election"),
            error: missing_partition(),
        };

        assert_eq!(
            selector.source().map(ToString::to_string),
            Some(unknown_selector().to_string())
        );
        assert_eq!(
            incident.source().map(ToString::to_string),
            Some(missing_partition().to_string())
        );
        assert!(RuleError::UnknownRule { name: rule("Flap") }.source().is_none());
    }
}
