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
///
/// # One message says it all
///
/// The message of an error is complete.
/// A variant that holds the error of another crate writes that error into its own message,
/// because a file reader and the UI show one line, and not a chain.
/// No variant therefore has a [`source`](std::error::Error::source):
/// a reporter that walks the chain would print the inner message twice.
/// The inner error is the `error` field of its variant, for code that wants to look at it.
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
    /// A rule only fires by hand, and its incident takes a subject from the call.
    /// A hand firing has no call, so the rule could never fire.
    ManualWithoutSubject { rule: RuleName, error: IncidentError },
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
            Self::InvalidStateChange { from, change } => write!(f, "a rule that is {from} cannot {change}"),
            Self::UnknownState { name } => write!(f, "no state is called {name:?}"),
        }
    }
}

/// The message is complete, so no variant has a source. See [`RuleError`].
impl std::error::Error for RuleError {}

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
                RuleError::ManualWithoutSubject {
                    rule: rule("Election"),
                    error: missing_partition(),
                },
                "rule \"Election\" can never fire, because its trigger is manual and a hand firing has no call: \
                 leader-moves needs a partition, and none is named",
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

    /// Writes an error the way a reporter does that walks the chain: every message, and then its source.
    fn chain(error: &dyn Error) -> String {
        let mut messages = vec![error.to_string()];
        let mut source = error.source();
        while let Some(inner) = source {
            messages.push(inner.to_string());
            source = inner.source();
        }
        messages.join(": ")
    }

    #[test]
    fn the_message_holds_the_error_of_another_crate_and_the_chain_prints_it_once() {
        let selector = unknown_selector().to_string();
        let incident = missing_partition().to_string();
        let cases = [
            (
                RuleError::UnknownSelector {
                    rule: rule("Flap"),
                    error: unknown_selector(),
                },
                &selector,
            ),
            (
                RuleError::ManualWithoutSubject {
                    rule: rule("Election"),
                    error: missing_partition(),
                },
                &incident,
            ),
            (
                RuleError::NoCallForIncident {
                    rule: rule("Election"),
                    error: missing_partition(),
                },
                &incident,
            ),
        ];

        for (error, inner) in cases {
            assert!(error.source().is_none(), "{error}");
            assert_eq!(chain(&error), error.to_string());
            assert_eq!(chain(&error).matches(inner.as_str()).count(), 1, "{error}");
        }
    }
}
