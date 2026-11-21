//! Every error says everything in one message, and none has a source.

use std::error::Error;

use gaslightmq_incident::{IncidentError, IncidentKind};
use gaslightmq_lie::LieKind;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::SelectorSetError;

fn rule(text: &str) -> RuleName {
    text.parse().unwrap()
}

fn unknown_selector() -> SelectorSetError {
    SelectorSetError::UnknownSelector {
        name: "Order producers".parse().unwrap(),
    }
}

fn missing_partition() -> IncidentError {
    IncidentError::MissingSubject {
        incident: IncidentKind::LeaderMoves,
        subject: "partition",
    }
}

fn every_error() -> Vec<(Box<dyn Error>, &'static str)> {
    vec![
        (Box::new(RuleNameError::Empty), "rule name \"\" is empty"),
        (
            Box::new(PresetNameError::SurroundingWhiteSpace { input: " x".into() }),
            "preset name \" x\" starts or ends with white space",
        ),
        (
            Box::new(RuleSetError::DuplicateName { name: rule("Flap") }),
            "two rules are named \"Flap\"",
        ),
        (
            Box::new(RuleSetError::UnknownRule { name: rule("Flap") }),
            "no rule is named \"Flap\"",
        ),
        (
            Box::new(PresetSetError::DuplicateName {
                name: "Delay 1 s".parse().unwrap(),
            }),
            "two presets are named \"Delay 1 s\"",
        ),
        (
            Box::new(SnapshotError::DuplicateName { name: rule("Flap") }),
            "two rules are named \"Flap\"",
        ),
        (
            Box::new(ResolvedRuleError::UnknownSelector {
                rule: rule("Flap"),
                error: unknown_selector(),
            }),
            "rule \"Flap\": no selector is named \"Order producers\"",
        ),
        (
            Box::new(ResolvedRuleError::UnknownPreset {
                rule: rule("Flap"),
                preset: "Leader flap".parse().unwrap(),
            }),
            "rule \"Flap\": no preset is named \"Leader flap\"",
        ),
        (
            Box::new(ResolvedRuleError::LieBeforeTruth {
                rule: rule("Flap"),
                lie: LieKind::Refusal,
            }),
            "rule \"Flap\" tests a response field, and its refusal acts before forwarding, \
             when the truth is not known yet. Give the lie `stage: after`",
        ),
        (
            Box::new(ResolvedRuleError::ManualWithoutSubject {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            "rule \"Election\" can never fire, because its trigger is manual and a hand firing has no call: \
             leader-moves needs a partition, and none is named",
        ),
        (
            Box::new(HandFiringError::NoCallForIncident {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            "rule \"Election\" cannot fire by hand, because there is no call: \
             leader-moves needs a partition, and none is named",
        ),
        (
            Box::new(RuleStateError::InvalidChange {
                from: RuleState::Armed,
                change: "arm",
            }),
            "a rule that is armed cannot arm",
        ),
        (
            Box::new(RuleStateError::Unknown { name: "Armed".into() }),
            "no state is called \"Armed\"",
        ),
    ]
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
fn every_error_says_what_is_wrong() {
    for (error, message) in every_error() {
        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn no_error_has_a_source_so_a_chain_prints_every_message_once() {
    for (error, message) in every_error() {
        assert!(error.source().is_none(), "{error}");
        assert_eq!(chain(error.as_ref()), message);
    }
}

#[test]
fn the_message_holds_the_error_of_another_crate_once() {
    let selector = unknown_selector().to_string();
    let incident = missing_partition().to_string();
    let cases: [(Box<dyn Error>, &String); 3] = [
        (
            Box::new(ResolvedRuleError::UnknownSelector {
                rule: rule("Flap"),
                error: unknown_selector(),
            }),
            &selector,
        ),
        (
            Box::new(ResolvedRuleError::ManualWithoutSubject {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            &incident,
        ),
        (
            Box::new(HandFiringError::NoCallForIncident {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            &incident,
        ),
    ];

    for (error, inner) in cases {
        assert_eq!(chain(error.as_ref()).matches(inner.as_str()).count(), 1, "{error}");
    }
}
