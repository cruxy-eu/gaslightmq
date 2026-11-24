//! Every error says everything in one message, and none has a source.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

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
        name: "order-producers".parse().unwrap(),
    }
}

fn missing_partition() -> IncidentError {
    IncidentError::MissingSubject {
        incident: IncidentKind::LeaderMoves,
        subject: "partition",
    }
}

/// What an error says: its message, and what a reporter prints that walks the chain of sources.
struct Said {
    message: String,
    chain: String,
}

fn said<E: Error>(error: E) -> Said {
    Said {
        message: error.to_string(),
        chain: chain(&error),
    }
}

fn every_error() -> Vec<(Said, &'static str)> {
    vec![
        (said(RuleNameError::Empty), "rule name \"\" is empty"),
        (
            said(PresetNameError::SurroundingHyphen { input: "-x".into() }),
            "preset name \"-x\" starts or ends with a hyphen",
        ),
        (
            said(RuleUuidError { input: "flap".into() }),
            "rule UUID \"flap\" is no UUID of version 7",
        ),
        (
            said(PresetUuidError {
                input: "delay-1s".into(),
            }),
            "preset UUID \"delay-1s\" is no UUID of version 7",
        ),
        (said(RuleTitleError::Empty), "rule title \"\" is empty"),
        (
            said(PresetTitleError::SurroundingWhiteSpace { input: " x".into() }),
            "preset title \" x\" starts or ends with white space",
        ),
        (
            said(RuleSetError::DuplicateName { name: rule("flap") }),
            "two rules are named \"flap\"",
        ),
        (
            said(PresetSetError::DuplicateName {
                name: "delay-1s".parse().unwrap(),
            }),
            "two presets are named \"delay-1s\"",
        ),
        (
            said(SnapshotError::DuplicateName { name: rule("flap") }),
            "two rules are named \"flap\"",
        ),
        (
            said(RuleError::UnknownSelector {
                rule: rule("flap"),
                error: unknown_selector(),
            }),
            "rule \"flap\": no selector is named \"order-producers\"",
        ),
        (
            said(RuleError::UnknownPreset {
                rule: rule("flap"),
                preset: "leader-flap".parse().unwrap(),
            }),
            "rule \"flap\": no preset is named \"leader-flap\"",
        ),
        (
            said(RuleError::LieBeforeTruth {
                rule: rule("flap"),
                lie: LieKind::Refusal,
            }),
            "rule \"flap\" tests a response field, and its refusal acts before forwarding, \
             when the truth is not known yet. Give the lie `stage: after`",
        ),
        (
            said(RuleError::ManualWithoutSubject {
                rule: rule("election"),
                error: missing_partition(),
            }),
            "rule \"election\" can never fire, because its trigger is manual and a hand firing has no call: \
             leader-moves needs a partition, and none is named",
        ),
        (
            said(RuleError::NoCallForIncident {
                rule: rule("election"),
                error: missing_partition(),
            }),
            "rule \"election\" cannot fire by hand, because there is no call: \
             leader-moves needs a partition, and none is named",
        ),
        (
            said(RuleError::InvalidChange {
                from: RuleState::Armed,
                change: "arm",
            }),
            "a rule that is armed cannot arm",
        ),
        (
            said(RuleStateError::Unknown { name: "Armed".into() }),
            "no state is called \"Armed\"",
        ),
    ]
}

/// Writes an error the way a reporter does that walks the chain: every message, and then its source.
fn chain<E: Error>(error: &E) -> String {
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
    for (said, message) in every_error() {
        assert_eq!(said.message, message);
    }
}

#[test]
fn no_error_has_a_source_so_a_chain_prints_every_message_once() {
    for (said, message) in every_error() {
        assert_eq!(said.chain, message);
    }
}

#[test]
fn the_message_holds_the_error_of_another_crate_once() {
    let selector = unknown_selector().to_string();
    let incident = missing_partition().to_string();
    let cases: [(Said, &String); 3] = [
        (
            said(RuleError::UnknownSelector {
                rule: rule("flap"),
                error: unknown_selector(),
            }),
            &selector,
        ),
        (
            said(RuleError::ManualWithoutSubject {
                rule: rule("election"),
                error: missing_partition(),
            }),
            &incident,
        ),
        (
            said(RuleError::NoCallForIncident {
                rule: rule("election"),
                error: missing_partition(),
            }),
            &incident,
        ),
    ];

    for (said, inner) in cases {
        assert_eq!(said.chain.matches(inner.as_str()).count(), 1, "{}", said.message);
    }
}
