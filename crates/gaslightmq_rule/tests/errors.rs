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
        name: "Order producers".parse().unwrap(),
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
            said(RuleSetError::DuplicateName { name: rule("Flap") }),
            "two rules are named \"Flap\"",
        ),
        (
            said(PresetSetError::DuplicateName {
                name: "Delay 1 s".parse().unwrap(),
            }),
            "two presets are named \"Delay 1 s\"",
        ),
        (
            said(SnapshotError::DuplicateName { name: rule("Flap") }),
            "two rules are named \"Flap\"",
        ),
        (
            said(RuleError::UnknownSelector {
                rule: rule("Flap"),
                error: unknown_selector(),
            }),
            "rule \"Flap\": no selector is named \"Order producers\"",
        ),
        (
            said(RuleError::UnknownPreset {
                rule: rule("Flap"),
                preset: "Leader flap".parse().unwrap(),
            }),
            "rule \"Flap\": no preset is named \"Leader flap\"",
        ),
        (
            said(RuleError::LieBeforeTruth {
                rule: rule("Flap"),
                lie: LieKind::Refusal,
            }),
            "rule \"Flap\" tests a response field, and its refusal acts before forwarding, \
             when the truth is not known yet. Give the lie `stage: after`",
        ),
        (
            said(RuleError::ManualWithoutSubject {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            "rule \"Election\" can never fire, because its trigger is manual and a hand firing has no call: \
             leader-moves needs a partition, and none is named",
        ),
        (
            said(RuleError::NoCallForIncident {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            "rule \"Election\" cannot fire by hand, because there is no call: \
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
                rule: rule("Flap"),
                error: unknown_selector(),
            }),
            &selector,
        ),
        (
            said(RuleError::ManualWithoutSubject {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            &incident,
        ),
        (
            said(RuleError::NoCallForIncident {
                rule: rule("Election"),
                error: missing_partition(),
            }),
            &incident,
        ),
    ];

    for (said, inner) in cases {
        assert_eq!(said.chain.matches(inner.as_str()).count(), 1, "{}", said.message);
    }
}
