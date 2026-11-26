//! ⌘K: only the answer to the sentence typed now counts, Tab takes a completion, and Enter on a whole sentence
//! creates the live rule in the environment shown, armed, with one operation.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{PresetName, Rule};
use gaslightmq_control_api::selector::SelectorName;
use gaslightmq_ui::palette::{Jump, Palette, PaletteItem};

/// The rule that `delay-1s order-producers` gives.
fn delayed_producers() -> Rule {
    Rule::new(
        "delay-1s-on-order-producers".parse().unwrap(),
        "order-producers".parse::<SelectorName>().unwrap(),
        "delay-1s".parse::<PresetName>().unwrap(),
    )
}

/// The completion of `prod` to the API `produce`.
fn produce() -> WordCompletion {
    WordCompletion {
        kind: WordKind::Api,
        word: "produce".to_owned(),
        title: None,
        sentence: "delay produce ".to_owned(),
    }
}

#[test]
fn an_answer_to_an_older_sentence_changes_nothing() {
    let mut palette = Palette::default();
    let old = palette.type_text("delay prod".to_owned());
    let new = palette.type_text("delay-1s order-producers".to_owned());

    palette.answered(
        old.number,
        ParsedSentence {
            rule: None,
            completions: vec![produce()],
            unread: Vec::new(),
        },
    );
    assert_eq!(palette.parsed(), None);
    palette.answered(
        new.number,
        ParsedSentence {
            rule: Some(delayed_producers()),
            completions: Vec::new(),
            unread: Vec::new(),
        },
    );

    assert_eq!(palette.rule(), Some(&delayed_producers()));
    assert!(!palette.is_latest(old.number));
}

#[test]
fn the_list_offers_the_rule_first_then_the_completions_then_the_views() {
    let mut palette = Palette::default();
    let question = palette.type_text("delay-1s order-producers".to_owned());
    palette.answered(
        question.number,
        ParsedSentence {
            rule: Some(delayed_producers()),
            completions: vec![produce()],
            unread: Vec::new(),
        },
    );

    assert_eq!(
        palette.items(),
        [
            PaletteItem::Create(delayed_producers()),
            PaletteItem::Complete(produce()),
            PaletteItem::Jump(Jump::Record),
            PaletteItem::Jump(Jump::Rules),
            PaletteItem::Jump(Jump::Pads),
        ]
    );
    palette.highlight(-1);
    assert_eq!(
        palette.chosen(),
        Some(PaletteItem::Jump(Jump::Pads)),
        "the highlight wraps around"
    );
}

#[test]
fn tab_takes_the_completion_and_asks_again() {
    let mut palette = Palette::default();
    let question = palette.type_text("delay prod".to_owned());
    palette.answered(
        question.number,
        ParsedSentence {
            rule: None,
            completions: vec![produce()],
            unread: Vec::new(),
        },
    );

    let completion = palette.completion().unwrap();
    let next = palette.type_text(completion.sentence);

    assert_eq!(next.text, "delay produce ");
    assert!(palette.is_latest(next.number));
    assert_eq!(palette.parsed(), None, "the answer to the old sentence is gone");
}

#[test]
fn a_whole_sentence_creates_the_live_rule_in_the_environment_shown_armed_with_one_operation() {
    let mut palette = Palette::default();
    let environment: EnvironmentName = "orders-sim".parse().unwrap();
    let question = palette.type_text("delay-1s order-producers".to_owned());
    palette.answered(
        question.number,
        ParsedSentence {
            rule: Some(delayed_producers()),
            completions: Vec::new(),
            unread: Vec::new(),
        },
    );

    assert_eq!(
        palette.creation(&environment),
        Some(CreateLiveRule {
            environment,
            rule: delayed_producers(),
            armed: true,
        })
    );
}

#[test]
fn a_sentence_with_a_word_that_was_not_read_creates_nothing() {
    let mut palette = Palette::default();
    let question = palette.type_text("delay-1s order-producers 40%%".to_owned());
    palette.answered(
        question.number,
        ParsedSentence {
            rule: Some(delayed_producers()),
            completions: Vec::new(),
            unread: vec![UnreadWord {
                word: "40%%".to_owned(),
                start: 25,
                reason: "`40%%` is no chance".to_owned(),
            }],
        },
    );

    assert_eq!(palette.rule(), None);
    assert_eq!(palette.creation(&"orders-sim".parse().unwrap()), None);
    assert!(
        !palette
            .items()
            .iter()
            .any(|item| matches!(item, PaletteItem::Create(_))),
        "Enter cannot create a rule that leaves a word out"
    );
}
