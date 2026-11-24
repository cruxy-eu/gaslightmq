//! What the word at the end of a sentence can become.
//!
//! Every table has the typed text on the left, and on the right each completion: its kind, and the word it writes.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use common::Shop;
use gaslightmq_quick_inject::prelude::*;

/// A completion as a table writes it: what it is, and the word.
type Offered = (CompletionKind, &'static str);

/// A completion as a table writes it, with the title that a surface shows next to the word.
type Titled = (CompletionKind, &'static str, Option<&'static str>);

const LIE: CompletionKind = CompletionKind::Lie;
const PRESET: CompletionKind = CompletionKind::Preset;
const API: CompletionKind = CompletionKind::Api;
const SELECTOR: CompletionKind = CompletionKind::Selector;
const TOPIC: CompletionKind = CompletionKind::Topic;
const CLIENT: CompletionKind = CompletionKind::Client;
const ERROR_CODE: CompletionKind = CompletionKind::ErrorCode;

#[test]
fn the_word_at_the_end_completes_to_the_words_that_start_like_it() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences: [(&str, &[Offered]); 7] = [
        ("thr", &[(LIE, "throttle"), (ERROR_CODE, "THROTTLING_QUOTA_EXCEEDED")]),
        ("delay fetchs", &[(API, "FetchSnapshot")]),
        ("delay FETCHS", &[(API, "FetchSnapshot")]),
        (
            "delay produce ord",
            &[(TOPIC, "orders.eu"), (TOPIC, "orders.us"), (CLIENT, "order-ingest")],
        ),
        (
            "error produce not_c",
            &[(ERROR_CODE, "NOT_COORDINATOR"), (ERROR_CODE, "NOT_CONTROLLER")],
        ),
        (
            "lea",
            &[
                (PRESET, "leader-election"),
                (PRESET, "leader-flap"),
                (API, "LeaveGroup"),
                (ERROR_CODE, "LEADER_NOT_AVAILABLE"),
            ],
        ),
        // The word is there already, so nothing completes it.
        ("delay heartbeat", &[]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word()))
            .collect();

        assert_eq!(completions, offered, "{typed}");
    }
}

#[test]
fn a_completion_gives_the_whole_sentence_with_room_for_the_next_word() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("del", "delay "),
        ("delay prod", "delay Produce "),
        ("delay produce  ord", "delay produce  orders.eu "),
        ("delay produce client ", "delay produce client order-ingest "),
        ("delay order-producers ", "delay order-producers order-ingest "),
        ("leader-fl", "leader-flap "),
        // A word of a title finds the name, and the sentence takes the name.
        ("flap", "leader-flap "),
        ("delay producers", "delay order-producers "),
        ("leader-flap on order-p", "leader-flap on order-producers "),
    ];

    for (typed, completed) in sentences {
        let sentence = quick_inject.read(typed, &known);

        assert_eq!(
            sentence.completions().first().map(Completion::sentence),
            Some(completed),
            "{typed}"
        );
    }
}

#[test]
fn a_preset_and_a_selector_complete_from_a_word_of_their_title() {
    let quick_inject = QuickInject::default();
    let known = Shop::known().with_presets([("election-storm".parse().unwrap(), None)]);

    let sentences: [(&str, &[Titled]); 8] = [
        ("flap", &[(PRESET, "leader-flap", Some("Leader flap"))]),
        (
            "delay prod",
            &[
                (API, "Produce", None),
                (SELECTOR, "order-producers", Some("Order producers")),
            ],
        ),
        (
            "delay PROD",
            &[
                (API, "Produce", None),
                (SELECTOR, "order-producers", Some("Order producers")),
            ],
        ),
        // The whole word of a title finds the name too.
        (
            "delay producers",
            &[(SELECTOR, "order-producers", Some("Order producers"))],
        ),
        // A name that starts like the word comes before a name with such a title.
        (
            "elec",
            &[
                (PRESET, "election-storm", None),
                (PRESET, "leader-election", Some("Leader election")),
                (API, "ElectLeaders", None),
                (ERROR_CODE, "ELECTION_NOT_NEEDED", None),
            ],
        ),
        // A name that starts like the word carries its title as well.
        ("order-p", &[(SELECTOR, "order-producers", Some("Order producers"))]),
        // The word starts a word of the title, and does not stand somewhere inside one.
        ("delay ducers", &[]),
        // The name is there already.
        ("leader-flap", &[]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str, Option<&str>)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word(), completion.title()))
            .collect();

        assert_eq!(completions, offered, "{typed}");
    }
}

#[test]
fn a_topic_and_a_client_have_no_title() {
    let quick_inject = QuickInject::default();
    let sentence = quick_inject.read("delay produce or", &Shop::known());

    let titles: Vec<(CompletionKind, Option<&str>)> = sentence
        .completions()
        .iter()
        .map(|completion| (completion.kind(), completion.title()))
        .collect();

    assert_eq!(titles, [(TOPIC, None), (TOPIC, None), (CLIENT, None)]);
}

#[test]
fn a_sentence_that_ends_between_two_words_offers_the_next_word() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences: [(&str, &[Offered]); 4] = [
        (
            "",
            &[
                (LIE, "delay"),
                (LIE, "throttle"),
                (LIE, "error"),
                (LIE, "blackhole"),
                (LIE, "disconnect"),
            ],
        ),
        (
            "delay ",
            &[
                (API, "Produce"),
                (API, "Fetch"),
                (API, "ListOffsets"),
                (API, "Metadata"),
                (API, "OffsetCommit"),
            ],
        ),
        ("delay client ", &[(CLIENT, "order-ingest"), (CLIENT, "payments")]),
        ("delay broker ", &[]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word()))
            .collect();

        assert_eq!(completions, offered, "{typed}");
    }
}

#[test]
fn a_sentence_offers_only_what_it_still_has_a_place_for() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences: [(&str, &[Offered]); 5] = [
        // No second lie and no preset, and no error code for a delay.
        ("delay lea", &[(API, "LeaveGroup")]),
        ("delay thr", &[]),
        // No selector next to an API, and no API next to a selector.
        (
            "delay produce or",
            &[(TOPIC, "orders.eu"), (TOPIC, "orders.us"), (CLIENT, "order-ingest")],
        ),
        ("delay order-producers pro", &[]),
        // No second error code.
        ("error NOT_CONTROLLER not_c", &[]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word()))
            .collect();

        assert_eq!(completions, offered, "{typed}");
    }
}

#[test]
fn the_word_that_is_being_typed_does_not_decide_what_it_can_become() {
    let quick_inject = QuickInject::default();
    let known = Shop::known().with_presets([("delay-1s".parse().unwrap(), None)]);

    let sentences: [(&str, &[Offered]); 3] = [
        // `delay` is the lie already, and it can still become the preset.
        ("delay", &[(PRESET, "delay-1s")]),
        // After `on`, `order-p` is a topic already, and it can still become the selector.
        ("leader-flap on order-p", &[(SELECTOR, "order-producers")]),
        // The words before it do decide: a sentence with an API takes no selector.
        ("leader-flap produce on order-p", &[]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word()))
            .collect();

        assert_eq!(completions, offered, "{typed}");
    }
}

#[test]
fn a_name_completes_to_words_that_read_back_as_that_name() {
    let quick_inject = QuickInject::default();
    let known = Shop::known()
        .with_topics(["fetch".parse().unwrap()])
        .with_presets([("blackhole".parse().unwrap(), None)]);

    let sentences: [(&str, &[Offered]); 6] = [
        (
            "delay or",
            &[
                (SELECTOR, "order-producers"),
                (TOPIC, "orders.eu"),
                (TOPIC, "orders.us"),
                (CLIENT, "order-ingest"),
            ],
        ),
        // `payments` is a topic first, so the client needs its word.
        ("delay pay", &[(TOPIC, "payments"), (CLIENT, "client payments")]),
        // After `for` it is a client first, so the topic needs its word.
        ("delay for pay", &[(CLIENT, "payments"), (TOPIC, "topic payments")]),
        ("delay client pay", &[(CLIENT, "payments")]),
        // `fetch` is an API first and `blackhole` is a lie first, so the topic and the preset need their word.
        (
            "delay fet",
            &[(API, "Fetch"), (API, "FetchSnapshot"), (TOPIC, "topic fetch")],
        ),
        ("bla", &[(LIE, "blackhole"), (PRESET, "preset blackhole")]),
    ];

    for (typed, offered) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let completions: Vec<(CompletionKind, &str)> = sentence
            .completions()
            .iter()
            .map(|completion| (completion.kind(), completion.word()))
            .collect();
        assert_eq!(completions, offered, "{typed}");

        for completion in sentence.completions() {
            let completed = quick_inject.read(completion.sentence(), &known);
            assert_eq!(completed.unread(), [], "{}", completion.sentence());
        }
    }
}

#[test]
fn a_name_with_white_space_is_not_offered_because_no_sentence_says_it() {
    let quick_inject = QuickInject::default();
    let known = KnownNames::new().with_clients(["batch 1".parse().unwrap(), "batch-2".parse().unwrap()]);

    let sentence = quick_inject.read("delay client ", &known);
    let words: Vec<&str> = sentence.completions().iter().map(Completion::word).collect();

    assert_eq!(words, ["batch-2"]);
}

#[test]
fn the_configuration_limits_how_many_completions_a_sentence_offers() {
    let known = Shop::known();
    let every_api_starts_with_nothing = "delay ";

    for limit in [0, 1, 2, 12] {
        let mut configuration = QuickInjectConfiguration::default();
        configuration.completion_limit = limit;
        let quick_inject = QuickInject::new(configuration);

        let sentence = quick_inject.read(every_api_starts_with_nothing, &known);

        assert_eq!(sentence.completions().len(), limit);
    }
}

#[test]
fn a_surface_shows_the_kind_next_to_a_completion() {
    let names: Vec<String> = CompletionKind::ALL.iter().map(ToString::to_string).collect();

    assert_eq!(
        names,
        [
            "lie",
            "preset",
            "API",
            "selector",
            "topic",
            "group",
            "client",
            "error code"
        ]
    );
    assert_eq!(CompletionKind::Group.name(), "group");
}
