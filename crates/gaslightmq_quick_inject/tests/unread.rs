//! What the parser could not read: the word, where it stands, and why.
//!
//! Every table has the typed text on the left, and on the right each word that was not read, with what it says.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use std::error::Error;

use common::Shop;
use gaslightmq_quick_inject::prelude::*;

#[test]
fn a_word_that_means_nothing_is_unknown() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "delay prodcue 2s",
            "`prodcue` is no word of a sentence, and no name that is known",
        ),
        (
            "delay Orders.eu",
            "`Orders.eu` is no word of a sentence, and no name that is known",
        ),
        (
            "delay leader-fl",
            "`leader-fl` is no word of a sentence, and no name that is known",
        ),
    ];

    for (typed, message) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let [unread] = sentence.unread() else {
            panic!("{typed}: one word is unknown")
        };

        assert!(matches!(unread, Unread::Unknown { .. }), "{typed}");
        assert_eq!(unread.to_string(), message);
    }
}

#[test]
fn a_value_that_is_not_one_says_what_is_wrong_with_it() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        (
            "blackhole metadata 200%",
            "`200%`: chance `200%` is not between 0% and 100%",
        ),
        (
            "delay 1.5s",
            "`1.5s`: invalid time span `1.5s`: unknown unit `.5s`, expected ms, s, m or h",
        ),
        (
            "delay 750",
            "`750`: invalid time span `750`: missing unit, expected ms, s, m or h",
        ),
        (
            "delay for 0 firings",
            "`0 firings`: lifetime `0 firings`: a count is at least one",
        ),
        ("delay broker two", "`two`: broker ID is not a number: \"two\""),
        // The name of a selector is a slug.
        (
            "delay selector Order",
            "`Order`: selector name \"Order\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen",
        ),
        (
            "delay topic orders/eu",
            "`orders/eu`: topic: topic name \"orders/eu\" has the invalid character '/' at position 6",
        ),
    ];

    for (typed, message) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let [unread] = sentence.unread() else {
            panic!("{typed}: one value is not valid")
        };

        assert!(matches!(unread, Unread::Invalid { .. }), "{typed}");
        assert_eq!(unread.to_string(), message);
    }
}

#[test]
fn a_word_the_sentence_has_no_place_for_says_which_rule_it_breaks() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("delay error produce", "`error`: a sentence names one lie or one preset"),
        (
            "delay leader-flap",
            "`leader-flap`: a sentence names one lie or one preset",
        ),
        (
            "delay 1s 2s",
            "`2s`: a sentence names one time span, next to the one of its lifetime",
        ),
        (
            "error NOT_CONTROLLER NOT_COORDINATOR",
            "`NOT_COORDINATOR`: a refusal has one error code",
        ),
        ("delay 10% 20%", "`20%`: a sentence names one chance"),
        ("delay for 10s for 20s", "`20s`: a sentence names one lifetime"),
        ("delay selector a selector b", "`b`: a sentence names one selector"),
        ("blackhole 2s", "`2s`: only a delay and a throttle take a time span"),
        ("leader-flap 2s", "`2s`: only a delay and a throttle take a time span"),
        (
            "delay NOT_LEADER_OR_FOLLOWER",
            "`NOT_LEADER_OR_FOLLOWER`: only a refusal takes an error code",
        ),
        (
            "delay orders.* orders.eu",
            "`orders.eu`: a field takes one pattern, or one or more names",
        ),
        (
            "delay orders.eu orders.*",
            "`orders.*`: a field takes one pattern, or one or more names",
        ),
        (
            "delay order-producers produce",
            "`produce`: a sentence names a selector, or it says the calls itself with an API, a topic or a group",
        ),
        (
            "delay produce order-producers",
            "`order-producers`: a sentence names a selector, or it says the calls itself with an API, a topic or a group",
        ),
    ];

    for (typed, message) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let [unread] = sentence.unread() else {
            panic!("{typed}: one word has no place")
        };

        assert!(matches!(unread, Unread::Conflict { .. }), "{typed}");
        assert_eq!(unread.to_string(), message);
        assert!(sentence.rule().is_some(), "{typed}");
    }
}

#[test]
fn a_word_that_waits_for_the_next_one_says_what_it_needs() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("delay produce for", "`for` needs a client or a lifetime"),
        ("delay produce to", "`to` needs a topic"),
        ("delay produce by", "`by` needs a time span"),
        ("error produce with", "`with` needs an error code"),
        ("delay client", "`client` needs the ID of a client"),
        ("delay topic", "`topic` needs a topic"),
        ("delay group", "`group` needs a group"),
        ("delay broker", "`broker` needs the number of a broker"),
        ("preset", "`preset` needs the name of a preset"),
        ("delay selector", "`selector` needs the name of a selector"),
    ];

    for (typed, message) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let unfinished: Vec<String> = sentence
            .unread()
            .iter()
            .filter(|unread| matches!(unread, Unread::Unfinished { .. }))
            .map(ToString::to_string)
            .collect();

        assert_eq!(unfinished.last().map(String::as_str), Some(message), "{typed}");
    }
}

#[test]
fn a_refusal_without_its_error_code_is_no_rule_yet() {
    let quick_inject = QuickInject::default();
    let sentence = quick_inject.read("error produce 40%", &Shop::known());

    let [unread] = sentence.unread() else {
        panic!("the refusal has no error code")
    };
    assert!(matches!(unread, Unread::Unfinished { .. }));
    assert_eq!(unread.to_string(), "`error` needs an error code");
    assert_eq!(sentence.rule(), None);
}

#[test]
fn an_unread_word_stands_at_its_span() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("delay prodcue 2s", "prodcue", 6, 13),
        ("delay for 0 firings", "0 firings", 10, 19),
        ("délai produce", "délai", 0, 6),
        ("delay leader-fl", "leader-fl", 6, 15),
        ("delay produce for", "for", 14, 17),
    ];

    for (typed, word, start, end) in sentences {
        let sentence = quick_inject.read(typed, &known);
        let span = sentence.unread()[0].span();

        assert_eq!(sentence.unread()[0].word(), word, "{typed}");
        assert_eq!((span.start(), span.end()), (start, end), "{typed}");
        assert_eq!(&typed[span.range()], word, "{typed}");
        assert_eq!(span.len(), word.len(), "{typed}");
    }
}

#[test]
fn unread_words_stand_in_the_order_of_the_sentence() {
    let quick_inject = QuickInject::default();
    let sentence = quick_inject.read("blackhole 2s prodcue 200% NOT_CONTROLLER for", &Shop::known());

    let words: Vec<&str> = sentence.unread().iter().map(Unread::word).collect();

    assert_eq!(words, ["2s", "prodcue", "200%", "NOT_CONTROLLER", "for"]);
}

#[test]
fn a_word_that_was_not_read_changes_nothing_about_the_rule() {
    let quick_inject = QuickInject::default();
    let known = Shop::known();

    let sentences = [
        ("delay prodcue 2s", "delay 2s"),
        ("blackhole metadata 200% 2s", "blackhole metadata"),
        ("delay error produce for", "delay produce"),
    ];

    for (typed, read) in sentences {
        let rule = quick_inject.read(read, &known).into_rule();

        assert!(rule.is_some(), "{read}");
        assert_eq!(quick_inject.read(typed, &known).into_rule(), rule, "{typed}");
    }
}

#[test]
fn every_message_is_complete_and_has_no_source() {
    let quick_inject = QuickInject::default();
    let sentence = quick_inject.read("blackhole 2s prodcue 200% for", &Shop::known());

    assert_eq!(sentence.unread().len(), 4);
    for unread in sentence.unread() {
        assert!(unread.source().is_none(), "{unread}");
        assert!(unread.to_string().contains(unread.word()), "{unread}");
        assert!(!unread.to_string().contains('\n'), "{unread}");
    }
}
