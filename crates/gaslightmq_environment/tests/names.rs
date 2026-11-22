//! The names of this crate follow the one rule for names, which the names of selectors, rules and presets follow.
//!
//! This crate does not reach the crate of the selectors, so it holds the rule a second time.
//! These tests hold the two together.

use gaslightmq_environment::prelude::*;
use gaslightmq_selector::prelude::*;

/// Whether the name of a selector and the name of an environment take the text, after checking that they agree.
///
/// The name of a world takes what the name of an environment takes: one check serves both.
fn is_a_name(text: &str) -> bool {
    let selector = SelectorName::check(text).is_ok();
    assert_eq!(EnvironmentName::new(text).is_ok(), selector, "{text:?}");
    selector
}

#[test]
fn every_character_is_in_a_name_of_an_environment_when_it_is_in_the_name_of_a_selector() {
    let mut refused = 0;
    let mut text = String::new();
    for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
        // In the middle of a name, where white space is no reason to refuse it.
        text.clear();
        text.extend(['a', character, 'b']);
        if !is_a_name(&text) {
            refused += 1;
        }
    }
    // The 65 control characters and the 170 format characters.
    assert_eq!(refused, 235);
}

#[test]
fn white_space_starts_and_ends_no_name_of_an_environment_as_it_does_for_a_selector() {
    let mut white_space = 0;
    for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
        if character.is_whitespace() {
            assert!(!is_a_name(&format!("{character}shop")));
            assert!(!is_a_name(&format!("shop{character}")));
            white_space += 1;
        }
    }
    assert_eq!(white_space, 25);
}

#[test]
fn a_text_breaks_the_same_part_of_the_rule_for_every_name() {
    let cases = [
        ("", "is empty"),
        (" shop", "starts or ends with white space"),
        ("shop\u{a0}", "starts or ends with white space"),
        ("orders\nsim", "holds a control character"),
        ("orders\u{200b}sim", "holds a format character"),
        // White space at an end is named before a control character in the middle.
        ("orders\nsim ", "starts or ends with white space"),
        // A control character is named before a format character.
        ("orders\u{200b}\tsim", "holds a control character"),
    ];

    for (text, part) in cases {
        let selector = SelectorName::new(text).unwrap_err().to_string();
        let environment = EnvironmentName::new(text).unwrap_err().to_string();
        let world = WorldName::new(text).unwrap_err().to_string();

        assert!(selector.ends_with(part), "{selector}");
        assert_eq!(environment, selector.replacen("selector name", "environment name", 1));
        assert_eq!(world, selector.replacen("selector name", "world name", 1));
    }
}
