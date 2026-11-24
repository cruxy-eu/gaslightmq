//! The name a call record knows a rule by.

gaslightmq_resource::name! {
    /// It is the name a rule had when a call met it, as the record of that call keeps it.
    /// An explanation names every rule the call touched, and the rule that won a conflict.
    ///
    /// A record outlives the rules it names: someone can delete a rule after the call,
    /// and make another rule under its name.
    /// A recorded name is therefore only a name, and nobody looks it up.
    RecordedRuleName, RecordedRuleNameError, "recorded rule name"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_recorded_name_keeps_its_text() {
        for text in ["slow-order-producers", "x", "7", "a--b"] {
            let name = RecordedRuleName::new(text).unwrap();

            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(name.as_ref(), text);
            assert_eq!(text.parse(), Ok(name.clone()));
            assert_eq!(RecordedRuleName::try_from(text), Ok(name.clone()));
            assert_eq!(RecordedRuleName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_recorded_name_that_breaks_the_rule_for_names_is_an_error() {
        use RecordedRuleNameError::{Empty, InvalidCharacter, SurroundingHyphen, TooLong};

        let input = |text: &str| text.to_owned();
        let long = "a".repeat(RecordedRuleName::MAX_LENGTH + 1);
        let cases = [
            ("", Empty, "recorded rule name \"\" is empty"),
            (
                "Leader flap",
                InvalidCharacter {
                    input: input("Leader flap"),
                    character: 'L',
                    position: 0,
                },
                "recorded rule name \"Leader flap\" holds 'L' at position 0, which is no lowercase letter, digit or hyphen",
            ),
            (
                "leader-flap-",
                SurroundingHyphen {
                    input: input("leader-flap-"),
                },
                "recorded rule name \"leader-flap-\" starts or ends with a hyphen",
            ),
            (
                long.as_str(),
                TooLong { length: 64 },
                "recorded rule name of 64 characters is longer than 63 characters",
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(RecordedRuleName::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(RecordedRuleName::new("leader-flap").unwrap(), 1);

        assert_eq!(by_name.get("leader-flap"), Some(&1));
        assert_eq!(by_name.get("leader-election"), None);
    }

    #[test]
    fn a_recorded_name_shows_as_its_text_when_debugging() {
        let name = RecordedRuleName::new("leader-flap").unwrap();

        assert_eq!(format!("{name:?}"), "RecordedRuleName(\"leader-flap\")");
    }

    #[test]
    fn a_recorded_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", RecordedRuleName::new("flap").unwrap()), "    flap|");
    }
}
