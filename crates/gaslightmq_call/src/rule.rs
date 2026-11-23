//! The name a call record knows a rule by.

gaslightmq_resource::name! {
    /// It is the name a rule had when a call met it, as the record of that call keeps it.
    /// An explanation names every rule the call touched, and the rule that won a conflict.
    ///
    /// A record outlives the rules it names: someone can rename or delete a rule after the call.
    /// A recorded name is therefore only text that names a rule, and nobody looks it up.
    RecordedRuleName, RecordedRuleNameError, "recorded rule name"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_recorded_name_keeps_its_text() {
        for text in ["Slow down order producers", "x", "Ünïcode ✓", "a  b"] {
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
        use RecordedRuleNameError::{ControlCharacter, Empty, FormatCharacter, SurroundingWhiteSpace};

        let input = |text: &str| text.to_owned();
        let cases = [
            ("", Empty, "recorded rule name \"\" is empty"),
            (
                " Leader flap",
                SurroundingWhiteSpace {
                    input: input(" Leader flap"),
                },
                "recorded rule name \" Leader flap\" starts or ends with white space",
            ),
            (
                "Leader\nflap",
                ControlCharacter {
                    input: input("Leader\nflap"),
                },
                "recorded rule name \"Leader\\nflap\" holds a control character",
            ),
            (
                "Leader\u{200b}flap",
                FormatCharacter {
                    input: input("Leader\u{200b}flap"),
                },
                "recorded rule name \"Leader\\u{200b}flap\" holds a format character",
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
        by_name.insert(RecordedRuleName::new("Leader flap").unwrap(), 1);

        assert_eq!(by_name.get("Leader flap"), Some(&1));
        assert_eq!(by_name.get("leader flap"), None);
    }

    #[test]
    fn a_recorded_name_shows_as_its_text_when_debugging() {
        let name = RecordedRuleName::new("Leader flap").unwrap();

        assert_eq!(format!("{name:?}"), "RecordedRuleName(\"Leader flap\")");
    }

    #[test]
    fn a_recorded_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", RecordedRuleName::new("Flap").unwrap()), "    Flap|");
    }
}
