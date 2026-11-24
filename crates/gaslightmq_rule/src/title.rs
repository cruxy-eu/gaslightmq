gaslightmq_resource::title! {
    /// It says what the rule is for, such as `Slow down order producers`.
    /// A [`Rule`](crate::Rule) has it as an optional part, next to its name.
    RuleTitle, RuleTitleError, "rule title"
}

gaslightmq_resource::title! {
    /// It says what the preset does, such as `Delay 1 s`.
    /// A [`Preset`](crate::Preset) has it as an optional part, next to its name.
    PresetTitle, PresetTitleError, "preset title"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_title_keeps_its_text() {
        for text in ["Slow down order producers", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let title = RuleTitle::new(text).unwrap();
            assert_eq!(title.as_str(), text);
            assert_eq!(title.to_string(), text);
            assert_eq!(title, text);
            assert_eq!(title, *text);
            assert_eq!(title.as_ref(), text);
            assert_eq!(text.parse::<RuleTitle>(), Ok(title.clone()));
            assert_eq!(RuleTitle::try_from(text), Ok(title.clone()));
            assert_eq!(RuleTitle::try_from(text.to_owned()), Ok(title.clone()));
            assert_eq!(String::from(title), text);
        }
    }

    #[test]
    fn a_title_that_breaks_the_rule_is_an_error() {
        use RuleTitleError::{ControlCharacter, Empty, FormatCharacter, SurroundingWhiteSpace, TooLong};

        let input = |text: &str| text.to_owned();
        let long = "é".repeat(RuleTitle::MAX_LENGTH + 1);
        let cases = [
            ("", Empty, "rule title \"\" is empty"),
            (
                long.as_str(),
                TooLong { length: 2001 },
                "rule title of 2001 characters is longer than 2000 characters",
            ),
            (
                " Leader flap",
                SurroundingWhiteSpace {
                    input: input(" Leader flap"),
                },
                "rule title \" Leader flap\" starts or ends with white space",
            ),
            (
                "Leader\nflap",
                ControlCharacter {
                    input: input("Leader\nflap"),
                },
                "rule title \"Leader\\nflap\" holds a control character",
            ),
            // A zero-width space hides between the two words.
            (
                "Leader\u{200b}flap",
                FormatCharacter {
                    input: input("Leader\u{200b}flap"),
                },
                "rule title \"Leader\\u{200b}flap\" holds a format character",
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(RuleTitle::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_title_has_at_most_2000_characters() {
        // Every `é` is one character of two bytes.
        let longest = "é".repeat(RuleTitle::MAX_LENGTH);

        assert_eq!(RuleTitle::MAX_LENGTH, 2000);
        assert_eq!(RuleTitle::new(&longest).unwrap(), *longest);
    }

    #[test]
    fn a_title_shows_as_its_text_when_debugging() {
        let title = RuleTitle::new("Leader flap").unwrap();

        assert_eq!(format!("{title:?}"), "RuleTitle(\"Leader flap\")");
    }

    #[test]
    fn the_error_of_a_preset_title_says_whose_title_it_is() {
        assert_eq!(PresetTitle::new(""), Err(PresetTitleError::Empty));
        assert_eq!(
            PresetTitle::new("Flap ").unwrap_err().to_string(),
            "preset title \"Flap \" starts or ends with white space"
        );
        assert_eq!(PresetTitle::MAX_LENGTH, 2000);
    }
}
