gaslightmq_resource::title! {
    /// It says what the environment is, such as `The proxy in front of staging`.
    /// An [`Environment`](crate::Environment) has it as an optional part, next to its name.
    EnvironmentTitle, EnvironmentTitleError, "environment title"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_title_keeps_its_text() {
        for text in [
            "The proxy in front of staging",
            "x",
            "Ünïcode ✓",
            "a  b",
            "orders.* (EU)",
        ] {
            let title = EnvironmentTitle::new(text).unwrap();
            assert_eq!(title.as_str(), text);
            assert_eq!(title.to_string(), text);
            assert_eq!(title, text);
            assert_eq!(title, *text);
            assert_eq!(title.as_ref(), text);
            assert_eq!(text.parse::<EnvironmentTitle>(), Ok(title.clone()));
            assert_eq!(EnvironmentTitle::try_from(text), Ok(title.clone()));
            assert_eq!(EnvironmentTitle::try_from(text.to_owned()), Ok(title.clone()));
            assert_eq!(String::from(title), text);
        }
    }

    #[test]
    fn a_title_that_breaks_the_rule_is_an_error() {
        use EnvironmentTitleError::{ControlCharacter, Empty, FormatCharacter, SurroundingWhiteSpace, TooLong};

        let input = |text: &str| text.to_owned();
        let long = "é".repeat(EnvironmentTitle::MAX_LENGTH + 1);
        let cases = [
            ("", Empty, "environment title \"\" is empty"),
            (
                long.as_str(),
                TooLong { length: 2001 },
                "environment title of 2001 characters is longer than 2000 characters",
            ),
            (
                " Leader flap",
                SurroundingWhiteSpace {
                    input: input(" Leader flap"),
                },
                "environment title \" Leader flap\" starts or ends with white space",
            ),
            (
                "Leader\nflap",
                ControlCharacter {
                    input: input("Leader\nflap"),
                },
                "environment title \"Leader\\nflap\" holds a control character",
            ),
            // A zero-width space hides between the two words.
            (
                "Leader\u{200b}flap",
                FormatCharacter {
                    input: input("Leader\u{200b}flap"),
                },
                "environment title \"Leader\\u{200b}flap\" holds a format character",
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(EnvironmentTitle::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_title_has_at_most_2000_characters() {
        // Every `é` is one character of two bytes.
        let longest = "é".repeat(EnvironmentTitle::MAX_LENGTH);

        assert_eq!(EnvironmentTitle::MAX_LENGTH, 2000);
        assert_eq!(EnvironmentTitle::new(&longest).unwrap(), *longest);
    }

    #[test]
    fn a_title_shows_as_its_text_when_debugging() {
        let title = EnvironmentTitle::new("Leader flap").unwrap();

        assert_eq!(format!("{title:?}"), "EnvironmentTitle(\"Leader flap\")");
    }
}
