gaslightmq_resource::name!(SelectorName, SelectorNameError, "selector name");

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["Order producers", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let name = SelectorName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(name.as_ref(), text);
            assert_eq!(text.parse::<SelectorName>(), Ok(name.clone()));
            assert_eq!(SelectorName::try_from(text), Ok(name.clone()));
            assert_eq!(SelectorName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        use SelectorNameError::{ControlCharacter, FormatCharacter, SurroundingWhiteSpace};

        type Breaks = fn(String) -> SelectorNameError;
        let cases: [(&str, Breaks); 10] = [
            (" ", |input| SurroundingWhiteSpace { input }),
            (" Order producers", |input| SurroundingWhiteSpace { input }),
            ("Order producers\n", |input| SurroundingWhiteSpace { input }),
            ("Order\nproducers", |input| ControlCharacter { input }),
            ("Order\tproducers", |input| ControlCharacter { input }),
            // A zero-width space, a right-to-left override and a byte order mark.
            ("Order\u{200b}producers", |input| FormatCharacter { input }),
            ("Order \u{202e}producers", |input| FormatCharacter { input }),
            ("\u{feff}Order producers", |input| FormatCharacter { input }),
            ("Order producers\u{200d}", |input| FormatCharacter { input }),
            ("Order\u{ad}producers", |input| FormatCharacter { input }),
        ];

        assert_eq!(SelectorName::new(""), Err(SelectorNameError::Empty));
        for (text, error) in cases {
            assert_eq!(SelectorName::new(text), Err(error(text.to_owned())), "{text:?}");
        }
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let cases = [
            ("", "selector name \"\" is empty"),
            (
                "Order producers ",
                "selector name \"Order producers \" starts or ends with white space",
            ),
            (
                "Order\nproducers",
                "selector name \"Order\\nproducers\" holds a control character",
            ),
            (
                "Order\u{2060}producers",
                "selector name \"Order\\u{2060}producers\" holds a format character",
            ),
        ];

        for (text, message) in cases {
            assert_eq!(SelectorName::new(text).unwrap_err().to_string(), message);
        }
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        let name = SelectorName::new("Order producers").unwrap();

        assert_eq!(format!("{name:?}"), "SelectorName(\"Order producers\")");
    }

    #[test]
    fn a_name_is_exact() {
        assert_ne!(
            SelectorName::new("Order producers").unwrap(),
            SelectorName::new("order producers").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let name = || SelectorName::new("Order producers").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("Order producers"), Some(&1));
        assert_eq!(sorted.get("Order consumers"), None);
        assert_eq!(hashed.get("Order producers"), Some(&1));
        assert_eq!(hashed.get("Order consumers"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", SelectorName::new("Flap").unwrap()), "    Flap|");
    }
}
