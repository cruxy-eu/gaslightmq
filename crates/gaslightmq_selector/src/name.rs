gaslightmq_resource::name!(SelectorName, SelectorNameError, "selector name");

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["order-producers", "x", "7", "a--b", "orders-eu-2"] {
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
        use SelectorNameError::{InvalidCharacter, SurroundingHyphen, TooLong};

        let invalid = |input: &str, character, position| InvalidCharacter {
            input: input.to_owned(),
            character,
            position,
        };
        let hyphen = |input: &str| SurroundingHyphen {
            input: input.to_owned(),
        };
        let long = "a".repeat(SelectorName::MAX_LENGTH + 1);
        let cases = [
            ("Order producers", invalid("Order producers", 'O', 0)),
            ("order producers", invalid("order producers", ' ', 5)),
            ("orders.*", invalid("orders.*", '.', 6)),
            ("order_producers", invalid("order_producers", '_', 5)),
            ("order-producers\n", invalid("order-producers\n", '\n', 15)),
            // A zero-width space hides between the two words.
            (
                "order\u{200b}producers",
                invalid("order\u{200b}producers", '\u{200b}', 5),
            ),
            ("-order-producers", hyphen("-order-producers")),
            ("order-producers-", hyphen("order-producers-")),
            (long.as_str(), TooLong { length: 64 }),
        ];

        assert_eq!(SelectorName::new(""), Err(SelectorNameError::Empty));
        for (text, error) in cases {
            assert_eq!(SelectorName::new(text), Err(error), "{text:?}");
        }
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let long = "a".repeat(SelectorName::MAX_LENGTH + 1);
        let cases = [
            ("", "selector name \"\" is empty"),
            (
                "Order producers",
                "selector name \"Order producers\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen",
            ),
            (
                "order-producers-",
                "selector name \"order-producers-\" starts or ends with a hyphen",
            ),
            (
                long.as_str(),
                "selector name of 64 characters is longer than 63 characters",
            ),
        ];

        for (text, message) in cases {
            assert_eq!(SelectorName::new(text).unwrap_err().to_string(), message);
        }
    }

    #[test]
    fn a_name_has_at_most_63_characters() {
        let longest = "a".repeat(SelectorName::MAX_LENGTH);

        assert_eq!(SelectorName::MAX_LENGTH, 63);
        assert_eq!(SelectorName::new(&longest).unwrap(), *longest);
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        let name = SelectorName::new("order-producers").unwrap();

        assert_eq!(format!("{name:?}"), "SelectorName(\"order-producers\")");
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let name = || SelectorName::new("order-producers").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("order-producers"), Some(&1));
        assert_eq!(sorted.get("order-consumers"), None);
        assert_eq!(hashed.get("order-producers"), Some(&1));
        assert_eq!(hashed.get("order-consumers"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", SelectorName::new("flap").unwrap()), "    flap|");
    }
}
