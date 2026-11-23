use gaslightmq_trigger::{DiceKey, DiceKeyError};

gaslightmq_resource::name!(RuleName, RuleNameError, "rule name");

gaslightmq_resource::name!(PresetName, PresetNameError, "preset name");

impl RuleName {
    /// The dice key of a rule that sets none: the text of its name.
    ///
    /// A name has at least one character, which is all a dice key asks for.
    pub fn dice_key(&self) -> DiceKey {
        match DiceKey::new(self.as_str()) {
            Ok(key) => key,
            Err(DiceKeyError::Empty) => unreachable!("a rule name has at least one character"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in [
            "Slow down order producers",
            "x",
            "Ünïcode ✓",
            "a  b",
            "Refuse orders.* (EU)",
        ] {
            let name = RuleName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(name.as_ref(), text);
            assert_eq!(text.parse::<RuleName>(), Ok(name.clone()));
            assert_eq!(RuleName::try_from(text), Ok(name.clone()));
            assert_eq!(RuleName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        let cases = [
            ("", "is empty"),
            (" ", "starts or ends with white space"),
            (" Delay 1 s", "starts or ends with white space"),
            ("Delay 1 s\n", "starts or ends with white space"),
            ("Delay\n1 s", "holds a control character"),
            ("Delay\t1 s", "holds a control character"),
            // A zero-width space, a right-to-left override and a byte order mark.
            ("Delay\u{200b}1 s", "holds a format character"),
            ("Delay \u{202e}1 s", "holds a format character"),
            ("\u{feff}Delay 1 s", "holds a format character"),
        ];

        for (text, reason) in cases {
            assert_eq!(
                RuleName::new(text).unwrap_err().to_string(),
                format!("rule name {text:?} {reason}")
            );
            assert_eq!(
                PresetName::new(text).unwrap_err().to_string(),
                format!("preset name {text:?} {reason}")
            );
        }
    }

    #[test]
    fn every_name_has_its_own_error_with_a_variant_for_each_part_of_the_rule() {
        let input = || "Delay\n1 s".to_owned();

        assert_eq!(RuleName::new(""), Err(RuleNameError::Empty));
        assert_eq!(
            RuleName::new(" x"),
            Err(RuleNameError::SurroundingWhiteSpace { input: " x".to_owned() })
        );
        assert_eq!(
            RuleName::new(input()),
            Err(RuleNameError::ControlCharacter { input: input() })
        );
        assert_eq!(PresetName::new(""), Err(PresetNameError::Empty));
        assert_eq!(
            PresetName::new("x\u{200b}"),
            Err(PresetNameError::FormatCharacter {
                input: "x\u{200b}".to_owned()
            })
        );
    }

    #[test]
    fn names_that_would_look_the_same_cannot_both_exist() {
        let seen = RuleName::new("Stale metadata").unwrap();

        for hidden in ['\u{200b}', '\u{200e}', '\u{202e}', '\u{2060}', '\u{feff}'] {
            let text = format!("Stale{hidden} metadata");
            assert!(RuleName::new(&text).is_err(), "{text:?}");
            assert!(PresetName::new(&text).is_err(), "{text:?}");
        }
        assert_eq!(RuleName::new("Stale metadata"), Ok(seen));
    }

    #[test]
    fn a_name_is_exact() {
        assert_ne!(
            RuleName::new("Stale metadata").unwrap(),
            RuleName::new("stale metadata").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(PresetName::new("Delay 1 s").unwrap(), 1);

        assert_eq!(by_name.get("Delay 1 s"), Some(&1));
        assert_eq!(by_name.get("Leader flap"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", PresetName::new("Flap").unwrap()), "    Flap|");
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        assert_eq!(format!("{:?}", RuleName::new("Flap").unwrap()), "RuleName(\"Flap\")");
        assert_eq!(
            format!("{:?}", PresetName::new("Flap").unwrap()),
            "PresetName(\"Flap\")"
        );
    }

    #[test]
    fn the_dice_key_of_a_rule_name_is_its_text() {
        let name = RuleName::new("Slow down order producers").unwrap();

        assert_eq!(name.dice_key().as_str(), "Slow down order producers");
    }
}
