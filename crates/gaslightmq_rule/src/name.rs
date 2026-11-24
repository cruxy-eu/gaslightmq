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
        for text in ["slow-order-producers", "x", "7", "a--b", "refuse-every-2nd"] {
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
        let long = "a".repeat(RuleName::MAX_LENGTH + 1);
        let cases = [
            ("", "\"\" is empty"),
            (
                "Delay 1 s",
                "\"Delay 1 s\" holds 'D' at position 0, which is no lowercase letter, digit or hyphen",
            ),
            (
                "delay 1s",
                "\"delay 1s\" holds ' ' at position 5, which is no lowercase letter, digit or hyphen",
            ),
            (
                "delay_1s",
                "\"delay_1s\" holds '_' at position 5, which is no lowercase letter, digit or hyphen",
            ),
            // A zero-width space hides in the name.
            (
                "delay\u{200b}1s",
                "\"delay\\u{200b}1s\" holds '\\u{200b}' at position 5, which is no lowercase letter, digit or hyphen",
            ),
            ("-delay-1s", "\"-delay-1s\" starts or ends with a hyphen"),
            ("delay-1s-", "\"delay-1s-\" starts or ends with a hyphen"),
            (long.as_str(), "of 64 characters is longer than 63 characters"),
        ];

        for (text, reason) in cases {
            assert_eq!(
                RuleName::new(text).unwrap_err().to_string(),
                format!("rule name {reason}")
            );
            assert_eq!(
                PresetName::new(text).unwrap_err().to_string(),
                format!("preset name {reason}")
            );
        }
    }

    #[test]
    fn every_name_has_its_own_error_with_a_variant_for_each_part_of_the_rule() {
        let long = "a".repeat(RuleName::MAX_LENGTH + 1);

        assert_eq!(RuleName::new(""), Err(RuleNameError::Empty));
        assert_eq!(
            RuleName::new("Delay 1 s"),
            Err(RuleNameError::InvalidCharacter {
                input: "Delay 1 s".to_owned(),
                character: 'D',
                position: 0
            })
        );
        assert_eq!(
            RuleName::new("delay-1s-"),
            Err(RuleNameError::SurroundingHyphen {
                input: "delay-1s-".to_owned()
            })
        );
        assert_eq!(RuleName::new(long), Err(RuleNameError::TooLong { length: 64 }));
        assert_eq!(PresetName::new(""), Err(PresetNameError::Empty));
        assert_eq!(
            PresetName::new("-delay-1s"),
            Err(PresetNameError::SurroundingHyphen {
                input: "-delay-1s".to_owned()
            })
        );
    }

    #[test]
    fn a_name_has_at_most_63_characters() {
        let longest = "a".repeat(RuleName::MAX_LENGTH);

        assert_eq!(RuleName::MAX_LENGTH, 63);
        assert_eq!(PresetName::MAX_LENGTH, 63);
        assert_eq!(RuleName::new(&longest).unwrap(), *longest);
        assert_eq!(PresetName::new(&longest).unwrap(), *longest);
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(PresetName::new("delay-1s").unwrap(), 1);

        assert_eq!(by_name.get("delay-1s"), Some(&1));
        assert_eq!(by_name.get("leader-flap"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", PresetName::new("flap").unwrap()), "    flap|");
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        assert_eq!(format!("{:?}", RuleName::new("flap").unwrap()), "RuleName(\"flap\")");
        assert_eq!(
            format!("{:?}", PresetName::new("flap").unwrap()),
            "PresetName(\"flap\")"
        );
    }

    #[test]
    fn the_dice_key_of_a_rule_name_is_its_text() {
        let name = RuleName::new("slow-order-producers").unwrap();

        assert_eq!(name.dice_key().as_str(), "slow-order-producers");
    }
}
