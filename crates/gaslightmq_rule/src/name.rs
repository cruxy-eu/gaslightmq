use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use gaslightmq_selector::{SelectorName, SelectorNameError};
use gaslightmq_trigger::DiceKey;

/// Define a name: text that something else references a definition by.
macro_rules! name {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Checks the rule for names, which is [the one of selector names](SelectorName#the-rule-for-names).
            ///
            /// # Errors
            ///
            /// The error of the name, which says which part of the rule the text breaks: the text is empty, starts or ends with white space,
            /// or holds a control character or a format character.
            pub fn new(name: impl AsRef<str>) -> Result<Self, $error> {
                let name = name.as_ref();
                // A selector name has the one rule for names, and its error has a variant for every part of it.
                match SelectorName::check(name) {
                    Ok(()) => Ok(Self(name.into())),
                    Err(SelectorNameError::Empty) => Err($error::Empty),
                    Err(SelectorNameError::SurroundingWhiteSpace { input }) => {
                        Err($error::SurroundingWhiteSpace { input })
                    }
                    Err(SelectorNameError::ControlCharacter { input }) => Err($error::ControlCharacter { input }),
                    Err(SelectorNameError::FormatCharacter { input }) => Err($error::FormatCharacter { input }),
                }
            }

            /// The name as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = $error;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(text: &str) -> Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        #[doc = concat!("Why a text is not a [`", stringify!($name), "`]: the part of the rule for names it breaks.")]
        ///
        /// The variants are those of [`SelectorNameError`], because every name takes the same rule.
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no character.
            Empty,
            /// The text starts or ends with white space.
            SurroundingWhiteSpace { input: String },
            /// The text holds a control character, such as a line break.
            ControlCharacter { input: String },
            /// The text holds a format character, such as a zero-width space, which nobody sees.
            FormatCharacter { input: String },
        }

        impl fmt::Display for $error {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Empty => f.write_str(concat!($what, " \"\" is empty")),
                    Self::SurroundingWhiteSpace { input } => {
                        write!(f, concat!($what, " {:?} starts or ends with white space"), input)
                    }
                    Self::ControlCharacter { input } => {
                        write!(f, concat!($what, " {:?} holds a control character"), input)
                    }
                    Self::FormatCharacter { input } => {
                        write!(f, concat!($what, " {:?} holds a format character"), input)
                    }
                }
            }
        }

        impl std::error::Error for $error {}

        impl From<$name> for String {
            fn from(name: $name) -> Self {
                name.as_str().to_owned()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.pad(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        /// A map by name finds its entry with plain text.
        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(text).map_err(serde::de::Error::custom)
            }
        }
    };
}

name! {
    /// The name of a rule, such as `Slow down order producers`.
    ///
    /// A scenario, a step and a pad reference a rule by its name, and so does the explanation of a call.
    /// A name is unique among rules, and it is exact: `Stale metadata` and `stale metadata` are two names.
    ///
    /// A name has at least one character.
    /// It does not start or end with white space, and it holds no control character such as a line break.
    /// It holds no format character either, such as a zero-width space or a mark that turns the text direction:
    /// nobody sees those, so two names could look the same and still differ.
    /// This is [the rule of selector names](SelectorName#the-rule-for-names), and one place checks it.
    ///
    /// ```
    /// use gaslightmq_rule::prelude::*;
    ///
    /// let name: RuleName = "Slow down order producers".parse()?;
    /// assert_eq!(name, "Slow down order producers");
    /// assert_eq!(name.to_string(), "Slow down order producers");
    ///
    /// assert!(RuleName::new("").is_err());
    /// assert!(RuleName::new(" Slow down").is_err());
    /// // A zero-width space hides between the two words.
    /// assert!(RuleName::new("Slow\u{200b}down").is_err());
    /// # Ok::<(), RuleNameError>(())
    /// ```
    RuleName, RuleNameError, "rule name"
}

name! {
    /// The name of a preset, such as `Delay 1 s`.
    ///
    /// A rule references a preset by its name, and so do a pad grid column and a ⌘K suggestion.
    /// A name is unique among presets, and it is exact.
    ///
    /// A name follows [the rule of selector names](SelectorName#the-rule-for-names), like the name of a rule.
    /// It has at least one character, and it does not start or end with white space.
    /// It holds no control character such as a line break, and no format character such as a zero-width space.
    ///
    /// ```
    /// use gaslightmq_rule::prelude::*;
    ///
    /// let name: PresetName = "Delay 1 s".parse()?;
    /// assert_eq!(name, "Delay 1 s");
    /// assert!(PresetName::new("Delay 1 s\n").is_err());
    /// # Ok::<(), PresetNameError>(())
    /// ```
    PresetName, PresetNameError, "preset name"
}

impl RuleName {
    /// The dice key of a rule that sets none: the text of its name.
    ///
    /// A name has at least one character, which is all a dice key asks for.
    pub fn dice_key(&self) -> DiceKey {
        match DiceKey::new(self.as_str()) {
            Ok(key) => key,
            Err(_) => unreachable!("a rule name has at least one character"),
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
    fn the_dice_key_of_a_rule_name_is_its_text() {
        let name = RuleName::new("Slow down order producers").unwrap();

        assert_eq!(name.dice_key().as_str(), "Slow down order producers");
    }
}
