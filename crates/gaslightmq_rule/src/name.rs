use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use gaslightmq_trigger::DiceKey;

use crate::RuleError;

/// Define a name: text that something else references a definition by.
macro_rules! name {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Checks the rule for names.
            ///
            /// # Errors
            ///
            /// [`RuleError::InvalidName`] when the text is empty, starts or ends with white space,
            /// or holds a control character.
            pub fn new(name: impl AsRef<str>) -> Result<Self, RuleError> {
                let name = name.as_ref();
                let reason = if name.is_empty() {
                    "is empty"
                } else if name.trim() != name {
                    "starts or ends with white space"
                } else if name.chars().any(char::is_control) {
                    "holds a control character"
                } else {
                    return Ok(Self(name.into()));
                };
                Err(RuleError::InvalidName {
                    what: $what,
                    input: name.to_owned(),
                    reason,
                })
            }

            /// The name as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = RuleError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = RuleError;

            fn try_from(text: &str) -> Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl TryFrom<String> for $name {
            type Error = RuleError;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                Self::new(text)
            }
        }

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
    /// The names of selectors follow the same rule.
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
    /// # Ok::<(), RuleError>(())
    /// ```
    RuleName, "rule name"
}

name! {
    /// The name of a preset, such as `Delay 1 s`.
    ///
    /// A rule references a preset by its name, and so do a pad grid column and a ⌘K suggestion.
    /// A name is unique among presets, and it is exact.
    ///
    /// A name has at least one character.
    /// It does not start or end with white space, and it holds no control character such as a line break.
    ///
    /// ```
    /// use gaslightmq_rule::prelude::*;
    ///
    /// let name: PresetName = "Delay 1 s".parse()?;
    /// assert_eq!(name, "Delay 1 s");
    /// assert!(PresetName::new("Delay 1 s\n").is_err());
    /// # Ok::<(), RuleError>(())
    /// ```
    PresetName, "preset name"
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
        ];

        for (text, reason) in cases {
            assert_eq!(
                RuleName::new(text),
                Err(RuleError::InvalidName {
                    what: "rule name",
                    input: text.to_owned(),
                    reason
                }),
                "{text:?}"
            );
            assert_eq!(
                PresetName::new(text),
                Err(RuleError::InvalidName {
                    what: "preset name",
                    input: text.to_owned(),
                    reason
                }),
                "{text:?}"
            );
        }
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
