//! The name a call record knows a rule by.

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

use gaslightmq_resource::{ResourceName, ResourceNameError};

/// The name a rule had when a call met it, as the record of that call keeps it.
///
/// An explanation names every rule the call touched, and the rule that won a conflict.
/// A record outlives the rules it names: someone can rename or delete a rule after the call.
/// A recorded name is therefore only text that names a rule, and nobody looks it up.
///
/// It is exact: `Stale metadata` and `stale metadata` are two names.
///
/// # The rule for names
///
/// A recorded name follows the rule that the name of the rule followed.
/// It has at least one character, and it does not start or end with white space.
/// It holds no control character such as a line break, and no format character such as a zero-width space.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let name: RecordedRuleName = "Slow down order producers".parse()?;
/// assert_eq!(name, "Slow down order producers");
/// assert_eq!(RecordedRuleName::new(""), Err(RecordedRuleNameError::Empty));
/// # Ok::<(), RecordedRuleNameError>(())
/// ```
///
/// With the `serde` feature, a recorded name serializes as its text.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordedRuleName(ResourceName);

impl RecordedRuleName {
    /// Checks the [rule for names](Self#the-rule-for-names), and keeps the text.
    ///
    /// # Errors
    ///
    /// The [`RecordedRuleNameError`] that says which part of the rule the text breaks:
    /// it is empty, starts or ends with white space, or holds a control character or a format character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, RecordedRuleNameError> {
        match ResourceName::new(name) {
            Ok(name) => Ok(Self(name)),
            Err(ResourceNameError::Empty) => Err(RecordedRuleNameError::Empty),
            Err(ResourceNameError::SurroundingWhiteSpace { input }) => {
                Err(RecordedRuleNameError::SurroundingWhiteSpace { input })
            }
            Err(ResourceNameError::ControlCharacter { input }) => {
                Err(RecordedRuleNameError::ControlCharacter { input })
            }
            Err(ResourceNameError::FormatCharacter { input }) => Err(RecordedRuleNameError::FormatCharacter { input }),
        }
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for RecordedRuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RecordedRuleName").field(&self.as_str()).finish()
    }
}

impl FromStr for RecordedRuleName {
    type Err = RecordedRuleNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl TryFrom<&str> for RecordedRuleName {
    type Error = RecordedRuleNameError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl TryFrom<String> for RecordedRuleName {
    type Error = RecordedRuleNameError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<RecordedRuleName> for String {
    fn from(name: RecordedRuleName) -> Self {
        name.0.into()
    }
}

impl fmt::Display for RecordedRuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for RecordedRuleName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for RecordedRuleName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for RecordedRuleName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for RecordedRuleName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Why a text is not a [`RecordedRuleName`]: the part of the [rule for names](RecordedRuleName#the-rule-for-names) it
/// breaks.
///
/// The rule has four parts, and the error has one variant for each.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordedRuleNameError {
    /// The text has no character, and every rule has a name.
    #[error("the recorded name of a rule is empty")]
    Empty,
    /// The text starts or ends with white space.
    #[error("the recorded name of a rule {input:?} starts or ends with white space")]
    SurroundingWhiteSpace { input: String },
    /// The text holds a control character, such as a line break.
    #[error("the recorded name of a rule {input:?} holds a control character")]
    ControlCharacter { input: String },
    /// The text holds a format character, such as a zero-width space, which nobody sees.
    #[error("the recorded name of a rule {input:?} holds a format character")]
    FormatCharacter { input: String },
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
            ("", Empty, "the recorded name of a rule is empty"),
            (
                " Leader flap",
                SurroundingWhiteSpace {
                    input: input(" Leader flap"),
                },
                "the recorded name of a rule \" Leader flap\" starts or ends with white space",
            ),
            (
                "Leader\nflap",
                ControlCharacter {
                    input: input("Leader\nflap"),
                },
                "the recorded name of a rule \"Leader\\nflap\" holds a control character",
            ),
            (
                "Leader\u{200b}flap",
                FormatCharacter {
                    input: input("Leader\u{200b}flap"),
                },
                "the recorded name of a rule \"Leader\\u{200b}flap\" holds a format character",
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
