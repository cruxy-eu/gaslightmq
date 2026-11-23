use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

use gaslightmq_resource::{ResourceName, ResourceNameError};

use crate::text::text_form;

/// The name of a selector, such as `Order producers`.
///
/// Things reference a selector by its name: a rule, a pad layout, and a selector that includes another.
/// A name is unique among selectors, and it is exact: `Order producers` and `order producers` are two names.
///
/// # The rule for names
///
/// A name has at least one character.
/// It does not start or end with white space, and it holds no control character such as a line break.
/// It holds no format character either, such as a zero-width space or a mark that turns the text direction.
/// Nobody sees those, so two names that hold them can look the same and still differ.
///
/// The format characters are those of the Unicode general category `Cf`, as of Unicode 16.0.
/// The zero-width joiner is one of them, so a name cannot hold an emoji that joins others with it.
///
/// The rule does not compare how two names look.
/// Letters of different scripts that look alike, and one letter written in two ways, still make two names.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let name: SelectorName = "Order producers".parse()?;
/// assert_eq!(name, "Order producers");
/// assert_eq!(name.to_string(), "Order producers");
///
/// assert!(SelectorName::new("").is_err());
/// assert!(SelectorName::new("Order producers ").is_err());
/// // A zero-width space hides between the two words.
/// assert!(SelectorName::new("Order\u{200b}producers").is_err());
/// # Ok::<(), SelectorNameError>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectorName(ResourceName);

impl SelectorName {
    /// Checks the [rule for names](Self#the-rule-for-names).
    ///
    /// # Errors
    ///
    /// The [`SelectorNameError`] that says which part of the rule the text breaks:
    /// it is empty, starts or ends with white space, or holds a control character or a format character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, SelectorNameError> {
        match ResourceName::new(name) {
            Ok(name) => Ok(Self(name)),
            Err(ResourceNameError::Empty) => Err(SelectorNameError::Empty),
            Err(ResourceNameError::SurroundingWhiteSpace { input }) => {
                Err(SelectorNameError::SurroundingWhiteSpace { input })
            }
            Err(ResourceNameError::ControlCharacter { input }) => Err(SelectorNameError::ControlCharacter { input }),
            Err(ResourceNameError::FormatCharacter { input }) => Err(SelectorNameError::FormatCharacter { input }),
        }
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for SelectorName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SelectorName").field(&self.as_str()).finish()
    }
}

impl FromStr for SelectorName {
    type Err = SelectorNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for SelectorName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for SelectorName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for SelectorName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for SelectorName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SelectorName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

text_form!(SelectorName, SelectorNameError);

/// Why a text is not a [`SelectorName`]: the part of the [rule for names](SelectorName#the-rule-for-names) it breaks.
///
/// The rule has four parts, and the error has one variant for each.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let error = SelectorName::new("Order\u{feff}producers").unwrap_err();
/// assert_eq!(error, SelectorNameError::FormatCharacter { input: "Order\u{feff}producers".to_owned() });
/// assert_eq!(error.to_string(), "selector name \"Order\\u{feff}producers\" holds a format character");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SelectorNameError {
    /// The text has no character.
    #[error("selector name \"\" is empty")]
    Empty,
    /// The text starts or ends with white space.
    #[error("selector name {input:?} starts or ends with white space")]
    SurroundingWhiteSpace { input: String },
    /// The text holds a control character, such as a line break.
    #[error("selector name {input:?} holds a control character")]
    ControlCharacter { input: String },
    /// The text holds a format character, such as a zero-width space, which nobody sees.
    #[error("selector name {input:?} holds a format character")]
    FormatCharacter { input: String },
}

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
