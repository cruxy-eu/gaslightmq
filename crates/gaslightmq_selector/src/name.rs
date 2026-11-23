use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectorName(Arc<str>);

impl SelectorName {
    /// Checks the [rule for names](Self#the-rule-for-names).
    ///
    /// # Errors
    ///
    /// The [`SelectorNameError`] that says which part of the rule the text breaks:
    /// it is empty, starts or ends with white space, or holds a control character or a format character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, SelectorNameError> {
        let name = name.as_ref();
        Self::check(name)?;
        Ok(Self(name.into()))
    }

    /// Checks `text` against the [rule for names](Self#the-rule-for-names), and keeps nothing.
    ///
    /// Whatever else has a name takes it by this one rule, without building a selector name:
    /// a rule and a preset do, and they say the same in an error of their own.
    ///
    /// ```
    /// use gaslightmq_selector::prelude::*;
    ///
    /// assert_eq!(SelectorName::check("Order producers"), Ok(()));
    /// assert_eq!(SelectorName::check(""), Err(SelectorNameError::Empty));
    /// ```
    ///
    /// # Errors
    ///
    /// The [`SelectorNameError`] that says which part of the rule the text breaks.
    pub fn check(text: &str) -> Result<(), SelectorNameError> {
        let input = || text.to_owned();
        if text.is_empty() {
            Err(SelectorNameError::Empty)
        } else if text.trim() != text {
            Err(SelectorNameError::SurroundingWhiteSpace { input: input() })
        } else if text.chars().any(char::is_control) {
            Err(SelectorNameError::ControlCharacter { input: input() })
        } else if text.chars().any(is_format) {
            Err(SelectorNameError::FormatCharacter { input: input() })
        } else {
            Ok(())
        }
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether the character is a format character: one of the Unicode general category `Cf`, as of Unicode 16.0.
///
/// The standard library has no table of it, so the ranges stand here.
const fn is_format(character: char) -> bool {
    matches!(
        character,
        // The soft hyphen.
        '\u{00AD}'
        // The Arabic number signs, the Arabic letter mark and the end of ayah.
        | '\u{0600}'..='\u{0605}' | '\u{061C}' | '\u{06DD}' | '\u{0890}'..='\u{0891}' | '\u{08E2}'
        // The Syriac abbreviation mark and the Mongolian vowel separator.
        | '\u{070F}' | '\u{180E}'
        // The zero-width space, non-joiner and joiner, and the left-to-right and right-to-left marks.
        | '\u{200B}'..='\u{200F}'
        // The embeddings and overrides of the text direction.
        | '\u{202A}'..='\u{202E}'
        // The word joiner and the invisible operators.
        | '\u{2060}'..='\u{2064}'
        // The isolates of the text direction, and the deprecated format characters.
        | '\u{2066}'..='\u{206F}'
        // The byte order mark, which is also the zero-width no-break space.
        | '\u{FEFF}'
        // The interlinear annotation marks.
        | '\u{FFF9}'..='\u{FFFB}'
        // The Kaithi number signs, and the format controls of Egyptian hieroglyphs, of shorthand and of music.
        | '\u{110BD}' | '\u{110CD}' | '\u{13430}'..='\u{1343F}' | '\u{1BCA0}'..='\u{1BCA3}'
        | '\u{1D173}'..='\u{1D17A}'
        // The language tag and the tag characters.
        | '\u{E0001}' | '\u{E0020}'..='\u{E007F}'
    )
}

impl FromStr for SelectorName {
    type Err = SelectorNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for SelectorName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for SelectorName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for SelectorName {
    fn borrow(&self) -> &str {
        &self.0
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
/// It is exhaustive on purpose.
/// Whatever else has a name takes it by this one rule, and says the same in an error of its own:
/// a rule and a preset do, and they match on every variant.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let error = SelectorName::new("Order\u{feff}producers").unwrap_err();
/// assert_eq!(error, SelectorNameError::FormatCharacter { input: "Order\u{feff}producers".to_owned() });
/// assert_eq!(error.to_string(), "selector name \"Order\\u{feff}producers\" holds a format character");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["Order producers", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let name = SelectorName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(text.parse::<SelectorName>(), Ok(name.clone()));
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
    fn the_format_characters_are_those_of_the_category_cf() {
        // Every range of the general category Cf in Unicode 16.0, which holds 170 characters.
        let ranges = [
            (0x00AD, 0x00AD),
            (0x0600, 0x0605),
            (0x061C, 0x061C),
            (0x06DD, 0x06DD),
            (0x070F, 0x070F),
            (0x0890, 0x0891),
            (0x08E2, 0x08E2),
            (0x180E, 0x180E),
            (0x200B, 0x200F),
            (0x202A, 0x202E),
            (0x2060, 0x2064),
            (0x2066, 0x206F),
            (0xFEFF, 0xFEFF),
            (0xFFF9, 0xFFFB),
            (0x110BD, 0x110BD),
            (0x110CD, 0x110CD),
            (0x13430, 0x1343F),
            (0x1BCA0, 0x1BCA3),
            (0x1D173, 0x1D17A),
            (0xE0001, 0xE0001),
            (0xE0020, 0xE007F),
        ];

        let format: Vec<u32> = (0..=u32::from(char::MAX))
            .filter(|code| char::from_u32(*code).is_some_and(is_format))
            .collect();
        let expected: Vec<u32> = ranges.iter().flat_map(|(first, last)| *first..=*last).collect();

        assert_eq!(format.len(), 170);
        assert_eq!(format, expected);
    }

    #[test]
    fn a_name_takes_the_characters_next_to_the_format_characters() {
        // A no-break space, a combining accent, a variation selector and an emoji are no format characters.
        for text in [
            "Order\u{a0}producers",
            "Orde\u{301}r",
            "Order \u{2764}\u{fe0f}",
            "Order 🚀",
        ] {
            assert!(SelectorName::new(text).is_ok(), "{text:?}");
        }
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
        let mut by_name = BTreeMap::new();
        by_name.insert(SelectorName::new("Order producers").unwrap(), 1);

        assert_eq!(by_name.get("Order producers"), Some(&1));
        assert_eq!(by_name.get("Order consumers"), None);
    }
}
