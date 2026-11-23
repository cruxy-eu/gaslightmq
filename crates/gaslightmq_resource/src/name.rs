use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// The name of a resource, such as `Order producers`.
///
/// Things reference a resource by its name: a rule names its selector and its preset, and a command names an
/// environment.
/// A name is exact: `Order producers` and `order producers` are two names.
///
/// A resource name does not say which kind of resource it names.
/// Every kind has a name type of its own that wraps this one, and code speaks in those.
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
/// use gaslightmq_resource::{ResourceName, ResourceNameError};
///
/// let name: ResourceName = "Order producers".parse()?;
/// assert_eq!(name, "Order producers");
/// assert_eq!(name.to_string(), "Order producers");
///
/// assert_eq!(ResourceName::new(""), Err(ResourceNameError::Empty));
/// assert!(ResourceName::new("Order producers ").is_err());
/// // A zero-width space hides between the two words.
/// assert!(ResourceName::new("Order\u{200b}producers").is_err());
/// # Ok::<(), ResourceNameError>(())
/// ```
///
/// With the `serde` feature, a name serializes as its text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceName(Arc<str>);

impl ResourceName {
    /// Checks the [rule for names](Self#the-rule-for-names), and keeps the text as it is.
    ///
    /// # Errors
    ///
    /// The [`ResourceNameError`] that says which part of the rule the text breaks:
    /// it is empty, starts or ends with white space, or holds a control character or a format character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, ResourceNameError> {
        let name = name.as_ref();
        let input = || name.to_owned();
        if name.is_empty() {
            Err(ResourceNameError::Empty)
        } else if name.trim() != name {
            Err(ResourceNameError::SurroundingWhiteSpace { input: input() })
        } else if name.chars().any(char::is_control) {
            Err(ResourceNameError::ControlCharacter { input: input() })
        } else if name.chars().any(is_format) {
            Err(ResourceNameError::FormatCharacter { input: input() })
        } else {
            Ok(Self(name.into()))
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

impl FromStr for ResourceName {
    type Err = ResourceNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl TryFrom<&str> for ResourceName {
    type Error = ResourceNameError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl TryFrom<String> for ResourceName {
    type Error = ResourceNameError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<ResourceName> for String {
    fn from(name: ResourceName) -> Self {
        name.as_str().to_owned()
    }
}

impl fmt::Display for ResourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for ResourceName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for ResourceName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for ResourceName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ResourceName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for ResourceName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ResourceName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(text).map_err(serde::de::Error::custom)
    }
}

/// Why a text is not a [`ResourceName`]: the part of the [rule for names](ResourceName#the-rule-for-names) it breaks.
///
/// The rule has four parts, and the error has one variant for each.
/// It is exhaustive on purpose.
/// The name of every kind of resource has an error of its own with the same four variants,
/// and its constructor matches on every variant of this one to say the same about its own name.
///
/// ```
/// use gaslightmq_resource::{ResourceName, ResourceNameError};
///
/// let error = ResourceName::new("Order\u{feff}producers").unwrap_err();
/// assert_eq!(error, ResourceNameError::FormatCharacter { input: "Order\u{feff}producers".to_owned() });
/// assert_eq!(error.to_string(), "name \"Order\\u{feff}producers\" holds a format character");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceNameError {
    /// The text has no character.
    #[error("name \"\" is empty")]
    Empty,
    /// The text starts or ends with white space.
    #[error("name {input:?} starts or ends with white space")]
    SurroundingWhiteSpace { input: String },
    /// The text holds a control character, such as a line break.
    #[error("name {input:?} holds a control character")]
    ControlCharacter { input: String },
    /// The text holds a format character, such as a zero-width space, which nobody sees.
    #[error("name {input:?} holds a format character")]
    FormatCharacter { input: String },
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["Order producers", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let name = ResourceName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(name.as_ref(), text);
            assert_eq!(text.parse::<ResourceName>(), Ok(name.clone()));
            assert_eq!(ResourceName::try_from(text), Ok(name.clone()));
            assert_eq!(ResourceName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        use ResourceNameError::{ControlCharacter, FormatCharacter, SurroundingWhiteSpace};

        type Breaks = fn(String) -> ResourceNameError;
        let cases: [(&str, Breaks); 13] = [
            (" ", |input| SurroundingWhiteSpace { input }),
            (" Order producers", |input| SurroundingWhiteSpace { input }),
            ("Order producers\n", |input| SurroundingWhiteSpace { input }),
            ("Order producers\u{a0}", |input| SurroundingWhiteSpace { input }),
            ("Order\nproducers", |input| ControlCharacter { input }),
            ("Order\tproducers", |input| ControlCharacter { input }),
            // A zero-width space, a right-to-left override and a byte order mark.
            ("Order\u{200b}producers", |input| FormatCharacter { input }),
            ("Order \u{202e}producers", |input| FormatCharacter { input }),
            ("\u{feff}Order producers", |input| FormatCharacter { input }),
            ("Order producers\u{200d}", |input| FormatCharacter { input }),
            ("Order\u{ad}producers", |input| FormatCharacter { input }),
            // White space at an end is named before a control character in the middle.
            ("Order\nproducers ", |input| SurroundingWhiteSpace { input }),
            // A control character is named before a format character.
            ("Order\u{200b}\tproducers", |input| ControlCharacter { input }),
        ];

        assert_eq!(ResourceName::new(""), Err(ResourceNameError::Empty));
        for (text, error) in cases {
            assert_eq!(ResourceName::new(text), Err(error(text.to_owned())), "{text:?}");
        }
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let cases = [
            ("", "name \"\" is empty"),
            (
                "Order producers ",
                "name \"Order producers \" starts or ends with white space",
            ),
            (
                "Order\nproducers",
                "name \"Order\\nproducers\" holds a control character",
            ),
            (
                "Order\u{2060}producers",
                "name \"Order\\u{2060}producers\" holds a format character",
            ),
        ];

        for (text, message) in cases {
            assert_eq!(ResourceName::new(text).unwrap_err().to_string(), message);
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
    fn a_name_takes_every_character_in_its_middle_but_the_control_and_the_format_characters() {
        let mut refused = 0;
        let mut text = String::new();
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            // In the middle of a name, where white space is no reason to refuse it.
            text.clear();
            text.extend(['a', character, 'b']);
            if ResourceName::new(&text).is_err() {
                assert!(character.is_control() || is_format(character), "{text:?}");
                refused += 1;
            }
        }
        // The 65 control characters and the 170 format characters.
        assert_eq!(refused, 235);
    }

    #[test]
    fn white_space_starts_and_ends_no_name() {
        let mut white_space = 0;
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            if character.is_whitespace() {
                assert!(ResourceName::new(format!("{character}shop")).is_err());
                assert!(ResourceName::new(format!("shop{character}")).is_err());
                white_space += 1;
            }
        }
        assert_eq!(white_space, 25);
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
            assert!(ResourceName::new(text).is_ok(), "{text:?}");
        }
    }

    #[test]
    fn names_that_would_look_the_same_cannot_both_exist() {
        let seen = ResourceName::new("Stale metadata").unwrap();

        for hidden in ['\u{200b}', '\u{200e}', '\u{202e}', '\u{2060}', '\u{feff}'] {
            let text = format!("Stale{hidden} metadata");
            assert!(ResourceName::new(&text).is_err(), "{text:?}");
        }
        assert_eq!(ResourceName::new("Stale metadata"), Ok(seen));
    }

    #[test]
    fn a_name_is_exact() {
        assert_ne!(
            ResourceName::new("Order producers").unwrap(),
            ResourceName::new("order producers").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let name = || ResourceName::new("Order producers").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("Order producers"), Some(&1));
        assert_eq!(sorted.get("Order consumers"), None);
        assert_eq!(hashed.get("Order producers"), Some(&1));
        assert_eq!(hashed.get("Order consumers"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", ResourceName::new("Flap").unwrap()), "    Flap|");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_name_is_its_text_in_serde() {
        let name = ResourceName::new("Order producers").unwrap();

        assert_eq!(serde_json::to_string(&name).unwrap(), "\"Order producers\"");
        assert_eq!(
            serde_json::from_str::<ResourceName>("\"Order producers\"").unwrap(),
            name
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_text_that_breaks_the_rule_does_not_read_as_a_name() {
        let error = serde_json::from_str::<ResourceName>("\" Order producers\"").unwrap_err();

        assert_eq!(
            error.to_string(),
            "name \" Order producers\" starts or ends with white space"
        );
        assert!(serde_json::from_str::<ResourceName>("7").is_err());
    }
}
