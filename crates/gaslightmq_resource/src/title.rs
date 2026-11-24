use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// The title of a resource, such as `Slow down order producers`.
///
/// A title says what a resource is for, in the words of whoever wrote it.
/// It belongs to the resource itself, next to its name, and it is never part of the name.
/// Nothing references a resource by its title, so a title changes freely, and two resources can have the same one.
///
/// A resource title does not say which kind of resource it describes.
/// Every kind has a title type of its own that wraps this one, and code speaks in those.
///
/// # The rule for titles
///
/// A title has at least one character, and at most [`MAX_LENGTH`](Self::MAX_LENGTH), which is 2000.
/// The length counts characters, and not bytes.
/// A title does not start or end with white space, and it holds no control character such as a line break.
/// It holds no format character either, such as a zero-width space or a mark that turns the text direction.
/// Nobody sees those.
///
/// The format characters are those of the Unicode general category `Cf`, as of Unicode 16.0.
/// The zero-width joiner is one of them, so a title cannot hold an emoji that joins others with it.
///
/// ```
/// use gaslightmq_resource::{ResourceTitle, ResourceTitleError};
///
/// let title: ResourceTitle = "Slow down order producers".parse()?;
/// assert_eq!(title, "Slow down order producers");
/// assert_eq!(title.to_string(), "Slow down order producers");
///
/// assert_eq!(ResourceTitle::new(""), Err(ResourceTitleError::Empty));
/// assert!(ResourceTitle::new("Slow down order producers ").is_err());
/// // A zero-width space hides between the two words.
/// assert!(ResourceTitle::new("Order\u{200b}producers").is_err());
/// # Ok::<(), ResourceTitleError>(())
/// ```
///
/// With the `serde` feature, a title serializes as its text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceTitle(Arc<str>);

impl ResourceTitle {
    /// The most characters a title has.
    pub const MAX_LENGTH: usize = 2000;

    /// Checks the [rule for titles](Self#the-rule-for-titles), and keeps the text as it is.
    ///
    /// # Errors
    ///
    /// The [`ResourceTitleError`] that says which part of the rule the text breaks:
    /// it is empty or too long, starts or ends with white space, or holds a control character or a format character.
    /// A text that breaks several parts gets the first of them, in this order.
    pub fn new(title: impl AsRef<str>) -> Result<Self, ResourceTitleError> {
        let title = title.as_ref();
        let input = || title.to_owned();
        let length = title.chars().count();
        if title.is_empty() {
            Err(ResourceTitleError::Empty)
        } else if length > Self::MAX_LENGTH {
            Err(ResourceTitleError::TooLong { length })
        } else if title.trim() != title {
            Err(ResourceTitleError::SurroundingWhiteSpace { input: input() })
        } else if title.chars().any(char::is_control) {
            Err(ResourceTitleError::ControlCharacter { input: input() })
        } else if title.chars().any(is_format) {
            Err(ResourceTitleError::FormatCharacter { input: input() })
        } else {
            Ok(Self(title.into()))
        }
    }

    /// The title as text.
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

impl FromStr for ResourceTitle {
    type Err = ResourceTitleError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl TryFrom<&str> for ResourceTitle {
    type Error = ResourceTitleError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl TryFrom<String> for ResourceTitle {
    type Error = ResourceTitleError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<ResourceTitle> for String {
    fn from(title: ResourceTitle) -> Self {
        title.as_str().to_owned()
    }
}

impl fmt::Display for ResourceTitle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for ResourceTitle {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for ResourceTitle {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ResourceTitle {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for ResourceTitle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ResourceTitle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(text).map_err(serde::de::Error::custom)
    }
}

/// Why a text is not a [`ResourceTitle`]: the part of the [rule for titles](ResourceTitle#the-rule-for-titles) it
/// breaks.
///
/// The rule has five parts, and the error has one variant for each.
/// It is exhaustive on purpose.
/// The title of every kind of resource has an error of its own with the same five variants,
/// and its constructor matches on every variant of this one to say the same about its own title.
///
/// ```
/// use gaslightmq_resource::{ResourceTitle, ResourceTitleError};
///
/// let error = ResourceTitle::new("Order\u{feff}producers").unwrap_err();
/// assert_eq!(error, ResourceTitleError::FormatCharacter { input: "Order\u{feff}producers".to_owned() });
/// assert_eq!(error.to_string(), "title \"Order\\u{feff}producers\" holds a format character");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceTitleError {
    /// The text has no character.
    #[error("title \"\" is empty")]
    Empty,
    /// The text has `length` characters, which is more than a title has.
    #[error(
        "title of {length} characters is longer than {} characters",
        ResourceTitle::MAX_LENGTH
    )]
    TooLong { length: usize },
    /// The text starts or ends with white space.
    #[error("title {input:?} starts or ends with white space")]
    SurroundingWhiteSpace { input: String },
    /// The text holds a control character, such as a line break.
    #[error("title {input:?} holds a control character")]
    ControlCharacter { input: String },
    /// The text holds a format character, such as a zero-width space, which nobody sees.
    #[error("title {input:?} holds a format character")]
    FormatCharacter { input: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_title_keeps_its_text() {
        for text in [
            "Slow down order producers",
            "x",
            "Ünïcode ✓",
            "a  b",
            "Refuse orders.* (EU)",
        ] {
            let title = ResourceTitle::new(text).unwrap();
            assert_eq!(title.as_str(), text);
            assert_eq!(title.to_string(), text);
            assert_eq!(title, text);
            assert_eq!(title, *text);
            assert_eq!(title.as_ref(), text);
            assert_eq!(text.parse::<ResourceTitle>(), Ok(title.clone()));
            assert_eq!(ResourceTitle::try_from(text), Ok(title.clone()));
            assert_eq!(ResourceTitle::try_from(text.to_owned()), Ok(title.clone()));
            assert_eq!(String::from(title), text);
        }
    }

    #[test]
    fn a_title_that_breaks_the_rule_is_an_error() {
        use ResourceTitleError::{ControlCharacter, FormatCharacter, SurroundingWhiteSpace};

        type Breaks = fn(String) -> ResourceTitleError;
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

        assert_eq!(ResourceTitle::new(""), Err(ResourceTitleError::Empty));
        for (text, error) in cases {
            assert_eq!(ResourceTitle::new(text), Err(error(text.to_owned())), "{text:?}");
        }
    }

    #[test]
    fn a_title_has_at_most_2000_characters() {
        let longest = "a".repeat(ResourceTitle::MAX_LENGTH);
        let too_long = "a".repeat(ResourceTitle::MAX_LENGTH + 1);

        assert_eq!(ResourceTitle::MAX_LENGTH, 2000);
        assert_eq!(ResourceTitle::new(&longest).unwrap(), *longest);
        assert_eq!(
            ResourceTitle::new(&too_long),
            Err(ResourceTitleError::TooLong { length: 2001 })
        );
    }

    #[test]
    fn the_length_of_a_title_counts_characters_and_not_bytes() {
        // Every `é` is one character of two bytes, and every `✓` is one of three.
        let longest = "é".repeat(ResourceTitle::MAX_LENGTH);
        let too_long = "✓".repeat(ResourceTitle::MAX_LENGTH + 1);

        assert_eq!(longest.len(), 4000);
        assert_eq!(ResourceTitle::new(&longest).unwrap(), *longest);
        assert_eq!(
            ResourceTitle::new(&too_long),
            Err(ResourceTitleError::TooLong { length: 2001 })
        );
    }

    #[test]
    fn the_length_is_named_before_what_a_long_text_holds() {
        let text = format!(" {}\n", "a".repeat(ResourceTitle::MAX_LENGTH));

        assert_eq!(
            ResourceTitle::new(text),
            Err(ResourceTitleError::TooLong { length: 2002 })
        );
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let long = "a".repeat(ResourceTitle::MAX_LENGTH + 1);
        let cases = [
            ("", "title \"\" is empty"),
            (long.as_str(), "title of 2001 characters is longer than 2000 characters"),
            (
                "Order producers ",
                "title \"Order producers \" starts or ends with white space",
            ),
            (
                "Order\nproducers",
                "title \"Order\\nproducers\" holds a control character",
            ),
            (
                "Order\u{2060}producers",
                "title \"Order\\u{2060}producers\" holds a format character",
            ),
        ];

        for (text, message) in cases {
            assert_eq!(ResourceTitle::new(text).unwrap_err().to_string(), message);
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
    fn a_title_takes_every_character_in_its_middle_but_the_control_and_the_format_characters() {
        let mut refused = 0;
        let mut text = String::new();
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            // In the middle of a title, where white space is no reason to refuse it.
            text.clear();
            text.extend(['a', character, 'b']);
            if ResourceTitle::new(&text).is_err() {
                assert!(character.is_control() || is_format(character), "{text:?}");
                refused += 1;
            }
        }
        // The 65 control characters and the 170 format characters.
        assert_eq!(refused, 235);
    }

    #[test]
    fn white_space_starts_and_ends_no_title() {
        let mut white_space = 0;
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            if character.is_whitespace() {
                assert!(ResourceTitle::new(format!("{character}shop")).is_err());
                assert!(ResourceTitle::new(format!("shop{character}")).is_err());
                white_space += 1;
            }
        }
        assert_eq!(white_space, 25);
    }

    #[test]
    fn a_title_takes_the_characters_next_to_the_format_characters() {
        // A no-break space, a combining accent, a variation selector and an emoji are no format characters.
        for text in [
            "Order\u{a0}producers",
            "Orde\u{301}r",
            "Order \u{2764}\u{fe0f}",
            "Order 🚀",
        ] {
            assert!(ResourceTitle::new(text).is_ok(), "{text:?}");
        }
    }

    #[test]
    fn a_title_is_exact() {
        assert_ne!(
            ResourceTitle::new("Order producers").unwrap(),
            ResourceTitle::new("order producers").unwrap()
        );
    }

    #[test]
    fn a_title_pads_like_text() {
        assert_eq!(format!("{:>8}|", ResourceTitle::new("Flap").unwrap()), "    Flap|");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_title_is_its_text_in_serde() {
        let title = ResourceTitle::new("Order producers").unwrap();

        assert_eq!(serde_json::to_string(&title).unwrap(), "\"Order producers\"");
        assert_eq!(
            serde_json::from_str::<ResourceTitle>("\"Order producers\"").unwrap(),
            title
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_text_that_breaks_the_rule_does_not_read_as_a_title() {
        let error = serde_json::from_str::<ResourceTitle>("\" Order producers\"").unwrap_err();

        assert_eq!(
            error.to_string(),
            "title \" Order producers\" starts or ends with white space"
        );
        assert!(serde_json::from_str::<ResourceTitle>("7").is_err());
    }
}
