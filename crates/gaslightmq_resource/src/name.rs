use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// The name of a resource, such as `order-producers`.
///
/// A name is the identity of a resource between domains, and nothing else.
/// Things reference a resource by its name: a rule names its selector and its preset, and a command names an
/// environment.
/// A resource keeps its name for as long as it exists.
/// Its UUID stands next to its name, and only the workspace tells resources apart by it.
///
/// A resource name does not say which kind of resource it names.
/// Every kind has a name type of its own that wraps this one, and code speaks in those.
///
/// # The rule for names
///
/// A name is a slug, like the name of a resource in Kubernetes.
/// It holds lowercase letters from `a` to `z`, digits and hyphens.
/// It starts and ends with a letter or a digit.
/// It has at least one character, and at most [`MAX_LENGTH`](Self::MAX_LENGTH), which is 63.
///
/// ```
/// use gaslightmq_resource::{ResourceName, ResourceNameError};
///
/// let name: ResourceName = "order-producers".parse()?;
/// assert_eq!(name, "order-producers");
/// assert_eq!(name.to_string(), "order-producers");
///
/// assert_eq!(ResourceName::new(""), Err(ResourceNameError::Empty));
/// // Free text is for the title of a resource, and never for its name.
/// assert!(ResourceName::new("Order producers").is_err());
/// assert!(ResourceName::new("order-producers-").is_err());
/// # Ok::<(), ResourceNameError>(())
/// ```
///
/// With the `serde` feature, a name serializes as its text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceName(Arc<str>);

impl ResourceName {
    /// The most characters a name has.
    pub const MAX_LENGTH: usize = 63;

    /// Checks the [rule for names](Self#the-rule-for-names), and keeps the text as it is.
    ///
    /// # Errors
    ///
    /// The [`ResourceNameError`] that says which part of the rule the text breaks:
    /// it is empty, holds a character that no name holds, starts or ends with a hyphen, or is too long.
    /// A text that breaks several parts gets the first of them, in this order.
    pub fn new(name: impl AsRef<str>) -> Result<Self, ResourceNameError> {
        let name = name.as_ref();
        let invalid = name
            .chars()
            .enumerate()
            .find(|(_, character)| !is_name_character(*character));
        if name.is_empty() {
            Err(ResourceNameError::Empty)
        } else if let Some((position, character)) = invalid {
            Err(ResourceNameError::InvalidCharacter {
                input: name.to_owned(),
                character,
                position,
            })
        } else if name.starts_with('-') || name.ends_with('-') {
            Err(ResourceNameError::SurroundingHyphen { input: name.to_owned() })
        } else if name.len() > Self::MAX_LENGTH {
            // Every character that a name holds is one byte.
            Err(ResourceNameError::TooLong { length: name.len() })
        } else {
            Ok(Self(name.into()))
        }
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether a name holds the character: a lowercase letter from `a` to `z`, a digit or a hyphen.
const fn is_name_character(character: char) -> bool {
    matches!(character, 'a'..='z' | '0'..='9' | '-')
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
/// let error = ResourceName::new("Order producers").unwrap_err();
/// assert_eq!(
///     error,
///     ResourceNameError::InvalidCharacter { input: "Order producers".to_owned(), character: 'O', position: 0 }
/// );
/// assert_eq!(
///     error.to_string(),
///     "name \"Order producers\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceNameError {
    /// The text has no character.
    #[error("name \"\" is empty")]
    Empty,
    /// The text holds a character that no name holds, such as an uppercase letter, a space or a dot.
    /// It is the first such character, and `position` counts characters from zero.
    #[error("name {input:?} holds {character:?} at position {position}, which is no lowercase letter, digit or hyphen")]
    InvalidCharacter {
        input: String,
        character: char,
        position: usize,
    },
    /// The text starts or ends with a hyphen.
    #[error("name {input:?} starts or ends with a hyphen")]
    SurroundingHyphen { input: String },
    /// The text has `length` characters, which is more than a name has.
    #[error("name of {length} characters is longer than {} characters", ResourceName::MAX_LENGTH)]
    TooLong { length: usize },
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    /// A name of `length` characters.
    fn name_of(length: usize) -> String {
        "a".repeat(length)
    }

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["order-producers", "x", "7", "delay-1s", "a--b", "2nd-produce"] {
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
        use ResourceNameError::{InvalidCharacter, SurroundingHyphen, TooLong};

        let invalid = |input: &str, character, position| InvalidCharacter {
            input: input.to_owned(),
            character,
            position,
        };
        let hyphen = |input: &str| SurroundingHyphen {
            input: input.to_owned(),
        };
        let long = name_of(ResourceName::MAX_LENGTH + 1);
        let cases = [
            ("Order producers", invalid("Order producers", 'O', 0)),
            ("order producers", invalid("order producers", ' ', 5)),
            ("orders.eu", invalid("orders.eu", '.', 6)),
            ("order_producers", invalid("order_producers", '_', 5)),
            (" orders", invalid(" orders", ' ', 0)),
            ("orders\n", invalid("orders\n", '\n', 6)),
            // The position counts characters, and not bytes.
            ("ördérs", invalid("ördérs", 'ö', 0)),
            ("ordérs!", invalid("ordérs!", 'é', 3)),
            // A zero-width space hides between the two words.
            (
                "order\u{200b}producers",
                invalid("order\u{200b}producers", '\u{200b}', 5),
            ),
            ("-orders", hyphen("-orders")),
            ("orders-", hyphen("orders-")),
            ("-", hyphen("-")),
            // A character that no name holds is named before a hyphen at an end.
            ("-Orders", invalid("-Orders", 'O', 1)),
            (long.as_str(), TooLong { length: 64 }),
        ];

        assert_eq!(ResourceName::new(""), Err(ResourceNameError::Empty));
        for (text, error) in cases {
            assert_eq!(ResourceName::new(text), Err(error), "{text:?}");
        }
    }

    #[test]
    fn a_hyphen_at_an_end_is_named_before_the_length() {
        let text = format!("-{}", name_of(ResourceName::MAX_LENGTH));

        assert_eq!(
            ResourceName::new(&text),
            Err(ResourceNameError::SurroundingHyphen { input: text })
        );
    }

    #[test]
    fn a_name_has_at_most_63_characters() {
        let longest = name_of(ResourceName::MAX_LENGTH);
        let too_long = name_of(ResourceName::MAX_LENGTH + 1);

        assert_eq!(ResourceName::MAX_LENGTH, 63);
        assert_eq!(ResourceName::new(&longest).unwrap(), *longest);
        assert_eq!(
            ResourceName::new(&too_long),
            Err(ResourceNameError::TooLong { length: 64 })
        );
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let long = name_of(ResourceName::MAX_LENGTH + 1);
        let cases = [
            ("", "name \"\" is empty"),
            (
                "Order producers",
                "name \"Order producers\" holds 'O' at position 0, which is no lowercase letter, digit or hyphen",
            ),
            (
                "order\nproducers",
                "name \"order\\nproducers\" holds '\\n' at position 5, which is no lowercase letter, digit or hyphen",
            ),
            (
                "order-producers-",
                "name \"order-producers-\" starts or ends with a hyphen",
            ),
            (long.as_str(), "name of 64 characters is longer than 63 characters"),
        ];

        for (text, message) in cases {
            assert_eq!(ResourceName::new(text).unwrap_err().to_string(), message);
        }
    }

    #[test]
    fn a_name_holds_lowercase_letters_digits_and_hyphens_and_no_other_character() {
        let mut held = String::new();
        let mut text = String::new();
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            // In the middle of a name, where a hyphen is no reason to refuse it.
            text.clear();
            text.extend(['a', character, 'b']);
            if ResourceName::new(&text).is_ok() {
                held.push(character);
            }
        }

        assert_eq!(held, "-0123456789abcdefghijklmnopqrstuvwxyz");
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let name = || ResourceName::new("order-producers").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("order-producers"), Some(&1));
        assert_eq!(sorted.get("order-consumers"), None);
        assert_eq!(hashed.get("order-producers"), Some(&1));
        assert_eq!(hashed.get("order-consumers"), None);
    }

    #[test]
    fn a_name_pads_like_text() {
        assert_eq!(format!("{:>8}|", ResourceName::new("flap").unwrap()), "    flap|");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_name_is_its_text_in_serde() {
        let name = ResourceName::new("order-producers").unwrap();

        assert_eq!(serde_json::to_string(&name).unwrap(), "\"order-producers\"");
        assert_eq!(
            serde_json::from_str::<ResourceName>("\"order-producers\"").unwrap(),
            name
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_text_that_breaks_the_rule_does_not_read_as_a_name() {
        let error = serde_json::from_str::<ResourceName>("\"-order-producers\"").unwrap_err();

        assert_eq!(
            error.to_string(),
            "name \"-order-producers\" starts or ends with a hyphen"
        );
        assert!(serde_json::from_str::<ResourceName>("7").is_err());
    }
}
