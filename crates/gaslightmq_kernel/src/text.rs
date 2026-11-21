//! What the text types share: the rules for names and IDs, the conversions and the `serde` form.

/// The most bytes a string has in a Kafka message, which writes the length as an `i16`.
pub(crate) const MAX_STRING_LEN: usize = 32_767;

/// The most characters Kafka gives a topic name.
pub(crate) const MAX_NAME_LEN: usize = 249;

/// The first character that Kafka does not allow in a topic name, with its position counted from zero.
///
/// Kafka allows `[a-zA-Z0-9._-]`, and it applies the same rule to a group instance ID.
pub(crate) fn invalid_character(value: &str) -> Option<(usize, char)> {
    let allowed = |character: char| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-');
    value.chars().enumerate().find(|(_, character)| !allowed(*character))
}

/// Whether Kafka reserves the name: `.` and `..` are paths, and never names.
pub(crate) fn is_reserved(value: &str) -> bool {
    matches!(value, "." | "..")
}

/// Define a name or an ID that holds checked text, and the error that says how a text breaks its rule.
///
/// `rule` is one of three:
///
/// | Rule | Takes |
/// |---|---|
/// | `name` | Kafka's rule for a topic name: 1 to `max` characters of `[a-zA-Z0-9._-]`, and not `.` or `..`. |
/// | `non_empty` | An ID that Kafka does not restrict: at least one byte, and no more than `max`. |
/// | `any` | An ID that may be empty: no more bytes than `max`. |
///
/// The error only has the variants that the rule can give.
macro_rules! text_value {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal, max: $max:expr, rule: $rule:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(std::sync::Arc<str>);

        impl $name {
            #[doc = concat!("The most bytes a ", $what, " has.")]
            pub const MAX_LEN: usize = $max;

            #[doc = concat!("Check `value` against the rule for a ", $what, ", and keep it.")]
            ///
            /// # Errors
            ///
            #[doc = concat!("The [`", stringify!($error), "`] that says how `value` breaks the rule.")]
            pub fn new(value: impl AsRef<str>) -> Result<Self, $error> {
                let value = value.as_ref();
                $crate::text::text_rule!($rule, $error, value, Self::MAX_LEN);
                Ok(Self(value.into()))
            }

            #[doc = concat!("The ", $what, " as text.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        $crate::text::text_error!($rule, $name, $error, $what);

        impl std::str::FromStr for $name {
            type Err = $error;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.as_str().to_owned()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(self.as_str(), f)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        /// Lets a map or a set with this key look a value up by `&str`.
        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
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

        impl PartialEq<String> for $name {
            fn eq(&self, other: &String) -> bool {
                self.as_str() == other.as_str()
            }
        }

        impl PartialEq<$name> for str {
            fn eq(&self, other: &$name) -> bool {
                self == other.as_str()
            }
        }

        impl PartialEq<$name> for &str {
            fn eq(&self, other: &$name) -> bool {
                *self == other.as_str()
            }
        }

        impl PartialEq<$name> for String {
            fn eq(&self, other: &$name) -> bool {
                self.as_str() == other.as_str()
            }
        }

        $crate::text::text_serde!($name);
    };
}

/// Check a text against one of the rules of [`text_value`], and return the error of the type when it breaks the rule.
macro_rules! text_rule {
    (name, $error:ident, $value:ident, $max:expr) => {
        $crate::text::text_rule!(non_empty, $error, $value, $max);
        if let Some((position, character)) = $crate::text::invalid_character($value) {
            return Err($error::InvalidCharacter {
                input: $value.to_owned(),
                character,
                position,
            });
        }
        if $crate::text::is_reserved($value) {
            return Err($error::Reserved {
                value: $value.to_owned(),
            });
        }
    };
    (non_empty, $error:ident, $value:ident, $max:expr) => {
        if $value.is_empty() {
            return Err($error::Empty);
        }
        $crate::text::text_rule!(any, $error, $value, $max);
    };
    (any, $error:ident, $value:ident, $max:expr) => {
        if $value.len() > $max {
            return Err($error::TooLong {
                len: $value.len(),
                max: $max,
            });
        }
    };
}

/// Define the error of a text type, with the variants that its rule can give.
macro_rules! text_error {
    (name, $name:ident, $error:ident, $what:literal) => {
        #[doc = concat!("Why a text is not a [`", stringify!($name), "`].")]
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no characters.
            Empty,
            #[doc = concat!("The text has more bytes than a ", $what, " has at most.")]
            TooLong { len: usize, max: usize },
            #[doc = concat!("The text has a character that Kafka does not allow in a ", $what, ".")]
            /// `position` counts characters from zero.
            InvalidCharacter {
                input: String,
                character: char,
                position: usize,
            },
            /// Kafka reserves the text: `.` and `..` are paths.
            Reserved { value: String },
        }

        impl std::fmt::Display for $error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::Empty => f.write_str(concat!($what, " is empty")),
                    Self::TooLong { len, max } => {
                        write!(
                            f,
                            concat!($what, " of {} bytes exceeds the limit of {}"),
                            len, max
                        )
                    }
                    Self::InvalidCharacter {
                        input,
                        character,
                        position,
                    } => write!(
                        f,
                        concat!($what, " {:?} has the invalid character {:?} at position {}"),
                        input, character, position
                    ),
                    Self::Reserved { value } => write!(f, concat!($what, " cannot be {:?}"), value),
                }
            }
        }

        impl std::error::Error for $error {}
    };
    (non_empty, $name:ident, $error:ident, $what:literal) => {
        #[doc = concat!("Why a text is not a [`", stringify!($name), "`].")]
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no characters.
            Empty,
            #[doc = concat!("The text has more bytes than a ", $what, " has at most.")]
            TooLong { len: usize, max: usize },
        }

        impl std::fmt::Display for $error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::Empty => f.write_str(concat!($what, " is empty")),
                    Self::TooLong { len, max } => {
                        write!(
                            f,
                            concat!($what, " of {} bytes exceeds the limit of {}"),
                            len, max
                        )
                    }
                }
            }
        }

        impl std::error::Error for $error {}
    };
    (any, $name:ident, $error:ident, $what:literal) => {
        #[doc = concat!("Why a text is not a [`", stringify!($name), "`].")]
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            #[doc = concat!("The text has more bytes than a ", $what, " has at most.")]
            TooLong { len: usize, max: usize },
        }

        impl std::fmt::Display for $error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::TooLong { len, max } => {
                        write!(
                            f,
                            concat!($what, " of {} bytes exceeds the limit of {}"),
                            len, max
                        )
                    }
                }
            }
        }

        impl std::error::Error for $error {}
    };
}

/// Give a type that has a text form the conversions from and to owned text, and the text form in `serde`.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
/// `error` is the error of the type's `FromStr`.
macro_rules! text_form {
    ($name:ident, $error:ident) => {
        $crate::text::text_conversions!($name, $error);
        $crate::text::text_serde!($name);
    };
}

/// Give a type that has a text form the conversions from and to owned text.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
/// `error` is the error of the type's `FromStr`.
macro_rules! text_conversions {
    ($name:ident, $error:ident) => {
        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(text: &str) -> Result<Self, Self::Error> {
                text.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                text.parse()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }
    };
}

/// Map a type to `serde` as its text form.
///
/// Reading goes through `FromStr`, so it applies the type's rule.
macro_rules! text_serde {
    ($name:ident) => {
        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use {text_conversions, text_error, text_form, text_rule, text_serde, text_value};

#[cfg(test)]
mod tests {
    use crate::{
        ClientId, ClientIdError, GroupId, GroupIdError, GroupInstanceId, GroupInstanceIdError, TopicName,
        TopicNameError,
    };

    #[test]
    fn a_name_takes_letters_digits_dots_underscores_and_hyphens() {
        for value in ["orders.eu", "a", "A-Z_0.9", "...", "__consumer_offsets", "-", "_"] {
            assert!(TopicName::new(value).is_ok(), "{value:?}");
        }
    }

    #[test]
    fn a_name_is_not_empty() {
        assert_eq!(TopicName::new(""), Err(TopicNameError::Empty));
    }

    #[test]
    fn a_name_has_at_most_249_characters() {
        assert!(TopicName::new("a".repeat(249)).is_ok());
        assert_eq!(
            TopicName::new("a".repeat(250)),
            Err(TopicNameError::TooLong { len: 250, max: 249 })
        );
    }

    #[test]
    fn a_name_rejects_every_other_character() {
        for (value, character, position) in [
            ("orders/3", '/', 6),
            ("orders@3", '@', 6),
            ("orders eu", ' ', 6),
            ("*", '*', 0),
            ("ordérs", 'é', 3),
        ] {
            assert_eq!(
                TopicName::new(value),
                Err(TopicNameError::InvalidCharacter {
                    input: value.to_owned(),
                    character,
                    position
                }),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_name_is_not_one_or_two_dots() {
        for value in [".", ".."] {
            assert_eq!(
                TopicName::new(value),
                Err(TopicNameError::Reserved {
                    value: value.to_owned()
                })
            );
        }
    }

    #[test]
    fn every_type_with_the_rule_for_names_has_its_own_error() {
        assert_eq!(GroupInstanceId::new(""), Err(GroupInstanceIdError::Empty));
        assert_eq!(
            GroupInstanceId::new("etl 0"),
            Err(GroupInstanceIdError::InvalidCharacter {
                input: "etl 0".to_owned(),
                character: ' ',
                position: 3
            })
        );
    }

    #[test]
    fn a_free_id_takes_any_text_that_fits_a_message() {
        assert!(GroupId::new("analytics etl/é").is_ok());
        assert!(GroupId::new("a".repeat(32_767)).is_ok());
        assert_eq!(
            GroupId::new("a".repeat(32_768)),
            Err(GroupIdError::TooLong {
                len: 32_768,
                max: 32_767
            })
        );
        assert_eq!(GroupId::new(""), Err(GroupIdError::Empty));
    }

    #[test]
    fn the_limit_counts_bytes() {
        assert!(ClientId::new("é".repeat(16_383)).is_ok());
        assert_eq!(
            ClientId::new("é".repeat(16_384)),
            Err(ClientIdError::TooLong {
                len: 32_768,
                max: 32_767
            })
        );
    }

    #[test]
    fn an_id_that_may_be_empty_takes_no_text() {
        assert!(ClientId::new("").is_ok());
    }

    #[test]
    fn an_error_names_the_value_and_what_is_wrong_with_it() {
        let cases = [
            (TopicNameError::Empty.to_string(), "topic name is empty"),
            (
                TopicNameError::TooLong { len: 250, max: 249 }.to_string(),
                "topic name of 250 bytes exceeds the limit of 249",
            ),
            (
                TopicNameError::InvalidCharacter {
                    input: "orders/eu".to_owned(),
                    character: '/',
                    position: 6,
                }
                .to_string(),
                "topic name \"orders/eu\" has the invalid character '/' at position 6",
            ),
            (
                TopicNameError::Reserved { value: "..".into() }.to_string(),
                "topic name cannot be \"..\"",
            ),
            (GroupIdError::Empty.to_string(), "group ID is empty"),
            (
                ClientIdError::TooLong {
                    len: 32_768,
                    max: 32_767,
                }
                .to_string(),
                "client ID of 32768 bytes exceeds the limit of 32767",
            ),
        ];

        for (message, expected) in cases {
            assert_eq!(message, expected);
        }
    }
}
