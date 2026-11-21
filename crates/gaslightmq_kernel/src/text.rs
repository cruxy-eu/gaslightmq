//! What the text types share: the rules for names and IDs, the conversions and the `serde` form.

use crate::KernelError;

/// The most bytes a string has in a Kafka message, which writes the length as an `i16`.
pub(crate) const MAX_STRING_LEN: usize = 32_767;

/// The most characters Kafka gives a topic name.
pub(crate) const MAX_NAME_LEN: usize = 249;

/// Kafka's rule for a topic name, which it also applies to a group instance ID:
/// 1 to 249 characters of `[a-zA-Z0-9._-]`, and not `.` or `..`.
pub(crate) fn name(what: &'static str, value: &str, max: usize) -> Result<(), KernelError> {
    non_empty(what, value, max)?;
    let allowed = |character: char| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-');
    if let Some((position, character)) = value.chars().enumerate().find(|(_, character)| !allowed(*character)) {
        return Err(KernelError::InvalidCharacter { what, character, position });
    }
    if matches!(value, "." | "..") {
        return Err(KernelError::Reserved { what, value: value.to_owned() });
    }
    Ok(())
}

/// The rule for an ID that Kafka does not restrict: at least one byte, and no more than a message can carry.
pub(crate) fn non_empty(what: &'static str, value: &str, max: usize) -> Result<(), KernelError> {
    if value.is_empty() {
        return Err(KernelError::Empty { what });
    }
    any(what, value, max)
}

/// The rule for an ID that may be empty: no more bytes than a message can carry.
pub(crate) fn any(what: &'static str, value: &str, max: usize) -> Result<(), KernelError> {
    if value.len() > max {
        return Err(KernelError::TooLong { what, len: value.len(), max });
    }
    Ok(())
}

/// Define a name or an ID that holds checked text.
///
/// `rule` is one of the functions of this module.
macro_rules! text_value {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal, max: $max:expr, rule: $rule:path
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(std::sync::Arc<str>);

        impl $name {
            #[doc = concat!("The most bytes a ", $what, " has.")]
            pub const MAX_LEN: usize = $max;

            #[doc = concat!("Check `value` against the rule for a ", $what, ", and keep it.")]
            pub fn new(value: impl AsRef<str>) -> Result<Self, $crate::KernelError> {
                let value = value.as_ref();
                $rule($what, value, Self::MAX_LEN)?;
                Ok(Self(value.into()))
            }

            #[doc = concat!("The ", $what, " as text.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::KernelError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $crate::KernelError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::KernelError;

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

/// Give a type that has a text form the conversions from and to owned text.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
macro_rules! text_form {
    ($name:ident) => {
        impl TryFrom<&str> for $name {
            type Error = $crate::KernelError;

            fn try_from(text: &str) -> Result<Self, Self::Error> {
                text.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::KernelError;

            fn try_from(text: String) -> Result<Self, Self::Error> {
                text.parse()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        $crate::text::text_serde!($name);
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

pub(crate) use {text_form, text_serde, text_value};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_takes_letters_digits_dots_underscores_and_hyphens() {
        for value in ["orders.eu", "a", "A-Z_0.9", "...", "__consumer_offsets", "-", "_"] {
            assert_eq!(name("topic name", value, MAX_NAME_LEN), Ok(()), "{value:?}");
        }
    }

    #[test]
    fn a_name_is_not_empty() {
        assert_eq!(name("topic name", "", MAX_NAME_LEN), Err(KernelError::Empty { what: "topic name" }));
    }

    #[test]
    fn a_name_has_at_most_249_characters() {
        assert_eq!(name("topic name", &"a".repeat(249), MAX_NAME_LEN), Ok(()));
        assert_eq!(
            name("topic name", &"a".repeat(250), MAX_NAME_LEN),
            Err(KernelError::TooLong { what: "topic name", len: 250, max: 249 })
        );
    }

    #[test]
    fn a_name_rejects_every_other_character() {
        for (value, character, position) in
            [("orders/3", '/', 6), ("orders@3", '@', 6), ("orders eu", ' ', 6), ("*", '*', 0), ("ordérs", 'é', 3)]
        {
            assert_eq!(
                name("topic name", value, MAX_NAME_LEN),
                Err(KernelError::InvalidCharacter { what: "topic name", character, position }),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_name_is_not_one_or_two_dots() {
        for value in [".", ".."] {
            assert_eq!(
                name("topic name", value, MAX_NAME_LEN),
                Err(KernelError::Reserved { what: "topic name", value: value.to_owned() })
            );
        }
    }

    #[test]
    fn a_free_id_takes_any_text_that_fits_a_message() {
        assert_eq!(non_empty("group ID", "analytics etl/é", MAX_STRING_LEN), Ok(()));
        assert_eq!(non_empty("group ID", &"a".repeat(32_767), MAX_STRING_LEN), Ok(()));
        assert_eq!(
            non_empty("group ID", &"a".repeat(32_768), MAX_STRING_LEN),
            Err(KernelError::TooLong { what: "group ID", len: 32_768, max: 32_767 })
        );
        assert_eq!(non_empty("group ID", "", MAX_STRING_LEN), Err(KernelError::Empty { what: "group ID" }));
    }

    #[test]
    fn the_limit_counts_bytes() {
        assert_eq!(any("client ID", "ééé", 6), Ok(()));
        assert_eq!(any("client ID", "éééé", 6), Err(KernelError::TooLong { what: "client ID", len: 8, max: 6 }));
    }

    #[test]
    fn an_id_that_may_be_empty_takes_no_text() {
        assert_eq!(any("client ID", "", MAX_STRING_LEN), Ok(()));
    }
}
