//! What the API key and the error code share: a number with a table of names.

/// The numbers Kafka gives a name, each with that name, in the order of the numbers.
pub(crate) type Names = &'static [(i16, &'static str)];

/// The name Kafka gives `code`.
pub(crate) fn name(names: Names, code: i16) -> Option<&'static str> {
    names
        .binary_search_by_key(&code, |(code, _)| *code)
        .ok()
        .and_then(|index| names.get(index))
        .map(|(_, name)| *name)
}

/// Why a text is not a name of a table, and not a number that fits `i16` either.
///
/// The API key and the error code each have an error of their own, with one variant for each of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Flaw {
    Empty,
    UnknownName,
    /// The number, which stops at [`i64::MIN`] for a text with more digits than that.
    TooSmall(i64),
    /// The number, which stops at [`u64::MAX`] for a text with more digits than that.
    TooLarge(u64),
}

/// The lowest number of an API key or an error code, as an error shows it.
pub(crate) const MIN: i64 = i16::MIN as i64;

/// The highest number of an API key or an error code, as an error shows it.
pub(crate) const MAX: u64 = i16::MAX as u64;

/// Read a name of the table, or a decimal number that fits `i16`.
pub(crate) fn parse(names: Names, text: &str) -> Result<i16, Flaw> {
    if text.is_empty() {
        return Err(Flaw::Empty);
    }
    if let Some((code, _)) = names.iter().find(|(_, name)| *name == text) {
        return Ok(*code);
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Flaw::UnknownName);
    }
    // A number that does not fit the type it is read as is outside `i16` on the side of its sign.
    if text.starts_with('-') {
        let value = text.parse::<i64>().unwrap_or(i64::MIN);
        i16::try_from(value).map_err(|_| Flaw::TooSmall(value))
    } else {
        let value = text.parse::<u64>().unwrap_or(u64::MAX);
        i16::try_from(value).map_err(|_| Flaw::TooLarge(value))
    }
}

/// Define the error of a number with a table of names: the four ways a text is not one,
/// and the variants that only this type has.
macro_rules! code_error {
    (
        $(#[$meta:meta])*
        $error:ident, $what:literal
        $(, $(#[$extra_meta:meta])* $extra:ident { $field:ident: $type:ty } => $message:literal)?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no characters.
            #[error("{} is empty", $what)]
            Empty,
            #[doc = concat!("The text is not a name that Kafka gives an ", $what, ", and not a number either.")]
            #[error("unknown {}: {input:?}", $what)]
            UnknownName { input: String },
            #[doc = concat!("The number is below the lowest ", $what, ".")]
            /// `value` stops at [`i64::MIN`] for a text with more digits than that.
            #[error("{} {value} is below the minimum of {min}", $what)]
            TooSmall { value: i64, min: i64 },
            #[doc = concat!("The number is above the highest ", $what, ".")]
            /// `value` stops at [`u64::MAX`] for a text with more digits than that.
            #[error("{} {value} exceeds the maximum of {max}", $what)]
            TooLarge { value: u64, max: u64 },
            $(
                $(#[$extra_meta])*
                #[error("{} {}", $what, format_args!($message, .$field))]
                $extra { $field: $type },
            )?
        }

        impl $error {
            /// The error of this type for a text that is no name and no number of the table.
            fn of(flaw: $crate::code::Flaw, text: &str) -> Self {
                use $crate::code::{Flaw, MAX, MIN};

                match flaw {
                    Flaw::Empty => Self::Empty,
                    Flaw::UnknownName => Self::UnknownName { input: text.to_owned() },
                    Flaw::TooSmall(value) => Self::TooSmall { value, min: MIN },
                    Flaw::TooLarge(value) => Self::TooLarge { value, max: MAX },
                }
            }
        }
    };
}

/// Map a number with a name to `serde`.
///
/// A format that people read gets the name, or the number when Kafka gives it no name.
/// It reads back a name, a number, or a number as text.
/// Every other format gets the number.
/// Reading applies the type's rule through `FromStr` and `TryFrom<i16>`.
macro_rules! code_serde {
    ($name:ident, $expecting:literal) => {
        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self.name() {
                    Some(name) if serializer.is_human_readable() => serializer.serialize_str(name),
                    _ => serializer.serialize_i16(self.value()),
                }
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                use serde::de::{Error, Unexpected};

                struct Visitor;

                impl Visitor {
                    fn number<E: Error>(self, number: Option<i16>, unexpected: Unexpected<'_>) -> Result<$name, E> {
                        let number = number.ok_or_else(|| E::invalid_value(unexpected, &self))?;
                        $name::try_from(number).map_err(E::custom)
                    }
                }

                impl serde::de::Visitor<'_> for Visitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str($expecting)
                    }

                    fn visit_str<E: Error>(self, text: &str) -> Result<$name, E> {
                        text.parse().map_err(E::custom)
                    }

                    fn visit_i64<E: Error>(self, number: i64) -> Result<$name, E> {
                        self.number(i16::try_from(number).ok(), Unexpected::Signed(number))
                    }

                    fn visit_u64<E: Error>(self, number: u64) -> Result<$name, E> {
                        self.number(i16::try_from(number).ok(), Unexpected::Unsigned(number))
                    }
                }

                if deserializer.is_human_readable() {
                    deserializer.deserialize_any(Visitor)
                } else {
                    deserializer.deserialize_i16(Visitor)
                }
            }
        }
    };
}

pub(crate) use {code_error, code_serde};

#[cfg(test)]
mod tests {
    use super::*;

    const NAMES: Names = &[(-1, "MINUS_ONE"), (0, "Zero"), (3, "Three"), (40, "Forty")];

    #[test]
    fn finds_the_name_of_a_number() {
        assert_eq!(name(NAMES, -1), Some("MINUS_ONE"));
        assert_eq!(name(NAMES, 0), Some("Zero"));
        assert_eq!(name(NAMES, 40), Some("Forty"));
        assert_eq!(name(NAMES, 1), None);
        assert_eq!(name(NAMES, i16::MIN), None);
        assert_eq!(name(NAMES, i16::MAX), None);
        assert_eq!(name(&[], 0), None);
    }

    #[test]
    fn reads_a_name_as_kafka_spells_it() {
        assert_eq!(parse(NAMES, "Three"), Ok(3));
        assert_eq!(parse(NAMES, "MINUS_ONE"), Ok(-1));
        for text in ["three", "THREE", "Thre", "Three ", " Three", "Minus_One"] {
            assert_eq!(parse(NAMES, text), Err(Flaw::UnknownName), "{text:?}");
        }
    }

    #[test]
    fn reads_a_number_with_or_without_a_name() {
        assert_eq!(parse(NAMES, "3"), Ok(3));
        assert_eq!(parse(NAMES, "92"), Ok(92));
        assert_eq!(parse(NAMES, "-1"), Ok(-1));
        assert_eq!(parse(NAMES, "007"), Ok(7));
        assert_eq!(parse(NAMES, "32767"), Ok(i16::MAX));
        assert_eq!(parse(NAMES, "-32768"), Ok(i16::MIN));
    }

    #[test]
    fn a_number_fits_i16() {
        assert_eq!(parse(NAMES, "32768"), Err(Flaw::TooLarge(32_768)));
        assert_eq!(parse(NAMES, "-32769"), Err(Flaw::TooSmall(-32_769)));
        assert_eq!(parse(NAMES, "99999999999999999999999"), Err(Flaw::TooLarge(u64::MAX)));
        assert_eq!(parse(NAMES, "-99999999999999999999999"), Err(Flaw::TooSmall(i64::MIN)));
    }

    #[test]
    fn anything_else_is_an_unknown_name() {
        for text in ["+3", "3.0", "-", "--1", "0x10", "3 ", "Prodcue"] {
            assert_eq!(parse(NAMES, text), Err(Flaw::UnknownName), "{text:?}");
        }
        assert_eq!(parse(NAMES, ""), Err(Flaw::Empty));
    }
}
