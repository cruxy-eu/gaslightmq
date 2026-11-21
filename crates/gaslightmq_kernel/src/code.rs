//! What the API key and the error code share: a number with a table of names.

use crate::KernelError;

/// The numbers Kafka gives a name, each with that name, in the order of the numbers.
pub(crate) type Names = &'static [(i16, &'static str)];

/// The name Kafka gives `code`.
pub(crate) fn name(names: Names, code: i16) -> Option<&'static str> {
    names.binary_search_by_key(&code, |(code, _)| *code).ok().and_then(|index| names.get(index)).map(|(_, name)| *name)
}

/// Read a name of the table, or a decimal number that fits `i16`.
pub(crate) fn parse(what: &'static str, names: Names, text: &str) -> Result<i16, KernelError> {
    if text.is_empty() {
        return Err(KernelError::Empty { what });
    }
    if let Some((code, _)) = names.iter().find(|(_, name)| *name == text) {
        return Ok(*code);
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KernelError::UnknownName { what, input: text.to_owned() });
    }
    // A number that does not fit the type it is read as is outside `i16` on the side of its sign.
    if text.starts_with('-') {
        let value = text.parse::<i64>().unwrap_or(i64::MIN);
        i16::try_from(value).map_err(|_| KernelError::TooSmall { what, value, min: i16::MIN.into() })
    } else {
        let value = text.parse::<u64>().unwrap_or(u64::MAX);
        i16::try_from(value).map_err(|_| KernelError::TooLarge { what, value, max: i16::MAX.unsigned_abs().into() })
    }
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

pub(crate) use code_serde;

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
        assert_eq!(parse("code", NAMES, "Three"), Ok(3));
        assert_eq!(parse("code", NAMES, "MINUS_ONE"), Ok(-1));
        for text in ["three", "THREE", "Thre", "Three ", " Three", "Minus_One"] {
            assert_eq!(
                parse("code", NAMES, text),
                Err(KernelError::UnknownName { what: "code", input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn reads_a_number_with_or_without_a_name() {
        assert_eq!(parse("code", NAMES, "3"), Ok(3));
        assert_eq!(parse("code", NAMES, "92"), Ok(92));
        assert_eq!(parse("code", NAMES, "-1"), Ok(-1));
        assert_eq!(parse("code", NAMES, "007"), Ok(7));
        assert_eq!(parse("code", NAMES, "32767"), Ok(i16::MAX));
        assert_eq!(parse("code", NAMES, "-32768"), Ok(i16::MIN));
    }

    #[test]
    fn a_number_fits_i16() {
        assert_eq!(
            parse("code", NAMES, "32768"),
            Err(KernelError::TooLarge { what: "code", value: 32_768, max: 32_767 })
        );
        assert_eq!(
            parse("code", NAMES, "-32769"),
            Err(KernelError::TooSmall { what: "code", value: -32_769, min: -32_768 })
        );
        assert_eq!(
            parse("code", NAMES, "99999999999999999999999"),
            Err(KernelError::TooLarge { what: "code", value: u64::MAX, max: 32_767 })
        );
        assert_eq!(
            parse("code", NAMES, "-99999999999999999999999"),
            Err(KernelError::TooSmall { what: "code", value: i64::MIN, min: -32_768 })
        );
    }

    #[test]
    fn anything_else_is_an_unknown_name() {
        for text in ["+3", "3.0", "-", "--1", "0x10", "3 ", "Prodcue"] {
            assert_eq!(
                parse("code", NAMES, text),
                Err(KernelError::UnknownName { what: "code", input: text.to_owned() }),
                "{text:?}"
            );
        }
        assert_eq!(parse("code", NAMES, ""), Err(KernelError::Empty { what: "code" }));
    }
}
