//! What the types with a text form share: conversions from and to owned text, and their `serde` form.

/// Give a type that has a text form the conversions from and to owned text, and the text form in `serde`.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
macro_rules! text_form {
    ($name:ident) => {
        $crate::text::text_conversions!($name);

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

/// Give a type that holds a number the conversions from and to owned text, and the number in `serde`.
///
/// The type implements `FromStr`, `Display`, `TryFrom<$value>` and `value`.
/// A format that people read also accepts the text form, such as `"#7"` for a connection number.
macro_rules! number_form {
    ($name:ident, $value:ty, $expecting:literal) => {
        $crate::text::text_conversions!($name);

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serde::Serialize::serialize(&self.value(), serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                use serde::de::{Error, Unexpected};

                struct Visitor;

                impl Visitor {
                    fn number<E: Error>(self, number: Option<$value>, unexpected: Unexpected<'_>) -> Result<$name, E> {
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
                        self.number(<$value>::try_from(number).ok(), Unexpected::Signed(number))
                    }

                    fn visit_u64<E: Error>(self, number: u64) -> Result<$name, E> {
                        self.number(<$value>::try_from(number).ok(), Unexpected::Unsigned(number))
                    }
                }

                if deserializer.is_human_readable() {
                    deserializer.deserialize_any(Visitor)
                } else {
                    <$value as serde::Deserialize>::deserialize(deserializer)
                        .and_then(|number| $name::try_from(number).map_err(Error::custom))
                }
            }
        }
    };
}

/// Give a type that has a text form the conversions from and to owned text.
///
/// The type implements `FromStr` and `Display`, and this macro builds on both.
macro_rules! text_conversions {
    ($name:ident) => {
        impl TryFrom<&str> for $name {
            type Error = $crate::FieldError;

            fn try_from(text: &str) -> Result<Self, $crate::FieldError> {
                text.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::FieldError;

            fn try_from(text: String) -> Result<Self, $crate::FieldError> {
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

pub(crate) use {number_form, text_conversions, text_form};

/// Reads decimal digits with an optional minus sign, and checks the number against `min` and `max`.
/// A number that `T` cannot hold is out of range as well.
pub(crate) fn number<T: TryFrom<i128>>(
    what: &'static str,
    text: &str,
    min: i64,
    max: u64,
) -> Result<T, crate::FieldError> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(crate::FieldError::malformed(what, text, "a whole number"));
    }
    text.parse::<i128>()
        .ok()
        .filter(|number| (i128::from(min)..=i128::from(max)).contains(number))
        .and_then(|number| T::try_from(number).ok())
        .ok_or_else(|| crate::FieldError::out_of_range(what, text, min, max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldError;

    #[test]
    fn reads_digits_within_the_range() {
        assert_eq!(number("acks", "0", -5, 9), Ok(0));
        assert_eq!(number("acks", "-5", -5, 9), Ok(-5));
        assert_eq!(number("acks", "9", -5, 9), Ok(9));
        assert_eq!(number("acks", "007", -5, 9), Ok(7));
        assert_eq!(
            number("connection number", "18446744073709551615", 0, u64::MAX),
            Ok(i128::from(u64::MAX))
        );
    }

    #[test]
    fn a_number_outside_the_range_is_out_of_range() {
        for text in ["10", "-6", "99999999999999999999999999999999999999999999"] {
            assert_eq!(
                number::<i16>("acks", text, -5, 9),
                Err(FieldError::out_of_range("acks", text, -5, 9)),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_number_the_type_cannot_hold_is_out_of_range() {
        assert_eq!(
            number::<i16>("acks", "40000", 0, 50_000),
            Err(FieldError::out_of_range("acks", "40000", 0, 50_000))
        );
    }

    #[test]
    fn anything_else_is_malformed() {
        for text in ["", "-", "+3", " 3", "3 ", "3.0", "--1", "0x10", "all", "٣"] {
            assert_eq!(
                number::<i16>("acks", text, -5, 9),
                Err(FieldError::malformed("acks", text, "a whole number")),
                "{text:?}"
            );
        }
    }
}
