//! What the number types share: a range check, the conversions and the text form.

/// Why a text is not a decimal number between zero and a maximum.
///
/// Every number type has an error of its own, with one variant for each of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Flaw {
    Empty,
    NotANumber,
    /// The number, which stops at [`i64::MIN`] for a text with more digits than that.
    Negative(i64),
    /// The number, which stops at [`u64::MAX`] for a text with more digits than that.
    TooLarge(u64),
}

/// Read a decimal number between zero and `max`.
pub(crate) fn parse<T>(text: &str, max: T) -> Result<T, Flaw>
where
    T: TryFrom<u64> + Into<u64>,
{
    let max: u64 = max.into();
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    if text.is_empty() {
        return Err(Flaw::Empty);
    }
    if digits(text) {
        // A text of digits that does not fit `u64` is above every maximum.
        let value = text.parse::<u64>().unwrap_or(u64::MAX);
        return match T::try_from(value) {
            Ok(number) if value <= max => Ok(number),
            _ => Err(Flaw::TooLarge(value)),
        };
    }
    match text.strip_prefix('-') {
        Some(rest) if digits(rest) => Err(Flaw::Negative(text.parse::<i64>().unwrap_or(i64::MIN))),
        _ => Err(Flaw::NotANumber),
    }
}

/// Define a number type that holds `0..=<wire>::MAX`, and the error that says why a value is not one.
///
/// `value` is the unsigned type the number is read as.
/// `wire` is the signed type Kafka writes it as.
/// `small` is the widest unsigned type of which every value fits.
macro_rules! number_value {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal, value: $value:ty, wire: $wire:ty, small: $small:ty
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($value);

        impl $name {
            const MAX_VALUE: $value = <$wire>::MAX as $value;

            #[doc = concat!("Check that `value` is at most [`", stringify!($wire), "::MAX`], and keep it.")]
            ///
            /// # Errors
            ///
            #[doc = concat!("[`", stringify!($error), "::TooLarge`] when `value` is above that.")]
            pub fn new(value: $value) -> Result<Self, $error> {
                if value > Self::MAX_VALUE {
                    return Err($error::TooLarge {
                        value: value.into(),
                        max: Self::MAX_VALUE.into(),
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("The ", $what, " as a plain number.")]
            pub fn value(self) -> $value {
                self.0
            }
        }

        #[doc = concat!("Why a number or a text is not a [`", stringify!($name), "`].")]
        #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
        pub enum $error {
            /// The text has no characters.
            #[error("{} is empty", $what)]
            Empty,
            /// The text is not a decimal number.
            #[error("{} is not a number: {input:?}", $what)]
            NotANumber { input: String },
            /// The number is below zero.
            /// `value` stops at [`i64::MIN`] for a text with more digits than that.
            #[error("{} cannot be negative: {value}", $what)]
            Negative { value: i64 },
            #[doc = concat!("The number is above the highest ", $what, ".")]
            /// `value` stops at [`u64::MAX`] for a text with more digits than that.
            #[error("{} {value} exceeds the maximum of {max}", $what)]
            TooLarge { value: u64, max: u64 },
        }

        impl From<$small> for $name {
            fn from(value: $small) -> Self {
                Self(value.into())
            }
        }

        impl TryFrom<$value> for $name {
            type Error = $error;

            fn try_from(value: $value) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        #[doc = concat!("Reads the type Kafka writes a ", $what, " as, and rejects every negative number.")]
        impl TryFrom<$wire> for $name {
            type Error = $error;

            fn try_from(raw: $wire) -> Result<Self, Self::Error> {
                <$value>::try_from(raw)
                    .map(Self)
                    .map_err(|_| $error::Negative { value: raw.into() })
            }
        }

        impl From<$name> for $value {
            fn from(number: $name) -> Self {
                number.0
            }
        }

        #[doc = concat!("Gives the type Kafka writes a ", $what, " as.")]
        impl From<$name> for $wire {
            fn from(number: $name) -> Self {
                // The value is at most `<wire>::MAX`, so the sign bit is clear and the cast keeps it.
                number.0.cast_signed()
            }
        }

        #[doc = concat!("Reads a ", $what, " from decimal digits, as [`Display`](std::fmt::Display) writes it.")]
        impl std::str::FromStr for $name {
            type Err = $error;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                use $crate::number::Flaw;

                $crate::number::parse(text, Self::MAX_VALUE)
                    .map(Self)
                    .map_err(|flaw| match flaw {
                        Flaw::Empty => $error::Empty,
                        Flaw::NotANumber => $error::NotANumber { input: text.to_owned() },
                        Flaw::Negative(value) => $error::Negative { value },
                        Flaw::TooLarge(value) => $error::TooLarge {
                            value,
                            max: Self::MAX_VALUE.into(),
                        },
                    })
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = <$value as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

/// Add the conversions for a number that Kafka writes as `-1` when there is none.
macro_rules! number_sentinel {
    ($name:ident, $error:ident, $what:literal, wire: $wire:ty, none: $none:literal) => {
        impl $name {
            #[doc = concat!("Read the number Kafka writes, in which `-1` means ", $none, ".")]
            ///
            /// Every other negative number is an error.
            ///
            /// # Errors
            ///
            #[doc = concat!("[`", stringify!($error), "::Negative`] when `raw` is below `-1`.")]
            pub fn from_wire(raw: $wire) -> Result<Option<Self>, $error> {
                if raw == -1 {
                    Ok(None)
                } else {
                    Self::try_from(raw).map(Some)
                }
            }

            #[doc = concat!("The number Kafka writes: the ", $what, ", or `-1` for `None`.")]
            pub fn to_wire(value: Option<Self>) -> $wire {
                value.map_or(-1, <$wire>::from)
            }
        }
    };
}

pub(crate) use {number_sentinel, number_value};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_digits_up_to_the_maximum() {
        assert_eq!(parse("0", 9_u64), Ok(0));
        assert_eq!(parse("9", 9_u64), Ok(9));
        assert_eq!(parse("007", 9_u64), Ok(7));
    }

    #[test]
    fn a_number_above_the_maximum_is_too_large() {
        assert_eq!(parse("10", 9_u64), Err(Flaw::TooLarge(10)));
    }

    #[test]
    fn digits_that_do_not_fit_u64_are_too_large() {
        assert_eq!(parse("99999999999999999999999", 9_u64), Err(Flaw::TooLarge(u64::MAX)));
    }

    #[test]
    fn a_negative_number_is_negative() {
        assert_eq!(parse("-1", 9_u64), Err(Flaw::Negative(-1)));
        assert_eq!(parse("-99999999999999999999999", 9_u64), Err(Flaw::Negative(i64::MIN)));
    }

    #[test]
    fn anything_else_is_not_a_number() {
        for text in ["x", "+3", " 3", "3 ", "3.0", "-", "--1", "0x10", "٣"] {
            assert_eq!(parse(text, 9_u64), Err(Flaw::NotANumber), "{text:?}");
        }
    }

    #[test]
    fn no_text_is_empty() {
        assert_eq!(parse("", 9_u64), Err(Flaw::Empty));
    }

    #[test]
    fn an_error_names_the_value_and_what_is_wrong_with_it() {
        use crate::{OffsetError, ProducerEpochError};

        let cases = [
            (OffsetError::Empty.to_string(), "offset is empty"),
            (
                OffsetError::NotANumber { input: "x".into() }.to_string(),
                "offset is not a number: \"x\"",
            ),
            (
                OffsetError::Negative { value: -2 }.to_string(),
                "offset cannot be negative: -2",
            ),
            (
                ProducerEpochError::TooLarge {
                    value: 40_000,
                    max: 32_767,
                }
                .to_string(),
                "producer epoch 40000 exceeds the maximum of 32767",
            ),
        ];

        for (message, expected) in cases {
            assert_eq!(message, expected);
        }
    }
}
