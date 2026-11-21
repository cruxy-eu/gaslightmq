//! What the number types share: a range check, the conversions and the text form.

use crate::KernelError;

/// Read a decimal number between zero and `max`.
pub(crate) fn parse<T>(what: &'static str, text: &str, max: T) -> Result<T, KernelError>
where
    T: TryFrom<u64> + Into<u64>,
{
    let max: u64 = max.into();
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    if text.is_empty() {
        return Err(KernelError::Empty { what });
    }
    if digits(text) {
        // A text of digits that does not fit `u64` is above every maximum.
        let value = text.parse::<u64>().unwrap_or(u64::MAX);
        return match T::try_from(value) {
            Ok(number) if value <= max => Ok(number),
            _ => Err(KernelError::TooLarge { what, value, max }),
        };
    }
    match text.strip_prefix('-') {
        Some(rest) if digits(rest) => Err(KernelError::Negative {
            what,
            value: text.parse::<i64>().unwrap_or(i64::MIN),
        }),
        _ => Err(KernelError::NotANumber {
            what,
            input: text.to_owned(),
        }),
    }
}

/// Define a number type that holds `0..=<wire>::MAX`.
///
/// `value` is the unsigned type the number is read as.
/// `wire` is the signed type Kafka writes it as.
/// `small` is the widest unsigned type of which every value fits.
macro_rules! number_value {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal, value: $value:ty, wire: $wire:ty, small: $small:ty
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
            /// [`KernelError::TooLarge`](crate::KernelError::TooLarge) when `value` is above that.
            pub fn new(value: $value) -> Result<Self, $crate::KernelError> {
                if value > Self::MAX_VALUE {
                    return Err($crate::KernelError::TooLarge {
                        what: $what,
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

        impl From<$small> for $name {
            fn from(value: $small) -> Self {
                Self(value.into())
            }
        }

        impl TryFrom<$value> for $name {
            type Error = $crate::KernelError;

            fn try_from(value: $value) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        #[doc = concat!("Reads the type Kafka writes a ", $what, " as, and rejects every negative number.")]
        impl TryFrom<$wire> for $name {
            type Error = $crate::KernelError;

            fn try_from(raw: $wire) -> Result<Self, Self::Error> {
                <$value>::try_from(raw)
                    .map(Self)
                    .map_err(|_| $crate::KernelError::Negative { what: $what, value: raw.into() })
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
            type Err = $crate::KernelError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                $crate::number::parse($what, text, Self::MAX_VALUE).map(Self)
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
    ($name:ident, $what:literal, wire: $wire:ty, none: $none:literal) => {
        impl $name {
            #[doc = concat!("Read the number Kafka writes, in which `-1` means ", $none, ".")]
            ///
            /// Every other negative number is an error.
            ///
            /// # Errors
            ///
            /// [`KernelError::Negative`](crate::KernelError::Negative) when `raw` is below `-1`.
            pub fn from_wire(raw: $wire) -> Result<Option<Self>, $crate::KernelError> {
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
        assert_eq!(parse("offset", "0", 9_u64), Ok(0));
        assert_eq!(parse("offset", "9", 9_u64), Ok(9));
        assert_eq!(parse("offset", "007", 9_u64), Ok(7));
    }

    #[test]
    fn a_number_above_the_maximum_is_too_large() {
        assert_eq!(
            parse("offset", "10", 9_u64),
            Err(KernelError::TooLarge {
                what: "offset",
                value: 10,
                max: 9
            })
        );
    }

    #[test]
    fn digits_that_do_not_fit_u64_are_too_large() {
        assert_eq!(
            parse("offset", "99999999999999999999999", 9_u64),
            Err(KernelError::TooLarge {
                what: "offset",
                value: u64::MAX,
                max: 9
            })
        );
    }

    #[test]
    fn a_negative_number_is_negative() {
        assert_eq!(
            parse("offset", "-1", 9_u64),
            Err(KernelError::Negative {
                what: "offset",
                value: -1
            })
        );
        assert_eq!(
            parse("offset", "-99999999999999999999999", 9_u64),
            Err(KernelError::Negative {
                what: "offset",
                value: i64::MIN
            })
        );
    }

    #[test]
    fn anything_else_is_not_a_number() {
        for text in ["x", "+3", " 3", "3 ", "3.0", "-", "--1", "0x10", "٣"] {
            assert_eq!(
                parse("offset", text, 9_u64),
                Err(KernelError::NotANumber {
                    what: "offset",
                    input: text.to_owned()
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn no_text_is_empty() {
        assert_eq!(parse("offset", "", 9_u64), Err(KernelError::Empty { what: "offset" }));
    }
}
