use std::fmt;

/// Why a raw value cannot become a value object of this crate.
///
/// `what` names the value object, such as `topic name` or `offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KernelError {
    /// The text has no characters.
    Empty { what: &'static str },
    /// The text has more bytes than `what` allows.
    TooLong { what: &'static str, len: usize, max: usize },
    /// The text has a character that `what` does not allow.
    /// `position` counts characters from zero.
    InvalidCharacter {
        what: &'static str,
        character: char,
        position: usize,
    },
    /// Kafka reserves the value, such as the topic name `..` or the topic ID of all zeros.
    Reserved { what: &'static str, value: String },
    /// The number is below zero.
    /// `value` stops at [`i64::MIN`] for a text with more digits than that.
    Negative { what: &'static str, value: i64 },
    /// The number is above the highest `what`.
    /// `value` stops at [`u64::MAX`] for a text with more digits than that.
    TooLarge { what: &'static str, value: u64, max: u64 },
    /// The number is below the lowest `what`.
    /// `value` stops at [`i64::MIN`] for a text with more digits than that.
    TooSmall { what: &'static str, value: i64, min: i64 },
    /// The text is not a decimal number.
    NotANumber { what: &'static str, input: String },
    /// The text is not a name that Kafka gives a `what`, and not a number either.
    UnknownName { what: &'static str, input: String },
    /// The text does not have the form of `what`, such as `topic/partition`.
    Malformed {
        what: &'static str,
        input: String,
        expected: &'static str,
    },
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { what } => write!(f, "{what} is empty"),
            Self::TooLong { what, len, max } => write!(f, "{what} of {len} bytes exceeds the limit of {max}"),
            Self::InvalidCharacter {
                what,
                character,
                position,
            } => {
                write!(
                    f,
                    "{what} has the invalid character {character:?} at position {position}"
                )
            }
            Self::Reserved { what, value } => write!(f, "{what} cannot be {value:?}"),
            Self::Negative { what, value } => write!(f, "{what} cannot be negative: {value}"),
            Self::TooLarge { what, value, max } => write!(f, "{what} {value} exceeds the maximum of {max}"),
            Self::TooSmall { what, value, min } => write!(f, "{what} {value} is below the minimum of {min}"),
            Self::NotANumber { what, input } => write!(f, "{what} is not a number: {input:?}"),
            Self::UnknownName { what, input } => write!(f, "unknown {what}: {input:?}"),
            Self::Malformed { what, input, expected } => {
                write!(f, "{what} {input:?} does not have the form {expected}")
            }
        }
    }
}

impl std::error::Error for KernelError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (KernelError::Empty { what: "topic name" }, "topic name is empty"),
            (
                KernelError::TooLong {
                    what: "topic name",
                    len: 250,
                    max: 249,
                },
                "topic name of 250 bytes exceeds the limit of 249",
            ),
            (
                KernelError::InvalidCharacter {
                    what: "topic name",
                    character: '/',
                    position: 6,
                },
                "topic name has the invalid character '/' at position 6",
            ),
            (
                KernelError::Reserved {
                    what: "topic name",
                    value: "..".into(),
                },
                "topic name cannot be \"..\"",
            ),
            (
                KernelError::Negative {
                    what: "offset",
                    value: -2,
                },
                "offset cannot be negative: -2",
            ),
            (
                KernelError::TooLarge {
                    what: "producer epoch",
                    value: 40_000,
                    max: 32_767,
                },
                "producer epoch 40000 exceeds the maximum of 32767",
            ),
            (
                KernelError::TooSmall {
                    what: "API key",
                    value: -40_000,
                    min: -32_768,
                },
                "API key -40000 is below the minimum of -32768",
            ),
            (
                KernelError::NotANumber {
                    what: "offset",
                    input: "x".into(),
                },
                "offset is not a number: \"x\"",
            ),
            (
                KernelError::UnknownName {
                    what: "API key",
                    input: "Prodcue".into(),
                },
                "unknown API key: \"Prodcue\"",
            ),
            (
                KernelError::Malformed {
                    what: "topic partition",
                    input: "orders".into(),
                    expected: "topic/partition",
                },
                "topic partition \"orders\" does not have the form topic/partition",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
