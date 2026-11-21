use std::fmt;

use gaslightmq_clock::TimeSpan;

/// Why a value cannot become a trigger, a lifetime, or a part of one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TriggerError {
    /// The text is not a percentage such as `40%`.
    NotAPercentage { text: String },
    /// The chance has more than four decimals, and a chance is a whole number of parts per million.
    ChanceTooFine { text: String },
    /// The chance is above `100%`.
    ChanceOutOfRange { chance: String },
    /// A die never rolls a whole million.
    RollOutOfRange { ppm: u32 },
    /// A count is at least one, and the first match has number one.
    Zero { what: &'static str },
    /// The window closes before it opens, or at the moment it opens.
    EmptyWindow { from: TimeSpan, until: TimeSpan },
    /// A key is at least one character.
    EmptyKey { what: &'static str },
    /// Counters are per rule or per session.
    UnknownPer { text: String },
    /// The text is not a lifetime.
    InvalidLifetime { text: String },
}

impl fmt::Display for TriggerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAPercentage { text } => {
                write!(f, "`{text}` is not a percentage such as `40%`")
            }
            Self::ChanceTooFine { text } => {
                write!(f, "chance `{text}` has more than four decimals")
            }
            Self::ChanceOutOfRange { chance } => {
                write!(f, "chance `{chance}` is not between 0% and 100%")
            }
            Self::RollOutOfRange { ppm } => {
                write!(f, "a roll of {ppm} parts per million is not below one million")
            }
            Self::Zero { what } => write!(f, "a {what} is at least one"),
            Self::EmptyWindow { from, until } => {
                write!(f, "a window between {from} and {until} never opens")
            }
            Self::EmptyKey { what } => write!(f, "a {what} is not empty"),
            Self::UnknownPer { text } => {
                write!(f, "counters are per `rule` or per `session`, not per `{text}`")
            }
            Self::InvalidLifetime { text } => write!(
                f,
                "`{text}` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
                 or a count such as `100 matches` or `3 firings`"
            ),
        }
    }
}

impl std::error::Error for TriggerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                TriggerError::NotAPercentage { text: "40".into() },
                "`40` is not a percentage such as `40%`",
            ),
            (
                TriggerError::ChanceTooFine {
                    text: "0.00001%".into(),
                },
                "chance `0.00001%` has more than four decimals",
            ),
            (
                TriggerError::ChanceOutOfRange { chance: "140%".into() },
                "chance `140%` is not between 0% and 100%",
            ),
            (
                TriggerError::RollOutOfRange { ppm: 1_000_000 },
                "a roll of 1000000 parts per million is not below one million",
            ),
            (TriggerError::Zero { what: "count" }, "a count is at least one"),
            (
                TriggerError::EmptyWindow {
                    from: TimeSpan::from_secs(40),
                    until: TimeSpan::from_secs(10),
                },
                "a window between 40s and 10s never opens",
            ),
            (TriggerError::EmptyKey { what: "dice key" }, "a dice key is not empty"),
            (
                TriggerError::UnknownPer {
                    text: "connection".into(),
                },
                "counters are per `rule` or per `session`, not per `connection`",
            ),
            (
                TriggerError::InvalidLifetime { text: "forever".into() },
                "`forever` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
                 or a count such as `100 matches` or `3 firings`",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn the_error_is_a_standard_error() {
        fn boxed() -> Result<(), Box<dyn std::error::Error>> {
            Err(TriggerError::Zero { what: "count" })?
        }
        assert!(boxed().is_err());
    }
}
