use std::fmt;

use crate::EventKind;

/// Why a value cannot become an event, or a part of one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventError {
    /// No event has this name.
    UnknownEvent { name: String },
    /// The event needs a `subject`, such as a partition or a group.
    /// The definition names none, and the call that fired it offers none.
    MissingSubject {
        event: EventKind,
        subject: &'static str,
    },
    /// The count is zero, or above the highest `what`.
    CountOutOfRange {
        what: &'static str,
        value: u64,
        max: u64,
    },
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownEvent { name } => write!(f, "no event is called {name:?}"),
            Self::MissingSubject { event, subject } => {
                write!(f, "{event} needs a {subject}, and none is named")
            }
            Self::CountOutOfRange { what, value, max } => {
                write!(f, "{what} {value} is not between 1 and {max}")
            }
        }
    }
}

impl std::error::Error for EventError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                EventError::UnknownEvent {
                    name: "leader-move".into(),
                },
                "no event is called \"leader-move\"",
            ),
            (
                EventError::MissingSubject {
                    event: EventKind::Rebalance,
                    subject: "group",
                },
                "rebalance needs a group, and none is named",
            ),
            (
                EventError::CountOutOfRange {
                    what: "partition count",
                    value: 0,
                    max: 2_147_483_647,
                },
                "partition count 0 is not between 1 and 2147483647",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
