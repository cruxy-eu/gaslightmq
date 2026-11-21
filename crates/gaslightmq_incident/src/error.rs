use std::fmt;

use crate::IncidentKind;

/// Why a value cannot become an incident, or a part of one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IncidentError {
    /// No incident has this name.
    UnknownIncident { name: String },
    /// The incident needs a `subject`, such as a partition or a group.
    /// The definition names none, and the call that fired it offers none.
    MissingSubject {
        incident: IncidentKind,
        subject: &'static str,
    },
    /// The count is zero, or above the highest `what`.
    CountOutOfRange { what: &'static str, value: u64, max: u64 },
}

impl fmt::Display for IncidentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownIncident { name } => write!(f, "no incident is called {name:?}"),
            Self::MissingSubject { incident, subject } => {
                write!(f, "{incident} needs a {subject}, and none is named")
            }
            Self::CountOutOfRange { what, value, max } => {
                write!(f, "{what} {value} is not between 1 and {max}")
            }
        }
    }
}

impl std::error::Error for IncidentError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                IncidentError::UnknownIncident {
                    name: "leader-move".into(),
                },
                "no incident is called \"leader-move\"",
            ),
            (
                IncidentError::MissingSubject {
                    incident: IncidentKind::Rebalance,
                    subject: "group",
                },
                "rebalance needs a group, and none is named",
            ),
            (
                IncidentError::CountOutOfRange {
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
