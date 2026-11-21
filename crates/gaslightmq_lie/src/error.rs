use std::fmt;

use crate::{LieKind, Stage};

/// Why a value cannot become a part of a lie.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LieError {
    /// No `what` has this name, where `what` is `lie`, `stage`, `combination class` or `aspect`.
    UnknownName { what: &'static str, name: String },
    /// The lie never acts at this stage.
    /// Silence always acts before forwarding, and a rewrite always acts after the response.
    StageNotAllowed { lie: LieKind, stage: Stage },
}

impl fmt::Display for LieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownName { what, name } => write!(f, "no {what} is called {name:?}"),
            Self::StageNotAllowed { lie, stage } => write!(f, "{lie} cannot act {}", stage.phrase()),
        }
    }
}

impl std::error::Error for LieError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                LieError::UnknownName {
                    what: "lie",
                    name: "hangup".into(),
                },
                "no lie is called \"hangup\"",
            ),
            (
                LieError::UnknownName {
                    what: "stage",
                    name: "during".into(),
                },
                "no stage is called \"during\"",
            ),
            (
                LieError::StageNotAllowed {
                    lie: LieKind::Silence,
                    stage: Stage::AfterResponse,
                },
                "silence cannot act after the response",
            ),
            (
                LieError::StageNotAllowed {
                    lie: LieKind::Topology,
                    stage: Stage::BeforeForwarding,
                },
                "topology cannot act before forwarding",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
