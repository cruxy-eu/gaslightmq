use std::fmt;

use gaslightmq_lie::prelude::*;

/// Why a lie cannot become a [`Claim`](crate::Claim).
///
/// No definition of today's `gaslightmq_lie` causes either error.
/// They stand for a definition that breaks its own word,
/// so that a mistake in a new kind of lie is an error here and never a lie that combines wrongly.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CombineError {
    /// The lie is a rewrite, and its definition names no aspect.
    /// Rewrites conflict per aspect, so nothing says what this one conflicts with.
    NoAspect { kind: LieKind },
    /// The lie is a modifier, and its definition is not a timing lie.
    /// Nothing says how long the call waits.
    NoTiming { kind: LieKind },
}

impl fmt::Display for CombineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAspect { kind } => write!(f, "a {kind} lie rewrites, and its definition names no aspect"),
            Self::NoTiming { kind } => write!(f, "a {kind} lie is a modifier, and its definition holds no timing"),
        }
    }
}

impl std::error::Error for CombineError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_names_what_was_wrong() {
        let cases = [
            (
                CombineError::NoAspect {
                    kind: LieKind::Progress,
                },
                "a progress lie rewrites, and its definition names no aspect",
            ),
            (
                CombineError::NoTiming { kind: LieKind::Timing },
                "a timing lie is a modifier, and its definition holds no timing",
            ),
        ];

        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
    }
}
