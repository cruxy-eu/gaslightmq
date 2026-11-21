use std::fmt;

use gaslightmq_clock::TimeSpan;

use crate::{LieKind, Stage, TimingLie};

/// Why a value cannot become a part of a lie.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LieError {
    /// No `what` has this name, where `what` is `lie`, `stage`, `combination class` or `aspect`.
    UnknownName { what: &'static str, name: String },
    /// The lie never acts at this stage.
    /// Silence always acts before forwarding, and a rewrite always acts after the response.
    StageNotAllowed { lie: LieKind, stage: Stage },
    /// The jitter of a timing lie is longer than its delay, so the shortest wait would be less than no time.
    JitterExceedsDelay { jitter: TimeSpan, delay: TimeSpan },
    /// The throttle of a timing lie is longer than [`TimingLie::MAX_THROTTLE`].
    ThrottleTooLong { throttle: TimeSpan },
    /// The target is a set of parts without a part, so the lie would change nothing.
    EmptyTarget,
}

impl fmt::Display for LieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownName { what, name } => write!(f, "no {what} is called {name:?}"),
            Self::StageNotAllowed { lie, stage } => write!(f, "{lie} cannot act {}", stage.phrase()),
            Self::JitterExceedsDelay { jitter, delay } => {
                write!(f, "a jitter of {jitter} is longer than the delay of {delay}")
            }
            Self::ThrottleTooLong { throttle } => write!(
                f,
                "a throttle of {throttle} is longer than {}, the longest that Kafka can tell a client",
                TimingLie::MAX_THROTTLE
            ),
            Self::EmptyTarget => f.write_str("a target needs at least one part"),
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
            (
                LieError::JitterExceedsDelay {
                    jitter: TimeSpan::from_millis(200),
                    delay: TimeSpan::ZERO,
                },
                "a jitter of 200ms is longer than the delay of 0s",
            ),
            (
                LieError::ThrottleTooLong {
                    throttle: TimeSpan::from_secs(3_000_000),
                },
                "a throttle of 50000m is longer than 2147483647ms, the longest that Kafka can tell a client",
            ),
            (LieError::EmptyTarget, "a target needs at least one part"),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
