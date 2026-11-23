use std::fmt;
use std::str::FromStr;

use crate::LieKind;

/// Where in a call a lie acts.
///
/// ```text
/// receive ─► [before forwarding] ─► backend ─► [after the response] ─► send
/// ```
///
/// The stage changes what really happens in the cluster.
/// A refusal before forwarding writes nothing.
/// A refusal after the response tells the client that a Produce failed, though the backend wrote the records.
///
/// A [`LieKind`] says at which stages it can act, and at which one it acts by default.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let stage: Stage = "after".parse()?;
/// assert_eq!(stage, Stage::AfterResponse);
/// assert_eq!(stage.to_string(), "after");
/// assert_eq!(stage.phrase(), "after the response");
/// # Ok::<(), StageError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    /// The backend never sees the request, or sees it late.
    BeforeForwarding,
    /// The backend answers truthfully, and the lie changes the answer before the client sees it.
    /// A lie at this stage can depend on the truth.
    AfterResponse,
}

impl Stage {
    /// Both stages, in the order a call passes them.
    pub const ALL: [Self; 2] = [Self::BeforeForwarding, Self::AfterResponse];

    /// The name a file uses: `before` or `after`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::BeforeForwarding => "before",
            Self::AfterResponse => "after",
        }
    }

    /// The stage as the words of a sentence: `before forwarding` or `after the response`.
    pub const fn phrase(self) -> &'static str {
        match self {
            Self::BeforeForwarding => "before forwarding",
            Self::AfterResponse => "after the response",
        }
    }
}

/// Writes the name a file uses: `before` or `after`.
impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name a file uses, as [`Display`](fmt::Display) writes it.
impl FromStr for Stage {
    type Err = StageError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|stage| stage.name() == name)
            .ok_or_else(|| StageError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not a [`Stage`], or why a lie cannot act at one.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let error = "during".parse::<Stage>().unwrap_err();
/// assert_eq!(error, StageError::Unknown { name: "during".to_owned() });
/// assert_eq!(error.to_string(), "no stage is called \"during\"");
///
/// let error = LieKind::Silence.stage_for(Some(Stage::AfterResponse)).unwrap_err();
/// assert_eq!(error.to_string(), "silence cannot act after the response");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StageError {
    /// No stage has this name.
    #[error("no stage is called {name:?}")]
    Unknown { name: String },
    /// The stage exists, and a lie of this kind never acts at it.
    /// Silence always acts before forwarding, and a rewrite always acts after the response.
    #[error("{kind} cannot act {}", .stage.phrase())]
    NotAllowed { kind: LieKind, stage: Stage },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_passes_the_stages_in_order() {
        assert_eq!(Stage::ALL, [Stage::BeforeForwarding, Stage::AfterResponse]);
        assert!(Stage::BeforeForwarding < Stage::AfterResponse);
    }

    #[test]
    fn a_stage_has_a_name_for_files_and_a_phrase_for_sentences() {
        assert_eq!(Stage::BeforeForwarding.name(), "before");
        assert_eq!(Stage::AfterResponse.name(), "after");
        assert_eq!(Stage::BeforeForwarding.phrase(), "before forwarding");
        assert_eq!(Stage::AfterResponse.phrase(), "after the response");
        for stage in Stage::ALL {
            assert!(stage.phrase().starts_with(stage.name()));
        }
    }

    #[test]
    fn a_stage_reads_back_from_the_name_it_writes() {
        for stage in Stage::ALL {
            assert_eq!(stage.to_string().parse(), Ok(stage));
        }
        assert_eq!(format!("[{:>8}]", Stage::AfterResponse), "[   after]");
    }

    #[test]
    fn a_name_that_no_stage_has_is_an_error() {
        for name in ["Before", "before forwarding", "during", ""] {
            assert_eq!(
                name.parse::<Stage>(),
                Err(StageError::Unknown { name: name.to_owned() }),
            );
        }
    }

    #[test]
    fn a_stage_error_says_what_is_wrong() {
        let cases = [
            (
                StageError::Unknown { name: "during".into() },
                "no stage is called \"during\"",
            ),
            (
                StageError::NotAllowed {
                    kind: LieKind::Silence,
                    stage: Stage::AfterResponse,
                },
                "silence cannot act after the response",
            ),
            (
                StageError::NotAllowed {
                    kind: LieKind::Topology,
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
