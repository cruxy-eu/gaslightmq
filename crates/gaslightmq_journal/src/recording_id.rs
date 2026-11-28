//! The ID of a recording: its environment and the instant it started, which is the name of its directory too.
//!
//! The control plane and the UI name a recording by it, so it lives with the words of the journal, which compile to
//! WebAssembly, and not with the storage of recordings, which reaches the Kafka codec.

use std::fmt;
use std::str::FromStr;

use gaslightmq_clock::{UtcInstant, UtcInstantError};

use crate::{RecordedName, RecordedNameError};

/// Which recording this is: its environment, and the instant it started in UTC, to the millisecond.
///
/// An environment writes one recording at a time, so no two recordings share both.
/// The text form writes both, with a `.` between them, and the colons of the time as hyphens,
/// so that the text is the name of a directory on every system: `orders-sim.2026-11-21T14-02-11.527Z`.
/// A name holds no `.`, so the first one ends the name of the environment.
///
/// A recording outlives its environment: someone can delete the environment after the recording,
/// and make another one under its name. The ID therefore holds the name that the environment had,
/// and nobody looks it up.
///
/// With the `serde` feature, an ID is its text.
///
/// ```
/// use gaslightmq_clock::UtcInstant;
/// use gaslightmq_journal::prelude::*;
///
/// let environment: RecordedName = "orders-sim".parse().expect("a name");
/// let id = RecordingId::new(environment, UtcInstant::new(1_795_269_731_527).expect("an instant in 2026"));
/// assert_eq!(id.to_string(), "orders-sim.2026-11-21T14-02-11.527Z");
///
/// let found: RecordingId = "orders-sim.2026-11-21T14-02-11.527Z".parse()?;
/// assert_eq!(found, id);
/// assert_eq!(found.environment(), "orders-sim");
/// assert_eq!(found.started().to_string(), "2026-11-21T14:02:11.527Z");
/// # Ok::<(), RecordingIdError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordingId {
    environment: RecordedName,
    started: UtcInstant,
}

impl RecordingId {
    /// The recording of `environment` that started at `started`.
    pub const fn new(environment: RecordedName, started: UtcInstant) -> RecordingId {
        RecordingId { environment, started }
    }

    /// The name that the environment of the recording had.
    pub const fn environment(&self) -> &RecordedName {
        &self.environment
    }

    /// When the recording started, in UTC.
    pub const fn started(&self) -> UtcInstant {
        self.started
    }
}

/// Writes the environment and the instant, with the colons of the time as hyphens.
impl fmt::Display for RecordingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.", self.environment)?;
        write!(f, "{}", self.started.in_a_name())
    }
}

impl FromStr for RecordingId {
    type Err = RecordingIdError;

    fn from_str(text: &str) -> Result<RecordingId, RecordingIdError> {
        let Some((environment, started)) = text.split_once('.') else {
            return Err(RecordingIdError::NoStart { input: text.to_owned() });
        };
        let environment = environment
            .parse()
            .map_err(|error| RecordingIdError::InvalidEnvironment { error })?;
        let started = UtcInstant::from_a_name(started).map_err(|error| RecordingIdError::InvalidStart { error })?;
        Ok(RecordingId { environment, started })
    }
}

/// Why a text is no [`RecordingId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordingIdError {
    /// The text has no `.`, so nothing says when the recording started.
    #[error("{input:?} is no recording ID: the name of an environment, a `.` and the instant the recording started")]
    NoStart { input: String },
    /// What comes before the first `.` is no name of an environment.
    #[error("the environment of a recording ID: {error}")]
    InvalidEnvironment { error: RecordedNameError },
    /// What comes after the first `.` is no instant.
    #[error("the start of a recording ID: {error}")]
    InvalidStart { error: UtcInstantError },
}

/// Writes the text of the ID.
#[cfg(feature = "serde")]
impl serde::Serialize for RecordingId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Reads the text of an ID, through its checks.
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for RecordingId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_that_is_no_recording_id_says_which_part_is_wrong() {
        assert_eq!(
            "orders-sim".parse::<RecordingId>().unwrap_err().to_string(),
            "\"orders-sim\" is no recording ID: the name of an environment, a `.` and the instant the recording started"
        );
        assert_eq!(
            "Orders.2026-11-21T14-02-11.527Z"
                .parse::<RecordingId>()
                .unwrap_err()
                .to_string(),
            "the environment of a recording ID: recorded name \"Orders\" holds 'O' at position 0, \
             which is no lowercase letter, digit or hyphen"
        );
        assert_eq!(
            "orders-sim.2026-11-21T14:02:11.527Z"
                .parse::<RecordingId>()
                .unwrap_err()
                .to_string(),
            "the start of a recording ID: \"2026-11-21T14:02:11.527Z\" is no instant in UTC to the millisecond"
        );
    }
}
