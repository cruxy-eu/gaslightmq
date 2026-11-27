//! What a recording is: written once, when the recording starts.

use bytes::Bytes;
use gaslightmq_clock::Instant;
use gaslightmq_environment::RecordingMode;
use gaslightmq_journal::{JournalEntryId, RecordedName};

use crate::encoding::{self, EncodingVersion, Framed, Kind, Written, read, v1, write};
use crate::{MessageError, RecordingId, UtcInstant};

/// What stood behind the proxy while a recording ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordedBackend {
    /// A real cluster.
    Upstream,
    /// The simulated broker.
    Simulated,
}

/// The scenario that was active when a recording started.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActiveScenario {
    /// The name that the scenario had.
    pub scenario: RecordedName,
    /// The step that the scenario was in, when it is a Sequence.
    pub step: Option<RecordedName>,
}

/// What a recording is. Whoever starts the recording writes it, once.
///
/// The journal entries of a recording begin where the recording begins.
/// [`armed`](Self::armed) and [`scenario`](Self::scenario) hold what the earlier entries would have said,
/// so a recording shows which rules were armed when its first call arrived.
/// They hold names, as an entry of the journal does.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_recording::prelude::*;
///
/// let description = Description {
///     environment: "orders-sim".parse().expect("a name"),
///     started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
///     clock: Instant::ZERO + Duration::from_secs(42),
///     mode: RecordingMode::MetadataOnly,
///     backend: RecordedBackend::Upstream,
///     written_by: "0.1.0".to_owned(),
///     armed: vec!["leader-flap".parse().expect("a name")],
///     scenario: None,
///     journal_after: None,
/// };
///
/// assert_eq!(description.id().to_string(), "orders-sim.2026-11-21T14-02-11.527Z");
///
/// // A layout keeps the description as a message, and reads it back.
/// let read = Description::decode(EncodingVersion::CURRENT, &description.encode())?;
/// assert_eq!(read, description);
/// # Ok::<(), MessageError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// The name that the environment had.
    pub environment: RecordedName,
    /// The instant the recording started, in UTC and to the millisecond.
    /// Whoever starts the recording hands it in: this crate reads no clock.
    pub started: UtcInstant,
    /// The instant of the clock of the environment at that moment.
    /// Every entry tells its time on that clock, so this pair says when an entry was.
    pub clock: Instant,
    /// How much of a call the recording keeps.
    pub mode: RecordingMode,
    /// What stood behind the proxy.
    pub backend: RecordedBackend,
    /// The version of GaslightMQ that wrote the recording.
    pub written_by: String,
    /// The names of the rules that were armed when the recording started, the highest priority first.
    pub armed: Vec<RecordedName>,
    /// The scenario that was active when the recording started, when one was.
    pub scenario: Option<ActiveScenario>,
    /// The ID of the last journal entry before the recording started, when there was one.
    pub journal_after: Option<JournalEntryId>,
}

impl Description {
    /// The ID of the recording: its environment and the instant it started.
    pub fn id(&self) -> RecordingId {
        RecordingId::new(self.environment.clone(), self.started)
    }

    /// The description as a message behind its head: what a layout keeps of it.
    pub fn encode(&self) -> Bytes {
        Framed::archive(Kind::Description, &write::description(self)).into_bytes()
    }

    /// Reads the description that [`encode`](Self::encode) wrote, in the version that its recording says.
    ///
    /// # Errors
    ///
    /// [`MessageError::Damaged`] when the bytes are no description.
    pub fn decode(version: EncodingVersion, bytes: &[u8]) -> Result<Description, MessageError> {
        let damaged = |reason| MessageError::Damaged { reason };
        let message = encoding::aligned_message(Kind::Description, bytes).map_err(damaged)?;
        match version.written() {
            Written::V1 => {
                let stored = rkyv::access::<v1::ArchivedDescription, rkyv::rancor::Error>(&message)
                    .map_err(|error| damaged(error.to_string()))?;
                read::description(stored).map_err(damaged)
            }
        }
    }
}
