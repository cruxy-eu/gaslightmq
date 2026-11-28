//! The recordings of the environments: starting and stopping one, listing them, and opening one.

use gaslightmq_clock::{Instant, UtcInstant};
use gaslightmq_environment::{EnvironmentName, RecordingMode};
use gaslightmq_journal::{JournalEntryId, RecordedName, RecordingId};
use serde::{Deserialize, Serialize};

use crate::{BackendKind, Operation};

/// Start a recording of an environment, which runs until someone stops it, its environment stops, or it stops by
/// itself. It answers with the ID of the recording: its environment, and the instant it started.
///
/// A start has no inverse, and leaves no revision: what is recorded has happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRecording {
    /// The environment, which writes one recording at a time.
    pub environment: EnvironmentName,
}

impl Operation for StartRecording {
    type Answer = RecordingId;
    type Error = StartRecordingError;
}

/// Stop the recording of an environment. It answers once the recording holds everything that its environment
/// handed over before the stop, and its end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopRecording {
    /// The environment.
    pub environment: EnvironmentName,
}

impl Operation for StopRecording {
    type Answer = RecordingEnd;
    type Error = StopRecordingError;
}

/// List every recording that the process keeps, of every environment, whether the environment runs or not:
/// by the environment, and then by the instant it started. A recording that runs is among them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRecordings {}

impl Operation for ListRecordings {
    type Answer = Vec<RecordingId>;
    type Error = ListRecordingsError;
}

/// Open a recording: what it is, how far it goes, and how it ended.
///
/// A recording that runs holds every entry that its environment handed over until the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRecording {
    /// The recording.
    pub recording: RecordingId,
}

impl Operation for OpenRecording {
    type Answer = RecordingStatus;
    type Error = OpenRecordingError;
}

/// A recording as the process keeps it: what it is, how many entries it holds, and its end once it ended in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingStatus {
    /// What the recording is, as it was written when it started.
    pub description: RecordingDescription,
    /// How many entries the recording holds: the sessions, the calls and the journal entries.
    pub entries: u64,
    /// The end of the recording, when it ended in order. A recording without an end still runs, or was cut short.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<RecordingEnd>,
}

/// What a recording is, as it was written when it started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RecordingDescription {
    /// The ID of the recording: its environment and the instant it started.
    pub recording: RecordingId,
    /// The instant of the clock of the environment when the recording started. Every entry tells its time on that
    /// clock, so this and the start of the [ID](Self::recording) say when an entry was.
    pub clock: Instant,
    /// How much of a call the recording keeps.
    pub mode: RecordingMode,
    /// What stood behind the proxy.
    pub backend: BackendKind,
    /// The version of GaslightMQ that wrote the recording.
    pub written_by: String,
    /// The names of the rules that were armed when the recording started, the highest priority first.
    pub armed: Vec<RecordedName>,
    /// The scenario that was active when the recording started, when one was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scenario: Option<RecordedScenario>,
    /// The ID of the last journal entry of the environment before the recording started, when there was one.
    /// The journal entries of the recording follow it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_after: Option<JournalEntryId>,
}

impl RecordingDescription {
    /// When the recording started, in UTC.
    pub const fn started(&self) -> UtcInstant {
        self.recording.started()
    }
}

/// The scenario that was active when a recording started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedScenario {
    /// The name that the scenario had.
    pub scenario: RecordedName,
    /// The step that the scenario was in, when it is a Sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<RecordedName>,
}

/// How a recording ended in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingEnd {
    /// How the recording ended.
    pub ending: RecordingEnding,
    /// When it ended, on the clock of the environment.
    pub at: Instant,
    /// How many entries the recording holds.
    pub entries: u64,
    /// How many of them are calls.
    pub calls: u64,
}

/// How a recording ended.
///
/// With `serde`, it is the name: `stopped`, `environment-stopped` or `fell-behind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecordingEnding {
    /// Someone stopped the recording.
    Stopped,
    /// The environment of the recording stopped.
    EnvironmentStopped,
    /// The recorder fell behind the traffic, and stopped the recording itself.
    /// The last entry of the recording is the journal entry that says so.
    FellBehind,
}

/// Why an environment did not [start a recording](StartRecording).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StartRecordingError::AlreadyRecording {
///     environment: "orders-sim".parse().expect("an environment name"),
///     recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("a recording ID"),
/// };
/// assert_eq!(
///     error.to_string(),
///     "environment \"orders-sim\" records already, into orders-sim.2026-11-21T14-02-11.527Z"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StartRecordingError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The environment records already. Whoever wants another recording stops this one first.
    #[error("environment {:?} records already, into {recording}", .environment.as_str())]
    AlreadyRecording {
        environment: EnvironmentName,
        recording: RecordingId,
    },
    /// The storage did not keep the recording, or its recorder did not start. `reason` says what stood in the way.
    #[error("environment {:?} did not start a recording: {reason}", .environment.as_str())]
    StorageFailed {
        environment: EnvironmentName,
        reason: String,
    },
}

/// Why an environment did not [stop its recording](StopRecording) in order.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = StopRecordingError::NotRecording { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" records nothing");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StopRecordingError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The environment records nothing: nobody started a recording, someone stopped it, or it stopped by itself.
    #[error("environment {:?} records nothing", .environment.as_str())]
    NotRecording { environment: EnvironmentName },
    /// The recording stopped, and its recorder could not write what waited, or its end.
    /// It holds what its last commit covers, and opens as a recording that was cut short.
    #[error("recording {recording} stopped without its end: {reason}")]
    CutShort { recording: RecordingId, reason: String },
}

/// Why the process did not [list its recordings](ListRecordings).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = ListRecordingsError::StorageUnavailable { reason: "permission denied".to_owned() };
/// assert_eq!(error.to_string(), "the recordings were not listed: permission denied");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ListRecordingsError {
    /// The storage of the recordings did not answer, and the recordings can be listed again.
    /// `reason` says what stood in the way.
    #[error("the recordings were not listed: {reason}")]
    StorageUnavailable { reason: String },
}

/// Why the process did not [open a recording](OpenRecording).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = OpenRecordingError::NotFound { recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("an ID") };
/// assert_eq!(error.to_string(), "no recording orders-sim.2026-11-21T14-02-11.527Z is there");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum OpenRecordingError {
    /// The process keeps no recording with that ID.
    #[error("no recording {recording} is there")]
    NotFound { recording: RecordingId },
    /// What is there is no recording that this build of the process reads: a later version wrote it, or it is no
    /// recording. It will not open, however often someone asks. `reason` says which.
    #[error("recording {recording} does not open: {reason}")]
    Unreadable { recording: RecordingId, reason: String },
    /// The storage did not answer, and the recording can be opened again.
    #[error("recording {recording} was not read: {reason}")]
    StorageUnavailable { recording: RecordingId, reason: String },
}
