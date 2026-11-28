//! The recordings of the environments: starting and stopping one, listing them, opening one, and querying one.

use gaslightmq_call::Call;
use gaslightmq_clock::{Instant, UtcInstant};
use gaslightmq_environment::{EnvironmentName, RecordingMode};
use gaslightmq_field::{Field, FieldValue};
use gaslightmq_journal::{JournalEntryId, RecordedName, RecordingId};
use gaslightmq_selector::{Conditions, Glob, Selector, SelectorRef};
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

/// Query the calls of a recording with a selector, saved or a draft that nobody saved yet.
///
/// The answer holds how many calls of the whole recording the selector matches, and the calls themselves, in the
/// order of the recording: those behind the first `skip` that it matches, as many as one answer of the process holds.
/// Whoever wants the calls behind them asks again, and skips those it has.
/// The engine reads only the stretches of the recording that can hold a call that the selector matches, and decides
/// every call there as a rule would. A query changes nothing: no file, no rule and no live state.
///
/// - **`selector`** is a selector of the workspace by its name, or inline conditions: a filter that someone built.
/// - **`draft`** is a selector definition that nobody saved yet. It stands in the workspace for the selector of its
///   name while the query resolves `selector`: a query that names the draft tests it against the recording, and one
///   that names a selector that includes the draft tests what the draft does to that one.
///
/// A recording that runs holds every entry that its environment handed over until the request.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// // Someone edits `order-producers`, and the UI asks which recorded calls the edit would match.
/// let request = serde_json::json!({
///     "from": { "surface": "ui" },
///     "operation": "query-recording",
///     "recording": "orders-sim.2026-11-21T14-02-11.527Z",
///     "selector": "order-producers",
///     "draft": { "name": "order-producers", "match": { "api": "Produce", "topic": "orders.*" } },
/// });
/// let request: OperationRequest = serde_json::from_value(request).expect("a request");
/// let AnyOperation::QueryRecording(query) = request.operation else { panic!("a query of a recording") };
/// assert_eq!(query.skip, 0);
///
/// let answer = serde_json::json!({ "matched": 0, "calls": [] });
/// let answer: <QueryRecording as Operation>::Answer = serde_json::from_value(answer).expect("the answer");
/// assert_eq!(answer, RecordedCalls { matched: 0, calls: Vec::new(), lost: 0 });
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRecording {
    /// The recording.
    pub recording: RecordingId,
    /// The selector: one of the workspace by its name, or inline conditions.
    pub selector: SelectorRef,
    /// A selector definition that nobody saved yet, which stands in for the selector of its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<Selector>,
    /// How many of the matched calls the answer leaves out at the start: those that the client has.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skip: u64,
}

impl Operation for QueryRecording {
    type Answer = RecordedCalls;
    type Error = QueryRecordingError;
}

/// The calls of a recording that a selector matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedCalls {
    /// How many calls of the whole recording the selector matches.
    pub matched: u64,
    /// The matched calls behind those that the query skipped, in the order of the recording, with their verdicts and
    /// their explanations. The engine blanks every credential of a call before it records it.
    pub calls: Vec<Call>,
    /// How many entries of the recording are damaged, so the engine could not read them. The count and the calls
    /// leave them out. A recording without damage leaves it out.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lost: u64,
}

/// Get the index of a recording: its calls grouped into facets, each with counts, under a filter.
///
/// Every facet is the values of one field, each with the calls that have it, and the patterns that the values
/// suggest. Its counts respect every condition of the filter but those on its own field, so a facet says what is
/// left if someone narrows down further. A click on a value adds its condition to the filter.
/// The engine builds the index from the recording when someone first asks for it, and reads on from there.
///
/// With `serde`, a request without a filter leaves `filter` out: every call.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let request = serde_json::json!({
///     "from": { "surface": "ui" },
///     "operation": "get-recording-index",
///     "recording": "orders-sim.2026-11-21T14-02-11.527Z",
///     "filter": { "api": "Produce" },
/// });
/// let request: OperationRequest = serde_json::from_value(request).expect("a request");
/// assert_eq!(request.operation.name(), "get-recording-index");
///
/// let answer = serde_json::json!({
///     "calls": 3,
///     "facets": [{
///         "field": "topic",
///         "values": [{ "value": { "topic": "orders.eu" }, "calls": 2 }, { "value": { "topic": "orders.us" }, "calls": 1 }],
///         "patterns": [{ "pattern": "orders.*", "values": 2, "calls": 3 }],
///     }],
/// });
/// let index: <GetRecordingIndex as Operation>::Answer = serde_json::from_value(answer).expect("the answer");
/// assert_eq!(index.facets[0].patterns[0].pattern.as_str(), "orders.*");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetRecordingIndex {
    /// The recording.
    pub recording: RecordingId,
    /// The filter: a selector of the workspace by its name, or the conditions that someone clicked.
    #[serde(default = "every_call", skip_serializing_if = "is_every_call")]
    pub filter: SelectorRef,
}

impl Operation for GetRecordingIndex {
    type Answer = RecordingIndex;
    type Error = QueryRecordingError;
}

/// The index of a recording under a filter: how many calls the filter leaves, and the facets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingIndex {
    /// How many calls the whole filter leaves.
    pub calls: u64,
    /// The facets, in the order that the process groups by.
    pub facets: Vec<IndexFacet>,
    /// How many entries of the recording are damaged, so the engine could not read them. The facets leave them out.
    /// A recording without damage leaves it out.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lost: u64,
}

/// One facet of an index: the values of one field, each with the calls that have it, and the patterns they suggest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexFacet {
    /// The field whose values the facet groups the calls by.
    pub field: Field,
    /// The values, with the most calls first.
    pub values: Vec<IndexValue>,
    /// The patterns that the values suggest, with the most calls first.
    pub patterns: Vec<IndexPattern>,
}

/// One value of a facet, with the calls that are left once the value narrows the filter down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexValue {
    /// The value, with its field: `{ "topic": "orders.eu" }`.
    pub value: FieldValue,
    /// How many calls have it, under the other conditions of the filter.
    pub calls: u64,
}

/// A pattern that several values of a facet share, such as `orders.*`, with the calls it leaves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexPattern {
    /// The pattern. A click on it adds `matches` with it to the filter.
    pub pattern: Glob,
    /// How many values of the facet the pattern stands for.
    pub values: u64,
    /// How many calls match it, under the other conditions of the filter.
    pub calls: u64,
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

/// Why the process did not [query a recording](QueryRecording), or did not give its [index](GetRecordingIndex).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = QueryRecordingError::Busy {
///     recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("an ID"),
///     reason: "the readers of this process hold 268435456 bytes of recordings, and \
///              `recordings.all_readers_memory` allows 268435456"
///         .to_owned(),
/// };
/// assert!(error.to_string().starts_with("recording orders-sim.2026-11-21T14-02-11.527Z was not read, because the engine is busy reading"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum QueryRecordingError {
    /// The process keeps no recording with that ID.
    #[error("no recording {recording} is there")]
    NotFound { recording: RecordingId },
    /// What is there is no recording that this build of the process reads: a later version wrote it, or it is no
    /// recording. It will not open, however often someone asks. `reason` says which.
    #[error("recording {recording} does not open: {reason}")]
    Unreadable { recording: RecordingId, reason: String },
    /// The storage did not answer, and the request can be sent again.
    #[error("recording {recording} was not read: {reason}")]
    StorageUnavailable { recording: RecordingId, reason: String },
    /// The engine is busy reading: its readers hold as much memory as a budget allows, so it refused the reading,
    /// and nothing waited. The client asks again a moment later. `reason` names the budget and its property.
    #[error("recording {recording} was not read, because the engine is busy reading: {reason}")]
    Busy { recording: RecordingId, reason: String },
    /// The selectors of the workspace could not be read. `reason` says what the store said.
    #[error("the recording was not read: {reason}")]
    StoreFailed { reason: String },
    /// The selector does not resolve against the selectors of the workspace and the draft: it names one that does not
    /// exist, or the selectors include each other in a circle. `reason` says which.
    #[error("{reason}")]
    DoesNotFit { reason: String },
}

/// Whether a count is zero, which its `serde` form leaves out.
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde hands the field over by reference"
)]
const fn is_zero(count: &u64) -> bool {
    *count == 0
}

/// A filter without a condition: every call.
fn every_call() -> SelectorRef {
    SelectorRef::Inline(Conditions::new())
}

/// Whether a filter is without a condition, which its `serde` form leaves out.
fn is_every_call(filter: &SelectorRef) -> bool {
    matches!(filter, SelectorRef::Inline(conditions) if conditions.is_empty())
}
