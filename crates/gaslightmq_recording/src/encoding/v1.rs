//! The stored types of version 1 of the encoding.
//!
//! A stored type is a plain type of numbers, texts, bytes, lists, choices and other stored types.
//! It holds no value that a check guards, and it is no type of a call or of a journal.
//! A message of a recording is the archive of one of the six at the top: [`SessionEvent`], [`Call`],
//! [`JournalEntry`], [`Description`], [`Summary`] and [`End`].
//!
//! # This module is frozen by the release that first writes version 1
//!
//! A field that is added, taken out, moved or widened gives other bytes for every value,
//! and so does a case that is added before another one.
//! Until a release writes version 1, its types change in place.
//! From then on nobody edits this module: a change is the module of version 2, with the way from these types to it.
//! The samples of this version and their bytes, in `v1_samples`, fail when a type here changes.
//!
//! No type here holds a collection without an order of its own, so the same value gives the same bytes.

use rkyv::{Archive, Deserialize, Serialize};

/// An instant on the clock of an environment, in nanoseconds since the clock started.
pub(crate) type Instant = u64;
/// A time span in milliseconds.
pub(crate) type Millis = u64;
/// The code of a Kafka error, which is never zero.
pub(crate) type ErrorCode = i16;

/// The address of a host on the internet.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Host {
    V4([u8; 4]),
    V6([u8; 16]),
}

/// A topic and one of its partitions.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct TopicPartition {
    pub(crate) topic: String,
    pub(crate) partition: u32,
}

/// A topic or a partition that a request names.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Part {
    Topic(String),
    Partition(TopicPartition),
}

/// What a selector matched, or what a lie applies to: the whole call, or parts of it.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Target {
    Call,
    Parts(Vec<Part>),
}

/// How a call ended for the backend.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Outcome {
    Ok,
    Error,
    Timeout,
}

/// One value of one field of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum FieldValue {
    Connection(u64),
    Address {
        host: Host,
        port: u16,
    },
    Host(Host),
    /// The broker of the endpoint, or nothing for the bootstrap endpoint.
    Endpoint(Option<u32>),
    ClientId(String),
    ClientSoftware {
        name: String,
        version: String,
    },
    Api(i16),
    ApiVersion(u16),
    Topic(String),
    Partition(u32),
    Acks(i16),
    Group(String),
    MemberId(String),
    GroupInstanceId(String),
    TransactionalId(String),
    Outcome(Outcome),
    ErrorCode(ErrorCode),
    Latency(Millis),
}

/// The truth of one part of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum PartTruth {
    OfCall,
    Succeeded,
    Failed(ErrorCode),
    None,
}

/// One part of a call, with its truth.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct PartFields {
    pub(crate) part: Part,
    pub(crate) truth: PartTruth,
}

/// The fields of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Fields {
    /// What the call and its session have, in the order of the fields, and each field's values in their order.
    pub(crate) values: Vec<FieldValue>,
    /// What the client context provides, in the same order.
    pub(crate) learned: Vec<FieldValue>,
    pub(crate) parts: Vec<PartFields>,
}

/// The answer of a response, for the call or for one of its parts.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Answer {
    Ok,
    Error(ErrorCode),
}

/// The answer of one part of a response.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct PartAnswer {
    pub(crate) part: Part,
    pub(crate) answer: Answer,
}

/// A response without its payload.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct ResponseSummary {
    pub(crate) answer: Answer,
    /// The parts with an answer of their own, in the order of the parts.
    pub(crate) parts: Vec<PartAnswer>,
}

/// What the client received.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Delivered {
    Response(ResponseSummary),
    UnreadResponse,
    Nothing,
    ConnectionClosed,
}

/// The moments of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Timing {
    pub(crate) arrived: Instant,
    pub(crate) forwarded: Option<Instant>,
    pub(crate) answered: Option<Instant>,
    pub(crate) delivered: Option<Instant>,
}

/// Where in a call a lie acts.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Stage {
    BeforeForwarding,
    AfterResponse,
}

/// A lie with its parameters.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Lie {
    Refusal {
        error: ErrorCode,
        stage: Stage,
    },
    Timing {
        delay: Millis,
        jitter: Millis,
        throttle: Millis,
        stage: Stage,
    },
    Silence,
    HangUp {
        stage: Stage,
    },
}

/// A lie and what it lies about.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct TargetedLie {
    pub(crate) lie: Lie,
    pub(crate) target: Target,
}

/// Where a lie lost, and to which rule.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct LostTarget {
    pub(crate) on: Target,
    pub(crate) to: String,
}

/// What became of a lie that fired.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct LieFate {
    pub(crate) lie: TargetedLie,
    pub(crate) applied: Option<Target>,
    pub(crate) shadowed: Vec<LostTarget>,
    pub(crate) pre_empted: Vec<LostTarget>,
    /// Why the lie could not be told.
    pub(crate) untold: Option<String>,
}

/// What a trigger decided.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum TriggerDecision {
    Fired,
    FiredByHand,
    Held,
    BudgetEmpty,
    NothingToActOn,
    LifetimeOver,
}

/// The roll of a die and the chance it rolled against, in parts per million.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct DieCheck {
    pub(crate) roll: u32,
    pub(crate) chance: u32,
}

/// The window of a trigger, and the time since its rule was armed.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct WindowCheck {
    pub(crate) opens_after: Millis,
    pub(crate) closes_after: Option<Millis>,
    pub(crate) since_armed: Millis,
}

/// What was left of a budget, and how much it allows.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct BudgetCheck {
    pub(crate) left: u64,
    pub(crate) limit: u64,
}

/// What a trigger looked at and decided.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct TriggerExplanation {
    pub(crate) decision: TriggerDecision,
    pub(crate) match_number: Option<u64>,
    pub(crate) count_agreed: Option<bool>,
    pub(crate) die: Option<DieCheck>,
    pub(crate) window: Option<WindowCheck>,
    pub(crate) budget: Option<BudgetCheck>,
    pub(crate) reason: Option<String>,
}

/// The coordinator of a group or of a transaction.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Coordinator {
    Group(String),
    Transaction(String),
}

/// Something that happened to the cluster.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Incident {
    LeaderMoves {
        partition: TopicPartition,
        to: Option<u32>,
    },
    BrokerGoesDown {
        broker: u32,
        failover: bool,
    },
    BrokerGoesUnresponsive {
        broker: u32,
    },
    BrokerComesBack {
        broker: u32,
    },
    Rebalance {
        group: String,
    },
    CoordinatorMoves {
        coordinator: Coordinator,
        to: Option<u32>,
    },
    PartitionAdded {
        topic: String,
        count: u32,
    },
    TopicDeleted {
        topic: String,
    },
    RecordsAppended {
        partition: TopicPartition,
        count: u64,
    },
    RecordsExpired {
        partition: TopicPartition,
        to: u64,
    },
    LogTruncated {
        partition: TopicPartition,
        to: u64,
    },
    CommittedOffsetReset {
        group: String,
        partition: TopicPartition,
        to: u64,
    },
    ProducerFenced {
        transactional_id: String,
    },
}

/// How far a rule got with a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Course {
    EndedBeforeTruth {
        ended_by: String,
    },
    Held {
        matched: Target,
        trigger: TriggerExplanation,
    },
    Lied {
        matched: Target,
        trigger: TriggerExplanation,
        lie: LieFate,
    },
    FiredIncidents {
        matched: Target,
        trigger: TriggerExplanation,
        incidents: Vec<Incident>,
    },
}

/// One line of an explanation: one rule that the call touched.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct RuleExplanation {
    pub(crate) rule: String,
    pub(crate) priority: u64,
    pub(crate) course: Course,
}

/// Why the verdict of a call came out as it did.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Explanation {
    pub(crate) rules: Vec<RuleExplanation>,
    /// What went wrong inside GaslightMQ while it handled the call.
    pub(crate) failure: Option<String>,
}

/// What a recording holds of a frame.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum FrameState {
    None,
    Whole,
    Redacted,
    Withheld,
}

/// One of the four frames of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct FrameSlot {
    pub(crate) state: FrameState,
    /// The bytes that the frame had on the wire.
    pub(crate) on_the_wire: u64,
    /// The bytes that the recording holds of it. The frames of an entry stand back to back, in the order of the slots.
    pub(crate) kept: u64,
}

/// A call record, and the slots of its frames.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Call {
    pub(crate) id: u64,
    pub(crate) connection: u64,
    pub(crate) fields: Fields,
    pub(crate) upstream: Option<ResponseSummary>,
    pub(crate) delivered: Delivered,
    pub(crate) explanation: Explanation,
    pub(crate) timing: Timing,
    /// The request, the upstream response, the truth and the delivered response.
    pub(crate) frames: [FrameSlot; 4],
}

/// Who closed a session.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Closer {
    Client,
    Backend,
    Lie,
    Proxy,
}

/// The name and the version of the software of a client.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct ClientSoftware {
    pub(crate) name: String,
    pub(crate) version: String,
}

/// What happened to a session.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum SessionChange {
    Opened {
        host: Host,
        port: u16,
        /// The broker of the endpoint, or nothing for the bootstrap endpoint.
        endpoint: Option<u32>,
        client_id: Option<String>,
        software: Option<ClientSoftware>,
    },
    Identified {
        client_id: Option<String>,
    },
    SoftwareNamed {
        software: ClientSoftware,
    },
    Closed {
        by: Closer,
    },
}

/// One event of one session.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct SessionEvent {
    pub(crate) connection: u64,
    pub(crate) at: Instant,
    pub(crate) change: SessionChange,
}

/// The surface that a change came from.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum ChangeSurface {
    Pad,
    QuickInject,
    Ui,
    Cli,
    Test,
    Api,
    Step,
    Lifetime,
    File,
}

/// Whether a change takes back or repeats an earlier one.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum ChangeCause {
    Undo,
    Redo,
}

/// One parameter of a rule.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Parameter {
    Chance,
    Every,
    Delay,
    Jitter,
    Throttle,
    Error,
    Lifetime,
}

/// The kind of a definition of the workspace.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum DefinitionKind {
    Selector,
    Preset,
    Rule,
    Scenario,
    Layout,
    Environment,
}

/// A definition of the workspace, by its kind and the name it had.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Definition {
    pub(crate) kind: DefinitionKind,
    pub(crate) name: String,
}

/// What an incident brought with it.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Consequence {
    Leaderless { partition: TopicPartition },
    CoordinatorUnavailable { coordinator: Coordinator },
    Incident(Incident),
}

/// An incident with its consequences.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct ExpandedIncident {
    pub(crate) incident: Incident,
    pub(crate) consequences: Vec<Consequence>,
}

/// Why a recording stopped by itself.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum RecordingStopCause {
    FellBehind,
    CouldNotWrite,
}

/// What changed in an environment.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Change {
    RuleArmed {
        rule: String,
    },
    RuleDisarmed {
        rule: String,
    },
    ParameterChanged {
        rule: String,
        parameter: Parameter,
        old: String,
        new: String,
    },
    RuleFiredByHand {
        rule: String,
    },
    LiveRuleMade {
        rule: String,
    },
    LiveRuleChanged {
        rule: String,
    },
    LiveRuleKept {
        rule: String,
    },
    LiveRuleDeleted {
        rule: String,
    },
    DefinitionCreated {
        definition: Definition,
    },
    DefinitionChanged {
        definition: Definition,
    },
    DefinitionDeleted {
        definition: Definition,
    },
    ScenarioActivated {
        scenario: String,
        step: Option<String>,
    },
    ScenarioDeactivated {
        scenario: String,
        step: Option<String>,
    },
    StepAdvanced {
        scenario: String,
        step: Option<String>,
    },
    IncidentApplied {
        incident: ExpandedIncident,
    },
    IncidentEmulated {
        incident: ExpandedIncident,
    },
    RecordingStopped {
        why: RecordingStopCause,
    },
}

/// One entry of the journal of an environment.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct JournalEntry {
    pub(crate) id: u64,
    pub(crate) at: Instant,
    pub(crate) revision: Option<u64>,
    pub(crate) surface: ChangeSurface,
    pub(crate) cause: Option<ChangeCause>,
    pub(crate) change: Change,
}

/// How much of a call a recording keeps.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum RecordingMode {
    Full,
    MetadataOnly,
}

/// What stood behind the proxy.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Backend {
    Upstream,
    Simulated,
}

/// The scenario that was active when a recording started.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct ActiveScenario {
    pub(crate) scenario: String,
    pub(crate) step: Option<String>,
}

/// What a recording is.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Description {
    pub(crate) environment: String,
    /// The instant the recording started, in milliseconds since 1970-01-01T00:00:00Z.
    pub(crate) started: u64,
    pub(crate) clock: Instant,
    pub(crate) mode: RecordingMode,
    pub(crate) backend: Backend,
    pub(crate) written_by: String,
    pub(crate) armed: Vec<String>,
    pub(crate) scenario: Option<ActiveScenario>,
    pub(crate) journal_after: Option<u64>,
}

/// The earliest and the latest instant of a stretch.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Instants {
    pub(crate) from: Instant,
    pub(crate) to: Instant,
}

/// The lowest and the highest ID of a stretch.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Ids {
    pub(crate) lowest: u64,
    pub(crate) highest: u64,
}

/// A field of a call.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Field {
    Connection,
    Address,
    Host,
    Endpoint,
    ClientId,
    ClientSoftware,
    Api,
    ApiVersion,
    Topic,
    Partition,
    Acks,
    Group,
    MemberId,
    GroupInstanceId,
    TransactionalId,
    Outcome,
    ErrorCode,
    Latency,
}

/// The values that the calls of a stretch have for one field.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum SummaryValues {
    /// Every value, each once, in their order.
    Listed(Vec<FieldValue>),
    /// More values than a summary lists.
    Any,
}

/// The values of one field in a summary.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct FieldSummary {
    pub(crate) field: Field,
    pub(crate) values: SummaryValues,
}

/// What a stretch of a recording holds.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct Summary {
    pub(crate) instants: Option<Instants>,
    pub(crate) calls: u64,
    pub(crate) call_ids: Option<Ids>,
    pub(crate) journal: u64,
    pub(crate) journal_ids: Option<Ids>,
    pub(crate) sessions: u64,
    /// The fields that a call of the stretch has, in the order of the fields.
    pub(crate) fields: Vec<FieldSummary>,
}

/// How a recording ended.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) enum Ending {
    Stopped,
    EnvironmentStopped,
    FellBehind,
}

/// The end of a recording.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub(crate) struct End {
    pub(crate) ending: Ending,
    pub(crate) at: Instant,
    pub(crate) entries: u64,
    pub(crate) calls: u64,
}
