//! Why an operation got no answer of its own, why a live stream ended, and why each call of an environment did not
//! do what it asked.

use gaslightmq_control_api::RequestError;
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_definition_text::ReadDefinitionError;

/// Why an operation got no answer of its own: neither its answer nor its error.
///
/// An operation that the control plane answered returns its own error instead, as itself,
/// so a caller decides on it: see [`ControlClient::send`](crate::ControlClient::send).
///
/// ```
/// use gaslightmq_control_client::SendError;
///
/// let error = SendError::Unreachable { reason: "connection refused".to_owned() };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    /// No answer came: the client could not reach the control plane, the connection broke,
    /// or the [request timeout](crate::ControlClientConfiguration::request_timeout) passed.
    /// The control plane may have done the operation. A caller tries again later.
    #[error("the control plane is unreachable: {reason}")]
    Unreachable { reason: String },
    /// The control plane did not take the request, and did nothing: it does not serve the operation yet,
    /// or it could not read the request. `error` says which.
    #[error("the control plane did not take the request: {error}")]
    Rejected { error: RequestError },
    /// Something answered that the client cannot read: no answer of the control plane to the operation.
    /// The other side is no control plane, or one of another version. Trying again gets the same.
    #[error("the control plane answered what the client cannot read: {reason}")]
    Unreadable { reason: String },
}

/// Why a live stream did not open, or why it ended.
///
/// A stream that ended no longer holds what the process holds,
/// so a caller [opens](crate::ControlClient::live_stream) a new one, which starts with a new snapshot.
///
/// ```
/// use gaslightmq_control_client::StreamError;
///
/// let error = StreamError::Unreachable { reason: "the control plane stopped".to_owned() };
/// assert_eq!(error.to_string(), "the live stream is unreachable: the control plane stopped");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    /// The stream did not open within the [request timeout](crate::ControlClientConfiguration::request_timeout),
    /// it broke, or the control plane closed it. A caller opens a new stream later.
    #[error("the live stream is unreachable: {reason}")]
    Unreachable { reason: String },
    /// Something sent what the client cannot read: no message of the live stream.
    /// The other side is no control plane, or one of another version. A new stream gets the same.
    #[error("the live stream sent what the client cannot read: {reason}")]
    Unreadable { reason: String },
}

/// Why a rule did not [arm](crate::Rules::arm) in an environment.
///
/// ```
/// use gaslightmq_control_client::{ArmError, ReadDefinitionError};
///
/// let error = ArmError::Unreadable {
///     error: ReadDefinitionError { path: Some("trigger.first".to_owned()), reason: "a count is at least one".to_owned() },
/// };
/// assert_eq!(error.to_string(), "the rule does not read: trigger.first: a count is at least one");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArmError {
    /// The rule does not read: `error` names the key that failed.
    #[error("the rule does not read: {error}")]
    Unreadable { error: ReadDefinitionError },
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not create the rule as a live rule, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    NotCreated { error: CreateLiveRuleError },
    /// The environment did not arm the rule of that name, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    NotArmed { error: RuleCommandError },
}

/// Why a rule did not [disarm](crate::EnvironmentRule::disarm).
///
/// ```
/// use gaslightmq_control_client::{DisarmError, SendError};
///
/// let error = DisarmError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DisarmError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not disarm the rule, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: RuleCommandError },
}

/// Why a rule did not [fire](crate::EnvironmentRule::fire) by hand.
///
/// ```
/// use gaslightmq_control_client::{FireError, SendError};
///
/// let error = FireError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FireError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not fire the rule, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: FireRuleError },
}

/// Why a scenario did not [activate](crate::Scenarios::activate) in an environment.
///
/// ```
/// use gaslightmq_control_client::{ActivateError, ReadDefinitionError};
///
/// let error = ActivateError::Unreadable {
///     error: ReadDefinitionError { path: Some("seed".to_owned()), reason: "missing field `seed`".to_owned() },
/// };
/// assert_eq!(error.to_string(), "the scenario does not read: seed: missing field `seed`");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivateError {
    /// The scenario does not read: `error` names the key that failed.
    #[error("the scenario does not read: {error}")]
    Unreadable { error: ReadDefinitionError },
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The workspace did not take the scenario, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    NotCreated { error: CreateDefinitionError },
    /// The environment did not activate the scenario, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    NotActivated { error: ActivateScenarioError },
}

/// Why the scenario of an environment did not [step](crate::ActiveScenario::step).
///
/// ```
/// use gaslightmq_control_client::{StepError, SendError};
///
/// let error = StepError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not step its scenario, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: StepScenarioError },
}

/// Why a scenario did not [deactivate](crate::ActiveScenario::deactivate).
///
/// ```
/// use gaslightmq_control_client::{DeactivateError, SendError};
///
/// let error = DeactivateError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeactivateError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not deactivate its scenario, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: DeactivateScenarioError },
}

/// Why the workspace did not get a [definition](crate::Definitions).
///
/// ```
/// use gaslightmq_control_client::{DefineError, ReadDefinitionError};
///
/// let error = DefineError::Unreadable { error: ReadDefinitionError { path: None, reason: "no map".to_owned() } };
/// assert_eq!(error.to_string(), "the definition does not read: no map");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DefineError {
    /// The definition does not read: `error` names the key that failed.
    #[error("the definition does not read: {error}")]
    Unreadable { error: ReadDefinitionError },
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The workspace did not create the definition, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    NotCreated { error: CreateDefinitionError },
}

/// Why a recording did not [start](crate::Recordings::start).
///
/// ```
/// use gaslightmq_control_client::{RecordingStartError, SendError};
///
/// let error = RecordingStartError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordingStartError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not start a recording, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: StartRecordingError },
}

/// Why a recording did not [stop](crate::Recording::stop) in order.
///
/// ```
/// use gaslightmq_control_client::{RecordingStopError, SendError};
///
/// let error = RecordingStopError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecordingStopError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment did not stop its recording in order, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: StopRecordingError },
}

/// Why a [recording](crate::Recording) was not queried.
///
/// ```
/// use gaslightmq_control_client::{QueryError, ReadDefinitionError};
///
/// let error = QueryError::Unreadable { error: ReadDefinitionError { path: Some("api".to_owned()), reason: "unknown API key: Nope".to_owned() } };
/// assert_eq!(error.to_string(), "the selector does not read: api: unknown API key: Nope");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    /// The selector does not read: `error` names the key that failed.
    #[error("the selector does not read: {error}")]
    Unreadable { error: ReadDefinitionError },
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The control plane did not query the recording, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: QueryRecordingError },
}

/// Why the [journal](crate::ControlledEnvironment::journal) of an environment was not read.
///
/// ```
/// use gaslightmq_control_client::{JournalError, SendError};
///
/// let error = JournalError::Unanswered { error: SendError::Unreachable { reason: "connection refused".to_owned() } };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    /// No answer of its own came: `error` says why. The control plane may have done what the call asked.
    #[error("{error}")]
    Unanswered { error: SendError },
    /// The environment does not run, and `error` is the error that the control plane refused with.
    #[error("{error}")]
    Refused { error: EnvironmentNotRunning },
}

/// Why the calls of an environment are not [followed](crate::ControlledEnvironment::follow).
///
/// ```
/// use gaslightmq_control_client::FollowError;
///
/// let error = FollowError::Refused { reason: "environment \"orders-sim\" does not run".to_owned() };
/// assert_eq!(error.to_string(), "the calls are not followed: environment \"orders-sim\" does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FollowError {
    /// The selector does not read: `error` names the key that failed.
    #[error("the selector does not read: {error}")]
    Unreadable { error: ReadDefinitionError },
    /// The live stream did not open, or it ended before the control plane followed the calls.
    #[error("{error}")]
    Unreachable { error: StreamError },
    /// The control plane cannot follow the calls: the environment does not run, or the selector does not exist.
    /// `reason` is what it said.
    #[error("the calls are not followed: {reason}")]
    Refused { reason: String },
}

/// Why fewer calls came than a caller [waited for](crate::FollowedCalls::wait_for).
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_control_client::WaitForCallsError;
///
/// let error = WaitForCallsError::TimedOut { wanted: 2, seen: 1, timeout: TimeSpan::from_secs(30) };
/// assert_eq!(error.to_string(), "1 of 2 calls came within 30s");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WaitForCallsError {
    /// `seen` of the `wanted` calls came within the [follow timeout](crate::ControlClientConfiguration::follow_timeout).
    #[error("{seen} of {wanted} calls came within {timeout}")]
    TimedOut { wanted: u64, seen: u64, timeout: TimeSpan },
    /// The live stream ended first, and `error` says why.
    #[error("{error}")]
    Ended { error: StreamError },
}

/// Why the state of an environment is not known: its [scenario](crate::ControlledEnvironment::scenario_status).
///
/// ```
/// use gaslightmq_control_client::EnvironmentStateError;
///
/// let error = EnvironmentStateError::NotRunning { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" does not run");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentStateError {
    /// The environment does not run, so the process holds no state of it.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The live stream that would say gave no snapshot, and `error` says why.
    #[error("{error}")]
    Unreachable { error: StreamError },
}

/// Why there is no [recording that runs](crate::Recordings::current).
///
/// ```
/// use gaslightmq_control_client::CurrentRecordingError;
///
/// let error = CurrentRecordingError::NotRecording { environment: "orders-sim".parse().expect("an environment name") };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" records nothing");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CurrentRecordingError {
    /// The environment records nothing.
    #[error("environment {:?} records nothing", .environment.as_str())]
    NotRecording { environment: EnvironmentName },
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The live stream that would say gave no snapshot, and `error` says why.
    #[error("{error}")]
    Unreachable { error: StreamError },
}

/// Why an instance did not do what a test [declared](crate::Preparation) before it started.
///
/// ```
/// use gaslightmq_control_client::{ArmError, PrepareError, ReadDefinitionError};
///
/// let error = PrepareError::NotArmed {
///     error: ArmError::Unreadable { error: ReadDefinitionError { path: Some("action".to_owned()), reason: "missing field `action`".to_owned() } },
/// };
/// assert_eq!(error.to_string(), "the rule does not read: action: missing field `action`");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrepareError {
    /// The workspace did not get a rule or a scenario, and `error` says why.
    #[error("{error}")]
    NotDefined { error: DefineError },
    /// A rule did not arm, and `error` says why.
    #[error("{error}")]
    NotArmed { error: ArmError },
    /// The recording from the start did not start, and `error` says why.
    #[error("{error}")]
    NotRecording { error: RecordingStartError },
    /// The scenario did not activate, and `error` says why.
    #[error("{error}")]
    NotActivated { error: ActivateError },
}

/// Why the [firings](crate::EnvironmentRule::firings) of a rule are not known.
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_control_client::FiringsError;
///
/// let error = FiringsError::TimedOut { timeout: TimeSpan::from_secs(30) };
/// assert_eq!(error.to_string(), "no statistics came within 30s");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FiringsError {
    /// The live stream that would say did not open, or it ended first, and `error` says why.
    #[error("{error}")]
    Unreachable { error: StreamError },
    /// No statistics of the environment came within the
    /// [follow timeout](crate::ControlClientConfiguration::follow_timeout).
    #[error("no statistics came within {timeout}")]
    TimedOut { timeout: TimeSpan },
}
