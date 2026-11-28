//! Why an engine could not do what it was asked to do: one error for each thing that fails in its own way.

use gaslightmq_environment::BrokerListenersError;
use gaslightmq_field::{ConnectionNumber, Endpoint};
use gaslightmq_recording::RecordingId;
use gaslightmq_rule::RuleName;
use gaslightmq_scenario::ScenarioName;

/// Why an endpoint of a proxy has no listener.
///
/// The engine opens a listener for every endpoint that its backend opens: when it starts,
/// and whenever the backend reports another endpoint from then on.
/// Each variant is a thing that whoever runs the proxy does something else about:
///
/// | Variant | What to do about it |
/// |---|---|
/// | [`AddressUnavailable`](Self::AddressUnavailable) | Pick another address, or free the one that is taken. |
/// | [`ListenerRejected`](Self::ListenerRejected) | Change what clients are told: the host they see, or the port of the listener. |
///
/// An engine that starts returns it inside [`EngineStartError::EndpointNotOpened`].
/// An engine that runs writes it to its log, because nobody waits for an endpoint that opens later.
///
/// ```
/// use gaslightmq_engine::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let error = EndpointOpenError::AddressUnavailable {
///     endpoint: Endpoint::Bootstrap,
///     reason: "cannot listen on 0.0.0.0:9092: address in use".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "endpoint `bootstrap` did not open: cannot listen on 0.0.0.0:9092: address in use"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointOpenError {
    /// The address of the endpoint did not take a listener.
    /// `reason` names the address, and says what stood in the way, such as another socket that holds the port.
    #[error("endpoint `{endpoint}` did not open: {reason}")]
    AddressUnavailable { endpoint: Endpoint, reason: String },
    /// The backend rejected the listener of the endpoint, because it cannot send clients there.
    /// `reason` names the address of the listener, and says why clients cannot be told about it.
    #[error("the backend rejected the listener of endpoint `{endpoint}`: {reason}")]
    ListenerRejected { endpoint: Endpoint, reason: String },
}

/// Why an [`Engine`](crate::Engine) did not start.
///
/// An engine starts once every endpoint of its backend has a listener.
///
/// | Variant | What is wrong |
/// |---|---|
/// | [`BrokersWithoutListener`](Self::BrokersWithoutListener) | The environment has no listener for a broker of the cluster. |
/// | [`EndpointNotOpened`](Self::EndpointNotOpened) | An endpoint has an address, and no listener on it. Its `error` says which of the two reasons it is. |
///
/// The message says everything in one line.
///
/// ```
/// use gaslightmq_engine::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let error = EngineStartError::EndpointNotOpened {
///     error: EndpointOpenError::AddressUnavailable {
///         endpoint: Endpoint::Bootstrap,
///         reason: "cannot listen on 0.0.0.0:9092: address in use".to_owned(),
///     },
/// };
/// assert_eq!(
///     error.to_string(),
///     "endpoint `bootstrap` did not open: cannot listen on 0.0.0.0:9092: address in use"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineStartError {
    /// The listeners of the environment have no address for a broker endpoint of the backend:
    /// a range has too few ports, or a broker has no pin.
    #[error("the listeners do not hold the brokers of the cluster: {error}")]
    BrokersWithoutListener { error: BrokerListenersError },
    /// An endpoint of the backend did not open. The error names the endpoint, and says why.
    #[error("{error}")]
    EndpointNotOpened { error: EndpointOpenError },
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not arm a rule.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = ArmError::AlreadyArmed { rule: "leader-flap".parse().expect("a rule name") };
/// assert_eq!(error.to_string(), "rule \"leader-flap\" is armed already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArmError {
    /// The rule is armed, and arming it again would reset it. Whoever wants that disarms it first.
    #[error("rule {:?} is armed already", .rule.as_str())]
    AlreadyArmed { rule: RuleName },
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not disarm a rule.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = DisarmError::NotArmed { rule: "leader-flap".parse().expect("a rule name") };
/// assert_eq!(error.to_string(), "rule \"leader-flap\" is not armed");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DisarmError {
    /// No rule of that name is armed: nobody armed it, someone disarmed it, or its lifetime ended.
    #[error("rule {:?} is not armed", .rule.as_str())]
    NotArmed { rule: RuleName },
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not retune a rule.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = RetuneError::NotArmed { rule: "leader-flap".parse().expect("a rule name") };
/// assert_eq!(error.to_string(), "rule \"leader-flap\" is not armed, so it has nothing to retune");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetuneError {
    /// No rule of that name is armed, so no arming keeps its counters. Whoever changes the rule arms it anew.
    #[error("rule {:?} is not armed, so it has nothing to retune", .rule.as_str())]
    NotArmed { rule: RuleName },
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not activate a scenario.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = ActivateScenarioError::AlreadyActive { scenario: "leader-election".parse().expect("a scenario name") };
/// assert_eq!(error.to_string(), "scenario \"leader-election\" is active already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivateScenarioError {
    /// A scenario is active, and at most one is active at a time. Whoever wants another deactivates this one first.
    #[error("scenario {:?} is active already", .scenario.as_str())]
    AlreadyActive { scenario: ScenarioName },
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not deactivate the active scenario.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// assert_eq!(DeactivateScenarioError::NotActive.to_string(), "no scenario is active");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeactivateScenarioError {
    /// No scenario is active, so none deactivates.
    #[error("no scenario is active")]
    NotActive,
}

/// Why the [`ArmedRules`](crate::ArmedRules) did not advance the active scenario to its next step.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = StepScenarioError::NoStep { scenario: "steady-jitter".parse().expect("a scenario name") };
/// assert_eq!(error.to_string(), "scenario \"steady-jitter\" runs no step that could end");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepScenarioError {
    /// No scenario is active.
    #[error("no scenario is active")]
    NotActive,
    /// The scenario runs no step: it is no Sequence, or it ran its last step already.
    #[error("scenario {:?} runs no step that could end", .scenario.as_str())]
    NoStep { scenario: ScenarioName },
}

/// No session is open on the connection that an engine was asked to [close](crate::Engine::close_session):
/// it closed already, or it never opened.
///
/// ```
/// use gaslightmq_engine::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let error = NotOpenError { connection: ConnectionNumber::new(7) };
/// assert_eq!(error.to_string(), "no session is open on connection #7");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no session is open on connection {connection}")]
pub struct NotOpenError {
    /// The connection that has no open session.
    pub connection: ConnectionNumber,
}

/// Why a listener of the [live events](crate::LiveEvents) of an engine has no next event.
///
/// A listener goes on after [`Missed`](Self::Missed), and stops after [`EngineStopped`](Self::EngineStopped).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = LiveEventsError::Missed { events: 12 };
/// assert_eq!(error.to_string(), "the listener fell behind, and missed 12 events of the engine");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveEventsError {
    /// The listener fell behind, and the engine did not keep these events for it.
    /// The events that follow are still there.
    #[error("the listener fell behind, and missed {events} events of the engine")]
    Missed { events: u64 },
    /// The engine stopped, and the listener received every event that the engine kept for it.
    #[error("the engine stopped")]
    EngineStopped,
}

/// Why the recorder of an engine did not [start a recording](crate::Recorder::start).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = StartRecordingError::AlreadyRecording { recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("an ID") };
/// assert_eq!(error.to_string(), "recording orders-sim.2026-11-21T14-02-11.527Z runs already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartRecordingError {
    /// A recording runs already. An environment writes one recording at a time, so whoever wants another stops it.
    #[error("recording {recording} runs already")]
    AlreadyRecording { recording: RecordingId },
    /// The storage did not keep the recording: it has one with the ID already, or it could not write.
    /// `reason` says which, or what else stood in the way of the recorder.
    #[error("recording {recording} did not start: {reason}")]
    StorageFailed { recording: RecordingId, reason: String },
}

/// Why the recorder of an engine did not [stop a recording](crate::Recorder::stop) in order.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// assert_eq!(StopRecordingError::NotRecording.to_string(), "no recording runs");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StopRecordingError {
    /// No recording runs: nobody started one, someone stopped it, or it stopped by itself.
    #[error("no recording runs")]
    NotRecording,
    /// The recording stopped, and its recorder could not write what waited, or its end.
    /// It holds what its last commit covers, and opens as a recording that was cut short.
    #[error("recording {recording} stopped without its end: {reason}")]
    CutShort { recording: RecordingId, reason: String },
}

/// Why the journal of an engine did not [start a capture](crate::Journal::start_capture).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// assert_eq!(StartCaptureError::AlreadyCapturing.to_string(), "a capture runs already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartCaptureError {
    /// A capture runs already. An environment runs one at a time, so whoever wants another stops this one first.
    #[error("a capture runs already")]
    AlreadyCapturing,
}

/// Why the journal of an engine did not [stop a capture](crate::Journal::stop_capture).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// assert_eq!(StopCaptureError::NotCapturing.to_string(), "no capture runs");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StopCaptureError {
    /// No capture runs: nobody started one, or someone stopped it.
    #[error("no capture runs")]
    NotCapturing,
}

/// Why the [recordings](crate::Recordings) were not listed.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = ListRecordingsError::StorageUnavailable { reason: "permission denied".to_owned() };
/// assert_eq!(error.to_string(), "the recordings were not listed: permission denied");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListRecordingsError {
    /// The storage did not answer. The recordings can be listed again.
    #[error("the recordings were not listed: {reason}")]
    StorageUnavailable { reason: String },
}

/// Why a recording did not [open](crate::Recordings::open).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = OpenRecordingError::NotFound { recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("an ID") };
/// assert_eq!(error.to_string(), "no recording orders-sim.2026-11-21T14-02-11.527Z is there");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenRecordingError {
    /// The storage has no recording with that ID.
    #[error("no recording {recording} is there")]
    NotFound { recording: RecordingId },
    /// What is there is no recording that this build reads: a later version wrote it, or it is no recording.
    /// It will not open, however often someone asks.
    #[error("recording {recording} does not open: {reason}")]
    Unreadable { recording: RecordingId, reason: String },
    /// The storage did not answer, and the recording can be opened again.
    #[error("recording {recording} was not read: {reason}")]
    StorageUnavailable { recording: RecordingId, reason: String },
}

/// Why a recording was not [queried](crate::Recordings::query), or did not give its [index](crate::Recordings::index).
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = QueryRecordingError::Busy {
///     recording: "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("an ID"),
///     reason: "the readers of this process hold 268435456 bytes of recordings, and \
///              `recordings.all_readers_memory` allows 268435456"
///         .to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "recording orders-sim.2026-11-21T14-02-11.527Z was not read, because the engine is busy reading: \
///      the readers of this process hold 268435456 bytes of recordings, and `recordings.all_readers_memory` allows \
///      268435456"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryRecordingError {
    /// The storage has no recording with that ID.
    #[error("no recording {recording} is there")]
    NotFound { recording: RecordingId },
    /// What is there is no recording that this build reads: a later version wrote it, or it is no recording.
    /// It will not open, however often someone asks.
    #[error("recording {recording} does not open: {reason}")]
    Unreadable { recording: RecordingId, reason: String },
    /// The storage did not answer, and the recording can be read again.
    #[error("recording {recording} was not read: {reason}")]
    StorageUnavailable { recording: RecordingId, reason: String },
    /// The readers of the process hold as much memory as a budget allows, so the reading was refused, and nothing
    /// waited. Whoever asked asks again a moment later. `reason` names the budget, and the property that sets it.
    #[error("recording {recording} was not read, because the engine is busy reading: {reason}")]
    Busy { recording: RecordingId, reason: String },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_environment::BrokerListeners;

    use super::*;

    fn rule() -> RuleName {
        "leader-flap".parse().unwrap()
    }

    #[test]
    fn an_endpoint_error_names_the_endpoint_and_the_reason_once() {
        let endpoint: Endpoint = "broker 2".parse().unwrap();
        let errors = [
            (
                EndpointOpenError::AddressUnavailable {
                    endpoint,
                    reason: "cannot listen on 0.0.0.0:19093: address in use".to_owned(),
                },
                "endpoint `broker 2` did not open: cannot listen on 0.0.0.0:19093: address in use",
            ),
            (
                EndpointOpenError::ListenerRejected {
                    endpoint,
                    reason: "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
                             broker 1 is advertised at gaslight.staging:19092"
                        .to_owned(),
                },
                "the backend rejected the listener of endpoint `broker 2`: \
                 clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
                 broker 1 is advertised at gaslight.staging:19092",
            ),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
            // An engine that does not start for it says the same, and no more.
            let not_started = EngineStartError::EndpointNotOpened { error };
            assert_eq!(not_started.to_string(), message);
            assert!(not_started.source().is_none(), "{not_started}");
        }
    }

    #[test]
    fn an_engine_without_a_listener_for_a_broker_says_what_the_listeners_lack() {
        let too_few_ports = "0.0.0.0:19092-19093"
            .parse::<BrokerListeners>()
            .unwrap()
            .assign([1u16, 2, 3].map(Into::into))
            .unwrap_err();

        let error = EngineStartError::BrokersWithoutListener { error: too_few_ports };

        assert_eq!(
            error.to_string(),
            "the listeners do not hold the brokers of the cluster: \
             3 brokers need a port each, and the range 19092-19093 has 2"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn the_errors_of_the_armed_rules_name_the_rule() {
        let already_armed = ArmError::AlreadyArmed { rule: rule() };
        let not_armed = DisarmError::NotArmed { rule: rule() };

        assert_eq!(already_armed.to_string(), "rule \"leader-flap\" is armed already");
        assert_eq!(not_armed.to_string(), "rule \"leader-flap\" is not armed");
        assert!(already_armed.source().is_none());
        assert!(not_armed.source().is_none());
    }

    #[test]
    fn a_listener_of_the_live_events_learns_what_it_missed_and_that_the_engine_stopped() {
        let missed = LiveEventsError::Missed { events: 12 };
        let stopped = LiveEventsError::EngineStopped;

        assert_eq!(
            missed.to_string(),
            "the listener fell behind, and missed 12 events of the engine"
        );
        assert_eq!(stopped.to_string(), "the engine stopped");
        assert!(missed.source().is_none());
        assert!(stopped.source().is_none());
    }
}
