//! Why an engine could not do what it was asked to do: one error for each thing that fails in its own way.

use gaslightmq_environment::BrokerListenersError;
use gaslightmq_field::Endpoint;
use gaslightmq_rule::RuleName;

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

/// Why a [`RuleStore`](crate::RuleStore) did not arm a rule.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = ArmError::AlreadyArmed { rule: "Leader flap".parse().expect("a rule name") };
/// assert_eq!(error.to_string(), "rule \"Leader flap\" is armed already");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArmError {
    /// The rule is armed, and arming it again would reset it. Whoever wants that disarms it first.
    #[error("rule {:?} is armed already", .rule.as_str())]
    AlreadyArmed { rule: RuleName },
}

/// Why a [`RuleStore`](crate::RuleStore) did not disarm a rule.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let error = DisarmError::NotArmed { rule: "Leader flap".parse().expect("a rule name") };
/// assert_eq!(error.to_string(), "rule \"Leader flap\" is not armed");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DisarmError {
    /// No rule of that name is armed: nobody armed it, someone disarmed it, or its lifetime ended.
    #[error("rule {:?} is not armed", .rule.as_str())]
    NotArmed { rule: RuleName },
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_environment::BrokerListeners;

    use super::*;

    fn rule() -> RuleName {
        "Leader flap".parse().unwrap()
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
    fn the_errors_of_the_rule_store_name_the_rule() {
        let already_armed = ArmError::AlreadyArmed { rule: rule() };
        let not_armed = DisarmError::NotArmed { rule: rule() };

        assert_eq!(already_armed.to_string(), "rule \"Leader flap\" is armed already");
        assert_eq!(not_armed.to_string(), "rule \"Leader flap\" is not armed");
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
