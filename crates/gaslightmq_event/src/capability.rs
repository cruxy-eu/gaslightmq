use std::collections::BTreeMap;
use std::fmt;

use crate::{EventKind, Requirement, Requirements};

/// What a backend does with one kind of event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Support {
    /// The backend applies the event: it really happens, and everything stays consistent.
    Applied,
    /// The backend emulates the event.
    /// Clients that go through GaslightMQ cannot tell, and the rest of the cluster does not change.
    Emulated,
    /// The backend can do neither.
    Unsupported,
}

impl Support {
    /// The name a file uses: `applied`, `emulated` or `unsupported`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Emulated => "emulated",
            Self::Unsupported => "unsupported",
        }
    }

    /// Whether the backend applies or emulates the event.
    pub const fn is_supported(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

/// Writes the name a file uses, such as `emulated`.
impl fmt::Display for Support {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// The events a backend can apply, the events it can only emulate, and so the events it cannot do.
///
/// A backend declares its capabilities once.
/// A kind it does not mention is unsupported.
///
/// A capability covers the consequences that are not events:
/// a backend that does "broker goes down" also reports the partitions as leaderless.
/// A consequence that is an event needs the capability of its own kind,
/// so a backend without "leader moves" does "broker goes down" without failover only.
///
/// ```
/// use gaslightmq_event::prelude::*;
///
/// let backend = Capabilities::none()
///     .applies([EventKind::RecordsAppended])
///     .emulates([EventKind::BrokerGoesDown, EventKind::BrokerComesBack]);
///
/// assert_eq!(backend.support(EventKind::BrokerGoesDown), Support::Emulated);
/// assert_eq!(backend.support(EventKind::LeaderMoves), Support::Unsupported);
///
/// // The scenario loads when the backend can do every event it needs.
/// let down = EventDefinition::BrokerGoesDown { broker: None, failover: false };
/// assert!(backend.check(&Requirements::from(&down)).is_ok());
///
/// // A failover moves leaders and coordinators, which this backend cannot do.
/// let failover = EventDefinition::BrokerGoesDown { broker: None, failover: true };
/// let unsupported = backend.check(&Requirements::from(&failover)).unwrap_err();
/// assert_eq!(
///     unsupported.to_string(),
///     "the backend cannot apply or emulate: leader-moves (a consequence of broker-goes-down), \
///      coordinator-moves (a consequence of broker-goes-down)",
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// Only the kinds the backend supports.
    supported: BTreeMap<EventKind, Support>,
}

impl Capabilities {
    /// No event at all: a backend that only answers calls.
    /// Lies need no capability, so every scenario that only lies still runs.
    pub fn none() -> Self {
        Self::default()
    }

    /// Declare the kinds of events the backend applies.
    /// The last declaration of a kind stands.
    pub fn applies(self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.declare(kinds, Support::Applied)
    }

    /// Declare the kinds of events the backend can only emulate.
    /// The last declaration of a kind stands.
    pub fn emulates(self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.declare(kinds, Support::Emulated)
    }

    pub(crate) fn declare(
        mut self,
        kinds: impl IntoIterator<Item = EventKind>,
        support: Support,
    ) -> Self {
        for kind in kinds {
            match support {
                Support::Unsupported => self.supported.remove(&kind),
                supported => self.supported.insert(kind, supported),
            };
        }
        self
    }

    /// What the backend does with an event of this kind.
    pub fn support(&self, kind: EventKind) -> Support {
        self.supported
            .get(&kind)
            .copied()
            .unwrap_or(Support::Unsupported)
    }

    /// Every kind with what the backend does with it, in the order of [`EventKind`].
    pub fn iter(&self) -> impl Iterator<Item = (EventKind, Support)> + '_ {
        EventKind::ALL
            .into_iter()
            .map(|kind| (kind, self.support(kind)))
    }

    /// Check that the backend applies or emulates every event the requirements name.
    ///
    /// The requirements include the consequences of each event,
    /// so the result also names a kind that only a consequence needs.
    pub fn check(&self, requirements: &Requirements) -> Result<(), UnsupportedEvents> {
        let events: Vec<Requirement> = requirements
            .iter()
            .filter(|requirement| !self.support(requirement.kind()).is_supported())
            .collect();
        if events.is_empty() {
            Ok(())
        } else {
            Err(UnsupportedEvents { events })
        }
    }
}

/// The events a scenario needs and its backend can neither apply nor emulate.
///
/// It names at least one event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedEvents {
    events: Vec<Requirement>,
}

impl UnsupportedEvents {
    /// The unsupported events, in the order of [`EventKind`].
    /// Each says whether a rule fires it, or which event has it as a consequence.
    pub fn events(&self) -> &[Requirement] {
        &self.events
    }
}

impl fmt::Display for UnsupportedEvents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the backend cannot apply or emulate: ")?;
        for (position, event) in self.events.iter().enumerate() {
            if position > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", event.kind())?;
            if let Some(cause) = event.consequence_of() {
                write!(f, " (a consequence of {cause})")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for UnsupportedEvents {}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::{Event, EventDefinition};

    /// The Upstream column of the capabilities table.
    fn upstream() -> Capabilities {
        Capabilities::none().emulates([
            EventKind::BrokerGoesDown,
            EventKind::BrokerGoesUnresponsive,
            EventKind::BrokerComesBack,
            EventKind::Rebalance,
            EventKind::RecordsExpired,
            EventKind::CommittedOffsetReset,
            EventKind::ProducerFenced,
        ])
    }

    /// The Simulated column of the capabilities table.
    /// "Producer fenced" comes later, with transactions.
    fn simulated() -> Capabilities {
        Capabilities::none().applies(
            EventKind::ALL
                .into_iter()
                .filter(|kind| *kind != EventKind::ProducerFenced),
        )
    }

    fn requirements(definitions: &[EventDefinition]) -> Requirements {
        definitions.iter().collect()
    }

    /// The unsupported events as pairs of a kind and the kind it is a consequence of.
    fn unsupported(
        backend: &Capabilities,
        definitions: &[EventDefinition],
    ) -> Vec<(EventKind, Option<EventKind>)> {
        match backend.check(&requirements(definitions)) {
            Ok(()) => vec![],
            Err(unsupported) => unsupported
                .events()
                .iter()
                .map(|event| (event.kind(), event.consequence_of()))
                .collect(),
        }
    }

    #[test]
    fn a_backend_supports_only_what_it_declares() {
        let nothing = Capabilities::none();
        assert_eq!(nothing, Capabilities::default());
        for kind in EventKind::ALL {
            assert_eq!(nothing.support(kind), Support::Unsupported);
        }
    }

    #[test]
    fn the_upstream_backend_emulates_what_its_overlay_can_hold() {
        use EventKind::*;
        use Support::*;
        let table: Vec<(EventKind, Support)> = upstream().iter().collect();
        assert_eq!(
            table,
            [
                (LeaderMoves, Unsupported),
                (BrokerGoesDown, Emulated),
                (BrokerGoesUnresponsive, Emulated),
                (BrokerComesBack, Emulated),
                (Rebalance, Emulated),
                (CoordinatorMoves, Unsupported),
                (PartitionAdded, Unsupported),
                (TopicDeleted, Unsupported),
                (RecordsAppended, Unsupported),
                (RecordsExpired, Emulated),
                (LogTruncated, Unsupported),
                (CommittedOffsetReset, Emulated),
                (ProducerFenced, Emulated),
            ]
        );
    }

    #[test]
    fn the_simulated_backend_applies_every_event_but_producer_fenced() {
        for (kind, support) in simulated().iter() {
            let expected = if kind == EventKind::ProducerFenced {
                Support::Unsupported
            } else {
                Support::Applied
            };
            assert_eq!(support, expected, "{kind}");
        }
    }

    #[test]
    fn the_last_declaration_of_a_kind_stands() {
        let backend = Capabilities::none()
            .applies([EventKind::Rebalance])
            .emulates([EventKind::Rebalance]);
        assert_eq!(backend.support(EventKind::Rebalance), Support::Emulated);
        assert_eq!(
            backend,
            Capabilities::none().emulates([EventKind::Rebalance])
        );
    }

    #[test]
    fn a_scenario_that_only_lies_runs_on_every_backend() {
        for backend in [Capabilities::none(), upstream(), simulated()] {
            assert_eq!(backend.check(&Requirements::none()), Ok(()));
        }
    }

    #[test]
    fn the_leader_election_scenario_runs_on_the_simulated_backend_only() {
        let rules = [EventDefinition::LeaderMoves {
            partition: Some("orders.eu/3".parse().unwrap()),
            to: None,
        }];

        assert_eq!(unsupported(&simulated(), &rules), []);
        assert_eq!(
            unsupported(&upstream(), &rules),
            [(EventKind::LeaderMoves, None)]
        );
    }

    #[test]
    fn the_upstream_backend_emulates_a_broker_that_goes_down_without_failover() {
        let down = [EventDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: false,
        }];
        assert_eq!(unsupported(&upstream(), &down), []);

        // "Broker goes down" with `failover: true` needs "leader moves".
        let failover = [EventDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: true,
        }];
        assert_eq!(
            unsupported(&upstream(), &failover),
            [
                (EventKind::LeaderMoves, Some(EventKind::BrokerGoesDown)),
                (EventKind::CoordinatorMoves, Some(EventKind::BrokerGoesDown)),
            ]
        );
        assert_eq!(unsupported(&simulated(), &failover), []);
    }

    #[test]
    fn a_backend_names_every_unsupported_event_of_a_scenario() {
        let rules = [
            EventDefinition::TopicDeleted { topic: None },
            EventDefinition::Rebalance { group: None },
            EventDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            EventDefinition::BrokerGoesDown {
                broker: None,
                failover: true,
            },
            EventDefinition::LeaderMoves {
                partition: None,
                to: None,
            },
            EventDefinition::BrokerComesBack { broker: None },
        ];
        assert_eq!(
            unsupported(&upstream(), &rules),
            [
                (EventKind::LeaderMoves, None),
                (EventKind::CoordinatorMoves, Some(EventKind::BrokerGoesDown)),
                (EventKind::TopicDeleted, None),
                (EventKind::LogTruncated, None),
            ]
        );
    }

    #[test]
    fn the_simulated_backend_fences_no_producer_yet() {
        let rules = [EventDefinition::ProducerFenced {
            transactional_id: None,
        }];
        assert_eq!(
            unsupported(&simulated(), &rules),
            [(EventKind::ProducerFenced, None)]
        );
        assert_eq!(unsupported(&upstream(), &rules), []);
    }

    #[test]
    fn a_backend_checks_an_event_that_fires_the_same_way() {
        let down = Event::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: true,
        };
        assert!(upstream().check(&Requirements::from(&down)).is_err());
        assert_eq!(simulated().check(&Requirements::from(&down)), Ok(()));
    }

    #[test]
    fn the_rejection_lists_the_unsupported_events() {
        let rules = [
            EventDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            EventDefinition::BrokerGoesDown {
                broker: None,
                failover: true,
            },
        ];
        let rejection = upstream().check(&requirements(&rules)).unwrap_err();
        assert_eq!(
            rejection.to_string(),
            "the backend cannot apply or emulate: leader-moves (a consequence of broker-goes-down), \
             coordinator-moves (a consequence of broker-goes-down), log-truncated",
        );
    }

    #[test]
    fn support_has_a_name_for_files() {
        assert_eq!(Support::Applied.to_string(), "applied");
        assert_eq!(Support::Emulated.to_string(), "emulated");
        assert_eq!(Support::Unsupported.to_string(), "unsupported");
        assert!(Support::Applied.is_supported());
        assert!(Support::Emulated.is_supported());
        assert!(!Support::Unsupported.is_supported());
    }
}
