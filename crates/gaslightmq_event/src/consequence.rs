use gaslightmq_kernel::prelude::*;

use crate::{Coordinator, Event, EventKind};

/// What an expansion has to know about the cluster: who leads what, and who hosts which coordinator.
///
/// This crate does not own the topology.
/// The World knows it for a simulated cluster, and the overlay learns it from the real cluster's answers.
/// Each implements this trait over the state it already holds.
///
/// The order of the answers does not matter, and neither do duplicates.
pub trait Topology {
    /// The partitions the broker leads.
    fn partitions_led_by(&self, broker: BrokerId) -> Vec<TopicPartition>;

    /// The coordinators the broker hosts, of groups and of transactional producers.
    fn coordinators_hosted_by(&self, broker: BrokerId) -> Vec<Coordinator>;
}

/// What an event brings about besides its own effect.
///
/// The cluster does not react to an event by itself.
/// Whatever follows from an event is a consequence that the engine adds,
/// so both backends see the same list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Consequence {
    /// The partition has no leader while the broker that led it is down.
    Leaderless { partition: TopicPartition },
    /// The coordinator is unavailable while the broker that hosts it is down.
    CoordinatorUnavailable { coordinator: Coordinator },
    /// Another event happens: a "leader moves" or a "coordinator moves".
    Event(Event),
}

/// An event with its consequences: what a backend applies or emulates, and what the journal records.
///
/// [`Event::expand`] is the only way to make one, apart from reading back one that was written.
/// The consequences are therefore always those of the event.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExpandedEvent {
    pub(crate) event: Event,
    pub(crate) consequences: Vec<Consequence>,
}

impl ExpandedEvent {
    /// The event that fired.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// What the event brings about: first the partitions in their order, then the coordinators in theirs.
    pub fn consequences(&self) -> &[Consequence] {
        &self.consequences
    }

    /// The event and its consequences, to take apart.
    pub fn into_parts(self) -> (Event, Vec<Consequence>) {
        (self.event, self.consequences)
    }
}

impl Event {
    /// Add the consequences this event has in a cluster with this topology.
    ///
    /// | Event | Consequences |
    /// |---|---|
    /// | Broker goes down | Every partition the broker led has no leader. Every coordinator it hosted is unavailable. |
    /// | Broker goes down, with failover | Every partition the broker led gets a "leader moves" event. Every coordinator it hosted gets a "coordinator moves" event. |
    /// | Every other event | None. |
    ///
    /// A move that follows from a failover names no broker, so the backend elects one.
    /// The same event and the same topology always give the same list, in the same order.
    ///
    /// ```
    /// use gaslightmq_event::prelude::*;
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// /// Broker 2 leads `orders.eu/3` and hosts the coordinator of `payments`.
    /// struct Cluster;
    ///
    /// impl Topology for Cluster {
    ///     fn partitions_led_by(&self, broker: BrokerId) -> Vec<TopicPartition> {
    ///         if broker == BrokerId::from(2) { vec!["orders.eu/3".parse().unwrap()] } else { vec![] }
    ///     }
    ///
    ///     fn coordinators_hosted_by(&self, broker: BrokerId) -> Vec<Coordinator> {
    ///         if broker == BrokerId::from(2) { vec![GroupId::new("payments").unwrap().into()] } else { vec![] }
    ///     }
    /// }
    ///
    /// let down = Event::BrokerGoesDown { broker: BrokerId::from(2), failover: false };
    /// assert_eq!(
    ///     down.expand(&Cluster).consequences(),
    ///     [
    ///         Consequence::Leaderless { partition: "orders.eu/3".parse()? },
    ///         Consequence::CoordinatorUnavailable { coordinator: GroupId::new("payments")?.into() },
    ///     ]
    /// );
    /// # Ok::<(), KernelError>(())
    /// ```
    pub fn expand<T: Topology + ?Sized>(self, topology: &T) -> ExpandedEvent {
        let consequences = match &self {
            Self::BrokerGoesDown { broker, failover } => {
                let mut partitions = topology.partitions_led_by(*broker);
                partitions.sort();
                partitions.dedup();
                let mut coordinators = topology.coordinators_hosted_by(*broker);
                coordinators.sort();
                coordinators.dedup();

                if *failover {
                    let leaders = partitions
                        .into_iter()
                        .map(|partition| Consequence::Event(Self::LeaderMoves { partition, to: None }));
                    let coordinators = coordinators
                        .into_iter()
                        .map(|coordinator| Consequence::Event(Self::CoordinatorMoves { coordinator, to: None }));
                    leaders.chain(coordinators).collect()
                } else {
                    let leaders = partitions
                        .into_iter()
                        .map(|partition| Consequence::Leaderless { partition });
                    let coordinators = coordinators
                        .into_iter()
                        .map(|coordinator| Consequence::CoordinatorUnavailable { coordinator });
                    leaders.chain(coordinators).collect()
                }
            }
            _ => Vec::new(),
        };
        ExpandedEvent {
            event: self,
            consequences,
        }
    }
}

/// The kinds of the events that an event of this kind can have as consequences.
///
/// It needs no topology: a failover needs "leader moves" whether or not the broker leads a partition today.
/// [`Event::expand`] never adds an event of another kind.
pub(crate) fn kinds_of_consequences(kind: EventKind, failover: bool) -> &'static [EventKind] {
    match kind {
        EventKind::BrokerGoesDown if failover => &[EventKind::LeaderMoves, EventKind::CoordinatorMoves],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PartitionCount, RecordCount};

    /// A cluster of three brokers.
    /// Broker 2 leads three partitions and hosts two coordinators.
    struct Cluster;

    impl Topology for Cluster {
        fn partitions_led_by(&self, broker: BrokerId) -> Vec<TopicPartition> {
            let partitions: &[&str] = match broker.value() {
                1 => &["orders.eu/0", "payments.settled/1"],
                // Out of order and with a duplicate, as a map of a World may give them.
                2 => &["payments.settled/0", "orders.eu/3", "orders.eu/1", "orders.eu/3"],
                _ => &[],
            };
            partitions.iter().map(|partition| partition.parse().unwrap()).collect()
        }

        fn coordinators_hosted_by(&self, broker: BrokerId) -> Vec<Coordinator> {
            match broker.value() {
                2 => vec![transaction("payments-tx-1"), group("payments"), group("analytics-etl")],
                _ => vec![],
            }
        }
    }

    fn partition(text: &str) -> TopicPartition {
        text.parse().unwrap()
    }

    fn group(text: &str) -> Coordinator {
        GroupId::new(text).unwrap().into()
    }

    fn transaction(text: &str) -> Coordinator {
        TransactionalId::new(text).unwrap().into()
    }

    /// One event of every kind, about broker 2 and what it leads and hosts.
    fn every_event(failover: bool) -> [Event; 13] {
        let broker = BrokerId::from(2);
        let topic = TopicName::new("orders.eu").unwrap();
        [
            Event::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: Some(BrokerId::from(1)),
            },
            Event::BrokerGoesDown { broker, failover },
            Event::BrokerGoesUnresponsive { broker },
            Event::BrokerComesBack { broker },
            Event::Rebalance {
                group: GroupId::new("payments").unwrap(),
            },
            Event::CoordinatorMoves {
                coordinator: group("payments"),
                to: None,
            },
            Event::PartitionAdded {
                topic: topic.clone(),
                count: PartitionCount::ONE,
            },
            Event::TopicDeleted { topic },
            Event::RecordsAppended {
                partition: partition("orders.eu/3"),
                count: RecordCount::ONE,
            },
            Event::RecordsExpired {
                partition: partition("orders.eu/3"),
                to: Offset::from(5000),
            },
            Event::LogTruncated {
                partition: partition("orders.eu/3"),
                to: Offset::from(40),
            },
            Event::CommittedOffsetReset {
                group: GroupId::new("payments").unwrap(),
                partition: partition("orders.eu/3"),
                to: Offset::from(0),
            },
            Event::ProducerFenced {
                transactional_id: TransactionalId::new("payments-tx-1").unwrap(),
            },
        ]
    }

    #[test]
    fn a_broker_that_goes_down_leaves_its_partitions_leaderless_and_its_coordinators_unavailable() {
        let down = Event::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: false,
        };
        let expanded = down.clone().expand(&Cluster);

        assert_eq!(expanded.event(), &down);
        assert_eq!(
            expanded.consequences(),
            [
                Consequence::Leaderless {
                    partition: partition("orders.eu/1")
                },
                Consequence::Leaderless {
                    partition: partition("orders.eu/3")
                },
                Consequence::Leaderless {
                    partition: partition("payments.settled/0")
                },
                Consequence::CoordinatorUnavailable {
                    coordinator: group("analytics-etl")
                },
                Consequence::CoordinatorUnavailable {
                    coordinator: group("payments")
                },
                Consequence::CoordinatorUnavailable {
                    coordinator: transaction("payments-tx-1")
                },
            ]
        );
    }

    #[test]
    fn a_broker_that_goes_down_with_failover_moves_its_leaders_and_its_coordinators() {
        let down = Event::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: true,
        };
        let leader_moves = |text: &str| {
            Consequence::Event(Event::LeaderMoves {
                partition: partition(text),
                to: None,
            })
        };
        let coordinator_moves =
            |coordinator: Coordinator| Consequence::Event(Event::CoordinatorMoves { coordinator, to: None });

        assert_eq!(
            down.expand(&Cluster).consequences(),
            [
                leader_moves("orders.eu/1"),
                leader_moves("orders.eu/3"),
                leader_moves("payments.settled/0"),
                coordinator_moves(group("analytics-etl")),
                coordinator_moves(group("payments")),
                coordinator_moves(transaction("payments-tx-1")),
            ]
        );
    }

    #[test]
    fn a_broker_that_leads_and_hosts_nothing_goes_down_without_consequences() {
        for failover in [false, true] {
            let down = Event::BrokerGoesDown {
                broker: BrokerId::from(3),
                failover,
            };
            assert_eq!(down.expand(&Cluster).consequences(), []);
        }
    }

    #[test]
    fn a_broker_that_comes_back_takes_nothing_back() {
        // "Leaders and coordinators stay where they are."
        let back = Event::BrokerComesBack {
            broker: BrokerId::from(2),
        };
        assert_eq!(back.expand(&Cluster).consequences(), []);
    }

    #[test]
    fn every_other_event_has_no_consequences() {
        for event in every_event(false) {
            if event.kind() == EventKind::BrokerGoesDown {
                continue;
            }
            let expanded = event.clone().expand(&Cluster);
            assert_eq!(expanded.consequences(), [], "{event:?}");
            assert_eq!(expanded.into_parts(), (event, vec![]));
        }
    }

    #[test]
    fn a_consequence_has_no_consequences_of_its_own() {
        let down = Event::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: true,
        };
        for consequence in down.expand(&Cluster).consequences() {
            let Consequence::Event(event) = consequence else {
                panic!("{consequence:?} is not an event")
            };
            assert_eq!(event.clone().expand(&Cluster).consequences(), []);
        }
    }

    #[test]
    fn an_expansion_only_adds_events_of_the_kinds_the_event_can_have() {
        for failover in [false, true] {
            for event in every_event(failover) {
                let allowed = kinds_of_consequences(event.kind(), failover);
                for consequence in event.expand(&Cluster).consequences() {
                    if let Consequence::Event(event) = consequence {
                        assert!(allowed.contains(&event.kind()), "{event:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn an_expansion_takes_any_topology() {
        let topology: &dyn Topology = &Cluster;
        let down = Event::BrokerGoesDown {
            broker: BrokerId::from(1),
            failover: false,
        };
        assert_eq!(
            down.expand(topology).consequences(),
            [
                Consequence::Leaderless {
                    partition: partition("orders.eu/0")
                },
                Consequence::Leaderless {
                    partition: partition("payments.settled/1")
                },
            ]
        );
    }
}
