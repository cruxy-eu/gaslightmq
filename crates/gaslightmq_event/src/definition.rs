use std::collections::BTreeSet;

use gaslightmq_kernel::prelude::*;

use crate::{Coordinator, Event, EventError, EventKind, PartitionCount, RecordCount};

/// An event as a rule's action or a preset writes it.
///
/// It has the thirteen variants of [`Event`], with the same parameters.
/// The difference is the subject: the partition, broker, group, topic or producer the event changes.
/// A definition may leave its subject out, and then the event is about the call that fires the rule.
/// The preset `event: leader-moves` moves the leader of whichever partition that call targets.
///
/// [`resolve`](Self::resolve) turns a definition into the events that happen.
///
/// ```
/// use gaslightmq_event::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// // "When the 3rd Produce arrives, move the leader."
/// let preset = EventDefinition::LeaderMoves { partition: None, to: None };
///
/// // That Produce targets two partitions, so two leaders move.
/// let produce = Subjects::new().with_partition("orders.eu/3".parse()?).with_partition("orders.eu/4".parse()?);
/// let events = preset.resolve(&produce)?;
/// assert_eq!(
///     events,
///     [
///         Event::LeaderMoves { partition: "orders.eu/3".parse()?, to: None },
///         Event::LeaderMoves { partition: "orders.eu/4".parse()?, to: None },
///     ]
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventDefinition {
    /// See [`Event::LeaderMoves`].
    LeaderMoves {
        partition: Option<TopicPartition>,
        to: Option<BrokerId>,
    },
    /// See [`Event::BrokerGoesDown`].
    BrokerGoesDown {
        broker: Option<BrokerId>,
        failover: bool,
    },
    /// See [`Event::BrokerGoesUnresponsive`].
    BrokerGoesUnresponsive { broker: Option<BrokerId> },
    /// See [`Event::BrokerComesBack`].
    BrokerComesBack { broker: Option<BrokerId> },
    /// See [`Event::Rebalance`].
    Rebalance { group: Option<GroupId> },
    /// See [`Event::CoordinatorMoves`].
    /// Without a coordinator, the coordinator of every group and transactional ID of the call moves.
    CoordinatorMoves {
        coordinator: Option<Coordinator>,
        to: Option<BrokerId>,
    },
    /// See [`Event::PartitionAdded`].
    PartitionAdded {
        topic: Option<TopicName>,
        count: PartitionCount,
    },
    /// See [`Event::TopicDeleted`].
    TopicDeleted { topic: Option<TopicName> },
    /// See [`Event::RecordsAppended`].
    RecordsAppended {
        partition: Option<TopicPartition>,
        count: RecordCount,
    },
    /// See [`Event::RecordsExpired`].
    RecordsExpired {
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Event::LogTruncated`].
    LogTruncated {
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Event::CommittedOffsetReset`].
    CommittedOffsetReset {
        group: Option<GroupId>,
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Event::ProducerFenced`].
    ProducerFenced {
        transactional_id: Option<TransactionalId>,
    },
}

impl EventDefinition {
    /// Which of the thirteen events this defines.
    pub fn kind(&self) -> EventKind {
        match self {
            Self::LeaderMoves { .. } => EventKind::LeaderMoves,
            Self::BrokerGoesDown { .. } => EventKind::BrokerGoesDown,
            Self::BrokerGoesUnresponsive { .. } => EventKind::BrokerGoesUnresponsive,
            Self::BrokerComesBack { .. } => EventKind::BrokerComesBack,
            Self::Rebalance { .. } => EventKind::Rebalance,
            Self::CoordinatorMoves { .. } => EventKind::CoordinatorMoves,
            Self::PartitionAdded { .. } => EventKind::PartitionAdded,
            Self::TopicDeleted { .. } => EventKind::TopicDeleted,
            Self::RecordsAppended { .. } => EventKind::RecordsAppended,
            Self::RecordsExpired { .. } => EventKind::RecordsExpired,
            Self::LogTruncated { .. } => EventKind::LogTruncated,
            Self::CommittedOffsetReset { .. } => EventKind::CommittedOffsetReset,
            Self::ProducerFenced { .. } => EventKind::ProducerFenced,
        }
    }

    /// Whether a broker that goes down hands its partitions and coordinators to another broker.
    pub(crate) fn failover(&self) -> bool {
        matches!(self, Self::BrokerGoesDown { failover: true, .. })
    }

    /// The events that happen when a rule with this action fires on a call about `subjects`.
    ///
    /// A subject the definition names stands, whatever the call is about.
    /// A subject it leaves out comes from the call, and every value the call offers gives one event:
    /// a call that targets three partitions moves three leaders.
    /// The events come in the order of their subjects.
    ///
    /// The result is never empty.
    /// When the definition leaves a subject out and the call offers none,
    /// the result is [`EventError::MissingSubject`].
    /// A rule that someone fires by hand has no call, so it resolves against [`Subjects::new`].
    pub fn resolve(&self, subjects: &Subjects) -> Result<Vec<Event>, EventError> {
        let kind = self.kind();
        let events = match self {
            Self::LeaderMoves { partition, to } => subjects
                .partitions(kind, partition)?
                .into_iter()
                .map(|partition| Event::LeaderMoves { partition, to: *to })
                .collect(),
            Self::BrokerGoesDown { broker, failover } => subjects
                .brokers(kind, broker)?
                .into_iter()
                .map(|broker| Event::BrokerGoesDown {
                    broker,
                    failover: *failover,
                })
                .collect(),
            Self::BrokerGoesUnresponsive { broker } => subjects
                .brokers(kind, broker)?
                .into_iter()
                .map(|broker| Event::BrokerGoesUnresponsive { broker })
                .collect(),
            Self::BrokerComesBack { broker } => subjects
                .brokers(kind, broker)?
                .into_iter()
                .map(|broker| Event::BrokerComesBack { broker })
                .collect(),
            Self::Rebalance { group } => subjects
                .groups(kind, group)?
                .into_iter()
                .map(|group| Event::Rebalance { group })
                .collect(),
            Self::CoordinatorMoves { coordinator, to } => subjects
                .coordinators(kind, coordinator)?
                .into_iter()
                .map(|coordinator| Event::CoordinatorMoves {
                    coordinator,
                    to: *to,
                })
                .collect(),
            Self::PartitionAdded { topic, count } => subjects
                .topics(kind, topic)?
                .into_iter()
                .map(|topic| Event::PartitionAdded {
                    topic,
                    count: *count,
                })
                .collect(),
            Self::TopicDeleted { topic } => subjects
                .topics(kind, topic)?
                .into_iter()
                .map(|topic| Event::TopicDeleted { topic })
                .collect(),
            Self::RecordsAppended { partition, count } => subjects
                .partitions(kind, partition)?
                .into_iter()
                .map(|partition| Event::RecordsAppended {
                    partition,
                    count: *count,
                })
                .collect(),
            Self::RecordsExpired { partition, to } => subjects
                .partitions(kind, partition)?
                .into_iter()
                .map(|partition| Event::RecordsExpired { partition, to: *to })
                .collect(),
            Self::LogTruncated { partition, to } => subjects
                .partitions(kind, partition)?
                .into_iter()
                .map(|partition| Event::LogTruncated { partition, to: *to })
                .collect(),
            Self::CommittedOffsetReset {
                group,
                partition,
                to,
            } => {
                let partitions = subjects.partitions(kind, partition)?;
                subjects
                    .groups(kind, group)?
                    .into_iter()
                    .flat_map(|group| {
                        partitions
                            .iter()
                            .map(move |partition| Event::CommittedOffsetReset {
                                group: group.clone(),
                                partition: partition.clone(),
                                to: *to,
                            })
                    })
                    .collect()
            }
            Self::ProducerFenced { transactional_id } => subjects
                .transactional_ids(kind, transactional_id)?
                .into_iter()
                .map(|transactional_id| Event::ProducerFenced { transactional_id })
                .collect(),
        };
        Ok(events)
    }
}

/// An event is a definition that names every subject.
impl From<Event> for EventDefinition {
    fn from(event: Event) -> Self {
        match event {
            Event::LeaderMoves { partition, to } => Self::LeaderMoves {
                partition: Some(partition),
                to,
            },
            Event::BrokerGoesDown { broker, failover } => Self::BrokerGoesDown {
                broker: Some(broker),
                failover,
            },
            Event::BrokerGoesUnresponsive { broker } => Self::BrokerGoesUnresponsive {
                broker: Some(broker),
            },
            Event::BrokerComesBack { broker } => Self::BrokerComesBack {
                broker: Some(broker),
            },
            Event::Rebalance { group } => Self::Rebalance { group: Some(group) },
            Event::CoordinatorMoves { coordinator, to } => Self::CoordinatorMoves {
                coordinator: Some(coordinator),
                to,
            },
            Event::PartitionAdded { topic, count } => Self::PartitionAdded {
                topic: Some(topic),
                count,
            },
            Event::TopicDeleted { topic } => Self::TopicDeleted { topic: Some(topic) },
            Event::RecordsAppended { partition, count } => Self::RecordsAppended {
                partition: Some(partition),
                count,
            },
            Event::RecordsExpired { partition, to } => Self::RecordsExpired {
                partition: Some(partition),
                to,
            },
            Event::LogTruncated { partition, to } => Self::LogTruncated {
                partition: Some(partition),
                to,
            },
            Event::CommittedOffsetReset {
                group,
                partition,
                to,
            } => Self::CommittedOffsetReset {
                group: Some(group),
                partition: Some(partition),
                to,
            },
            Event::ProducerFenced { transactional_id } => Self::ProducerFenced {
                transactional_id: Some(transactional_id),
            },
        }
    }
}

/// A definition that names every subject is one event.
/// One that leaves a subject out is an [`EventError::MissingSubject`].
impl TryFrom<EventDefinition> for Event {
    type Error = EventError;

    fn try_from(definition: EventDefinition) -> Result<Self, Self::Error> {
        let mut events = definition.resolve(&Subjects::new())?;
        // Without a call, every subject comes from the definition, and each gives exactly one value.
        Ok(events.swap_remove(0))
    }
}

/// What a call is about: the subjects it offers to an [`EventDefinition`] that names none.
///
/// The pipeline builds it for the call that fires a rule,
/// from the rule's target and from the call's fields.
///
/// | Subject | Comes from |
/// |---|---|
/// | Partitions | The partitions the rule's selector matched. |
/// | Topics | The topics of those partitions, and the topics a call names without a partition. |
/// | Broker | The broker whose endpoint the call arrived on. |
/// | Groups | The `group` field. |
/// | Transactional IDs | The `transactional.id` field. |
///
/// ```
/// use gaslightmq_event::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let heartbeat = Subjects::new().with_broker(BrokerId::from(2)).with_group(GroupId::new("analytics-etl")?);
///
/// let rebalance = EventDefinition::Rebalance { group: None };
/// assert_eq!(rebalance.resolve(&heartbeat)?, [Event::Rebalance { group: GroupId::new("analytics-etl")? }]);
///
/// // A Heartbeat names no partition.
/// let leader_moves = EventDefinition::LeaderMoves { partition: None, to: None };
/// assert_eq!(
///     leader_moves.resolve(&heartbeat),
///     Err(EventError::MissingSubject { event: EventKind::LeaderMoves, subject: "partition" }),
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subjects {
    partitions: BTreeSet<TopicPartition>,
    topics: BTreeSet<TopicName>,
    broker: Option<BrokerId>,
    groups: BTreeSet<GroupId>,
    transactional_ids: BTreeSet<TransactionalId>,
}

impl Subjects {
    /// No subject at all, as when someone fires a rule by hand.
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer a partition, and with it its topic.
    pub fn with_partition(mut self, partition: TopicPartition) -> Self {
        self.topics.insert(partition.topic().clone());
        self.partitions.insert(partition);
        self
    }

    /// Offer a topic that the call names without a partition.
    pub fn with_topic(mut self, topic: TopicName) -> Self {
        self.topics.insert(topic);
        self
    }

    /// Offer the broker the call arrived on.
    /// A call arrives on one broker, so a second broker replaces the first.
    pub fn with_broker(mut self, broker: BrokerId) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Offer a consumer group.
    pub fn with_group(mut self, group: GroupId) -> Self {
        self.groups.insert(group);
        self
    }

    /// Offer a transactional ID.
    pub fn with_transactional_id(mut self, transactional_id: TransactionalId) -> Self {
        self.transactional_ids.insert(transactional_id);
        self
    }

    fn partitions(
        &self,
        event: EventKind,
        named: &Option<TopicPartition>,
    ) -> Result<Vec<TopicPartition>, EventError> {
        named_or_offered(event, "partition", named, &self.partitions)
    }

    fn topics(
        &self,
        event: EventKind,
        named: &Option<TopicName>,
    ) -> Result<Vec<TopicName>, EventError> {
        named_or_offered(event, "topic", named, &self.topics)
    }

    fn brokers(
        &self,
        event: EventKind,
        named: &Option<BrokerId>,
    ) -> Result<Vec<BrokerId>, EventError> {
        named_or_offered(event, "broker", named, &self.broker)
    }

    fn groups(
        &self,
        event: EventKind,
        named: &Option<GroupId>,
    ) -> Result<Vec<GroupId>, EventError> {
        named_or_offered(event, "group", named, &self.groups)
    }

    fn transactional_ids(
        &self,
        event: EventKind,
        named: &Option<TransactionalId>,
    ) -> Result<Vec<TransactionalId>, EventError> {
        named_or_offered(event, "transactional ID", named, &self.transactional_ids)
    }

    fn coordinators(
        &self,
        event: EventKind,
        named: &Option<Coordinator>,
    ) -> Result<Vec<Coordinator>, EventError> {
        let offered: Vec<Coordinator> = (self.groups.iter().cloned().map(Coordinator::Group))
            .chain(
                self.transactional_ids
                    .iter()
                    .cloned()
                    .map(Coordinator::Transaction),
            )
            .collect();
        named_or_offered(event, "group or transactional ID", named, &offered)
    }
}

/// The subject the definition names, or else every subject the call offers.
fn named_or_offered<'a, T: Clone + 'a>(
    event: EventKind,
    subject: &'static str,
    named: &Option<T>,
    offered: impl IntoIterator<Item = &'a T>,
) -> Result<Vec<T>, EventError> {
    let subjects: Vec<T> = match named {
        Some(named) => vec![named.clone()],
        None => offered.into_iter().cloned().collect(),
    };
    if subjects.is_empty() {
        return Err(EventError::MissingSubject { event, subject });
    }
    Ok(subjects)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(text: &str) -> TopicPartition {
        text.parse().unwrap()
    }

    fn topic(text: &str) -> TopicName {
        TopicName::new(text).unwrap()
    }

    fn group(text: &str) -> GroupId {
        GroupId::new(text).unwrap()
    }

    fn transactional_id(text: &str) -> TransactionalId {
        TransactionalId::new(text).unwrap()
    }

    /// One definition of every kind that names no subject.
    fn every_definition_without_subject() -> [EventDefinition; 13] {
        [
            EventDefinition::LeaderMoves {
                partition: None,
                to: None,
            },
            EventDefinition::BrokerGoesDown {
                broker: None,
                failover: false,
            },
            EventDefinition::BrokerGoesUnresponsive { broker: None },
            EventDefinition::BrokerComesBack { broker: None },
            EventDefinition::Rebalance { group: None },
            EventDefinition::CoordinatorMoves {
                coordinator: None,
                to: None,
            },
            EventDefinition::PartitionAdded {
                topic: None,
                count: PartitionCount::ONE,
            },
            EventDefinition::TopicDeleted { topic: None },
            EventDefinition::RecordsAppended {
                partition: None,
                count: RecordCount::ONE,
            },
            EventDefinition::RecordsExpired {
                partition: None,
                to: Offset::from(5000),
            },
            EventDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            EventDefinition::CommittedOffsetReset {
                group: None,
                partition: None,
                to: Offset::from(0),
            },
            EventDefinition::ProducerFenced {
                transactional_id: None,
            },
        ]
    }

    /// A call that offers every kind of subject once.
    fn a_call_about_everything() -> Subjects {
        Subjects::new()
            .with_partition(partition("orders.eu/3"))
            .with_broker(BrokerId::from(2))
            .with_group(group("payments"))
            .with_transactional_id(transactional_id("payments-tx-1"))
    }

    #[test]
    fn a_definition_and_its_events_are_of_the_same_kind() {
        let definitions = every_definition_without_subject();
        let kinds: Vec<EventKind> = definitions.iter().map(EventDefinition::kind).collect();
        assert_eq!(kinds, EventKind::ALL);

        for definition in definitions {
            for event in definition.resolve(&a_call_about_everything()).unwrap() {
                assert_eq!(event.kind(), definition.kind());
            }
        }
    }

    #[test]
    fn a_named_subject_needs_no_call() {
        // The rule of the "Leader election" scenario.
        let definition = EventDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: None,
        };
        assert_eq!(
            definition.resolve(&Subjects::new()),
            Ok(vec![Event::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: None
            }]),
        );
    }

    #[test]
    fn a_named_subject_stands_whatever_the_call_is_about() {
        let definition = EventDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: Some(BrokerId::from(1)),
        };
        let call = Subjects::new().with_partition(partition("payments.settled/0"));
        assert_eq!(
            definition.resolve(&call),
            Ok(vec![Event::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: Some(BrokerId::from(1))
            }]),
        );
    }

    #[test]
    fn a_subject_that_is_left_out_comes_from_the_call() {
        // The "Leader election" preset: `event: leader-moves`.
        let preset = EventDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        let call = Subjects::new().with_partition(partition("orders.eu/3"));
        assert_eq!(
            preset.resolve(&call),
            Ok(vec![Event::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: None
            }]),
        );
    }

    #[test]
    fn every_subject_of_the_call_gives_one_event_in_order() {
        let preset = EventDefinition::RecordsExpired {
            partition: None,
            to: Offset::from(5000),
        };
        let call = Subjects::new()
            .with_partition(partition("orders.us/0"))
            .with_partition(partition("orders.eu/4"))
            .with_partition(partition("orders.eu/3"))
            .with_partition(partition("orders.eu/3"));
        assert_eq!(
            preset.resolve(&call),
            Ok(vec![
                Event::RecordsExpired {
                    partition: partition("orders.eu/3"),
                    to: Offset::from(5000)
                },
                Event::RecordsExpired {
                    partition: partition("orders.eu/4"),
                    to: Offset::from(5000)
                },
                Event::RecordsExpired {
                    partition: partition("orders.us/0"),
                    to: Offset::from(5000)
                },
            ]),
        );
    }

    #[test]
    fn a_subject_that_nobody_names_is_an_error() {
        let subjects = [
            "partition",
            "broker",
            "broker",
            "broker",
            "group",
            "group or transactional ID",
            "topic",
            "topic",
            "partition",
            "partition",
            "partition",
            "partition",
            "transactional ID",
        ];
        for (definition, subject) in every_definition_without_subject().into_iter().zip(subjects) {
            assert_eq!(
                definition.resolve(&Subjects::new()),
                Err(EventError::MissingSubject {
                    event: definition.kind(),
                    subject
                }),
            );
        }
    }

    #[test]
    fn a_call_offers_the_subjects_of_its_own_kind_only() {
        let produce = Subjects::new()
            .with_partition(partition("orders.eu/3"))
            .with_broker(BrokerId::from(2));
        assert_eq!(
            EventDefinition::Rebalance { group: None }.resolve(&produce),
            Err(EventError::MissingSubject {
                event: EventKind::Rebalance,
                subject: "group"
            }),
        );
        assert_eq!(
            EventDefinition::BrokerGoesDown {
                broker: None,
                failover: true
            }
            .resolve(&produce),
            Ok(vec![Event::BrokerGoesDown {
                broker: BrokerId::from(2),
                failover: true
            }]),
        );
    }

    #[test]
    fn a_partition_offers_its_topic() {
        let call = Subjects::new()
            .with_partition(partition("orders.eu/3"))
            .with_partition(partition("orders.eu/4"))
            .with_topic(topic("payments.settled"));
        assert_eq!(
            EventDefinition::TopicDeleted { topic: None }.resolve(&call),
            Ok(vec![
                Event::TopicDeleted {
                    topic: topic("orders.eu")
                },
                Event::TopicDeleted {
                    topic: topic("payments.settled")
                },
            ]),
        );

        let count = PartitionCount::new(2).unwrap();
        assert_eq!(
            EventDefinition::PartitionAdded {
                topic: Some(topic("orders.us")),
                count
            }
            .resolve(&call),
            Ok(vec![Event::PartitionAdded {
                topic: topic("orders.us"),
                count
            }]),
        );
    }

    #[test]
    fn a_topic_without_partition_offers_no_partition() {
        let metadata = Subjects::new().with_topic(topic("orders.eu"));
        assert_eq!(
            EventDefinition::RecordsAppended {
                partition: None,
                count: RecordCount::ONE
            }
            .resolve(&metadata),
            Err(EventError::MissingSubject {
                event: EventKind::RecordsAppended,
                subject: "partition"
            }),
        );
    }

    #[test]
    fn a_call_arrives_on_one_broker() {
        let call = Subjects::new()
            .with_broker(BrokerId::from(1))
            .with_broker(BrokerId::from(2));
        assert_eq!(
            EventDefinition::BrokerGoesUnresponsive { broker: None }.resolve(&call),
            Ok(vec![Event::BrokerGoesUnresponsive {
                broker: BrokerId::from(2)
            }]),
        );
        assert_eq!(
            EventDefinition::BrokerComesBack {
                broker: Some(BrokerId::from(3))
            }
            .resolve(&call),
            Ok(vec![Event::BrokerComesBack {
                broker: BrokerId::from(3)
            }]),
        );
    }

    #[test]
    fn a_committed_offset_resets_for_every_group_on_every_partition() {
        let definition = EventDefinition::CommittedOffsetReset {
            group: None,
            partition: None,
            to: Offset::from(0),
        };
        let commit = Subjects::new()
            .with_group(group("payments"))
            .with_group(group("analytics-etl"))
            .with_partition(partition("orders.eu/3"))
            .with_partition(partition("orders.eu/4"));
        let reset = |group_id: &str, topic_partition: &str| Event::CommittedOffsetReset {
            group: group(group_id),
            partition: partition(topic_partition),
            to: Offset::from(0),
        };
        assert_eq!(
            definition.resolve(&commit),
            Ok(vec![
                reset("analytics-etl", "orders.eu/3"),
                reset("analytics-etl", "orders.eu/4"),
                reset("payments", "orders.eu/3"),
                reset("payments", "orders.eu/4"),
            ]),
        );

        // One subject from the definition, the other from the call.
        let definition = EventDefinition::CommittedOffsetReset {
            group: Some(group("payments")),
            partition: None,
            to: Offset::from(0),
        };
        assert_eq!(
            definition.resolve(&Subjects::new().with_partition(partition("orders.eu/3"))),
            Ok(vec![reset("payments", "orders.eu/3")]),
        );
        assert_eq!(
            definition.resolve(&Subjects::new()),
            Err(EventError::MissingSubject {
                event: EventKind::CommittedOffsetReset,
                subject: "partition"
            }),
        );
    }

    #[test]
    fn the_coordinator_of_the_call_is_the_one_of_its_group_or_its_transactional_id() {
        let definition = EventDefinition::CoordinatorMoves {
            coordinator: None,
            to: Some(BrokerId::from(3)),
        };
        let moves = |coordinator: Coordinator| Event::CoordinatorMoves {
            coordinator,
            to: Some(BrokerId::from(3)),
        };

        let heartbeat = Subjects::new().with_group(group("payments"));
        assert_eq!(
            definition.resolve(&heartbeat),
            Ok(vec![moves(group("payments").into())])
        );

        let end_txn = Subjects::new().with_transactional_id(transactional_id("payments-tx-1"));
        assert_eq!(
            definition.resolve(&end_txn),
            Ok(vec![moves(transactional_id("payments-tx-1").into())])
        );

        // `TxnOffsetCommit` names both, and both coordinators move.
        let txn_offset_commit = heartbeat.with_transactional_id(transactional_id("payments-tx-1"));
        assert_eq!(
            definition.resolve(&txn_offset_commit),
            Ok(vec![
                moves(group("payments").into()),
                moves(transactional_id("payments-tx-1").into())
            ]),
        );
    }

    #[test]
    fn a_producer_is_fenced_by_its_transactional_id() {
        let call = Subjects::new().with_transactional_id(transactional_id("payments-tx-1"));
        assert_eq!(
            EventDefinition::ProducerFenced {
                transactional_id: None
            }
            .resolve(&call),
            Ok(vec![Event::ProducerFenced {
                transactional_id: transactional_id("payments-tx-1")
            }]),
        );
    }

    #[test]
    fn an_event_is_a_definition_that_names_every_subject() {
        for definition in every_definition_without_subject() {
            for event in definition.resolve(&a_call_about_everything()).unwrap() {
                let named = EventDefinition::from(event.clone());
                assert_eq!(named.kind(), event.kind());
                assert_eq!(Event::try_from(named.clone()), Ok(event.clone()));
                assert_eq!(named.resolve(&a_call_about_everything()), Ok(vec![event]));
            }
        }
    }

    #[test]
    fn a_definition_without_subject_is_not_an_event() {
        assert_eq!(
            Event::try_from(EventDefinition::LeaderMoves {
                partition: None,
                to: None
            }),
            Err(EventError::MissingSubject {
                event: EventKind::LeaderMoves,
                subject: "partition"
            }),
        );
    }
}
