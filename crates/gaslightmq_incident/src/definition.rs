use std::collections::BTreeSet;

use gaslightmq_kernel::prelude::*;

use crate::{Coordinator, Incident, IncidentError, IncidentKind, PartitionCount, RecordCount};

/// An incident as a rule's action or a preset writes it.
///
/// It has the thirteen variants of [`Incident`], with the same parameters.
/// The difference is the subject: the partition, broker, group, topic or producer the incident changes.
/// A definition may leave its subject out, and then the incident is about the call that fires the rule.
/// The preset `incident: leader-moves` moves the leader of whichever partition that call targets.
///
/// [`resolve`](Self::resolve) turns a definition into the incidents that happen.
///
/// ```
/// use gaslightmq_incident::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// // "When the 3rd Produce arrives, move the leader."
/// let preset = IncidentDefinition::LeaderMoves { partition: None, to: None };
///
/// // That Produce targets two partitions, so two leaders move.
/// let produce = Subjects::new().with_partition("orders.eu/3".parse()?).with_partition("orders.eu/4".parse()?);
/// let incidents = preset.resolve(&produce)?;
/// assert_eq!(
///     incidents,
///     [
///         Incident::LeaderMoves { partition: "orders.eu/3".parse()?, to: None },
///         Incident::LeaderMoves { partition: "orders.eu/4".parse()?, to: None },
///     ]
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IncidentDefinition {
    /// See [`Incident::LeaderMoves`].
    LeaderMoves {
        partition: Option<TopicPartition>,
        to: Option<BrokerId>,
    },
    /// See [`Incident::BrokerGoesDown`].
    BrokerGoesDown { broker: Option<BrokerId>, failover: bool },
    /// See [`Incident::BrokerGoesUnresponsive`].
    BrokerGoesUnresponsive { broker: Option<BrokerId> },
    /// See [`Incident::BrokerComesBack`].
    BrokerComesBack { broker: Option<BrokerId> },
    /// See [`Incident::Rebalance`].
    Rebalance { group: Option<GroupId> },
    /// See [`Incident::CoordinatorMoves`].
    /// Without a coordinator, the coordinator of every group and transactional ID of the call moves.
    CoordinatorMoves {
        coordinator: Option<Coordinator>,
        to: Option<BrokerId>,
    },
    /// See [`Incident::PartitionAdded`].
    PartitionAdded {
        topic: Option<TopicName>,
        count: PartitionCount,
    },
    /// See [`Incident::TopicDeleted`].
    TopicDeleted { topic: Option<TopicName> },
    /// See [`Incident::RecordsAppended`].
    RecordsAppended {
        partition: Option<TopicPartition>,
        count: RecordCount,
    },
    /// See [`Incident::RecordsExpired`].
    RecordsExpired {
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Incident::LogTruncated`].
    LogTruncated {
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Incident::CommittedOffsetReset`].
    CommittedOffsetReset {
        group: Option<GroupId>,
        partition: Option<TopicPartition>,
        to: Offset,
    },
    /// See [`Incident::ProducerFenced`].
    ProducerFenced { transactional_id: Option<TransactionalId> },
}

impl IncidentDefinition {
    /// Which of the thirteen incidents this defines.
    pub fn kind(&self) -> IncidentKind {
        match self {
            Self::LeaderMoves { .. } => IncidentKind::LeaderMoves,
            Self::BrokerGoesDown { .. } => IncidentKind::BrokerGoesDown,
            Self::BrokerGoesUnresponsive { .. } => IncidentKind::BrokerGoesUnresponsive,
            Self::BrokerComesBack { .. } => IncidentKind::BrokerComesBack,
            Self::Rebalance { .. } => IncidentKind::Rebalance,
            Self::CoordinatorMoves { .. } => IncidentKind::CoordinatorMoves,
            Self::PartitionAdded { .. } => IncidentKind::PartitionAdded,
            Self::TopicDeleted { .. } => IncidentKind::TopicDeleted,
            Self::RecordsAppended { .. } => IncidentKind::RecordsAppended,
            Self::RecordsExpired { .. } => IncidentKind::RecordsExpired,
            Self::LogTruncated { .. } => IncidentKind::LogTruncated,
            Self::CommittedOffsetReset { .. } => IncidentKind::CommittedOffsetReset,
            Self::ProducerFenced { .. } => IncidentKind::ProducerFenced,
        }
    }

    /// Whether a broker that goes down hands its partitions and coordinators to another broker.
    pub(crate) fn failover(&self) -> bool {
        matches!(self, Self::BrokerGoesDown { failover: true, .. })
    }

    /// The incidents that happen when a rule with this action fires on a call about `subjects`.
    ///
    /// A subject the definition names stands, whatever the call is about.
    /// A subject it leaves out comes from the call, and every value the call offers gives one incident:
    /// a call that targets three partitions moves three leaders.
    /// The incidents come in the order of their subjects.
    ///
    /// The result is never empty.
    /// A rule that someone fires by hand has no call, so it resolves against [`Subjects::new`].
    ///
    /// # Errors
    ///
    /// [`IncidentError::MissingSubject`] when the definition leaves a subject out and the call offers none.
    pub fn resolve(&self, subjects: &Subjects) -> Result<Vec<Incident>, IncidentError> {
        let kind = self.kind();
        let incidents = match self {
            Self::LeaderMoves { partition, to } => subjects
                .partitions(kind, partition.as_ref())?
                .into_iter()
                .map(|partition| Incident::LeaderMoves { partition, to: *to })
                .collect(),
            Self::BrokerGoesDown { broker, failover } => subjects
                .brokers(kind, *broker)?
                .into_iter()
                .map(|broker| Incident::BrokerGoesDown {
                    broker,
                    failover: *failover,
                })
                .collect(),
            Self::BrokerGoesUnresponsive { broker } => subjects
                .brokers(kind, *broker)?
                .into_iter()
                .map(|broker| Incident::BrokerGoesUnresponsive { broker })
                .collect(),
            Self::BrokerComesBack { broker } => subjects
                .brokers(kind, *broker)?
                .into_iter()
                .map(|broker| Incident::BrokerComesBack { broker })
                .collect(),
            Self::Rebalance { group } => subjects
                .groups(kind, group.as_ref())?
                .into_iter()
                .map(|group| Incident::Rebalance { group })
                .collect(),
            Self::CoordinatorMoves { coordinator, to } => subjects
                .coordinators(kind, coordinator.as_ref())?
                .into_iter()
                .map(|coordinator| Incident::CoordinatorMoves { coordinator, to: *to })
                .collect(),
            Self::PartitionAdded { topic, count } => subjects
                .topics(kind, topic.as_ref())?
                .into_iter()
                .map(|topic| Incident::PartitionAdded { topic, count: *count })
                .collect(),
            Self::TopicDeleted { topic } => subjects
                .topics(kind, topic.as_ref())?
                .into_iter()
                .map(|topic| Incident::TopicDeleted { topic })
                .collect(),
            Self::RecordsAppended { partition, count } => subjects
                .partitions(kind, partition.as_ref())?
                .into_iter()
                .map(|partition| Incident::RecordsAppended {
                    partition,
                    count: *count,
                })
                .collect(),
            Self::RecordsExpired { partition, to } => subjects
                .partitions(kind, partition.as_ref())?
                .into_iter()
                .map(|partition| Incident::RecordsExpired { partition, to: *to })
                .collect(),
            Self::LogTruncated { partition, to } => subjects
                .partitions(kind, partition.as_ref())?
                .into_iter()
                .map(|partition| Incident::LogTruncated { partition, to: *to })
                .collect(),
            Self::CommittedOffsetReset { group, partition, to } => {
                let partitions = subjects.partitions(kind, partition.as_ref())?;
                subjects
                    .groups(kind, group.as_ref())?
                    .into_iter()
                    .flat_map(|group| {
                        partitions.iter().map(move |partition| Incident::CommittedOffsetReset {
                            group: group.clone(),
                            partition: partition.clone(),
                            to: *to,
                        })
                    })
                    .collect()
            }
            Self::ProducerFenced { transactional_id } => subjects
                .transactional_ids(kind, transactional_id.as_ref())?
                .into_iter()
                .map(|transactional_id| Incident::ProducerFenced { transactional_id })
                .collect(),
        };
        Ok(incidents)
    }
}

/// An incident is a definition that names every subject.
impl From<Incident> for IncidentDefinition {
    fn from(incident: Incident) -> Self {
        match incident {
            Incident::LeaderMoves { partition, to } => Self::LeaderMoves {
                partition: Some(partition),
                to,
            },
            Incident::BrokerGoesDown { broker, failover } => Self::BrokerGoesDown {
                broker: Some(broker),
                failover,
            },
            Incident::BrokerGoesUnresponsive { broker } => Self::BrokerGoesUnresponsive { broker: Some(broker) },
            Incident::BrokerComesBack { broker } => Self::BrokerComesBack { broker: Some(broker) },
            Incident::Rebalance { group } => Self::Rebalance { group: Some(group) },
            Incident::CoordinatorMoves { coordinator, to } => Self::CoordinatorMoves {
                coordinator: Some(coordinator),
                to,
            },
            Incident::PartitionAdded { topic, count } => Self::PartitionAdded {
                topic: Some(topic),
                count,
            },
            Incident::TopicDeleted { topic } => Self::TopicDeleted { topic: Some(topic) },
            Incident::RecordsAppended { partition, count } => Self::RecordsAppended {
                partition: Some(partition),
                count,
            },
            Incident::RecordsExpired { partition, to } => Self::RecordsExpired {
                partition: Some(partition),
                to,
            },
            Incident::LogTruncated { partition, to } => Self::LogTruncated {
                partition: Some(partition),
                to,
            },
            Incident::CommittedOffsetReset { group, partition, to } => Self::CommittedOffsetReset {
                group: Some(group),
                partition: Some(partition),
                to,
            },
            Incident::ProducerFenced { transactional_id } => Self::ProducerFenced {
                transactional_id: Some(transactional_id),
            },
        }
    }
}

/// A definition that names every subject is one incident.
/// One that leaves a subject out is an [`IncidentError::MissingSubject`].
impl TryFrom<IncidentDefinition> for Incident {
    type Error = IncidentError;

    fn try_from(definition: IncidentDefinition) -> Result<Self, Self::Error> {
        let mut incidents = definition.resolve(&Subjects::new())?;
        // Without a call, every subject comes from the definition, and each gives exactly one value.
        Ok(incidents.swap_remove(0))
    }
}

/// What a call is about: the subjects it offers to an [`IncidentDefinition`] that names none.
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
/// use gaslightmq_incident::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let heartbeat = Subjects::new().with_broker(BrokerId::from(2)).with_group(GroupId::new("analytics-etl")?);
///
/// let rebalance = IncidentDefinition::Rebalance { group: None };
/// assert_eq!(rebalance.resolve(&heartbeat)?, [Incident::Rebalance { group: GroupId::new("analytics-etl")? }]);
///
/// // A Heartbeat names no partition.
/// let leader_moves = IncidentDefinition::LeaderMoves { partition: None, to: None };
/// assert_eq!(
///     leader_moves.resolve(&heartbeat),
///     Err(IncidentError::MissingSubject { incident: IncidentKind::LeaderMoves, subject: "partition" }),
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
    #[must_use]
    pub fn with_partition(mut self, partition: TopicPartition) -> Self {
        self.topics.insert(partition.topic().clone());
        self.partitions.insert(partition);
        self
    }

    /// Offer a topic that the call names without a partition.
    #[must_use]
    pub fn with_topic(mut self, topic: TopicName) -> Self {
        self.topics.insert(topic);
        self
    }

    /// Offer the broker the call arrived on.
    /// A call arrives on one broker, so a second broker replaces the first.
    #[must_use]
    pub fn with_broker(mut self, broker: BrokerId) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Offer a consumer group.
    #[must_use]
    pub fn with_group(mut self, group: GroupId) -> Self {
        self.groups.insert(group);
        self
    }

    /// Offer a transactional ID.
    #[must_use]
    pub fn with_transactional_id(mut self, transactional_id: TransactionalId) -> Self {
        self.transactional_ids.insert(transactional_id);
        self
    }

    fn partitions(
        &self,
        incident: IncidentKind,
        named: Option<&TopicPartition>,
    ) -> Result<Vec<TopicPartition>, IncidentError> {
        named_or_offered(incident, "partition", named, &self.partitions)
    }

    fn topics(&self, incident: IncidentKind, named: Option<&TopicName>) -> Result<Vec<TopicName>, IncidentError> {
        named_or_offered(incident, "topic", named, &self.topics)
    }

    fn brokers(&self, incident: IncidentKind, named: Option<BrokerId>) -> Result<Vec<BrokerId>, IncidentError> {
        named_or_offered(incident, "broker", named.as_ref(), &self.broker)
    }

    fn groups(&self, incident: IncidentKind, named: Option<&GroupId>) -> Result<Vec<GroupId>, IncidentError> {
        named_or_offered(incident, "group", named, &self.groups)
    }

    fn transactional_ids(
        &self,
        incident: IncidentKind,
        named: Option<&TransactionalId>,
    ) -> Result<Vec<TransactionalId>, IncidentError> {
        named_or_offered(incident, "transactional ID", named, &self.transactional_ids)
    }

    fn coordinators(
        &self,
        incident: IncidentKind,
        named: Option<&Coordinator>,
    ) -> Result<Vec<Coordinator>, IncidentError> {
        let offered: Vec<Coordinator> = (self.groups.iter().cloned().map(Coordinator::Group))
            .chain(self.transactional_ids.iter().cloned().map(Coordinator::Transaction))
            .collect();
        named_or_offered(incident, "group or transactional ID", named, &offered)
    }
}

/// The subject the definition names, or else every subject the call offers.
fn named_or_offered<'a, T: Clone + 'a>(
    incident: IncidentKind,
    subject: &'static str,
    named: Option<&T>,
    offered: impl IntoIterator<Item = &'a T>,
) -> Result<Vec<T>, IncidentError> {
    let subjects: Vec<T> = match named {
        Some(named) => vec![named.clone()],
        None => offered.into_iter().cloned().collect(),
    };
    if subjects.is_empty() {
        return Err(IncidentError::MissingSubject { incident, subject });
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
    fn every_definition_without_subject() -> [IncidentDefinition; 13] {
        [
            IncidentDefinition::LeaderMoves {
                partition: None,
                to: None,
            },
            IncidentDefinition::BrokerGoesDown {
                broker: None,
                failover: false,
            },
            IncidentDefinition::BrokerGoesUnresponsive { broker: None },
            IncidentDefinition::BrokerComesBack { broker: None },
            IncidentDefinition::Rebalance { group: None },
            IncidentDefinition::CoordinatorMoves {
                coordinator: None,
                to: None,
            },
            IncidentDefinition::PartitionAdded {
                topic: None,
                count: PartitionCount::ONE,
            },
            IncidentDefinition::TopicDeleted { topic: None },
            IncidentDefinition::RecordsAppended {
                partition: None,
                count: RecordCount::ONE,
            },
            IncidentDefinition::RecordsExpired {
                partition: None,
                to: Offset::from(5000),
            },
            IncidentDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            IncidentDefinition::CommittedOffsetReset {
                group: None,
                partition: None,
                to: Offset::from(0),
            },
            IncidentDefinition::ProducerFenced { transactional_id: None },
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
    fn a_definition_and_its_incidents_are_of_the_same_kind() {
        let definitions = every_definition_without_subject();
        let kinds: Vec<IncidentKind> = definitions.iter().map(IncidentDefinition::kind).collect();
        assert_eq!(kinds, IncidentKind::ALL);

        for definition in definitions {
            for incident in definition.resolve(&a_call_about_everything()).unwrap() {
                assert_eq!(incident.kind(), definition.kind());
            }
        }
    }

    #[test]
    fn a_named_subject_needs_no_call() {
        // The rule of the "Leader election" scenario.
        let definition = IncidentDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: None,
        };
        assert_eq!(
            definition.resolve(&Subjects::new()),
            Ok(vec![Incident::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: None
            }]),
        );
    }

    #[test]
    fn a_named_subject_stands_whatever_the_call_is_about() {
        let definition = IncidentDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: Some(BrokerId::from(1)),
        };
        let call = Subjects::new().with_partition(partition("payments.settled/0"));
        assert_eq!(
            definition.resolve(&call),
            Ok(vec![Incident::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: Some(BrokerId::from(1))
            }]),
        );
    }

    #[test]
    fn a_subject_that_is_left_out_comes_from_the_call() {
        // The "Leader election" preset: `incident: leader-moves`.
        let preset = IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        let call = Subjects::new().with_partition(partition("orders.eu/3"));
        assert_eq!(
            preset.resolve(&call),
            Ok(vec![Incident::LeaderMoves {
                partition: partition("orders.eu/3"),
                to: None
            }]),
        );
    }

    #[test]
    fn every_subject_of_the_call_gives_one_incident_in_order() {
        let preset = IncidentDefinition::RecordsExpired {
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
                Incident::RecordsExpired {
                    partition: partition("orders.eu/3"),
                    to: Offset::from(5000)
                },
                Incident::RecordsExpired {
                    partition: partition("orders.eu/4"),
                    to: Offset::from(5000)
                },
                Incident::RecordsExpired {
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
                Err(IncidentError::MissingSubject {
                    incident: definition.kind(),
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
            IncidentDefinition::Rebalance { group: None }.resolve(&produce),
            Err(IncidentError::MissingSubject {
                incident: IncidentKind::Rebalance,
                subject: "group"
            }),
        );
        assert_eq!(
            IncidentDefinition::BrokerGoesDown {
                broker: None,
                failover: true
            }
            .resolve(&produce),
            Ok(vec![Incident::BrokerGoesDown {
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
            IncidentDefinition::TopicDeleted { topic: None }.resolve(&call),
            Ok(vec![
                Incident::TopicDeleted {
                    topic: topic("orders.eu")
                },
                Incident::TopicDeleted {
                    topic: topic("payments.settled")
                },
            ]),
        );

        let count = PartitionCount::new(2).unwrap();
        assert_eq!(
            IncidentDefinition::PartitionAdded {
                topic: Some(topic("orders.us")),
                count
            }
            .resolve(&call),
            Ok(vec![Incident::PartitionAdded {
                topic: topic("orders.us"),
                count
            }]),
        );
    }

    #[test]
    fn a_topic_without_partition_offers_no_partition() {
        let metadata = Subjects::new().with_topic(topic("orders.eu"));
        assert_eq!(
            IncidentDefinition::RecordsAppended {
                partition: None,
                count: RecordCount::ONE
            }
            .resolve(&metadata),
            Err(IncidentError::MissingSubject {
                incident: IncidentKind::RecordsAppended,
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
            IncidentDefinition::BrokerGoesUnresponsive { broker: None }.resolve(&call),
            Ok(vec![Incident::BrokerGoesUnresponsive {
                broker: BrokerId::from(2)
            }]),
        );
        assert_eq!(
            IncidentDefinition::BrokerComesBack {
                broker: Some(BrokerId::from(3))
            }
            .resolve(&call),
            Ok(vec![Incident::BrokerComesBack {
                broker: BrokerId::from(3)
            }]),
        );
    }

    #[test]
    fn a_committed_offset_resets_for_every_group_on_every_partition() {
        let definition = IncidentDefinition::CommittedOffsetReset {
            group: None,
            partition: None,
            to: Offset::from(0),
        };
        let commit = Subjects::new()
            .with_group(group("payments"))
            .with_group(group("analytics-etl"))
            .with_partition(partition("orders.eu/3"))
            .with_partition(partition("orders.eu/4"));
        let reset = |group_id: &str, topic_partition: &str| Incident::CommittedOffsetReset {
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
        let definition = IncidentDefinition::CommittedOffsetReset {
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
            Err(IncidentError::MissingSubject {
                incident: IncidentKind::CommittedOffsetReset,
                subject: "partition"
            }),
        );
    }

    #[test]
    fn the_coordinator_of_the_call_is_the_one_of_its_group_or_its_transactional_id() {
        let definition = IncidentDefinition::CoordinatorMoves {
            coordinator: None,
            to: Some(BrokerId::from(3)),
        };
        let moves = |coordinator: Coordinator| Incident::CoordinatorMoves {
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
            IncidentDefinition::ProducerFenced { transactional_id: None }.resolve(&call),
            Ok(vec![Incident::ProducerFenced {
                transactional_id: transactional_id("payments-tx-1")
            }]),
        );
    }

    #[test]
    fn an_incident_is_a_definition_that_names_every_subject() {
        for definition in every_definition_without_subject() {
            for incident in definition.resolve(&a_call_about_everything()).unwrap() {
                let named = IncidentDefinition::from(incident.clone());
                assert_eq!(named.kind(), incident.kind());
                assert_eq!(Incident::try_from(named.clone()), Ok(incident.clone()));
                assert_eq!(named.resolve(&a_call_about_everything()), Ok(vec![incident]));
            }
        }
    }

    #[test]
    fn a_definition_without_subject_is_not_an_incident() {
        assert_eq!(
            Incident::try_from(IncidentDefinition::LeaderMoves {
                partition: None,
                to: None
            }),
            Err(IncidentError::MissingSubject {
                incident: IncidentKind::LeaderMoves,
                subject: "partition"
            }),
        );
    }
}
