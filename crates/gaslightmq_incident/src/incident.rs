use std::fmt;

use gaslightmq_kernel::prelude::*;

use crate::{IncidentKind, PartitionCount, RecordCount};

/// A change to the truth, with everything it needs to happen.
///
/// A rule fires an incident, and a backend [expands](Incident::expand) it,
/// and applies it to its World or emulates it in its overlay.
/// After that, every call and every client sees the same new world.
///
/// An incident names what it changes.
/// The [`IncidentDefinition`](crate::IncidentDefinition) of a rule may leave that to the call that fires the rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Incident {
    /// The partition gets a new leader and a new leader epoch.
    LeaderMoves {
        partition: TopicPartition,
        /// The new leader.
        /// Without one, the backend elects the first in-sync replica that is live.
        to: Option<BrokerId>,
    },
    /// The broker refuses connections and drops existing ones.
    BrokerGoesDown {
        broker: BrokerId,
        /// Whether the partitions the broker led and the coordinators it hosted move to another broker.
        /// Without failover, they have no leader and are unavailable while the broker is down.
        failover: bool,
    },
    /// The broker accepts connections and never answers.
    BrokerGoesUnresponsive { broker: BrokerId },
    /// The broker serves traffic again.
    BrokerComesBack { broker: BrokerId },
    /// The consumer group starts a new generation or epoch.
    Rebalance { group: GroupId },
    /// The coordinator of a group or of a transactional producer moves to another broker.
    CoordinatorMoves {
        coordinator: Coordinator,
        /// The broker that hosts the coordinator from now on.
        /// Without one, the backend picks a live broker.
        to: Option<BrokerId>,
    },
    /// The topic gets `count` more partitions.
    PartitionAdded { topic: TopicName, count: PartitionCount },
    /// The topic disappears.
    TopicDeleted { topic: TopicName },
    /// `count` records arrive on the partition.
    /// The topic's record source makes them up.
    RecordsAppended {
        partition: TopicPartition,
        count: RecordCount,
    },
    /// The partition's log start offset moves forward to `to`.
    RecordsExpired { partition: TopicPartition, to: Offset },
    /// The partition's end offset moves back to `to`, as after an unclean leader election.
    LogTruncated { partition: TopicPartition, to: Offset },
    /// The group's committed offset on the partition becomes `to`.
    CommittedOffsetReset {
        group: GroupId,
        partition: TopicPartition,
        to: Offset,
    },
    /// The epoch of the transactional producer is bumped, so the producer that holds the old one is fenced.
    ProducerFenced { transactional_id: TransactionalId },
}

impl Incident {
    /// Which of the thirteen incidents this is.
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
}

/// Why an [`IncidentDefinition`](crate::IncidentDefinition) does not become an [`Incident`].
///
/// An incident names what it changes, and a definition may leave that to the call that fires the rule.
/// The definition does not become an incident when it names no subject and the call offers none.
/// [`IncidentDefinition::resolve`](crate::IncidentDefinition::resolve) returns this error then.
/// The `TryFrom` that reads a definition as one incident has no call, so it returns it for any subject left out.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IncidentError {
    /// The incident needs a `subject`, such as a partition or a group.
    /// The definition names none, and the call that fired it offers none.
    MissingSubject {
        incident: IncidentKind,
        subject: &'static str,
    },
}

impl fmt::Display for IncidentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSubject { incident, subject } => {
                write!(f, "{incident} needs a {subject}, and none is named")
            }
        }
    }
}

impl std::error::Error for IncidentError {}

/// Whose coordinator: the one of a consumer group, or the one of a transactional producer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Coordinator {
    /// The group coordinator of this group.
    Group(GroupId),
    /// The transaction coordinator of this transactional ID.
    Transaction(TransactionalId),
}

impl From<GroupId> for Coordinator {
    fn from(group: GroupId) -> Self {
        Self::Group(group)
    }
}

impl From<TransactionalId> for Coordinator {
    fn from(transactional_id: TransactionalId) -> Self {
        Self::Transaction(transactional_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_error_of_an_incident_names_the_incident_and_the_subject_it_misses() {
        let cases = [
            (
                IncidentKind::Rebalance,
                "group",
                "rebalance needs a group, and none is named",
            ),
            (
                IncidentKind::LeaderMoves,
                "partition",
                "leader-moves needs a partition, and none is named",
            ),
            (
                IncidentKind::CoordinatorMoves,
                "group or transactional ID",
                "coordinator-moves needs a group or transactional ID, and none is named",
            ),
        ];

        for (incident, subject, message) in cases {
            let error = IncidentError::MissingSubject { incident, subject };
            assert_eq!(error.to_string(), message);
        }
    }
}
