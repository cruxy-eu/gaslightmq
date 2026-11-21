use gaslightmq_kernel::prelude::*;

use crate::{EventKind, PartitionCount, RecordCount};

/// A change to the truth, with everything it needs to happen.
///
/// A rule fires an event, the engine [expands](Event::expand) it,
/// and a backend applies it to its World or emulates it in its overlay.
/// After that, every call and every client sees the same new world.
///
/// An event names what it changes.
/// The [`EventDefinition`](crate::EventDefinition) of a rule may leave that to the call that fires the rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Event {
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

impl Event {
    /// Which of the thirteen events this is.
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
}

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
