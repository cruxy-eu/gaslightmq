use std::fmt;
use std::str::FromStr;

/// An incident without its parameters: which of the thirteen incidents it is.
///
/// A backend declares its [`IncidentSupport`](crate::IncidentSupport) per kind,
/// and a scenario's [`Requirements`](crate::Requirements) name kinds.
/// The order is the order of the table in the specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IncidentKind {
    /// A partition gets a new leader and a new leader epoch.
    LeaderMoves,
    /// A broker refuses connections and drops existing ones.
    BrokerGoesDown,
    /// A broker accepts connections and never answers.
    BrokerGoesUnresponsive,
    /// A broker serves traffic again.
    BrokerComesBack,
    /// A consumer group starts a new generation or epoch.
    Rebalance,
    /// A group or transaction coordinator moves to another broker.
    CoordinatorMoves,
    /// A topic gets more partitions.
    PartitionAdded,
    /// A topic disappears.
    TopicDeleted,
    /// Data arrives on a partition.
    RecordsAppended,
    /// A partition's log start offset moves forward.
    RecordsExpired,
    /// A partition's end offset moves back, as after an unclean leader election.
    LogTruncated,
    /// A group's committed offset on a partition changes.
    CommittedOffsetReset,
    /// A transactional producer's epoch is bumped.
    ProducerFenced,
}

impl IncidentKind {
    /// Every kind, in the order of the specification.
    pub const ALL: [Self; 13] = [
        Self::LeaderMoves,
        Self::BrokerGoesDown,
        Self::BrokerGoesUnresponsive,
        Self::BrokerComesBack,
        Self::Rebalance,
        Self::CoordinatorMoves,
        Self::PartitionAdded,
        Self::TopicDeleted,
        Self::RecordsAppended,
        Self::RecordsExpired,
        Self::LogTruncated,
        Self::CommittedOffsetReset,
        Self::ProducerFenced,
    ];

    /// The name a file uses, such as `leader-moves`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::LeaderMoves => "leader-moves",
            Self::BrokerGoesDown => "broker-goes-down",
            Self::BrokerGoesUnresponsive => "broker-goes-unresponsive",
            Self::BrokerComesBack => "broker-comes-back",
            Self::Rebalance => "rebalance",
            Self::CoordinatorMoves => "coordinator-moves",
            Self::PartitionAdded => "partition-added",
            Self::TopicDeleted => "topic-deleted",
            Self::RecordsAppended => "records-appended",
            Self::RecordsExpired => "records-expired",
            Self::LogTruncated => "log-truncated",
            Self::CommittedOffsetReset => "committed-offset-reset",
            Self::ProducerFenced => "producer-fenced",
        }
    }

    /// The name a person reads, such as `Leader moves`.
    pub const fn title(self) -> &'static str {
        match self {
            Self::LeaderMoves => "Leader moves",
            Self::BrokerGoesDown => "Broker goes down",
            Self::BrokerGoesUnresponsive => "Broker goes unresponsive",
            Self::BrokerComesBack => "Broker comes back",
            Self::Rebalance => "Rebalance",
            Self::CoordinatorMoves => "Coordinator moves",
            Self::PartitionAdded => "Partition added",
            Self::TopicDeleted => "Topic deleted",
            Self::RecordsAppended => "Records appended",
            Self::RecordsExpired => "Records expired",
            Self::LogTruncated => "Log truncated",
            Self::CommittedOffsetReset => "Committed offset reset",
            Self::ProducerFenced => "Producer fenced",
        }
    }
}

/// Writes the name a file uses, such as `leader-moves`.
impl fmt::Display for IncidentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name a file uses, as [`Display`](fmt::Display) writes it.
impl FromStr for IncidentKind {
    type Err = IncidentKindError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.name() == name)
            .ok_or_else(|| IncidentKindError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not an [`IncidentKind`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IncidentKindError {
    /// No incident has this name.
    #[error("no incident is called {name:?}")]
    Unknown { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_are_thirteen_kinds_in_the_order_of_the_specification() {
        let titles: Vec<&str> = IncidentKind::ALL.into_iter().map(IncidentKind::title).collect();
        assert_eq!(
            titles,
            [
                "Leader moves",
                "Broker goes down",
                "Broker goes unresponsive",
                "Broker comes back",
                "Rebalance",
                "Coordinator moves",
                "Partition added",
                "Topic deleted",
                "Records appended",
                "Records expired",
                "Log truncated",
                "Committed offset reset",
                "Producer fenced",
            ]
        );

        let mut sorted = IncidentKind::ALL;
        sorted.sort();
        assert_eq!(sorted, IncidentKind::ALL);
    }

    #[test]
    fn the_name_in_a_file_is_the_title_in_lower_case_with_hyphens() {
        for kind in IncidentKind::ALL {
            assert_eq!(kind.name(), kind.title().to_lowercase().replace(' ', "-"));
        }
    }

    #[test]
    fn a_kind_reads_back_from_the_name_it_writes() {
        for kind in IncidentKind::ALL {
            assert_eq!(kind.to_string().parse(), Ok(kind));
        }
        assert_eq!("leader-moves".parse(), Ok(IncidentKind::LeaderMoves));
    }

    #[test]
    fn a_name_that_no_incident_has_is_an_error() {
        for name in ["leader-move", "Leader moves", "LEADER-MOVES", ""] {
            assert_eq!(
                name.parse::<IncidentKind>(),
                Err(IncidentKindError::Unknown { name: name.to_owned() }),
            );
        }
    }

    #[test]
    fn the_error_of_a_kind_quotes_the_name_that_no_incident_has() {
        let cases = [
            ("leader-move", "no incident is called \"leader-move\""),
            ("Leader moves", "no incident is called \"Leader moves\""),
            ("", "no incident is called \"\""),
        ];

        for (name, message) in cases {
            let error = name.parse::<IncidentKind>().unwrap_err();
            assert_eq!(error.to_string(), message);
        }
    }
}
