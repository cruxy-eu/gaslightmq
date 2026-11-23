use std::collections::BTreeMap;
use std::fmt;

use crate::{IncidentKind, Requirement, Requirements};

/// What a backend does with one kind of incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Support {
    /// The backend applies the incident: it really happens, and everything stays consistent.
    Applied,
    /// The backend emulates the incident.
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

    /// Whether the backend applies or emulates the incident.
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

/// The support a backend has for every kind of incident:
/// the kinds it applies, the kinds it can only emulate, and so the kinds it cannot do.
///
/// A backend declares its support once.
/// A kind it does not mention is unsupported.
///
/// It covers incidents alone.
/// What else a backend can do, such as run a virtual clock, is not part of it.
///
/// Support for a kind covers its consequences that are not incidents:
/// a backend that does "broker goes down" also reports the partitions as leaderless.
/// A consequence that is an incident needs support for its own kind,
/// so a backend without "leader moves" does "broker goes down" without failover only.
///
/// ```
/// use gaslightmq_incident::prelude::*;
///
/// let backend = IncidentSupport::none()
///     .applies([IncidentKind::RecordsAppended])
///     .emulates([IncidentKind::BrokerGoesDown, IncidentKind::BrokerComesBack]);
///
/// assert_eq!(backend.support(IncidentKind::BrokerGoesDown), Support::Emulated);
/// assert_eq!(backend.support(IncidentKind::LeaderMoves), Support::Unsupported);
///
/// // The scenario loads when the backend can do every incident it needs.
/// let down = IncidentDefinition::BrokerGoesDown { broker: None, failover: false };
/// assert!(backend.check(&Requirements::from(&down)).is_ok());
///
/// // A failover moves leaders and coordinators, which this backend cannot do.
/// let failover = IncidentDefinition::BrokerGoesDown { broker: None, failover: true };
/// let unsupported = backend.check(&Requirements::from(&failover)).unwrap_err();
/// assert_eq!(
///     unsupported.to_string(),
///     "the backend cannot apply or emulate: leader-moves (a consequence of broker-goes-down), \
///      coordinator-moves (a consequence of broker-goes-down)",
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncidentSupport {
    /// Only the kinds the backend supports.
    supported: BTreeMap<IncidentKind, Support>,
}

impl IncidentSupport {
    /// No incident at all: a backend that only answers calls.
    /// Lies need no support, so every scenario that only lies still runs.
    pub fn none() -> Self {
        Self::default()
    }

    /// Declare the kinds of incidents the backend applies.
    /// The last declaration of a kind stands.
    #[must_use]
    pub fn applies(self, kinds: impl IntoIterator<Item = IncidentKind>) -> Self {
        self.declare(kinds, Support::Applied)
    }

    /// Declare the kinds of incidents the backend can only emulate.
    /// The last declaration of a kind stands.
    #[must_use]
    pub fn emulates(self, kinds: impl IntoIterator<Item = IncidentKind>) -> Self {
        self.declare(kinds, Support::Emulated)
    }

    pub(crate) fn declare(mut self, kinds: impl IntoIterator<Item = IncidentKind>, support: Support) -> Self {
        for kind in kinds {
            match support {
                Support::Unsupported => self.supported.remove(&kind),
                supported => self.supported.insert(kind, supported),
            };
        }
        self
    }

    /// What the backend does with an incident of this kind.
    pub fn support(&self, kind: IncidentKind) -> Support {
        self.supported.get(&kind).copied().unwrap_or(Support::Unsupported)
    }

    /// Every kind with what the backend does with it, in the order of [`IncidentKind`].
    pub fn iter(&self) -> impl Iterator<Item = (IncidentKind, Support)> + '_ {
        IncidentKind::ALL.into_iter().map(|kind| (kind, self.support(kind)))
    }

    /// Check that the backend applies or emulates every incident the requirements name.
    ///
    /// The requirements include the consequences of each incident,
    /// so the result also names a kind that only a consequence needs.
    ///
    /// # Errors
    ///
    /// [`UnsupportedIncidents`] with every requirement the backend can neither apply nor emulate.
    pub fn check(&self, requirements: &Requirements) -> Result<(), UnsupportedIncidents> {
        let incidents: Vec<Requirement> = requirements
            .iter()
            .filter(|requirement| !self.support(requirement.kind()).is_supported())
            .collect();
        if incidents.is_empty() {
            Ok(())
        } else {
            Err(UnsupportedIncidents { incidents })
        }
    }
}

/// The incidents a scenario needs and its backend can neither apply nor emulate.
///
/// It names at least one incident.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the backend cannot apply or emulate: {}", listed(.incidents))]
pub struct UnsupportedIncidents {
    incidents: Vec<Requirement>,
}

impl UnsupportedIncidents {
    /// The unsupported incidents, in the order of [`IncidentKind`].
    /// Each says whether a rule fires it, or which incident has it as a consequence.
    pub fn incidents(&self) -> &[Requirement] {
        &self.incidents
    }
}

/// The incidents as the message lists them: each kind, and the incident it follows from when it has one.
fn listed(incidents: &[Requirement]) -> String {
    let incidents: Vec<String> = incidents
        .iter()
        .map(|incident| match incident.consequence_of() {
            Some(cause) => format!("{} (a consequence of {cause})", incident.kind()),
            None => incident.kind().to_string(),
        })
        .collect();
    incidents.join(", ")
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::{Incident, IncidentDefinition};

    /// The Upstream column of the capabilities table.
    fn upstream() -> IncidentSupport {
        IncidentSupport::none().emulates([
            IncidentKind::BrokerGoesDown,
            IncidentKind::BrokerGoesUnresponsive,
            IncidentKind::BrokerComesBack,
            IncidentKind::Rebalance,
            IncidentKind::RecordsExpired,
            IncidentKind::CommittedOffsetReset,
            IncidentKind::ProducerFenced,
        ])
    }

    /// The Simulated column of the capabilities table.
    /// "Producer fenced" comes later, with transactions.
    fn simulated() -> IncidentSupport {
        IncidentSupport::none().applies(
            IncidentKind::ALL
                .into_iter()
                .filter(|kind| *kind != IncidentKind::ProducerFenced),
        )
    }

    fn requirements(definitions: &[IncidentDefinition]) -> Requirements {
        definitions.iter().collect()
    }

    /// The unsupported incidents as pairs of a kind and the kind it is a consequence of.
    fn unsupported(
        backend: &IncidentSupport,
        definitions: &[IncidentDefinition],
    ) -> Vec<(IncidentKind, Option<IncidentKind>)> {
        match backend.check(&requirements(definitions)) {
            Ok(()) => vec![],
            Err(unsupported) => unsupported
                .incidents()
                .iter()
                .map(|incident| (incident.kind(), incident.consequence_of()))
                .collect(),
        }
    }

    #[test]
    fn a_backend_supports_only_what_it_declares() {
        let nothing = IncidentSupport::none();
        assert_eq!(nothing, IncidentSupport::default());
        for kind in IncidentKind::ALL {
            assert_eq!(nothing.support(kind), Support::Unsupported);
        }
    }

    #[test]
    fn the_upstream_backend_emulates_what_its_overlay_can_hold() {
        use IncidentKind::*;
        use Support::*;
        let table: Vec<(IncidentKind, Support)> = upstream().iter().collect();
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
    fn the_simulated_backend_applies_every_incident_but_producer_fenced() {
        for (kind, support) in simulated().iter() {
            let expected = if kind == IncidentKind::ProducerFenced {
                Support::Unsupported
            } else {
                Support::Applied
            };
            assert_eq!(support, expected, "{kind}");
        }
    }

    #[test]
    fn the_last_declaration_of_a_kind_stands() {
        let backend = IncidentSupport::none()
            .applies([IncidentKind::Rebalance])
            .emulates([IncidentKind::Rebalance]);
        assert_eq!(backend.support(IncidentKind::Rebalance), Support::Emulated);
        assert_eq!(backend, IncidentSupport::none().emulates([IncidentKind::Rebalance]));
    }

    #[test]
    fn a_scenario_that_only_lies_runs_on_every_backend() {
        for backend in [IncidentSupport::none(), upstream(), simulated()] {
            assert_eq!(backend.check(&Requirements::none()), Ok(()));
        }
    }

    #[test]
    fn the_leader_election_scenario_runs_on_the_simulated_backend_only() {
        let rules = [IncidentDefinition::LeaderMoves {
            partition: Some("orders.eu/3".parse().unwrap()),
            to: None,
        }];

        assert_eq!(unsupported(&simulated(), &rules), []);
        assert_eq!(unsupported(&upstream(), &rules), [(IncidentKind::LeaderMoves, None)]);
    }

    #[test]
    fn the_upstream_backend_emulates_a_broker_that_goes_down_without_failover() {
        let down = [IncidentDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: false,
        }];
        assert_eq!(unsupported(&upstream(), &down), []);

        // "Broker goes down" with `failover: true` needs "leader moves".
        let failover = [IncidentDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: true,
        }];
        assert_eq!(
            unsupported(&upstream(), &failover),
            [
                (IncidentKind::LeaderMoves, Some(IncidentKind::BrokerGoesDown)),
                (IncidentKind::CoordinatorMoves, Some(IncidentKind::BrokerGoesDown)),
            ]
        );
        assert_eq!(unsupported(&simulated(), &failover), []);
    }

    #[test]
    fn a_backend_names_every_unsupported_incident_of_a_scenario() {
        let rules = [
            IncidentDefinition::TopicDeleted { topic: None },
            IncidentDefinition::Rebalance { group: None },
            IncidentDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            IncidentDefinition::BrokerGoesDown {
                broker: None,
                failover: true,
            },
            IncidentDefinition::LeaderMoves {
                partition: None,
                to: None,
            },
            IncidentDefinition::BrokerComesBack { broker: None },
        ];
        assert_eq!(
            unsupported(&upstream(), &rules),
            [
                (IncidentKind::LeaderMoves, None),
                (IncidentKind::CoordinatorMoves, Some(IncidentKind::BrokerGoesDown)),
                (IncidentKind::TopicDeleted, None),
                (IncidentKind::LogTruncated, None),
            ]
        );
    }

    #[test]
    fn the_simulated_backend_fences_no_producer_yet() {
        let rules = [IncidentDefinition::ProducerFenced { transactional_id: None }];
        assert_eq!(
            unsupported(&simulated(), &rules),
            [(IncidentKind::ProducerFenced, None)]
        );
        assert_eq!(unsupported(&upstream(), &rules), []);
    }

    #[test]
    fn a_backend_checks_an_incident_that_fires_the_same_way() {
        let down = Incident::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: true,
        };
        assert!(upstream().check(&Requirements::from(&down)).is_err());
        assert_eq!(simulated().check(&Requirements::from(&down)), Ok(()));
    }

    #[test]
    fn the_rejection_lists_the_unsupported_incidents() {
        let rules = [
            IncidentDefinition::LogTruncated {
                partition: None,
                to: Offset::from(40),
            },
            IncidentDefinition::BrokerGoesDown {
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
