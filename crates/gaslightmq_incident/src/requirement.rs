use std::collections::BTreeMap;
use std::fmt;

use crate::consequence::kinds_of_consequences;
use crate::{Incident, IncidentDefinition, IncidentKind};

/// One kind of incident that a scenario needs from its backend, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Requirement {
    kind: IncidentKind,
    consequence_of: Option<IncidentKind>,
}

impl Requirement {
    /// The kind of incident the backend must apply or emulate.
    pub fn kind(&self) -> IncidentKind {
        self.kind
    }

    /// The kind of incident that has this one as a consequence.
    /// It is `None` when a rule fires this kind of incident itself.
    pub fn consequence_of(&self) -> Option<IncidentKind> {
        self.consequence_of
    }
}

/// The kind, and the incident it follows from when it has one: `leader-moves (a consequence of broker-goes-down)`.
impl fmt::Display for Requirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.consequence_of {
            Some(cause) => write!(f, "{} (a consequence of {cause})", self.kind),
            None => write!(f, "{}", self.kind),
        }
    }
}

/// The kinds of incidents a scenario needs from its backend.
///
/// The requirements follow from the incident definitions of the scenario's rules.
/// They include the incidents each definition can have as [consequences](crate::Consequence):
/// "broker goes down" with failover needs "leader moves" and "coordinator moves".
/// A failover needs them whether or not the broker leads a partition,
/// so a scenario is rejected when it loads, and not halfway through a run.
///
/// [`IncidentSupport::check`](crate::IncidentSupport::check) holds the requirements against a backend.
///
/// ```
/// use gaslightmq_incident::prelude::*;
///
/// let rules = [IncidentDefinition::BrokerGoesDown { broker: None, failover: true }];
/// let requirements: Requirements = rules.iter().collect();
///
/// let kinds: Vec<IncidentKind> = requirements.iter().map(|requirement| requirement.kind()).collect();
/// assert_eq!(kinds, [IncidentKind::LeaderMoves, IncidentKind::BrokerGoesDown, IncidentKind::CoordinatorMoves]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Requirements {
    /// Each kind, with the kind it is a consequence of.
    kinds: BTreeMap<IncidentKind, Option<IncidentKind>>,
}

impl Requirements {
    /// No incidents, as for a scenario that only lies.
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether the scenario needs no incident, and so runs on every backend.
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// Every requirement, in the order of [`IncidentKind`].
    pub fn iter(&self) -> impl Iterator<Item = Requirement> + '_ {
        self.kinds
            .iter()
            .map(|(&kind, &consequence_of)| Requirement { kind, consequence_of })
    }

    /// Need an incident of this kind, and the incidents it can have as consequences.
    fn require(&mut self, kind: IncidentKind, failover: bool) {
        // A rule that fires the kind itself says more than a consequence does.
        self.kinds.insert(kind, None);
        for &consequence in kinds_of_consequences(kind, failover) {
            self.kinds.entry(consequence).or_insert(Some(kind));
        }
    }
}

/// The requirements of a scenario are those of the incident definitions of its rules.
impl<'a> Extend<&'a IncidentDefinition> for Requirements {
    fn extend<I: IntoIterator<Item = &'a IncidentDefinition>>(&mut self, definitions: I) {
        for definition in definitions {
            self.require(definition.kind(), definition.failover());
        }
    }
}

/// The requirements of a scenario are those of the incident definitions of its rules.
impl<'a> FromIterator<&'a IncidentDefinition> for Requirements {
    fn from_iter<I: IntoIterator<Item = &'a IncidentDefinition>>(definitions: I) -> Self {
        let mut requirements = Self::none();
        requirements.extend(definitions);
        requirements
    }
}

/// What one definition needs.
impl From<&IncidentDefinition> for Requirements {
    fn from(definition: &IncidentDefinition) -> Self {
        [definition].into_iter().collect()
    }
}

/// What one incident needs, for a backend that checks an incident when it fires.
impl From<&Incident> for Requirements {
    fn from(incident: &Incident) -> Self {
        let mut requirements = Self::none();
        requirements.require(incident.kind(), incident.failover());
        requirements
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn requirement(kind: IncidentKind) -> Requirement {
        Requirement {
            kind,
            consequence_of: None,
        }
    }

    fn consequence(kind: IncidentKind, of: IncidentKind) -> Requirement {
        Requirement {
            kind,
            consequence_of: Some(of),
        }
    }

    fn list(requirements: &Requirements) -> Vec<Requirement> {
        requirements.iter().collect()
    }

    #[test]
    fn a_scenario_that_only_lies_needs_no_incident() {
        let no_definitions: [IncidentDefinition; 0] = [];
        let requirements: Requirements = no_definitions.iter().collect();
        assert!(requirements.is_empty());
        assert_eq!(requirements, Requirements::none());
        assert_eq!(list(&requirements), []);
    }

    #[test]
    fn a_scenario_needs_the_kinds_of_its_incidents_once_each() {
        let rules = [
            IncidentDefinition::Rebalance { group: None },
            IncidentDefinition::LeaderMoves {
                partition: Some("orders.eu/3".parse().unwrap()),
                to: None,
            },
            IncidentDefinition::LeaderMoves {
                partition: None,
                to: Some(BrokerId::from(1)),
            },
        ];
        let requirements: Requirements = rules.iter().collect();
        assert!(!requirements.is_empty());
        assert_eq!(
            list(&requirements),
            [
                requirement(IncidentKind::LeaderMoves),
                requirement(IncidentKind::Rebalance)
            ]
        );
    }

    #[test]
    fn a_broker_that_goes_down_needs_nothing_else() {
        let down = IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: false,
        };
        assert_eq!(
            list(&Requirements::from(&down)),
            [requirement(IncidentKind::BrokerGoesDown)]
        );
    }

    #[test]
    fn a_failover_needs_the_moves_as_consequences() {
        let down = IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        assert_eq!(
            list(&Requirements::from(&down)),
            [
                consequence(IncidentKind::LeaderMoves, IncidentKind::BrokerGoesDown),
                requirement(IncidentKind::BrokerGoesDown),
                consequence(IncidentKind::CoordinatorMoves, IncidentKind::BrokerGoesDown),
            ]
        );
    }

    #[test]
    fn a_kind_that_a_rule_fires_itself_is_not_listed_as_a_consequence() {
        let leader_moves = IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        let down = IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        let expected = [
            requirement(IncidentKind::LeaderMoves),
            requirement(IncidentKind::BrokerGoesDown),
            consequence(IncidentKind::CoordinatorMoves, IncidentKind::BrokerGoesDown),
        ];

        // Whichever rule comes first.
        assert_eq!(list(&[&leader_moves, &down].into_iter().collect()), expected);
        assert_eq!(list(&[&down, &leader_moves].into_iter().collect()), expected);
    }

    #[test]
    fn requirements_grow_with_more_definitions() {
        let mut requirements = Requirements::from(&IncidentDefinition::TopicDeleted { topic: None });
        requirements.extend(&[IncidentDefinition::Rebalance { group: None }]);
        assert_eq!(
            list(&requirements),
            [
                requirement(IncidentKind::Rebalance),
                requirement(IncidentKind::TopicDeleted)
            ]
        );
    }

    #[test]
    fn an_incident_needs_what_its_definition_needs() {
        for failover in [false, true] {
            let incident = Incident::BrokerGoesDown {
                broker: BrokerId::from(2),
                failover,
            };
            let definition = IncidentDefinition::from(incident.clone());
            assert_eq!(Requirements::from(&incident), Requirements::from(&definition));
        }
    }
}
