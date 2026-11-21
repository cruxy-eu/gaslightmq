use std::collections::BTreeMap;

use crate::consequence::kinds_of_consequences;
use crate::{Event, EventDefinition, EventKind};

/// One kind of event that a scenario needs from its backend, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Requirement {
    kind: EventKind,
    consequence_of: Option<EventKind>,
}

impl Requirement {
    /// The kind of event the backend must apply or emulate.
    pub fn kind(&self) -> EventKind {
        self.kind
    }

    /// The kind of event that has this one as a consequence.
    /// It is `None` when a rule fires this kind of event itself.
    pub fn consequence_of(&self) -> Option<EventKind> {
        self.consequence_of
    }
}

/// The kinds of events a scenario needs from its backend.
///
/// The requirements follow from the event definitions of the scenario's rules.
/// They include the events each definition can have as [consequences](crate::Consequence):
/// "broker goes down" with failover needs "leader moves" and "coordinator moves".
/// A failover needs them whether or not the broker leads a partition,
/// so a scenario is rejected when it loads, and not halfway through a run.
///
/// [`Capabilities::check`](crate::Capabilities::check) holds the requirements against a backend.
///
/// ```
/// use gaslightmq_event::prelude::*;
///
/// let rules = [EventDefinition::BrokerGoesDown { broker: None, failover: true }];
/// let requirements: Requirements = rules.iter().collect();
///
/// let kinds: Vec<EventKind> = requirements.iter().map(|requirement| requirement.kind()).collect();
/// assert_eq!(kinds, [EventKind::LeaderMoves, EventKind::BrokerGoesDown, EventKind::CoordinatorMoves]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Requirements {
    /// Each kind, with the kind it is a consequence of.
    kinds: BTreeMap<EventKind, Option<EventKind>>,
}

impl Requirements {
    /// No events, as for a scenario that only lies.
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether the scenario needs no event, and so runs on every backend.
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    /// Every requirement, in the order of [`EventKind`].
    pub fn iter(&self) -> impl Iterator<Item = Requirement> + '_ {
        self.kinds
            .iter()
            .map(|(&kind, &consequence_of)| Requirement { kind, consequence_of })
    }

    /// Need an event of this kind, and the events it can have as consequences.
    fn require(&mut self, kind: EventKind, failover: bool) {
        // A rule that fires the kind itself says more than a consequence does.
        self.kinds.insert(kind, None);
        for &consequence in kinds_of_consequences(kind, failover) {
            self.kinds.entry(consequence).or_insert(Some(kind));
        }
    }
}

/// The requirements of a scenario are those of the event definitions of its rules.
impl<'a> Extend<&'a EventDefinition> for Requirements {
    fn extend<I: IntoIterator<Item = &'a EventDefinition>>(&mut self, definitions: I) {
        for definition in definitions {
            self.require(definition.kind(), definition.failover());
        }
    }
}

/// The requirements of a scenario are those of the event definitions of its rules.
impl<'a> FromIterator<&'a EventDefinition> for Requirements {
    fn from_iter<I: IntoIterator<Item = &'a EventDefinition>>(definitions: I) -> Self {
        let mut requirements = Self::none();
        requirements.extend(definitions);
        requirements
    }
}

/// What one definition needs.
impl From<&EventDefinition> for Requirements {
    fn from(definition: &EventDefinition) -> Self {
        [definition].into_iter().collect()
    }
}

/// What one event needs, for a backend that checks an event when it fires.
impl From<&Event> for Requirements {
    fn from(event: &Event) -> Self {
        let mut requirements = Self::none();
        requirements.require(event.kind(), event.failover());
        requirements
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn requirement(kind: EventKind) -> Requirement {
        Requirement {
            kind,
            consequence_of: None,
        }
    }

    fn consequence(kind: EventKind, of: EventKind) -> Requirement {
        Requirement {
            kind,
            consequence_of: Some(of),
        }
    }

    fn list(requirements: &Requirements) -> Vec<Requirement> {
        requirements.iter().collect()
    }

    #[test]
    fn a_scenario_that_only_lies_needs_no_event() {
        let no_definitions: [EventDefinition; 0] = [];
        let requirements: Requirements = no_definitions.iter().collect();
        assert!(requirements.is_empty());
        assert_eq!(requirements, Requirements::none());
        assert_eq!(list(&requirements), []);
    }

    #[test]
    fn a_scenario_needs_the_kinds_of_its_events_once_each() {
        let rules = [
            EventDefinition::Rebalance { group: None },
            EventDefinition::LeaderMoves {
                partition: Some("orders.eu/3".parse().unwrap()),
                to: None,
            },
            EventDefinition::LeaderMoves {
                partition: None,
                to: Some(BrokerId::from(1)),
            },
        ];
        let requirements: Requirements = rules.iter().collect();
        assert!(!requirements.is_empty());
        assert_eq!(
            list(&requirements),
            [requirement(EventKind::LeaderMoves), requirement(EventKind::Rebalance)]
        );
    }

    #[test]
    fn a_broker_that_goes_down_needs_nothing_else() {
        let down = EventDefinition::BrokerGoesDown {
            broker: None,
            failover: false,
        };
        assert_eq!(
            list(&Requirements::from(&down)),
            [requirement(EventKind::BrokerGoesDown)]
        );
    }

    #[test]
    fn a_failover_needs_the_moves_as_consequences() {
        let down = EventDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        assert_eq!(
            list(&Requirements::from(&down)),
            [
                consequence(EventKind::LeaderMoves, EventKind::BrokerGoesDown),
                requirement(EventKind::BrokerGoesDown),
                consequence(EventKind::CoordinatorMoves, EventKind::BrokerGoesDown),
            ]
        );
    }

    #[test]
    fn a_kind_that_a_rule_fires_itself_is_not_listed_as_a_consequence() {
        let leader_moves = EventDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        let down = EventDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        let expected = [
            requirement(EventKind::LeaderMoves),
            requirement(EventKind::BrokerGoesDown),
            consequence(EventKind::CoordinatorMoves, EventKind::BrokerGoesDown),
        ];

        // Whichever rule comes first.
        assert_eq!(list(&[&leader_moves, &down].into_iter().collect()), expected);
        assert_eq!(list(&[&down, &leader_moves].into_iter().collect()), expected);
    }

    #[test]
    fn requirements_grow_with_more_definitions() {
        let mut requirements = Requirements::from(&EventDefinition::TopicDeleted { topic: None });
        requirements.extend(&[EventDefinition::Rebalance { group: None }]);
        assert_eq!(
            list(&requirements),
            [requirement(EventKind::Rebalance), requirement(EventKind::TopicDeleted)]
        );
    }

    #[test]
    fn an_event_needs_what_its_definition_needs() {
        for failover in [false, true] {
            let event = Event::BrokerGoesDown {
                broker: BrokerId::from(2),
                failover,
            };
            let definition = EventDefinition::from(event.clone());
            assert_eq!(Requirements::from(&event), Requirements::from(&definition));
        }
    }
}
