use std::fmt;

use gaslightmq_incident::{Incident, IncidentDefinition};
use gaslightmq_lie::{HangUpLie, LieDefinition, RefusalLie, TimingLie};

use crate::PresetName;

/// What happens when a rule fires: a lie or an incident.
///
/// - A **lie** applies to its target on the call that fired the rule.
/// - An **incident** changes the world.
///   The call that fired the rule passes through unchanged, unless another rule lies about it.
///
/// An action is a definition.
/// It does not say what it lies about, nor which partition an incident without a subject changes:
/// the call that fires the rule does, and [`FiredAction`](crate::FiredAction) holds the answer.
///
/// ```
/// use gaslightmq_incident::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// let flap = Action::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// assert!(flap.is_lie());
/// assert_eq!(flap.to_string(), "NOT_LEADER_OR_FOLLOWER");
///
/// let election = Action::from(IncidentDefinition::LeaderMoves { partition: None, to: None });
/// assert_eq!(election.incident().map(IncidentDefinition::kind), Some(IncidentKind::LeaderMoves));
/// assert_eq!(election.to_string(), "leader-moves");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// The client is told something that differs from the truth.
    Lie(LieDefinition),
    /// The truth changes.
    Incident(IncidentDefinition),
}

impl Action {
    /// Whether the action is a lie.
    pub const fn is_lie(&self) -> bool {
        matches!(self, Action::Lie(_))
    }

    /// Whether the action is an incident.
    pub const fn is_incident(&self) -> bool {
        matches!(self, Action::Incident(_))
    }

    /// The lie, when the action is one.
    pub const fn lie(&self) -> Option<&LieDefinition> {
        match self {
            Action::Lie(lie) => Some(lie),
            Action::Incident(_) => None,
        }
    }

    /// The incident, when the action is one.
    pub const fn incident(&self) -> Option<&IncidentDefinition> {
        match self {
            Action::Lie(_) => None,
            Action::Incident(incident) => Some(incident),
        }
    }
}

/// Writes a lie as a verdict lists it, and an incident as the name of its kind.
impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Lie(lie) => lie.fmt(f),
            Action::Incident(incident) => incident.kind().fmt(f),
        }
    }
}

/// How a rule says what happens when it fires: a preset name, or an inline action.
///
/// ```yaml
/// action: delay-1s
/// ```
///
/// ```yaml
/// action: { incident: leader-moves, partition: orders.eu/3 }
/// ```
///
/// A [`PresetSet`](crate::PresetSet) resolves either into an [`Action`].
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// let named = ActionRef::from("delay-1s".parse::<PresetName>()?);
/// let inline = ActionRef::from(LieDefinition::Silence);
///
/// assert_eq!(named.preset().map(PresetName::as_str), Some("delay-1s"));
/// assert_eq!(inline.preset(), None);
/// # Ok::<(), PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActionRef {
    /// The action of the preset of this name.
    Preset(PresetName),
    /// An action that belongs to the rule that writes it, and has no name.
    Inline(Action),
}

impl ActionRef {
    /// The name of the preset this references, or `None` for an inline action.
    pub const fn preset(&self) -> Option<&PresetName> {
        match self {
            ActionRef::Preset(name) => Some(name),
            ActionRef::Inline(_) => None,
        }
    }
}

impl From<PresetName> for ActionRef {
    fn from(name: PresetName) -> Self {
        ActionRef::Preset(name)
    }
}

impl From<Action> for ActionRef {
    fn from(action: Action) -> Self {
        ActionRef::Inline(action)
    }
}

impl From<LieDefinition> for Action {
    fn from(lie: LieDefinition) -> Self {
        Action::Lie(lie)
    }
}

impl From<IncidentDefinition> for Action {
    fn from(incident: IncidentDefinition) -> Self {
        Action::Incident(incident)
    }
}

/// Everything that is a lie or an incident is an action, and an inline action of a rule.
macro_rules! action_from {
    ($($definition:ident => $variant:ident,)*) => {
        $(
            impl From<$definition> for Action {
                fn from(definition: $definition) -> Self {
                    Action::$variant(definition.into())
                }
            }

            impl From<$definition> for ActionRef {
                fn from(definition: $definition) -> Self {
                    ActionRef::Inline(definition.into())
                }
            }
        )*
    };
}

action_from! {
    RefusalLie => Lie,
    TimingLie => Lie,
    HangUpLie => Lie,
    Incident => Incident,
}

impl From<LieDefinition> for ActionRef {
    fn from(lie: LieDefinition) -> Self {
        ActionRef::Inline(lie.into())
    }
}

impl From<IncidentDefinition> for ActionRef {
    fn from(incident: IncidentDefinition) -> Self {
        ActionRef::Inline(incident.into())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_incident::IncidentKind;
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn delay() -> TimingLie {
        TimingLie::new().with_delay(TimeSpan::from_secs(1))
    }

    fn leader_moves() -> IncidentDefinition {
        IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        }
    }

    #[test]
    fn an_action_is_a_lie_or_an_incident() {
        let lie = Action::from(delay());
        let incident = Action::from(leader_moves());

        assert!(lie.is_lie() && !lie.is_incident());
        assert_eq!(lie.lie(), Some(&LieDefinition::from(delay())));
        assert_eq!(lie.incident(), None);

        assert!(incident.is_incident() && !incident.is_lie());
        assert_eq!(incident.incident(), Some(&leader_moves()));
        assert_eq!(incident.lie(), None);
    }

    #[test]
    fn every_lie_and_incident_converts_into_an_action() {
        let refusal = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);
        let moved = Incident::LeaderMoves {
            partition: "orders.eu/3".parse().unwrap(),
            to: None,
        };

        assert_eq!(Action::from(refusal), Action::Lie(refusal.into()));
        assert_eq!(Action::from(delay()), Action::Lie(delay().into()));
        assert_eq!(Action::from(HangUpLie::new()), Action::Lie(HangUpLie::new().into()));
        assert_eq!(
            Action::from(LieDefinition::Silence),
            Action::Lie(LieDefinition::Silence)
        );
        assert_eq!(
            Action::from(moved.clone()),
            Action::Incident(IncidentDefinition::from(moved))
        );
    }

    #[test]
    fn an_action_writes_its_lie_or_the_kind_of_its_incident() {
        assert_eq!(Action::from(delay()).to_string(), "+1 s");
        assert_eq!(Action::from(LieDefinition::Silence).to_string(), "silence");
        assert_eq!(
            Action::from(leader_moves()).to_string(),
            IncidentKind::LeaderMoves.to_string()
        );
    }

    #[test]
    fn a_reference_is_a_preset_name_or_an_inline_action() {
        let name: PresetName = "delay-1s".parse().unwrap();
        let named = ActionRef::from(name.clone());

        assert_eq!(named, ActionRef::Preset(name.clone()));
        assert_eq!(named.preset(), Some(&name));

        let inline: [ActionRef; 6] = [
            Action::from(delay()).into(),
            delay().into(),
            LieDefinition::Silence.into(),
            RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).into(),
            HangUpLie::new().into(),
            leader_moves().into(),
        ];
        for action in inline {
            assert!(matches!(action, ActionRef::Inline(_)));
            assert_eq!(action.preset(), None);
        }
    }
}
