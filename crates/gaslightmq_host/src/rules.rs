//! A rule of an environment, as the environment holds it, and what an intent of a pad did to one.

use gaslightmq_rule::{Rule, RuleState};
use gaslightmq_surface::Hold;

/// A rule of an environment, and its state there.
///
/// An environment holds every rule of the workspace, and its own live rules.
/// A rule of the workspace has a state in each environment: arming it in one does not arm it in another.
/// A live rule belongs to one environment, and no rule of the workspace has its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentRule {
    /// The definition of the rule.
    pub rule: Rule,
    /// Whether the rule is a live rule: one that a pad or ⌘K made, and that only this environment holds.
    pub live: bool,
    /// Whether the rule is armed or disarmed in the environment.
    pub state: RuleState,
}

/// What an intent of a pad did in an environment: see [`send_intent`](crate::Host::send_intent).
///
/// An intent says the state it wants. One that finds that state there already changes nothing, and says so:
/// two people who press one pad to arm it both find it armed, and only the first press changed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentEffect {
    /// The rule of the pad as the environment holds it now, with its state there.
    /// `None` when the pad points at no rule, which a disarm and a release of such a pad leave as it is.
    pub rule: Option<EnvironmentRule>,
    /// Whether the intent made the rule: the pad had none, so the intent gave it the live rule of its position.
    pub made: bool,
    /// Whether the intent changed the rule: its state, or for a turn its definition.
    pub changed: bool,
    /// The hold that a press in hold added, which its release names.
    pub hold: Option<Hold>,
}
