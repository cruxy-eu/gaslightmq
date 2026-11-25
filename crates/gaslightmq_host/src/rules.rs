//! A rule of an environment, as the environment holds it.

use gaslightmq_rule::{Rule, RuleState};

/// A rule of an environment, and its state there.
///
/// An environment holds every rule of the workspace, and its own live rules.
/// A rule of the workspace has a state in each environment: arming it in one does not arm it in another.
/// A live rule belongs to one environment, and no rule of the workspace has its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentRule {
    /// The definition of the rule, with its UUID.
    pub rule: Rule,
    /// Whether the rule is a live rule: one that a pad or ⌘K made, and that only this environment holds.
    pub live: bool,
    /// Whether the rule is armed or disarmed in the environment.
    pub state: RuleState,
}
