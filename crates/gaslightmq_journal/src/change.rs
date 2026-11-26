//! What an entry records: one change of an environment.

use std::fmt;

use gaslightmq_incident::ExpandedIncident;

use crate::{RecordedDefinition, RecordedName};

/// What changed in an environment: the one thing that an entry records.
///
/// | Change | Records |
/// |---|---|
/// | [`RuleArmed`](Self::RuleArmed), [`RuleDisarmed`](Self::RuleDisarmed) | The rule. |
/// | [`ParameterChanged`](Self::ParameterChanged) | The live rule, the parameter, the old and the new value. |
/// | [`RuleFiredByHand`](Self::RuleFiredByHand) | The rule. |
/// | [`LiveRuleMade`](Self::LiveRuleMade), [`LiveRuleChanged`](Self::LiveRuleChanged), [`LiveRuleKept`](Self::LiveRuleKept), [`LiveRuleDeleted`](Self::LiveRuleDeleted) | The live rule. |
/// | [`DefinitionCreated`](Self::DefinitionCreated), [`DefinitionChanged`](Self::DefinitionChanged), [`DefinitionDeleted`](Self::DefinitionDeleted) | The definition of the workspace, and its kind. |
/// | [`IncidentApplied`](Self::IncidentApplied), [`IncidentEmulated`](Self::IncidentEmulated) | The incident and its consequences. |
///
/// An entry names a rule and a definition by the [name it had](RecordedName).
/// A name can go and come back, so a journal records every creation and every deletion:
/// a rule begins where it is made or created, and ends where it is deleted.
/// The entries in between are the history of one rule, and a live rule that someone keeps goes on as the rule
/// of the workspace that it became.
///
/// One change of an environment can be two entries, which then hold the same revision.
/// A live rule that is made armed is made and armed, and an armed live rule that is deleted is disarmed and deleted,
/// so every arming in a journal ends with a disarm.
///
/// What an environment does not do yet has no entry yet: a scenario that is activated or deactivated,
/// a step that advances, and a recording that stops by itself. Their entries arrive with scenarios and recordings.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// // Someone turns the delay of the rule of a pad up, from one second to one and a half.
/// let slower = Change::ParameterChanged {
///     rule: "delay-1s-on-order-producers".parse()?,
///     parameter: Parameter::Delay,
///     old: "1s".to_owned(),
///     new: "1500ms".to_owned(),
/// };
/// assert_ne!(slower, Change::RuleArmed { rule: "delay-1s-on-order-producers".parse()? });
/// # Ok::<(), RecordedNameError>(())
/// ```
///
/// With the `serde` feature, a change is a map whose `change` key says which one it is:
/// `{ "change": "rule-armed", "rule": "slow-order-producers" }`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "change", rename_all = "kebab-case", deny_unknown_fields)
)]
pub enum Change {
    /// The rule armed: a rule of the workspace, or a live rule.
    RuleArmed { rule: RecordedName },
    /// The rule disarmed: someone disarmed it, its lifetime ended, a firing spent its budget, or its last hold went.
    RuleDisarmed { rule: RecordedName },
    /// One parameter of a live rule changed, as with a turn of an encoder.
    /// `old` and `new` are the value before and after, each as a definition writes it: `1s`, `40%`, `3 firings`.
    ParameterChanged {
        rule: RecordedName,
        parameter: Parameter,
        old: String,
        new: String,
    },
    /// Someone fired the rule by hand, which skips its trigger once.
    /// A firing is what happens, and no change of what someone set, so its entry holds no revision.
    RuleFiredByHand { rule: RecordedName },
    /// Someone made the live rule: a pad, a ⌘K sentence or a request.
    LiveRuleMade { rule: RecordedName },
    /// Someone replaced the definition of the live rule. Everything but its name can differ.
    LiveRuleChanged { rule: RecordedName },
    /// Someone kept the live rule, and it goes on as a rule of the workspace, in the state it has.
    LiveRuleKept { rule: RecordedName },
    /// Someone deleted the live rule.
    LiveRuleDeleted { rule: RecordedName },
    /// Someone created the definition in the workspace.
    DefinitionCreated { definition: RecordedDefinition },
    /// Someone replaced the definition of the workspace. Everything but its name can differ.
    DefinitionChanged { definition: RecordedDefinition },
    /// Someone deleted the definition from the workspace.
    DefinitionDeleted { definition: RecordedDefinition },
    /// The backend applied the incident: it really happened, with these consequences.
    /// What happened is no change of what someone set, so the entry holds no revision.
    IncidentApplied { incident: ExpandedIncident },
    /// The backend emulated the incident: the clients that go through GaslightMQ cannot tell,
    /// and the rest of the cluster did not change.
    IncidentEmulated { incident: ExpandedIncident },
}

/// One parameter of a rule: a part of its trigger, of its action or its lifetime that changes by itself.
///
/// | Parameter | Is |
/// |---|---|
/// | [`Chance`](Self::Chance) | The chance of the trigger. |
/// | [`Every`](Self::Every) | The `every` count of the trigger. |
/// | [`Delay`](Self::Delay), [`Jitter`](Self::Jitter), [`Throttle`](Self::Throttle) | The delay, the jitter and the throttle of a timing lie. |
/// | [`Error`](Self::Error) | The error of a refusal. |
/// | [`Lifetime`](Self::Lifetime) | The lifetime of the rule. |
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// assert_eq!(Parameter::Chance.to_string(), "chance");
/// ```
///
/// With the `serde` feature, a parameter serializes as its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub enum Parameter {
    /// How often the rule fires among its matching calls.
    Chance,
    /// The step between the matching calls that the rule fires on.
    Every,
    /// How long a timing lie holds the call.
    Delay,
    /// How far the wait of a timing lie strays from its delay.
    Jitter,
    /// How long a timing lie tells the client to back off.
    Throttle,
    /// Which error a refusal tells the client.
    Error,
    /// How long the rule stays armed: until someone disarms it, for a time, or for a number of matches or firings.
    Lifetime,
}

impl Parameter {
    /// The name of the parameter in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            Parameter::Chance => "chance",
            Parameter::Every => "every",
            Parameter::Delay => "delay",
            Parameter::Jitter => "jitter",
            Parameter::Throttle => "throttle",
            Parameter::Error => "error",
            Parameter::Lifetime => "lifetime",
        }
    }
}

/// Writes the name of the parameter.
impl fmt::Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parameter_writes_its_name() {
        let parameters = [
            (Parameter::Chance, "chance"),
            (Parameter::Every, "every"),
            (Parameter::Delay, "delay"),
            (Parameter::Jitter, "jitter"),
            (Parameter::Throttle, "throttle"),
            (Parameter::Error, "error"),
            (Parameter::Lifetime, "lifetime"),
        ];

        for (parameter, name) in parameters {
            assert_eq!(parameter.to_string(), name);
        }
    }
}
