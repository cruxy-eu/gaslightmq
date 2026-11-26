//! What the Rules view lists, what someone can do with a rule, and how a change of a rule reaches the control plane.

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::field::Field;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::{Condition, Scope};
use gaslightmq_control_api::trigger::{Chance, Lifetime, Trigger, TriggerParts};

use crate::history::Subject;

/// Which rules of the environment the Rules view lists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum RuleTab {
    /// Every rule: the rules of the workspace and the live rules.
    #[default]
    All,
    /// The rules that are armed.
    Armed,
    /// The live rules, which only this environment holds, until someone keeps them.
    Live,
    /// The rules of the workspace.
    Workspace,
}

impl RuleTab {
    /// Every tab, in the order the view shows them.
    pub const ALL: [RuleTab; 4] = [RuleTab::All, RuleTab::Armed, RuleTab::Live, RuleTab::Workspace];

    /// The name of the tab.
    pub const fn name(self) -> &'static str {
        match self {
            RuleTab::All => "All",
            RuleTab::Armed => "Armed",
            RuleTab::Live => "Live",
            RuleTab::Workspace => "Workspace",
        }
    }

    /// Whether the tab lists the rule.
    pub fn lists(self, status: &RuleStatus) -> bool {
        match self {
            RuleTab::All => true,
            RuleTab::Armed => status.state == RuleState::Armed,
            RuleTab::Live => status.live,
            RuleTab::Workspace => !status.live,
        }
    }
}

/// The rules of one scope, as the Rules view groups them.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedRules {
    /// Whom the rules apply to.
    pub scope: Scope,
    /// The rules, in the order the environment holds them.
    pub rules: Vec<RuleStatus>,
}

/// The rules that `tab` lists, grouped by their scope: the rules for every client first, then each other scope in the
/// order of its conditions.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::{PresetName, Rule, RuleState};
/// use gaslightmq_control_api::selector::{Scope, SelectorName};
/// use gaslightmq_control_api::kernel::ClientId;
/// use gaslightmq_ui::rules::{RuleTab, by_scope};
///
/// let rule = |name: &str| Rule::new(
///     name.parse().expect("a rule name"),
///     "order-producers".parse::<SelectorName>().expect("a selector name"),
///     "delay-1s".parse::<PresetName>().expect("a preset name"),
/// );
/// let ingest = Scope::client(ClientId::new("order-ingest").expect("a client ID"));
/// // The revision that the control plane told beside each rule.
/// let revision = Revision::new(7);
/// let rules = [
///     RuleStatus { rule: rule("slow-ingest").scoped(ingest.clone()), live: true, state: RuleState::Armed, revision },
///     RuleStatus { rule: rule("slow-everyone"), live: false, state: RuleState::Disarmed, revision },
/// ];
///
/// let groups = by_scope(&rules, RuleTab::All);
/// assert_eq!(groups[0].scope, Scope::all());
/// assert_eq!(groups[1].scope, ingest);
/// assert_eq!(by_scope(&rules, RuleTab::Armed).len(), 1);
/// ```
pub fn by_scope(rules: &[RuleStatus], tab: RuleTab) -> Vec<ScopedRules> {
    let mut groups: Vec<ScopedRules> = Vec::new();
    for status in rules.iter().filter(|status| tab.lists(status)) {
        let scope = status.rule.scope();
        match groups.iter_mut().find(|group| &group.scope == scope) {
            Some(group) => group.rules.push(status.clone()),
            None => groups.push(ScopedRules {
                scope: scope.clone(),
                rules: vec![status.clone()],
            }),
        }
    }
    groups.sort_by(|a, b| a.scope.cmp(&b.scope));
    groups
}

/// What someone can do with a rule from where it stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleCommand {
    /// Arm a rule that is not armed.
    Arm,
    /// Disarm an armed rule.
    Disarm,
    /// Skip the trigger of an armed rule once.
    Fire,
    /// Save a live rule as a rule of the workspace.
    Keep,
    /// Take a live rule out of its environment.
    Delete,
}

impl RuleCommand {
    /// The command as a button says it.
    pub const fn name(self) -> &'static str {
        match self {
            RuleCommand::Arm => "Arm",
            RuleCommand::Disarm => "Disarm",
            RuleCommand::Fire => "Fire",
            RuleCommand::Keep => "Keep",
            RuleCommand::Delete => "Delete",
        }
    }
}

/// Whom a scope applies to, in a few words: `All clients`, the ID of one client, or its conditions.
///
/// ```
/// use gaslightmq_control_api::kernel::ClientId;
/// use gaslightmq_control_api::selector::Scope;
/// use gaslightmq_ui::rules::scope_label;
///
/// assert_eq!(scope_label(&Scope::all()), "All clients");
/// assert_eq!(scope_label(&Scope::client(ClientId::new("order-ingest").expect("a client ID"))), "order-ingest");
/// ```
pub fn scope_label(scope: &Scope) -> String {
    if scope.is_all() {
        return "All clients".to_owned();
    }
    let conditions = scope.conditions();
    match (conditions.len(), conditions.get(Field::ClientId)) {
        (1, Some(Condition::Is(client))) => client.to_string(),
        _ => scope.to_string(),
    }
}

/// What someone can do with the rule, in the order the inspector offers it.
///
/// An armed rule disarms and fires, and a disarmed one arms. A draft is never evaluated, so it does neither.
/// A live rule can be kept or deleted. A rule of the workspace changes in the workspace.
pub fn commands(status: &RuleStatus) -> Vec<RuleCommand> {
    let mut commands = match status.state {
        RuleState::Armed => vec![RuleCommand::Disarm, RuleCommand::Fire],
        RuleState::Disarmed => vec![RuleCommand::Arm],
        RuleState::Draft => Vec::new(),
    };
    if status.live {
        commands.extend([RuleCommand::Keep, RuleCommand::Delete]);
    }
    commands
}

/// An operation on a rule of an environment, as the Rules view sends it.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleOperation {
    /// Arm the rule.
    Arm(ArmRule),
    /// Disarm the rule.
    Disarm(DisarmRule),
    /// Fire the rule by hand.
    Fire(FireRule),
    /// Keep the live rule in the workspace.
    Keep(KeepRule),
    /// Take the live rule out of its environment.
    Delete(DeleteLiveRule),
    /// Replace the live rule of the environment.
    UpdateLive(UpdateLiveRule),
    /// Replace the rule of the workspace, for every environment.
    UpdateWorkspace(UpdateDefinition<Rule>),
}

impl RuleOperation {
    /// What the operation changes, which its revision belongs to, or `None` for a firing, which changes nothing.
    ///
    /// A live rule, and a rule of the workspace with its state, change in their environment.
    /// An update of a rule of the workspace changes its definition, for every environment.
    pub fn subject(&self) -> Option<Subject> {
        match self {
            RuleOperation::Arm(ArmRule { environment, rule, .. })
            | RuleOperation::Disarm(DisarmRule { environment, rule, .. })
            | RuleOperation::Keep(KeepRule { environment, rule, .. })
            | RuleOperation::Delete(DeleteLiveRule { environment, rule, .. }) => Some(Subject::rule(environment, rule)),
            RuleOperation::UpdateLive(UpdateLiveRule { environment, rule, .. }) => {
                Some(Subject::rule(environment, rule.name()))
            }
            RuleOperation::UpdateWorkspace(update) => Some(Subject::Definition(DefinitionRef::Rule(
                update.definition.name().clone(),
            ))),
            RuleOperation::Fire(_) => None,
        }
    }

    /// The same operation, expecting `expects` of its [subject](Self::subject): the run and the revision that the
    /// view showed. A firing expects nothing, and stays as it is.
    #[must_use]
    pub fn expecting(self, expects: Option<Expects>) -> Self {
        match self {
            RuleOperation::Arm(arm) => RuleOperation::Arm(ArmRule { expects, ..arm }),
            RuleOperation::Disarm(disarm) => RuleOperation::Disarm(DisarmRule { expects, ..disarm }),
            RuleOperation::Keep(keep) => RuleOperation::Keep(KeepRule { expects, ..keep }),
            RuleOperation::Delete(delete) => RuleOperation::Delete(DeleteLiveRule { expects, ..delete }),
            RuleOperation::UpdateLive(update) => RuleOperation::UpdateLive(UpdateLiveRule { expects, ..update }),
            RuleOperation::UpdateWorkspace(update) => {
                RuleOperation::UpdateWorkspace(UpdateDefinition { expects, ..update })
            }
            fire @ RuleOperation::Fire(_) => fire,
        }
    }
}

/// The operation that carries out `command` on the rule of an environment.
///
/// It expects nothing until the view says [what it expects](RuleOperation::expecting), as it does for every change.
pub fn operation(environment: &EnvironmentName, status: &RuleStatus, command: RuleCommand) -> RuleOperation {
    let environment = environment.clone();
    let rule = status.rule.name().clone();
    let expects = None;
    match command {
        RuleCommand::Arm => RuleOperation::Arm(ArmRule {
            environment,
            rule,
            expects,
        }),
        RuleCommand::Disarm => RuleOperation::Disarm(DisarmRule {
            environment,
            rule,
            expects,
        }),
        RuleCommand::Fire => RuleOperation::Fire(FireRule { environment, rule }),
        RuleCommand::Keep => RuleOperation::Keep(KeepRule {
            environment,
            rule,
            expects,
        }),
        RuleCommand::Delete => RuleOperation::Delete(DeleteLiveRule {
            environment,
            rule,
            expects,
        }),
    }
}

/// The operation that replaces a rule with `changed`, which has its name: a live rule changes in its environment,
/// and a rule of the workspace changes in the workspace, for every environment.
/// It expects nothing until the view says [what it expects](RuleOperation::expecting), as it does for every change.
pub fn update(environment: &EnvironmentName, status: &RuleStatus, changed: Rule) -> RuleOperation {
    if status.live {
        RuleOperation::UpdateLive(UpdateLiveRule {
            environment: environment.clone(),
            rule: changed,
            expects: None,
        })
    } else {
        RuleOperation::UpdateWorkspace(UpdateDefinition {
            expects: None,
            definition: changed,
        })
    }
}

/// The rule with `chance` as the chance of its trigger, and every other part as it was.
/// A rule that only fires by hand stays so.
pub fn with_chance(rule: &Rule, chance: Option<Chance>) -> Rule {
    let trigger = match rule.trigger() {
        Trigger::Manual => Trigger::Manual,
        Trigger::Parts(parts) => Trigger::Parts(match chance {
            Some(chance) => parts.with_chance(chance),
            None => parts.without_chance(),
        }),
    };
    rule.clone().with_trigger(trigger)
}

/// The rule with `lifetime`, and every other part as it was.
pub fn with_lifetime(rule: &Rule, lifetime: Lifetime) -> Rule {
    rule.clone().with_lifetime(lifetime)
}

/// When a trigger fires, in words: `every match`, `40% of matches`, `every 10th match, after 30s`, `by hand only`.
///
/// ```
/// use gaslightmq_control_api::trigger::{Chance, Count, Trigger, TriggerParts};
/// use gaslightmq_ui::rules::trigger_text;
///
/// assert_eq!(trigger_text(&Trigger::always()), "every match");
/// assert_eq!(trigger_text(&Trigger::Manual), "by hand only");
/// let parts = TriggerParts::new().with_chance(Chance::from_percent(40)?).with_every(Count::new(10).expect("a count"));
/// assert_eq!(trigger_text(&Trigger::from(parts)), "40% of every 10th match");
/// # Ok::<(), gaslightmq_control_api::trigger::ChanceError>(())
/// ```
pub fn trigger_text(trigger: &Trigger) -> String {
    let Some(parts) = trigger.parts() else {
        return "by hand only".to_owned();
    };
    let mut text = match parts.chance() {
        Some(chance) => format!("{chance} of "),
        None => String::new(),
    };
    text.push_str(&count_text(parts));
    if let Some(window) = parts.window() {
        text.push_str(", ");
        text.push_str(&window.to_string());
    }
    text
}

/// Which matches the count parts let through, in words.
fn count_text(parts: &TriggerParts) -> String {
    let mut words: Vec<String> = Vec::new();
    if let Some(skip) = parts.skip() {
        words.push(format!("after the first {}", skip.value()));
    }
    if let Some(nth) = parts.nth() {
        words.push(format!("match {} only", nth.value()));
    }
    if let Some(every) = parts.every() {
        words.push(format!("every {}th match", every.value()));
    }
    if let Some(first) = parts.first() {
        words.push(format!("the first {} matches", first.value()));
    }
    if words.is_empty() {
        return if parts.chance().is_some() {
            "matches"
        } else {
            "every match"
        }
        .to_owned();
    }
    words.join(", ")
}

/// The lifetimes the inspector offers, as the mockups do: until disarmed, a while, or a budget.
pub fn lifetimes() -> Vec<Lifetime> {
    ["until disarmed", "30s", "5m", "100 matches", "3 firings"]
        .into_iter()
        .filter_map(|text| text.parse().ok())
        .collect()
}

/// What a rule did in each interval of the statistics: on how many calls it matched, and how often it fired.
///
/// The statistics only grow, so each interval is the difference between two of them, the oldest first.
/// An interval in which the rule was gone, or counted from zero again, shows what it counted since.
pub fn activity<'a>(history: impl IntoIterator<Item = &'a Stats>, rule: &RuleName) -> Vec<RuleStats> {
    let counted: Vec<RuleStats> = history
        .into_iter()
        .map(|stats| stats.rules.get(rule).copied().unwrap_or_default())
        .collect();
    counted
        .windows(2)
        .map(|pair| {
            let (before, now) = (pair[0], pair[1]);
            let since = |now: u64, before: u64| now.checked_sub(before).unwrap_or(now);
            RuleStats {
                matches: since(now.matches, before.matches),
                firings: since(now.firings, before.firings),
                left: now.left,
            }
        })
        .collect()
}
