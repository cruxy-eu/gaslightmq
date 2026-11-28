//! A step of a Sequence, and the condition that ends it.

use gaslightmq_clock::TimeSpan;
use gaslightmq_rule::RuleName;
use gaslightmq_selector::{SelectorName, SelectorRef};
use gaslightmq_trigger::Count;

use crate::{ScenarioRule, StepName, StepTitle};

/// One stage of the story of a Sequence: the rules it arms, and the condition that ends it.
///
/// | Part | Meaning | When left out |
/// |---|---|---|
/// | Name | The identity of the step within its scenario. The journal and the UI name the step by it. | `step-` and its place among the steps, from 1. |
/// | Title | What happens in the step, in free text. | No title. |
/// | Arm | The rules that the step arms, in the order that resolves their conflicts: rules of the workspace by name, and rules of its own. | The step arms no rule. |
/// | Until | The condition that ends the step. | The step ends when someone advances or deactivates the scenario. |
///
/// ```yaml
/// name: leader-churn
/// title: Leader churn
/// arm: [refuse-with-not-leader]
/// until: { firings: 3, rule: refuse-with-not-leader }
/// ```
///
/// A rule of the step's own is a map, which needs no name: it has the name of what it [says](gaslightmq_rule::RulePhrase).
/// It belongs to the scenario, and the step arms it while it runs.
///
/// ```yaml
/// arm:
///   - selector: { api: Produce }
///     action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER }
/// until: { firings: 3 }
/// ```
///
/// A step that ends on `firings` without a `rule` ends on the one rule it arms.
/// The [`Scenario`](crate::Scenario) names a step without a name, and the rule of `firings`, when it reads them.
///
/// When the step ends, the next one starts, and the rules that this step armed disarm.
/// A step checks nothing on its own: the [`Scenario`](crate::Scenario) it is part of checks that its name is
/// unique, and that it arms each rule once and ends on a rule that is armed.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let refuse: RuleName = "refuse-with-not-leader".parse().expect("a rule name");
/// let three = Count::new(3).expect("a count");
/// let step = Step::new("leader-churn".parse()?)
///     .with_title("Leader churn".parse().expect("a step title"))
///     .arming([refuse.clone()])
///     .ending_on(Until::Firings { count: three, rule: refuse.clone() });
///
/// assert_eq!(step.name(), "leader-churn");
/// assert_eq!(step.arm(), [ScenarioRule::from(refuse.clone())]);
/// assert_eq!(step.until(), Some(&Until::Firings { count: three, rule: refuse }));
/// # Ok::<(), StepNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Step {
    name: StepName,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    title: Option<StepTitle>,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Vec::is_empty"))]
    arm: Vec<ScenarioRule>,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    until: Option<Until>,
}

impl Step {
    /// A step that arms no rule, and ends when someone advances or deactivates the scenario. It has no title.
    pub const fn new(name: StepName) -> Self {
        Self {
            name,
            title: None,
            arm: Vec::new(),
            until: None,
        }
    }

    /// The same step with a title, which says what happens in it.
    /// It replaces the title the step had before.
    #[must_use]
    pub fn with_title(self, title: StepTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same step without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
    }

    /// The same step, arming these rules instead of the ones it armed before:
    /// rules of the workspace by their names, and rules of its own.
    ///
    /// Their order resolves their conflicts, as in a Chain: the first rule wins.
    #[must_use]
    pub fn arming<R: Into<ScenarioRule>>(self, rules: impl IntoIterator<Item = R>) -> Self {
        Self {
            arm: rules.into_iter().map(Into::into).collect(),
            ..self
        }
    }

    /// The same step, ending on this condition instead of the one it ended on before.
    #[must_use]
    pub fn ending_on(self, until: Until) -> Self {
        Self {
            until: Some(until),
            ..self
        }
    }

    /// The same step, ending only when someone advances or deactivates the scenario.
    #[must_use]
    pub fn ending_by_hand(self) -> Self {
        Self { until: None, ..self }
    }

    /// The name of the step, unique among the steps of its scenario.
    pub const fn name(&self) -> &StepName {
        &self.name
    }

    /// What happens in the step, in free text, when someone wrote it down.
    pub const fn title(&self) -> Option<&StepTitle> {
        self.title.as_ref()
    }

    /// The rules that the step arms, in the order that resolves their conflicts.
    pub fn arm(&self) -> &[ScenarioRule] {
        &self.arm
    }

    /// The condition that ends the step, or `None` when the step ends only when someone advances or deactivates the
    /// scenario.
    pub const fn until(&self) -> Option<&Until> {
        self.until.as_ref()
    }
}

/// The condition that ends a [`Step`].
///
/// | Condition | The step ends |
/// |---|---|
/// | [`Calls`](Self::Calls) | After that many calls matched the selector. |
/// | [`Firings`](Self::Firings) | After that rule's action applied that many times. |
/// | [`After`](Self::After) | After that much time on the clock of the engine, from the start of the step. |
///
/// A counted condition is a budget: `firings: 3` lets the rule fire exactly three times,
/// even when several connections match it at the same moment.
///
/// With the `serde` feature, a condition is a map: `{ calls: 100, selector: order-producers }`,
/// `{ firings: 3, rule: refuse-with-not-leader }` or `{ after: 20s }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Until {
    /// After `count` calls matched `selector`: a selector of the workspace by name, or conditions of the step's own.
    Calls { count: Count, selector: SelectorRef },
    /// After the action of `rule` applied `count` times.
    /// The rule is one that the step arms, or one that the scenario keeps armed for the whole run.
    Firings { count: Count, rule: RuleName },
    /// After that much time on the clock of the engine, measured from the start of the step.
    After(TimeSpan),
}

impl Until {
    /// The selector of the workspace that the condition names, or `None` when it names none.
    pub fn selector(&self) -> Option<&SelectorName> {
        match self {
            Until::Calls { selector, .. } => selector.name(),
            Until::Firings { .. } | Until::After(_) => None,
        }
    }

    /// The rule whose firings the condition counts, or `None` when it counts none.
    pub const fn rule(&self) -> Option<&RuleName> {
        match self {
            Until::Firings { rule, .. } => Some(rule),
            Until::Calls { .. } | Until::After(_) => None,
        }
    }
}
