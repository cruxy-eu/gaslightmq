//! The scenario: a failure story, as a file writes it.

use std::collections::BTreeSet;

use gaslightmq_rule::{PresetName, Rule, RuleName};
use gaslightmq_selector::SelectorName;
use gaslightmq_trigger::Seed;

use crate::{Mode, ScenarioName, ScenarioTitle, Step, StepName, Until};

/// A scenario as a file or the UI writes it: a failure story that groups rules, decides how they combine, and can
/// play them out over time.
///
/// | Part | Meaning | When left out |
/// |---|---|---|
/// | Name | The identity of the scenario: a slug, unique among scenarios. | |
/// | Title | What the scenario shows, in free text. | No title. |
/// | Mode | How its rules combine: Live, Chain or Sequence, with the steps of a Sequence. | |
/// | Seed | The root of every die in the run. | |
/// | Rules | The rules it owns, or names in the workspace, in its order. They stay armed for the whole run. | No rule. |
///
/// ```yaml
/// name: producer-recovers-from-leader-churn
/// title: Producer recovers from leader churn
/// mode: sequence
/// seed: 7
/// rules:
///   - background-jitter
/// steps:
///   - name: warm-up
///     until: { calls: 100, selector: order-producers }
///   - name: leader-churn
///     arm: [refuse-with-not-leader]
///     until: { firings: 3, rule: refuse-with-not-leader }
///   - name: back-to-normal
/// ```
///
/// A scenario is a definition, and it holds only what someone writes.
/// Whether it is active, which step runs and what its rules counted is live state, and sits beside it.
/// What a backend must support follows from its rules: the incidents they fire.
/// The backend of an environment checks that when someone activates the scenario there.
///
/// # What always holds
///
/// - No rule stands in the scenario twice, whether it owns the rule or names it.
/// - A Sequence has at least one step, and no two steps share a name.
/// - A step arms no rule twice, and none that the scenario keeps armed for the whole run.
/// - A step that ends on the firings of a rule ends on a rule that is armed during the step:
///   one that it arms, or one of the scenario.
///
/// A scenario can still name a rule or a selector that does not exist, and own a rule that names a selector or a
/// preset that does not exist. Whoever holds the whole workspace checks that.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let jitter: RuleName = "background-jitter".parse().expect("a rule name");
/// let refuse: RuleName = "refuse-with-not-leader".parse().expect("a rule name");
/// let order_producers: SelectorName = "order-producers".parse().expect("a selector name");
/// let warm_up = Step::new("warm-up".parse().expect("a step name"))
///     .ending_on(Until::Calls { count: Count::new(100).expect("a count"), selector: order_producers.clone().into() });
/// let churn = Step::new("leader-churn".parse().expect("a step name"))
///     .arming([refuse.clone()])
///     .ending_on(Until::Firings { count: Count::new(3).expect("a count"), rule: refuse.clone() });
///
/// let scenario = Scenario::new(
///     "producer-recovers-from-leader-churn".parse().expect("a scenario name"),
///     Mode::Sequence(vec![warm_up, churn]),
///     Seed::new(7),
///     [ScenarioRule::from(jitter.clone())],
/// )?
/// .with_title("Producer recovers from leader churn".parse().expect("a scenario title"));
///
/// // The scenario names two rules of the workspace, and one selector.
/// assert_eq!(scenario.rule_references(), [&jitter, &refuse]);
/// assert_eq!(scenario.selector_references(), [&order_producers]);
///
/// // A step cannot arm a rule that the scenario keeps armed for the whole run.
/// let error = Scenario::new(
///     "jitter-twice".parse().expect("a scenario name"),
///     Mode::Sequence(vec![Step::new("again".parse().expect("a step name")).arming([jitter.clone()])]),
///     Seed::new(7),
///     [ScenarioRule::from(jitter)],
/// )
/// .unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "step \"again\" of scenario \"jitter-twice\" arms rule \"background-jitter\", \
///      which the scenario keeps armed for the whole run"
/// );
/// # Ok::<(), ScenarioError>(())
/// ```
///
/// With the `serde` feature, a scenario serializes as the map above.
/// Writing leaves out a title that the scenario does not have and an empty list of rules,
/// and only a Sequence writes its steps.
/// Reading rejects a key it does not know, and steps in a Live or a Chain scenario.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(into = "crate::serde::ScenarioMap")
)]
pub struct Scenario {
    name: ScenarioName,
    title: Option<ScenarioTitle>,
    mode: Mode,
    seed: Seed,
    rules: Vec<ScenarioRule>,
}

impl Scenario {
    /// A scenario of this mode and seed, with these rules in its order. It has no title.
    ///
    /// # Errors
    ///
    /// - [`ScenarioError::DuplicateRule`] when two of `rules` have the same name.
    /// - [`ScenarioError::NoStep`] when the mode is a Sequence without a step.
    /// - [`ScenarioError::DuplicateStep`] when two steps have the same name.
    /// - [`ScenarioError::DuplicateArm`] when a step arms a rule twice.
    /// - [`ScenarioError::ArmsScenarioRule`] when a step arms one of `rules`, which stay armed for the whole run.
    /// - [`ScenarioError::EndsOnUnarmedRule`] when a step ends on the firings of a rule that is not armed during it.
    pub fn new(
        name: ScenarioName,
        mode: Mode,
        seed: Seed,
        rules: impl IntoIterator<Item = ScenarioRule>,
    ) -> Result<Self, ScenarioError> {
        let rules: Vec<ScenarioRule> = rules.into_iter().collect();
        if let Some(rule) = first_repeated(rules.iter().map(ScenarioRule::name)) {
            return Err(ScenarioError::DuplicateRule {
                scenario: name,
                rule: rule.clone(),
            });
        }
        if let Mode::Sequence(steps) = &mode {
            if steps.is_empty() {
                return Err(ScenarioError::NoStep { scenario: name });
            }
            if let Some(step) = first_repeated(steps.iter().map(Step::name)) {
                return Err(ScenarioError::DuplicateStep {
                    scenario: name,
                    step: step.clone(),
                });
            }
            for step in steps {
                check_step(&name, step, &rules)?;
            }
        }
        Ok(Self {
            name,
            title: None,
            mode,
            seed,
            rules,
        })
    }

    /// The same scenario with a title, which says what it shows.
    /// It replaces the title the scenario had before.
    #[must_use]
    pub fn with_title(self, title: ScenarioTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same scenario without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
    }

    /// The name of the scenario: its identity, unique among scenarios.
    pub const fn name(&self) -> &ScenarioName {
        &self.name
    }

    /// What the scenario shows, in free text, when someone wrote it down.
    pub const fn title(&self) -> Option<&ScenarioTitle> {
        self.title.as_ref()
    }

    /// How the rules of the scenario combine, with the steps of a Sequence.
    pub const fn mode(&self) -> &Mode {
        &self.mode
    }

    /// The root of every die in the run.
    pub const fn seed(&self) -> Seed {
        self.seed
    }

    /// The rules of the scenario, in its order. They stay armed for the whole run.
    pub fn rules(&self) -> &[ScenarioRule] {
        &self.rules
    }

    /// The steps of a Sequence, in the order they run. A Live and a Chain scenario have none.
    pub fn steps(&self) -> &[Step] {
        self.mode.steps()
    }

    /// The rules that the scenario owns, in its order.
    pub fn owned_rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter().filter_map(ScenarioRule::owned)
    }

    /// The rules of the workspace that the scenario names, each once, where it names them first:
    /// among its own rules, and then in the steps.
    ///
    /// A rule that the scenario owns is no rule of the workspace, and is not among them.
    pub fn rule_references(&self) -> Vec<&RuleName> {
        let listed = self.rules.iter().filter(|rule| rule.owned().is_none());
        let armed = self.steps().iter().flat_map(Step::arm);
        once_each(listed.map(ScenarioRule::name).chain(armed))
    }

    /// The selectors of the workspace that the scenario names, each once, where it names them first:
    /// in the rules it owns, and then in the conditions that end its steps.
    pub fn selector_references(&self) -> Vec<&SelectorName> {
        let owned = self.owned_rules().filter_map(|rule| rule.selector().name());
        let until = self.steps().iter().filter_map(|step| step.until()?.selector());
        once_each(owned.chain(until))
    }

    /// The presets of the workspace that the rules of the scenario name, each once, where they name them first.
    pub fn preset_references(&self) -> Vec<&PresetName> {
        once_each(self.owned_rules().filter_map(|rule| rule.action().preset()))
    }

    /// The parts of the scenario, to write it down.
    #[cfg(feature = "serde")]
    pub(crate) fn into_parts(self) -> (ScenarioName, Option<ScenarioTitle>, Mode, Seed, Vec<ScenarioRule>) {
        (self.name, self.title, self.mode, self.seed, self.rules)
    }
}

/// A rule of a [`Scenario`]: one it owns, or one of the workspace that it names.
///
/// Rule names are unique among all rules, whether a scenario owns the rule or the workspace does.
/// Whoever holds the whole workspace checks that.
///
/// With the `serde` feature, a named rule is its name as text, and a rule that the scenario owns is the map of the
/// rule.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let named = ScenarioRule::from("background-jitter".parse::<RuleName>()?);
/// let owned = ScenarioRule::from(Rule::new(
///     "blackhole-metadata".parse()?,
///     Conditions::from(Condition::is(ApiKey::METADATA)),
///     LieDefinition::Silence,
/// ));
///
/// assert_eq!(named.name(), "background-jitter");
/// assert_eq!(named.owned(), None);
/// assert_eq!(owned.name(), "blackhole-metadata");
/// assert!(owned.owned().is_some());
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScenarioRule {
    /// The rule of the workspace of this name.
    Named(RuleName),
    /// A rule that belongs to the scenario, and to nothing else.
    Owned(Box<Rule>),
}

impl ScenarioRule {
    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        match self {
            ScenarioRule::Named(name) => name,
            ScenarioRule::Owned(rule) => rule.name(),
        }
    }

    /// The rule, when the scenario owns it, or `None` when it names a rule of the workspace.
    pub const fn owned(&self) -> Option<&Rule> {
        match self {
            ScenarioRule::Owned(rule) => Some(rule),
            ScenarioRule::Named(_) => None,
        }
    }
}

impl From<RuleName> for ScenarioRule {
    fn from(name: RuleName) -> Self {
        ScenarioRule::Named(name)
    }
}

impl From<Rule> for ScenarioRule {
    fn from(rule: Rule) -> Self {
        ScenarioRule::Owned(Box::new(rule))
    }
}

/// Why a [`Scenario`] cannot be made of its parts.
///
/// Each variant names the scenario, and the step where a step is wrong.
///
/// ```
/// use gaslightmq_scenario::prelude::*;
///
/// let error = ScenarioError::NoStep { scenario: "leader-churn".parse()? };
/// assert_eq!(error.to_string(), "scenario \"leader-churn\" is a sequence without a step");
/// # Ok::<(), ScenarioNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScenarioError {
    /// Two rules of the scenario have the same name, so one could not be told from the other.
    #[error("scenario {:?} has two rules named {:?}", .scenario.as_str(), .rule.as_str())]
    DuplicateRule { scenario: ScenarioName, rule: RuleName },
    /// The scenario is a Sequence, and it has no step to run.
    #[error("scenario {:?} is a sequence without a step", .scenario.as_str())]
    NoStep { scenario: ScenarioName },
    /// Two steps have the same name, so the journal and the UI could not tell them apart.
    #[error("scenario {:?} has two steps named {:?}", .scenario.as_str(), .step.as_str())]
    DuplicateStep { scenario: ScenarioName, step: StepName },
    /// A step arms the same rule twice.
    #[error(
        "step {:?} of scenario {:?} arms rule {:?} twice",
        .step.as_str(),
        .scenario.as_str(),
        .rule.as_str()
    )]
    DuplicateArm {
        scenario: ScenarioName,
        step: StepName,
        rule: RuleName,
    },
    /// A step arms a rule of the scenario, which is armed for the whole run already,
    /// and would disarm it when the step ends.
    #[error(
        "step {:?} of scenario {:?} arms rule {:?}, which the scenario keeps armed for the whole run",
        .step.as_str(),
        .scenario.as_str(),
        .rule.as_str()
    )]
    ArmsScenarioRule {
        scenario: ScenarioName,
        step: StepName,
        rule: RuleName,
    },
    /// A step ends on the firings of a rule that is not armed during the step, so it would never end that way.
    #[error(
        "step {:?} of scenario {:?} ends on the firings of rule {:?}, which is not armed during the step",
        .step.as_str(),
        .scenario.as_str(),
        .rule.as_str()
    )]
    EndsOnUnarmedRule {
        scenario: ScenarioName,
        step: StepName,
        rule: RuleName,
    },
}

/// What is wrong with one step of the scenario of that name, with those rules.
fn check_step(scenario: &ScenarioName, step: &Step, rules: &[ScenarioRule]) -> Result<(), ScenarioError> {
    let of_the_scenario = |rule: &RuleName| rules.iter().any(|listed| listed.name() == rule);
    if let Some(rule) = first_repeated(step.arm()) {
        return Err(ScenarioError::DuplicateArm {
            scenario: scenario.clone(),
            step: step.name().clone(),
            rule: rule.clone(),
        });
    }
    if let Some(rule) = step.arm().iter().find(|rule| of_the_scenario(rule)) {
        return Err(ScenarioError::ArmsScenarioRule {
            scenario: scenario.clone(),
            step: step.name().clone(),
            rule: rule.clone(),
        });
    }
    if let Some(rule) = step.until().and_then(Until::rule)
        && !step.arm().contains(rule)
        && !of_the_scenario(rule)
    {
        return Err(ScenarioError::EndsOnUnarmedRule {
            scenario: scenario.clone(),
            step: step.name().clone(),
            rule: rule.clone(),
        });
    }
    Ok(())
}

/// The first item that comes a second time.
fn first_repeated<T: Ord + Copy>(items: impl IntoIterator<Item = T>) -> Option<T> {
    let mut seen = BTreeSet::new();
    items.into_iter().find(|item| !seen.insert(*item))
}

/// The items, each where it comes first.
fn once_each<T: Ord + Copy>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut seen = BTreeSet::new();
    items.into_iter().filter(|item| seen.insert(*item)).collect()
}
