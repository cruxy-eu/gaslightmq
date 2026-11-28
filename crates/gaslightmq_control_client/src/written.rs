//! What a caller hands over as a definition: its text, as a file or a macro writes it, its value, or its name.

use gaslightmq_control_api::rule::{Rule, RuleName};
use gaslightmq_control_api::scenario::{Scenario, ScenarioName};
use gaslightmq_control_api::selector::{Conditions, SelectorName, SelectorRef};
use gaslightmq_definition_text::{DefinitionText, ReadDefinitionError};

/// A rule to arm in an environment: a rule of the workspace or of the environment by its name, or a rule to create
/// there as a live rule.
///
/// | From | Is |
/// |---|---|
/// | [`rule!`](crate::rule) | The rule that its map writes. |
/// | `&str` or `String` | A name, when the text is one: `order-acknowledgements-get-lost`. The YAML of a rule otherwise, as a file holds it. |
/// | [`RuleName`] | The rule of that name. |
/// | [`Rule`] | That rule. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleToArm(Given<Rule, RuleName>);

/// A scenario to activate in an environment: a scenario of the workspace by its name, or a scenario to create in the
/// workspace first.
///
/// | From | Is |
/// |---|---|
/// | [`scenario!`](crate::scenario) | The scenario that its map writes. It needs no name. |
/// | `&str` or `String` | A name, when the text is one: `leader-churn`. The YAML of a scenario otherwise, as a file holds it. |
/// | [`ScenarioName`] | The scenario of that name. |
/// | [`Scenario`] | That scenario. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioToActivate(Given<Scenario, ScenarioName>);

/// The calls that a selector matches, to follow or to look for in a recording.
///
/// | From | Is |
/// |---|---|
/// | [`selector!`](crate::selector) | The conditions that its map writes: `selector! { api: Produce }`. |
/// | `&str` | A selector of the workspace by its name, or the YAML of conditions. |
/// | [`SelectorRef`], [`SelectorName`], [`Conditions`] | That selector. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorToMatch(Written<SelectorRef>);

/// A definition as someone wrote it, or as a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Written<D> {
    Text(DefinitionText<D>),
    Value(D),
}

/// A definition, or the name of one that is there already.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Given<D, N> {
    Named(N),
    Written(Written<D>),
}

impl<D: serde::de::DeserializeOwned + Clone> Written<D> {
    /// The definition, read from its text.
    pub(crate) fn read(&self) -> Result<D, ReadDefinitionError> {
        match self {
            Written::Text(text) => text.read(),
            Written::Value(value) => Ok(value.clone()),
        }
    }
}

impl RuleToArm {
    pub(crate) fn given(&self) -> &Given<Rule, RuleName> {
        &self.0
    }
}

impl ScenarioToActivate {
    pub(crate) fn given(&self) -> &Given<Scenario, ScenarioName> {
        &self.0
    }
}

impl SelectorToMatch {
    /// The selector, read from its text.
    pub(crate) fn read(&self) -> Result<SelectorRef, ReadDefinitionError> {
        self.0.read()
    }
}

impl From<DefinitionText<Rule>> for RuleToArm {
    fn from(text: DefinitionText<Rule>) -> Self {
        Self(Given::Written(Written::Text(text)))
    }
}

impl From<&str> for RuleToArm {
    fn from(text: &str) -> Self {
        match RuleName::new(text) {
            Ok(name) => Self(Given::Named(name)),
            Err(_) => Self::from(DefinitionText::new(text)),
        }
    }
}

impl From<String> for RuleToArm {
    fn from(text: String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<RuleName> for RuleToArm {
    fn from(name: RuleName) -> Self {
        Self(Given::Named(name))
    }
}

impl From<Rule> for RuleToArm {
    fn from(rule: Rule) -> Self {
        Self(Given::Written(Written::Value(rule)))
    }
}

impl From<DefinitionText<Scenario>> for ScenarioToActivate {
    fn from(text: DefinitionText<Scenario>) -> Self {
        Self(Given::Written(Written::Text(text)))
    }
}

impl From<&str> for ScenarioToActivate {
    fn from(text: &str) -> Self {
        match ScenarioName::new(text) {
            Ok(name) => Self(Given::Named(name)),
            Err(_) => Self::from(DefinitionText::new(text)),
        }
    }
}

impl From<String> for ScenarioToActivate {
    fn from(text: String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<ScenarioName> for ScenarioToActivate {
    fn from(name: ScenarioName) -> Self {
        Self(Given::Named(name))
    }
}

impl From<Scenario> for ScenarioToActivate {
    fn from(scenario: Scenario) -> Self {
        Self(Given::Written(Written::Value(scenario)))
    }
}

impl From<DefinitionText<SelectorRef>> for SelectorToMatch {
    fn from(text: DefinitionText<SelectorRef>) -> Self {
        Self(Written::Text(text))
    }
}

impl From<&str> for SelectorToMatch {
    fn from(text: &str) -> Self {
        Self(Written::Text(DefinitionText::new(text)))
    }
}

impl From<SelectorRef> for SelectorToMatch {
    fn from(selector: SelectorRef) -> Self {
        Self(Written::Value(selector))
    }
}

impl From<SelectorName> for SelectorToMatch {
    fn from(name: SelectorName) -> Self {
        Self::from(SelectorRef::from(name))
    }
}

impl From<Conditions> for SelectorToMatch {
    fn from(conditions: Conditions) -> Self {
        Self::from(SelectorRef::from(conditions))
    }
}

/// The text of a [rule](gaslightmq_control_api::rule::Rule), written as the entries of its map, as a file holds it.
///
/// A bare word or a literal is a value of the YAML: `Produce`, `750ms`, `orders.*`, `40%`.
/// A Rust expression in parentheses is a value of the code, in its `serde` form: `(TRIES_REFUSED)`.
/// A rule without a `name` has the name of what it says.
/// See [`definition_text!`](gaslightmq_definition_text::definition_text) for every word.
///
/// ```
/// use gaslightmq_control_api::rule::Rule;
/// use gaslightmq_control_client::rule;
///
/// const TRIES_REFUSED: u64 = 1;
///
/// let written = rule! {
///     selector: { api: Produce },
///     action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER },
///     trigger: { first: (TRIES_REFUSED) },
/// };
/// let rule: Rule = written.read()?;
/// assert_eq!(rule.name(), "refuse-with-not-leader-or-follower-on-produce");
///
/// // What does not read names the key that failed.
/// let error = rule! { selector: { api: Produce }, action: { lie: silence }, trigger: { first: 0 } }.read().unwrap_err();
/// assert_eq!(error.to_string(), "trigger.first: a count is at least one");
/// # Ok::<(), gaslightmq_control_client::ReadDefinitionError>(())
/// ```
#[macro_export]
macro_rules! rule {
    ($($entries:tt)*) => {{
        let text: $crate::DefinitionText<$crate::__kinds::Rule> = $crate::definition_text!($($entries)*);
        text
    }};
}

/// The text of a [scenario](gaslightmq_control_api::scenario::Scenario), written as the entries of its map, as a file
/// holds it.
///
/// Its steps need no names, and the rules that they arm need none either: a step is `step-` and its place, and a rule
/// has the name of what it says. The words are those of [`rule!`](crate::rule).
///
/// ```
/// use gaslightmq_control_api::scenario::Scenario;
/// use gaslightmq_control_client::scenario;
///
/// let written = scenario! {
///     name: orders-survive-leader-churn,
///     mode: sequence,
///     seed: 7,
///     steps: [
///         { until: { calls: 1, selector: { api: Produce } } },
///         {
///             arm: [{ selector: { api: Produce }, action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER } }],
///             until: { firings: 2 },
///         },
///         { title: Recovery },
///     ],
/// };
/// let scenario: Scenario = written.read()?;
/// let steps: Vec<&str> = scenario.steps().iter().map(|step| step.name().as_str()).collect();
/// assert_eq!(steps, ["step-1", "step-2", "step-3"]);
/// assert_eq!(scenario.owned_rules().next().map(|rule| rule.name().as_str()), Some("refuse-with-not-leader-or-follower-on-produce"));
/// # Ok::<(), gaslightmq_control_client::ReadDefinitionError>(())
/// ```
#[macro_export]
macro_rules! scenario {
    ($($entries:tt)*) => {{
        let text: $crate::DefinitionText<$crate::__kinds::Scenario> = $crate::definition_text!($($entries)*);
        text
    }};
}

/// The text of the conditions of a selector, written as the entries of its map: `selector! { api: Produce }`.
///
/// ```
/// use gaslightmq_control_api::selector::SelectorRef;
/// use gaslightmq_control_client::selector;
///
/// let produces: SelectorRef = selector! { api: Produce, topic: orders.* }.read()?;
/// assert!(matches!(produces, SelectorRef::Inline(conditions) if conditions.len() == 2));
/// # Ok::<(), gaslightmq_control_client::ReadDefinitionError>(())
/// ```
#[macro_export]
macro_rules! selector {
    ($($entries:tt)*) => {{
        let text: $crate::DefinitionText<$crate::__kinds::SelectorRef> = $crate::definition_text!($($entries)*);
        text
    }};
}
