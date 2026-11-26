//! Where scenarios are kept: the store of the scenarios, and what only it is asked.

use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_rule::{PresetName, RuleName};
use gaslightmq_selector::SelectorName;

use crate::{Scenario, ScenarioName};

/// A scenario is a definition: a store holds it under its [`ScenarioName`].
impl Definition for Scenario {
    type Name = ScenarioName;

    const LABEL: &'static str = "scenario";

    fn name(&self) -> &ScenarioName {
        Scenario::name(self)
    }
}

/// What keeps the scenarios: the files of a workspace, memory, or anything else.
///
/// A store of scenarios keeps the [store contract](Store), and it answers three questions more.
/// A rule, a selector or a preset that a scenario names cannot go, so someone asks which scenarios name it.
///
/// Every store of scenarios has the answers: the trait finds them among the scenarios that the store
/// [lists](Store::list).
/// A store that knows a shorter way, such as an index, writes the methods itself.
/// A store with nothing to add is one line: `impl ScenarioStore for MyStore {}`.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
///
/// /// The names of the scenarios that would name nothing without `rule`.
/// async fn users_of(scenarios: &impl ScenarioStore, rule: &RuleName) -> Result<Vec<ScenarioName>, StoreReadError> {
///     let using = scenarios.using_rule(rule).await?;
///     Ok(using.iter().map(|scenario| scenario.name().clone()).collect())
/// }
/// ```
pub trait ScenarioStore: Store<Scenario> {
    /// The scenarios that name the rule of this name, in the order of their names:
    /// among their own rules, or in a step that arms it.
    ///
    /// A scenario that owns a rule of that name does not name it: the rule it owns is its own, and no rule of the
    /// workspace.
    /// The store need not hold what the name stands for: it holds scenarios, and no rule.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_rule(&self, rule: &RuleName) -> impl Future<Output = Result<Vec<Scenario>, StoreReadError>> + Send {
        async move {
            let mut scenarios = self.list().await?;
            scenarios.retain(|scenario| scenario.rule_references().contains(&rule));
            Ok(scenarios)
        }
    }

    /// The scenarios that name the selector of this name, in the order of their names:
    /// in a rule they own, or in the condition that ends a step.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_selector(
        &self,
        selector: &SelectorName,
    ) -> impl Future<Output = Result<Vec<Scenario>, StoreReadError>> + Send {
        async move {
            let mut scenarios = self.list().await?;
            scenarios.retain(|scenario| scenario.selector_references().contains(&selector));
            Ok(scenarios)
        }
    }

    /// The scenarios that own a rule with the preset of this name as its action, in the order of their names.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_preset(&self, preset: &PresetName) -> impl Future<Output = Result<Vec<Scenario>, StoreReadError>> + Send {
        async move {
            let mut scenarios = self.list().await?;
            scenarios.retain(|scenario| scenario.preset_references().contains(&preset));
            Ok(scenarios)
        }
    }
}

/// A store of scenarios in memory is a [`ScenarioStore`], with the answers of the trait.
#[cfg(feature = "memory")]
impl ScenarioStore for gaslightmq_definition_memory::MemoryStore<Scenario> {}
