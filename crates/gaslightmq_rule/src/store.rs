//! Where rules and presets are kept: the store of each, and what only the store of the rules is asked.

use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_selector::SelectorName;

use crate::{Preset, PresetName, Rule, RuleName};

/// A rule is a definition: a store holds it under its [`RuleName`].
impl Definition for Rule {
    type Name = RuleName;

    const LABEL: &'static str = "rule";

    fn name(&self) -> &RuleName {
        Rule::name(self)
    }
}

/// A preset is a definition: a store holds it under its [`PresetName`].
impl Definition for Preset {
    type Name = PresetName;

    const LABEL: &'static str = "preset";

    fn name(&self) -> &PresetName {
        Preset::name(self)
    }
}

/// What keeps the presets: the files of a workspace, memory, or anything else.
///
/// A store of presets keeps the [store contract](Store), and nothing asks it more than that.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// /// The titles of the presets, for a list that someone picks from.
/// async fn titles(presets: &impl PresetStore) -> Result<Vec<String>, StoreReadError> {
///     let presets = presets.list().await?;
///     Ok(presets.iter().filter_map(Preset::title).map(ToString::to_string).collect())
/// }
/// ```
pub trait PresetStore: Store<Preset> {}

/// What keeps the rules: the files of a workspace, memory, or anything else.
///
/// A store of rules keeps the [store contract](Store), and it answers two questions more.
/// A selector or a preset that a rule names cannot go, and only the store knows which rules name it
/// without reading every rule.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// /// The names of the rules that would match nothing without `selector`.
/// async fn users_of(rules: &impl RuleStore, selector: &SelectorName) -> Result<Vec<RuleName>, StoreReadError> {
///     let using = rules.using_selector(selector).await?;
///     Ok(using.iter().map(|rule| rule.name().clone()).collect())
/// }
/// ```
pub trait RuleStore: Store<Rule> {
    /// The rules whose [selector](Rule::selector) is the selector of this name, in the order of their names.
    ///
    /// A rule with inline conditions names no selector, and is never among them.
    /// The store need not hold what the name stands for: it holds rules, and no selector.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_selector(&self, selector: &SelectorName)
    -> impl Future<Output = Result<Vec<Rule>, StoreReadError>> + Send;

    /// The rules whose [action](Rule::action) is the preset of this name, in the order of their names.
    ///
    /// A rule with an inline action names no preset, and is never among them.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_preset(&self, preset: &PresetName) -> impl Future<Output = Result<Vec<Rule>, StoreReadError>> + Send;
}
