//! The stores of a workspace: one for each kind of definition.

use gaslightmq_environment::EnvironmentStore;
use gaslightmq_rule::{PresetStore, RuleStore};
use gaslightmq_selector::SelectorStore;
use gaslightmq_surface::LayoutStore;

/// The stores that a [`Workspace`](crate::Workspace) works over: one for each kind of definition.
///
/// Whatever keeps the definitions of a workspace implements this once, and names the store of each kind.
/// The files of a workspace are one such set of stores, and memory is another.
/// The five need not be of one sort: the rules can be in files while the layouts are in memory.
///
/// Reading goes to a store directly, because no rule of a workspace is about reading.
/// Changes go through the [`Workspace`](crate::Workspace), which checks them first.
///
/// Whoever holds the stores shares them between its tasks, so they are `Send` and `Sync`.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// /// The names of the rules, whichever stores hold the workspace.
/// async fn rule_names(stores: &impl Stores) -> Result<Vec<RuleName>, StoreReadError> {
///     let rules = stores.rules().list().await?;
///     Ok(rules.iter().map(|rule| rule.name().clone()).collect())
/// }
/// ```
pub trait Stores: Send + Sync {
    /// What keeps the selectors.
    type Selectors: SelectorStore;
    /// What keeps the presets.
    type Presets: PresetStore;
    /// What keeps the rules.
    type Rules: RuleStore;
    /// What keeps the pad layouts.
    type Layouts: LayoutStore;
    /// What keeps the environments.
    type Environments: EnvironmentStore;

    /// The store of the selectors.
    fn selectors(&self) -> &Self::Selectors;

    /// The store of the presets.
    fn presets(&self) -> &Self::Presets;

    /// The store of the rules.
    fn rules(&self) -> &Self::Rules;

    /// The store of the pad layouts.
    fn layouts(&self) -> &Self::Layouts;

    /// The store of the environments.
    fn environments(&self) -> &Self::Environments;
}
