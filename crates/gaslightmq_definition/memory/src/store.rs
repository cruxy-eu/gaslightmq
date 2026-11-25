//! The store that keeps the definitions of one kind in memory.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use gaslightmq_definition_api::{Definition, Store, StoreChangeError, StoreCreateError, StoreReadError};
use gaslightmq_environment::{Environment, EnvironmentStore};
use gaslightmq_rule::{Preset, PresetName, PresetStore, Rule, RuleStore};
use gaslightmq_selector::{Selector, SelectorName, SelectorStore};
use gaslightmq_surface::{LayoutStore, PadLayout};

/// The definitions of one kind, kept in memory.
///
/// A new store holds nothing, and it never fails to read or to keep a definition.
/// It refuses what the [contract](Store) refuses: a name that is taken, and a name that it does not hold.
/// It keeps the contract once, for every kind of definition, and it is the store of each kind on top of that:
/// a `MemoryStore<Rule>` is a [`RuleStore`], and answers which rules use a selector.
///
/// Whoever shares the store shares one set of definitions: put it behind an `Arc`, or lend it out.
/// A change is one step, so a task never sees half of it.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_definition_memory::MemoryStore;
/// use gaslightmq_selector::prelude::*;
///
/// let selectors = MemoryStore::new();
/// let fetches = Selector::new("fetches".parse().expect("a selector name"), Conditions::new());
///
/// // Updating never creates a definition.
/// assert_eq!(block_on(selectors.update(fetches.clone())), Err(StoreChangeError::NotFound));
///
/// block_on(selectors.create(fetches.clone())).expect("the name is free");
/// let titled = fetches.with_title("Every fetch".parse().expect("a selector title"));
/// block_on(selectors.update(titled.clone()))?;
/// assert_eq!(block_on(selectors.list()).expect("a store in memory reads"), [titled]);
/// # Ok::<(), StoreChangeError>(())
/// ```
pub struct MemoryStore<D: Definition> {
    by_name: Mutex<BTreeMap<D::Name, D>>,
}

impl<D: Definition> MemoryStore<D> {
    /// A store that holds no definition.
    pub fn new() -> Self {
        Self {
            by_name: Mutex::new(BTreeMap::new()),
        }
    }

    /// The definitions, for one step.
    ///
    /// A task that panicked while it held them left them whole: every step changes the map once, at its end.
    fn definitions(&self) -> MutexGuard<'_, BTreeMap<D::Name, D>> {
        self.by_name.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The definitions that `uses` holds for, in the order of their names.
    fn that(&self, uses: impl Fn(&D) -> bool) -> Vec<D> {
        self.definitions()
            .values()
            .filter(|definition| uses(definition))
            .cloned()
            .collect()
    }
}

impl<D: Definition> Default for MemoryStore<D> {
    fn default() -> Self {
        Self::new()
    }
}

/// Writes the word for the kind of the store, and the names it holds.
impl<D: Definition<Name: fmt::Debug>> fmt::Debug for MemoryStore<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let definitions = self.definitions();
        let names: Vec<&D::Name> = definitions.keys().collect();
        f.debug_struct("MemoryStore")
            .field("of", &D::LABEL)
            .field("names", &names)
            .finish()
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a store in memory has nothing to wait for"
)]
impl<D: Definition> Store<D> for MemoryStore<D> {
    async fn list(&self) -> Result<Vec<D>, StoreReadError> {
        Ok(self.definitions().values().cloned().collect())
    }

    async fn get(&self, name: &D::Name) -> Result<Option<D>, StoreReadError> {
        Ok(self.definitions().get(name).cloned())
    }

    async fn create(&self, definition: D) -> Result<(), StoreCreateError> {
        let mut definitions = self.definitions();
        let name = definition.name().clone();
        if definitions.contains_key(&name) {
            return Err(StoreCreateError::NameTaken);
        }
        definitions.insert(name, definition);
        Ok(())
    }

    async fn update(&self, definition: D) -> Result<(), StoreChangeError> {
        match self.definitions().get_mut(definition.name()) {
            Some(held) => {
                *held = definition;
                Ok(())
            }
            None => Err(StoreChangeError::NotFound),
        }
    }

    async fn delete(&self, name: &D::Name) -> Result<(), StoreChangeError> {
        match self.definitions().remove(name) {
            Some(_) => Ok(()),
            None => Err(StoreChangeError::NotFound),
        }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a store in memory has nothing to wait for"
)]
impl SelectorStore for MemoryStore<Selector> {
    async fn using_selector(&self, selector: &SelectorName) -> Result<Vec<Selector>, StoreReadError> {
        Ok(self.that(|held| held.include() == Some(selector)))
    }
}

impl PresetStore for MemoryStore<Preset> {}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a store in memory has nothing to wait for"
)]
impl RuleStore for MemoryStore<Rule> {
    async fn using_selector(&self, selector: &SelectorName) -> Result<Vec<Rule>, StoreReadError> {
        Ok(self.that(|rule| rule.selector().name() == Some(selector)))
    }

    async fn using_preset(&self, preset: &PresetName) -> Result<Vec<Rule>, StoreReadError> {
        Ok(self.that(|rule| rule.action().preset() == Some(preset)))
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a store in memory has nothing to wait for"
)]
impl LayoutStore for MemoryStore<PadLayout> {
    async fn using_selector(&self, selector: &SelectorName) -> Result<Vec<PadLayout>, StoreReadError> {
        Ok(self.that(|layout| layout.rows().contains(selector)))
    }

    async fn using_preset(&self, preset: &PresetName) -> Result<Vec<PadLayout>, StoreReadError> {
        Ok(self.that(|layout| layout.columns().contains(preset)))
    }
}

impl EnvironmentStore for MemoryStore<Environment> {}

#[cfg(test)]
mod tests {
    use gaslightmq_selector::Conditions;

    use super::*;

    #[test]
    fn a_store_shows_its_kind_and_its_names_when_debugging() {
        let selectors = MemoryStore::new();
        for name in ["order-producers", "fetches"] {
            let selector = Selector::new(name.parse().unwrap(), Conditions::new());
            futures::executor::block_on(selectors.create(selector)).unwrap();
        }

        assert_eq!(
            format!("{selectors:?}"),
            "MemoryStore { of: \"selector\", names: [SelectorName(\"fetches\"), SelectorName(\"order-producers\")] }"
        );
    }
}
