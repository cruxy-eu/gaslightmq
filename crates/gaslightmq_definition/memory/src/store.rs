//! The store that keeps the definitions of one kind in memory.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use gaslightmq_definition_api::{Definition, Store, StoreChangeError, StoreCreateError, StoreReadError};

/// The definitions of one kind, kept in memory.
///
/// A new store holds nothing, and it never fails to read or to keep a definition.
/// It refuses what the [contract](Store) refuses: a name that is taken, and a name that it does not hold.
/// It keeps the contract once, for every kind of definition, and it knows none of them.
/// The crate of a kind makes it the store of its kind, with the store trait that the crate holds.
///
/// Whoever shares the store shares one set of definitions: put it behind an `Arc`, or lend it out.
/// A change is one step, so a task never sees half of it.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_definition_memory::MemoryStore;
///
/// /// What a team says when it starts its day, under a name.
/// #[derive(Debug, Clone, PartialEq)]
/// struct Greeting {
///     name: String,
///     text: String,
/// }
///
/// impl Definition for Greeting {
///     type Name = String;
///
///     const LABEL: &'static str = "greeting";
///
///     fn name(&self) -> &String {
///         &self.name
///     }
/// }
///
/// let greetings = MemoryStore::new();
/// let morning = Greeting { name: "morning".to_owned(), text: "Good morning".to_owned() };
///
/// // Updating never creates a definition.
/// assert_eq!(block_on(greetings.update(morning.clone())), Err(StoreChangeError::NotFound));
///
/// block_on(greetings.create(morning.clone())).expect("the name is free");
/// let louder = Greeting { text: "Good morning, everyone".to_owned(), ..morning };
/// block_on(greetings.update(louder.clone()))?;
/// assert_eq!(block_on(greetings.list()).expect("a store in memory reads"), [louder]);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// What a team says when it starts its day, under a name.
    #[derive(Clone)]
    struct Greeting {
        name: String,
    }

    impl Definition for Greeting {
        type Name = String;

        const LABEL: &'static str = "greeting";

        fn name(&self) -> &String {
            &self.name
        }
    }

    #[test]
    fn a_store_shows_its_kind_and_its_names_when_debugging() {
        let greetings = MemoryStore::new();
        for name in ["morning", "evening"] {
            let greeting = Greeting { name: name.to_owned() };
            futures::executor::block_on(greetings.create(greeting)).unwrap();
        }

        assert_eq!(
            format!("{greetings:?}"),
            "MemoryStore { of: \"greeting\", names: [\"evening\", \"morning\"] }"
        );
    }
}
