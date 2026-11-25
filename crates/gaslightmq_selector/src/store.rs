//! Where selectors are kept: the store of the selectors, and what only it is asked.

use gaslightmq_definition_api::{Definition, Store, StoreReadError};

use crate::{Selector, SelectorName};

/// A selector is a definition: a store holds it under its [`SelectorName`].
impl Definition for Selector {
    type Name = SelectorName;

    const LABEL: &'static str = "selector";

    fn name(&self) -> &SelectorName {
        Selector::name(self)
    }
}

/// What keeps the selectors: the files of a workspace, memory, or anything else.
///
/// A store of selectors keeps the [store contract](Store), and it answers one question more.
/// A selector that another selector includes cannot go, so someone asks who includes it.
///
/// Every store of selectors has the answer: the trait finds it among the selectors that the store
/// [lists](Store::list).
/// A store that knows a shorter way, such as an index, writes the method itself.
/// A store with nothing to add is one line: `impl SelectorStore for MyStore {}`.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// /// The names of the selectors that would include nothing without `selector`.
/// async fn includers_of(
///     selectors: &impl SelectorStore,
///     selector: &SelectorName,
/// ) -> Result<Vec<SelectorName>, StoreReadError> {
///     let including = selectors.using_selector(selector).await?;
///     Ok(including.iter().map(|selector| selector.name().clone()).collect())
/// }
/// ```
pub trait SelectorStore: Store<Selector> {
    /// The selectors that [include](Selector::include) the selector of this name, in the order of their names.
    ///
    /// The store need not hold a selector of this name: a selector can include one that nobody defined.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_selector(
        &self,
        selector: &SelectorName,
    ) -> impl Future<Output = Result<Vec<Selector>, StoreReadError>> + Send {
        async move {
            let mut selectors = self.list().await?;
            selectors.retain(|held| held.include() == Some(selector));
            Ok(selectors)
        }
    }
}

/// A store of selectors in memory is a [`SelectorStore`], with the answers of the trait.
#[cfg(feature = "memory")]
impl SelectorStore for gaslightmq_definition_memory::MemoryStore<Selector> {}
