//! Where pad layouts are kept: the store of the layouts, and what only it is asked.

use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_rule::PresetName;
use gaslightmq_selector::SelectorName;

use crate::{LayoutName, PadLayout};

/// A pad layout is a definition: a store holds it under its [`LayoutName`].
impl Definition for PadLayout {
    type Name = LayoutName;

    const LABEL: &'static str = "layout";

    fn name(&self) -> &LayoutName {
        PadLayout::name(self)
    }
}

/// What keeps the pad layouts: the files of a workspace, memory, or anything else.
///
/// A store of layouts keeps the [store contract](Store), and it answers two questions more.
/// A selector with a row or a preset with a column cannot go, so someone asks which layouts have one.
///
/// Every store of layouts has the answers: the trait finds them among the layouts that the store
/// [lists](Store::list).
/// A store that knows a shorter way, such as an index, writes the methods itself.
/// A store with nothing to add is one line: `impl LayoutStore for MyStore {}`.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// /// The names of the layouts that would lose a column without `preset`.
/// async fn users_of(layouts: &impl LayoutStore, preset: &PresetName) -> Result<Vec<LayoutName>, StoreReadError> {
///     let using = layouts.using_preset(preset).await?;
///     Ok(using.iter().map(|layout| layout.name().clone()).collect())
/// }
/// ```
pub trait LayoutStore: Store<PadLayout> {
    /// The layouts with a [row](PadLayout::rows) for the selector of this name, in the order of their names.
    ///
    /// The store need not hold what the name stands for: it holds layouts, and no selector.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_selector(
        &self,
        selector: &SelectorName,
    ) -> impl Future<Output = Result<Vec<PadLayout>, StoreReadError>> + Send {
        async move {
            let mut layouts = self.list().await?;
            layouts.retain(|layout| layout.rows().contains(selector));
            Ok(layouts)
        }
    }

    /// The layouts with a [column](PadLayout::columns) for the preset of this name, in the order of their names.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn using_preset(&self, preset: &PresetName) -> impl Future<Output = Result<Vec<PadLayout>, StoreReadError>> + Send {
        async move {
            let mut layouts = self.list().await?;
            layouts.retain(|layout| layout.columns().contains(preset));
            Ok(layouts)
        }
    }
}

/// A store of pad layouts in memory is a [`LayoutStore`], with the answers of the trait.
#[cfg(feature = "memory")]
impl LayoutStore for gaslightmq_definition_memory::MemoryStore<PadLayout> {}
