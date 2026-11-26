//! What the stores would hold after a change, for whoever checks what only it knows before it asks for the change.

use super::Workspace;
use crate::contents::Contents;
use crate::{DefinitionName, WorkspaceContext, WorkspaceDefinition};

/// What the stores of a workspace would hold after one change, which nobody asked for yet.
///
/// Whoever holds what the workspace does not know reads it, before it asks for the change:
/// a host checks that every rule that is armed would still resolve. The preview checks nothing itself, and it
/// changes nothing: the change goes through [`update`](Workspace::update) or [`delete`](Workspace::delete), which
/// check it as they always do.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
/// block_on(async {
///     workspace.create(blackhole.clone()).await?;
///     let after = workspace.preview_delete(blackhole.name().clone()).await.expect("stores in memory read");
///     assert_eq!(after.definitions::<Preset>().count(), 0);
///     // Nothing changed yet.
///     assert_eq!(workspace.stores().presets().list().await.expect("a store in memory reads").len(), 1);
///     Ok::<(), CreateError>(())
/// })?;
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Preview {
    contents: Contents,
}

impl Preview {
    /// Every definition of the kind `D` that the stores would hold after the change, in the order of their names.
    pub fn definitions<D: WorkspaceDefinition>(&self) -> impl Iterator<Item = &D> {
        self.contents.of::<D>()
    }
}

impl<C: WorkspaceContext> Workspace<C> {
    /// What the stores would hold if `definition` took the place of the definition of its name, or came next to the
    /// others. `None` when a store could not read: the change itself fails then, and says why.
    pub async fn preview_update<D: WorkspaceDefinition>(&self, definition: D) -> Option<Preview> {
        let contents = self.contents().await.ok()?;
        Some(Preview {
            contents: contents.with(definition.into_any()),
        })
    }

    /// What the stores would hold without the definition of that kind and name.
    /// `None` when a store could not read: the change itself fails then, and says why.
    pub async fn preview_delete(&self, name: impl Into<DefinitionName>) -> Option<Preview> {
        let contents = self.contents().await.ok()?;
        Some(Preview {
            contents: contents.without(&name.into()),
        })
    }
}
