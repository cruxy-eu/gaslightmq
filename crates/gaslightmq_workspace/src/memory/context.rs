//! The context of a workspace that is kept in memory.

use crate::{MemoryStores, WorkspaceContext};

/// The context of a workspace that is kept in memory: [`MemoryStores`].
///
/// [`new`](Self::new) is the context of a test, and of an engine that runs without the files of a workspace:
/// nothing is written anywhere.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
///
/// block_on(workspace.create(blackhole.clone()))?;
///
/// // The store of the presets holds the preset.
/// assert_eq!(block_on(workspace.stores().presets().list()).expect("a store in memory reads"), [blackhole]);
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug, Default)]
pub struct MemoryContext {
    stores: MemoryStores,
}

impl MemoryContext {
    /// A context whose stores hold nothing.
    pub fn new() -> Self {
        Self::default()
    }
}

impl WorkspaceContext for MemoryContext {
    type Stores = MemoryStores;

    fn stores(&self) -> &MemoryStores {
        &self.stores
    }
}
