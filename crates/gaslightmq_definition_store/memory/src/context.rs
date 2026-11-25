//! The context of a workspace that is kept in memory.

use gaslightmq_workspace::{UuidSource, WorkspaceContext};

use crate::{KnownUuids, MemoryStores};

/// The context of a workspace that is kept in memory: [`MemoryStores`], and a source of UUIDs.
///
/// [`new`](Self::new) is the context of a test: nothing is written anywhere,
/// and the UUIDs are [`KnownUuids`], so the test knows each one before the workspace makes it.
/// [`with_uuids`](Self::with_uuids) takes another source, such as the one that reads the clock and the randomness
/// of the system for an engine that runs without the files of a workspace.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_store_api::prelude::*;
/// use gaslightmq_definition_store_memory::{KnownUuids, MemoryContext};
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
///
/// let created = block_on(workspace.create(blackhole))?;
///
/// // The preset got the first UUID of the source, and the store of the presets holds it.
/// assert_eq!(created.uuid().map(String::from), Some(KnownUuids::nth(1)));
/// assert_eq!(block_on(workspace.stores().presets().list()).expect("a store in memory reads"), [created]);
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug, Default)]
pub struct MemoryContext<Uuids = KnownUuids> {
    stores: MemoryStores,
    uuids: Uuids,
}

impl MemoryContext {
    /// A context whose stores hold nothing, and whose UUIDs are the [`KnownUuids`] from the first one on.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Uuids: UuidSource> MemoryContext<Uuids> {
    /// A context whose stores hold nothing, and which takes the seeds of its UUIDs from `uuids`.
    pub fn with_uuids(uuids: Uuids) -> Self {
        Self {
            stores: MemoryStores::new(),
            uuids,
        }
    }
}

impl<Uuids: UuidSource + Send + Sync> WorkspaceContext for MemoryContext<Uuids> {
    type Stores = MemoryStores;
    type Uuids = Uuids;

    fn stores(&self) -> &MemoryStores {
        &self.stores
    }

    fn uuids(&mut self) -> &mut Uuids {
        &mut self.uuids
    }
}
