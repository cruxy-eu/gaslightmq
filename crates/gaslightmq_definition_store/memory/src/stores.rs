//! The stores of a workspace that is kept in memory.

use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule};
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;
use gaslightmq_workspace::Stores;

use crate::MemoryStore;

/// The [`Stores`] of a workspace that is kept in memory: a [`MemoryStore`] for each kind of definition.
///
/// New stores hold nothing, and everything they hold is gone when they are.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_store_api::prelude::*;
/// use gaslightmq_definition_store_memory::MemoryStores;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let stores = MemoryStores::new();
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
///
/// block_on(stores.presets().create(blackhole.clone()))?;
/// assert_eq!(block_on(stores.presets().list()).expect("a store in memory reads"), [blackhole]);
/// assert_eq!(block_on(stores.rules().list()).expect("a store in memory reads"), []);
/// # Ok::<(), StoreCreateError>(())
/// ```
#[derive(Debug, Default)]
pub struct MemoryStores {
    selectors: MemoryStore<Selector>,
    presets: MemoryStore<Preset>,
    rules: MemoryStore<Rule>,
    layouts: MemoryStore<PadLayout>,
    environments: MemoryStore<Environment>,
}

impl MemoryStores {
    /// Stores that hold no definition.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Stores for MemoryStores {
    type Selectors = MemoryStore<Selector>;
    type Presets = MemoryStore<Preset>;
    type Rules = MemoryStore<Rule>;
    type Layouts = MemoryStore<PadLayout>;
    type Environments = MemoryStore<Environment>;

    fn selectors(&self) -> &MemoryStore<Selector> {
        &self.selectors
    }

    fn presets(&self) -> &MemoryStore<Preset> {
        &self.presets
    }

    fn rules(&self) -> &MemoryStore<Rule> {
        &self.rules
    }

    fn layouts(&self) -> &MemoryStore<PadLayout> {
        &self.layouts
    }

    fn environments(&self) -> &MemoryStore<Environment> {
        &self.environments
    }
}
