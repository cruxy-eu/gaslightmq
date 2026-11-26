//! The stores of a workspace that is kept in memory.

use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;

use crate::Stores;

/// The [`Stores`] of a workspace that is kept in memory: a store in memory for each kind of definition.
///
/// New stores hold nothing, and everything they hold is gone when they are.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_api::prelude::*;
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
    scenarios: MemoryStore<Scenario>,
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
    type Scenarios = MemoryStore<Scenario>;
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

    fn scenarios(&self) -> &MemoryStore<Scenario> {
        &self.scenarios
    }

    fn layouts(&self) -> &MemoryStore<PadLayout> {
        &self.layouts
    }

    fn environments(&self) -> &MemoryStore<Environment> {
        &self.environments
    }
}
