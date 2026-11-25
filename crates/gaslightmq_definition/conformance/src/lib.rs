//! What every store of definitions does, as checks that the tests of any store run.
//!
//! The [store contract](gaslightmq_definition_api::Store) says what a store keeps to, whatever stands
//! behind it, and the store trait of each kind says what only a store of that kind is asked.
//! This crate holds that behaviour once.
//! A check takes a way to make an empty store, and fails when the store does not do what its trait asks.
//! The tests of a store call the check of each kind that the store holds.
//!
//! | Item | Is |
//! |---|---|
//! | [`assert_store_contract`] | The contract of every store, for any kind of definition: each behaviour on a store of its own. |
//! | [`assert_selector_store`] | The contract for selectors, and who includes a selector. |
//! | [`assert_preset_store`] | The contract for presets. |
//! | [`assert_rule_store`] | The contract for rules, and which rules use a selector or a preset. |
//! | [`assert_layout_store`] | The contract for pad layouts, and which layouts use a selector or a preset. |
//! | [`assert_environment_store`] | The contract for environments. |
//! | [`Samples`] | The definitions that [`assert_store_contract`] stores, with a set for each kind. |
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::Mutex;
//!
//! use futures::executor::block_on;
//! use gaslightmq_definition_api::prelude::*;
//! use gaslightmq_definition_conformance::assert_preset_store;
//! use gaslightmq_rule::prelude::*;
//!
//! /// A store of presets that someone wrote for a test.
//! #[derive(Default)]
//! struct Presets(Mutex<BTreeMap<PresetName, Preset>>);
//!
//! impl Store<Preset> for Presets {
//!     async fn list(&self) -> Result<Vec<Preset>, StoreReadError> {
//!         Ok(self.0.lock().expect("no test panicked").values().cloned().collect())
//!     }
//!
//!     async fn get(&self, name: &PresetName) -> Result<Option<Preset>, StoreReadError> {
//!         Ok(self.0.lock().expect("no test panicked").get(name).cloned())
//!     }
//!
//!     async fn create(&self, preset: Preset) -> Result<(), StoreCreateError> {
//!         let mut presets = self.0.lock().expect("no test panicked");
//!         if presets.contains_key(preset.name()) {
//!             return Err(StoreCreateError::NameTaken);
//!         }
//!         presets.insert(preset.name().clone(), preset);
//!         Ok(())
//!     }
//!
//!     async fn update(&self, preset: Preset) -> Result<(), StoreChangeError> {
//!         let mut presets = self.0.lock().expect("no test panicked");
//!         let held = presets.get_mut(preset.name()).ok_or(StoreChangeError::NotFound)?;
//!         *held = preset;
//!         Ok(())
//!     }
//!
//!     async fn delete(&self, name: &PresetName) -> Result<(), StoreChangeError> {
//!         let removed = self.0.lock().expect("no test panicked").remove(name);
//!         removed.map(drop).ok_or(StoreChangeError::NotFound)
//!     }
//! }
//!
//! impl PresetStore for Presets {}
//!
//! // The check is a future, and the test of a store drives it with the executor it has.
//! block_on(assert_preset_store(Presets::default));
//! ```
//!
//! # No IO
//!
//! The crate depends on the store contract and on the definitions.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

mod contract;
mod environments;
mod layouts;
mod rules;
mod samples;
mod selectors;

pub use contract::assert_store_contract;
pub use environments::assert_environment_store;
pub use layouts::assert_layout_store;
pub use rules::{assert_preset_store, assert_rule_store};
pub use samples::Samples;
pub use selectors::assert_selector_store;
