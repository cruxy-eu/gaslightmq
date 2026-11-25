//! A store in memory does what the store trait of each kind asks.

use futures::executor::block_on;
use gaslightmq_definition_conformance::{
    assert_environment_store, assert_layout_store, assert_preset_store, assert_rule_store, assert_selector_store,
};
use gaslightmq_definition_memory::MemoryStore;

#[test]
fn a_store_of_selectors_keeps_the_contract_and_says_who_includes_a_selector() {
    block_on(assert_selector_store(MemoryStore::new));
}

#[test]
fn a_store_of_presets_keeps_the_contract() {
    block_on(assert_preset_store(MemoryStore::new));
}

#[test]
fn a_store_of_rules_keeps_the_contract_and_says_who_uses_a_selector_or_a_preset() {
    block_on(assert_rule_store(MemoryStore::new));
}

#[test]
fn a_store_of_layouts_keeps_the_contract_and_says_who_uses_a_selector_or_a_preset() {
    block_on(assert_layout_store(MemoryStore::new));
}

#[test]
fn a_store_of_environments_keeps_the_contract() {
    block_on(assert_environment_store(MemoryStore::new));
}
