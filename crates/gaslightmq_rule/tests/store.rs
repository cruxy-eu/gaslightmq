//! A store of rules and a store of presets in memory do what their store traits ask.

use futures::executor::block_on;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_rule::conformance::{assert_preset_store, assert_rule_store};

#[test]
fn a_store_of_presets_in_memory_keeps_the_contract() {
    block_on(assert_preset_store(MemoryStore::new));
}

#[test]
fn a_store_of_rules_in_memory_keeps_the_contract_and_says_who_uses_a_selector_or_a_preset() {
    block_on(assert_rule_store(MemoryStore::new));
}
