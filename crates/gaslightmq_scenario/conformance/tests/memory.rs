//! A store of scenarios in memory does what its store trait asks.

use futures::executor::block_on;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_scenario_conformance::assert_scenario_store;

#[test]
fn a_store_of_scenarios_in_memory_keeps_the_contract_and_says_who_names_a_rule_a_selector_or_a_preset() {
    block_on(assert_scenario_store(MemoryStore::new));
}
