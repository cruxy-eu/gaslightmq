//! A store of selectors in memory does what the store trait of the selectors asks.

use futures::executor::block_on;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_selector_conformance::assert_selector_store;

#[test]
fn a_store_of_selectors_in_memory_keeps_the_contract_and_says_who_includes_a_selector() {
    block_on(assert_selector_store(MemoryStore::new));
}
