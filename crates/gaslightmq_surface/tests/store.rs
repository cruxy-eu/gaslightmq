//! A store of pad layouts in memory does what the store trait of the pad layouts asks.

use futures::executor::block_on;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_surface::conformance::assert_layout_store;

#[test]
fn a_store_of_layouts_in_memory_keeps_the_contract_and_says_who_uses_a_selector_or_a_preset() {
    block_on(assert_layout_store(MemoryStore::new));
}
