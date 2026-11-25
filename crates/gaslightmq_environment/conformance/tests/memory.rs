//! A store of environments in memory does what the store trait of the environments asks.

use futures::executor::block_on;
use gaslightmq_definition_memory::MemoryStore;
use gaslightmq_environment_conformance::assert_environment_store;

#[test]
fn a_store_of_environments_in_memory_keeps_the_contract() {
    block_on(assert_environment_store(MemoryStore::new));
}
