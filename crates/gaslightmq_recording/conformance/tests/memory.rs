//! A storage of recordings in memory does what the storage contract asks.

use futures::executor::block_on;
use gaslightmq_recording_conformance::{BATCH_SIZE, MemoryStorage, READER_MEMORY, assert_recording_storage};

#[test]
fn a_storage_in_memory_keeps_the_contract() {
    let empty = || {
        MemoryStorage::new()
            .with_batch_size(BATCH_SIZE)
            .with_reader_memory(READER_MEMORY)
    };

    block_on(assert_recording_storage(empty));
}
