//! The segment layout keeps the storage contract.

use futures::executor::block_on;
use gaslightmq_recording_conformance::{BATCH_SIZE, READER_MEMORY, assert_recording_storage};
use gaslightmq_recording_segments::prelude::*;

#[test]
fn the_segment_layout_over_memory_keeps_the_contract() {
    // The recording of the check is a few hundred kibibytes. With these sizes it is many blocks, spans and segments,
    // and more than one index chunk, so every level of the layout opens and closes.
    let mut configuration = SegmentConfiguration::new();
    configuration.block_size = BATCH_SIZE as u64;
    configuration.span_size = 2 * BATCH_SIZE as u64;
    configuration.segment_size = 6 * BATCH_SIZE as u64;
    configuration.index_size = 4 * 1024;
    configuration.reader_memory = READER_MEMORY as u64;
    let empty = || SegmentStorage::new(MemoryBacking::new(), configuration);

    block_on(assert_recording_storage(empty));
}
