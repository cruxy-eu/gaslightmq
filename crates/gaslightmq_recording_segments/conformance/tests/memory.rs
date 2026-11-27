//! The backing in memory does what the backing contract asks.

use futures::executor::block_on;
use gaslightmq_recording_segments::prelude::*;
use gaslightmq_recording_segments_conformance::assert_segment_backing;

#[test]
fn the_backing_in_memory_keeps_the_contract() {
    block_on(assert_segment_backing(MemoryBacking::new));
}
