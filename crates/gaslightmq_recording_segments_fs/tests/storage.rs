//! The segment layout over files keeps the storage contract.

use gaslightmq_recording_conformance::{BATCH_SIZE, READER_MEMORY, assert_recording_storage};
use gaslightmq_recording_segments::prelude::*;
use gaslightmq_recording_segments_fs::prelude::*;

#[tokio::test]
async fn the_segment_layout_over_files_keeps_the_contract() -> anyhow::Result<()> {
    let recordings = tempfile::tempdir()?;
    // The recording of the check is a few hundred kibibytes. With these sizes it is many blocks, spans and segments,
    // and more than one index chunk, so every level of the layout opens and closes, and the check reads sealed
    // files through their maps and growing files through reads.
    let mut configuration = SegmentConfiguration::new();
    configuration.block_size = BATCH_SIZE as u64;
    configuration.span_size = 2 * BATCH_SIZE as u64;
    configuration.segment_size = 6 * BATCH_SIZE as u64;
    configuration.index_size = 4 * 1024;
    configuration.reader_memory = READER_MEMORY as u64;
    let empty = || {
        let directory = tempfile::tempdir_in(recordings.path()).expect("a directory for the recordings");
        SegmentStorage::new(
            FileBacking::new(directory.keep(), FileConfiguration::new()),
            configuration,
        )
    };

    assert_recording_storage(empty).await;
    Ok(())
}
