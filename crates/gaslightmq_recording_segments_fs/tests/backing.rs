//! The backing on files keeps the backing contract of the segment layout.

use gaslightmq_recording_segments_conformance::assert_segment_backing;
use gaslightmq_recording_segments_fs::prelude::*;

#[tokio::test]
async fn the_backing_on_files_keeps_the_contract() -> anyhow::Result<()> {
    let recordings = tempfile::tempdir()?;
    // Each behaviour of the check gets a directory of its own, which goes with `recordings` when the test ends.
    let empty = || {
        let directory = tempfile::tempdir_in(recordings.path()).expect("a directory for the recordings");
        FileBacking::new(directory.keep(), FileConfiguration::new())
    };

    assert_segment_backing(empty).await;
    Ok(())
}

#[tokio::test]
async fn the_backing_on_files_keeps_the_contract_with_the_smallest_reads() -> anyhow::Result<()> {
    let recordings = tempfile::tempdir()?;
    // A read of one byte at a time, and no read-ahead: every chunk of a file that grows takes reads of its own.
    let mut configuration = FileConfiguration::new();
    configuration.read_size = 1;
    configuration.read_ahead = 0;
    let empty = || {
        let directory = tempfile::tempdir_in(recordings.path()).expect("a directory for the recordings");
        FileBacking::new(directory.keep(), configuration)
    };

    assert_segment_backing(empty).await;
    Ok(())
}
