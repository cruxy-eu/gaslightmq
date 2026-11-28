//! A recording that a crash cut short on the disk opens, up to its last whole commit.
//!
//! The test writes a recording to files, and then shortens the file of its segment the way a crash in the middle of
//! an append leaves it: the first part of the last append is on the disk, and the rest is not.

use std::fs::{self, OpenOptions};

use gaslightmq_clock::Instant;
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::ConnectionNumber;
use gaslightmq_recording::prelude::*;
use gaslightmq_recording_segments::prelude::*;
use gaslightmq_recording_segments_fs::prelude::*;

#[tokio::test]
async fn a_segment_file_torn_inside_its_last_append_opens_up_to_the_commit_before() -> anyhow::Result<()> {
    let recordings = tempfile::tempdir()?;
    let description = Description {
        environment: "orders-sim".parse()?,
        started: "2026-11-21T14:02:11.527Z".parse()?,
        clock: Instant::ZERO,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Simulated,
        written_by: "0.1.0".to_owned(),
        armed: Vec::new(),
        scenario: None,
        journal_after: None,
    };
    let segment = recordings
        .path()
        .join("orders-sim.2026-11-21T14-02-11.527Z")
        .join("00000000000000000000.segment.open");
    let closed = |connection| {
        Entry::session(&RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    };
    let (committed_first, committed_last) = (0..3, 3..6);

    let storage = SegmentStorage::new(
        FileBacking::new(recordings.path(), FileConfiguration::new()),
        SegmentConfiguration::new(),
    );
    let mut writer = storage.start(description.clone()).await?;
    for connection in committed_first.clone() {
        writer.append(closed(connection)).await?;
    }
    writer.commit().await?;
    let first = fs::metadata(&segment)?.len();
    for connection in committed_last {
        writer.append(closed(connection)).await?;
    }
    writer.commit().await?;
    let last = fs::metadata(&segment)?.len();
    drop(writer);
    drop(storage);

    // The machine crashes while the last commit is half on the disk.
    let torn = first + (last - first) / 2;
    OpenOptions::new().write(true).open(&segment)?.set_len(torn)?;

    let storage = SegmentStorage::new(
        FileBacking::new(recordings.path(), FileConfiguration::new()),
        SegmentConfiguration::new(),
    );
    let reader = storage.open(&description.id()).await?;
    assert_eq!(
        reader.extent().await?,
        Extent {
            entries: committed_first.end,
            end: None
        },
        "a recording cut short on the disk goes as far as its last whole chunk, and has no end"
    );
    assert_eq!(
        reader.bytes_left_unread(),
        torn - first,
        "the reader says how many bytes of the file behind the last whole chunk it left unread"
    );
    let mut entries = reader
        .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
        .await?;
    let mut positions = Vec::new();
    while let Some(batch) = entries.next().await? {
        positions.extend(batch.entries().map(|(position, _)| position.number()));
    }
    assert_eq!(
        positions,
        committed_first.collect::<Vec<_>>(),
        "the stream hands out what the whole chunks on the disk hold"
    );
    Ok(())
}
