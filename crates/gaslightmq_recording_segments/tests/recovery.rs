//! A recording that a crash cut short, or whose bytes went bad, still opens, and says what it lost.
//!
//! Each test writes a recording with the segment layout over the backing in memory, and then breaks it the way a
//! crash or a disk does: it tears the last append, takes an object away, or turns the bits of a byte.

use futures::executor::block_on;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::ConnectionNumber;
use gaslightmq_recording::prelude::*;
use gaslightmq_recording_segments::prelude::*;

#[test]
fn a_recording_cut_short_inside_its_last_commit_opens_up_to_the_commit_before() -> anyhow::Result<()> {
    let backing = MemoryBacking::new();
    let storage = SegmentStorage::new(backing.clone(), SegmentConfiguration::new());
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
    let recording = description.id();
    let segment = Object::segment(Position::FIRST);
    let closed = |connection| {
        Entry::session(&RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    };
    let (committed_first, committed_last) = (0..3, 3..6);

    block_on(async {
        let mut writer = storage.start(description.clone()).await?;
        for connection in committed_first.clone() {
            writer.append(closed(connection)).await?;
        }
        writer.commit().await?;
        let [first] = backing.list(&recording, ObjectKind::Segment, Position::FIRST).await?[..] else {
            anyhow::bail!("the recording is one segment");
        };
        for connection in committed_last {
            writer.append(closed(connection)).await?;
        }
        writer.commit().await?;
        let [last] = backing.list(&recording, ObjectKind::Segment, Position::FIRST).await?[..] else {
            anyhow::bail!("the recording is one segment");
        };

        // The machine crashes while the last commit is half on the disk.
        let torn = first.length + (last.length - first.length) / 2;
        backing.cut_short(&recording, segment, torn);
        drop(writer);

        let reader = storage.open(&recording).await?;
        assert_eq!(
            reader.extent().await?,
            Extent {
                entries: committed_first.end,
                end: None
            },
            "a recording cut short goes as far as its last whole chunk, and has no end"
        );
        assert_eq!(
            reader.bytes_left_unread(),
            torn - first.length,
            "the reader says how many bytes behind the last whole chunk it left unread"
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
            "the stream hands out what the whole chunks hold"
        );
        Ok(())
    })
}

#[test]
fn a_recording_without_its_manifest_opens_from_its_segments() -> anyhow::Result<()> {
    let backing = MemoryBacking::new();
    let storage = SegmentStorage::new(backing.clone(), SegmentConfiguration::new());
    let description = Description {
        environment: "orders-sim".parse()?,
        started: "2026-11-21T14:02:11.527Z".parse()?,
        clock: Instant::ZERO + Duration::from_secs(42),
        mode: RecordingMode::MetadataOnly,
        backend: RecordedBackend::Upstream,
        written_by: "0.1.0".to_owned(),
        armed: vec!["leader-flap".parse()?],
        scenario: None,
        journal_after: None,
    };
    let recording = description.id();
    let connections = 0..6;
    let end = End {
        ending: Ending::Stopped,
        at: Instant::ZERO + Duration::from_secs(90),
        entries: connections.end,
        calls: 0,
    };

    block_on(async {
        let mut writer = storage.start(description.clone()).await?;
        for connection in connections {
            let event = RecordedSessionEvent {
                connection: ConnectionNumber::new(connection),
                at: Instant::ZERO,
                change: SessionChange::Closed { by: Closer::Client },
            };
            writer.append(Entry::session(&event)).await?;
        }
        writer.finish(end).await?;

        backing.drop_manifest(&recording);

        let reader = storage.open(&recording).await?;
        assert_eq!(
            reader.description(),
            &description,
            "the header of a segment says what the recording is"
        );
        assert_eq!(
            reader.extent().await?,
            Extent {
                entries: end.entries,
                end: Some(end)
            },
            "the footer of the last segment says how the recording ended"
        );
        Ok(())
    })
}

#[test]
fn a_recording_without_its_index_chunk_reads_the_footers_of_its_segments() -> anyhow::Result<()> {
    let backing = MemoryBacking::new();
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
    let recording = description.id();
    let closed = |connection| {
        Entry::session(&RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    };
    let mut configuration = SegmentConfiguration::new();
    // A block of two entries, and a segment that is sealed with its first block: six entries are three segments.
    configuration.block_size = 2 * closed(0).size() as u64;
    configuration.segment_size = 1;
    let storage = SegmentStorage::new(backing.clone(), configuration);
    let connections = 0..6;

    block_on(async {
        let mut writer = storage.start(description.clone()).await?;
        for connection in connections.clone() {
            writer.append(closed(connection)).await?;
        }
        writer
            .finish(End {
                ending: Ending::Stopped,
                at: Instant::ZERO,
                entries: connections.end,
                calls: 0,
            })
            .await?;
        let segments = backing.list(&recording, ObjectKind::Segment, Position::FIRST).await?;
        assert!(segments.len() > 2, "the recording is several segments");

        backing.drop_object(&recording, Object::index_chunk(Position::FIRST));

        let reader = storage.open(&recording).await?;
        let mut entries = reader
            .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
            .await?;
        let mut positions = Vec::new();
        while let Some(batch) = entries.next().await? {
            positions.extend(batch.entries().map(|(position, _)| position.number()));
        }
        assert_eq!(
            positions,
            connections.collect::<Vec<_>>(),
            "a reader that finds no index chunk reads the footers of the segments that it described"
        );
        Ok(())
    })
}

#[test]
fn a_reader_reads_an_item_that_a_crash_tore_again_from_the_footer_of_its_segment() -> anyhow::Result<()> {
    let backing = MemoryBacking::new();
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
    let recording = description.id();
    let closed = |connection| {
        Entry::session(&RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    };
    let mut configuration = SegmentConfiguration::new();
    // A block of two entries, and a segment that is sealed with its first block: eight entries are four segments.
    configuration.block_size = 2 * closed(0).size() as u64;
    configuration.segment_size = 1;
    let storage = SegmentStorage::new(backing.clone(), configuration);
    let connections = 0..8;
    let index_chunk = Object::index_chunk(Position::FIRST);

    block_on(async {
        let mut writer = storage.start(description.clone()).await?;
        for connection in connections.clone() {
            writer.append(closed(connection)).await?;
        }
        writer.commit().await?;
        drop(writer);
        let [chunk] = backing
            .list(&recording, ObjectKind::IndexChunk, Position::FIRST)
            .await?[..]
        else {
            anyhow::bail!("the recording has one index chunk");
        };

        // The crash tears the index chunk in the middle of its items.
        backing.cut_short(&recording, index_chunk, chunk.length / 2);

        let reader = storage.open(&recording).await?;
        assert_eq!(reader.extent().await?.entries, connections.end);
        let mut entries = reader
            .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
            .await?;
        let mut positions = Vec::new();
        while let Some(batch) = entries.next().await? {
            positions.extend(batch.entries().map(|(position, _)| position.number()));
        }
        assert_eq!(
            positions,
            connections.collect::<Vec<_>>(),
            "every entry reads, the items that are whole from the index chunk and the rest from the segments"
        );
        Ok(())
    })
}

#[test]
fn a_damaged_chunk_loses_the_rest_of_its_block_and_the_stream_goes_on_with_the_next() -> anyhow::Result<()> {
    let backing = MemoryBacking::new();
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
    let recording = description.id();
    let segment = Object::segment(Position::FIRST);
    let closed = |connection| {
        Entry::session(&RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    };
    let mut configuration = SegmentConfiguration::new();
    // A block of four entries, and a commit of two: every block is two chunks.
    configuration.block_size = 4 * closed(0).size() as u64;
    let storage = SegmentStorage::new(backing.clone(), configuration);
    let commits = [0..2, 2..4, 4..6, 6..8];

    block_on(async {
        let mut writer = storage.start(description.clone()).await?;
        let mut second_chunk_at = None;
        for commit in commits.clone() {
            for connection in commit {
                writer.append(closed(connection)).await?;
            }
            writer.commit().await?;
            let [segment] = backing.list(&recording, ObjectKind::Segment, Position::FIRST).await?[..] else {
                anyhow::bail!("the recording is one segment");
            };
            second_chunk_at.get_or_insert(segment.length);
        }
        let entries = commits.len() as u64 * 2;
        writer
            .finish(End {
                ending: Ending::Stopped,
                at: Instant::ZERO,
                entries,
                calls: 0,
            })
            .await?;

        // The disk goes bad at the first byte of the second chunk of the first block.
        let Some(second_chunk_at) = second_chunk_at else {
            anyhow::bail!("the first commit wrote a chunk");
        };
        backing.damage(&recording, segment, second_chunk_at);

        let reader = storage.open(&recording).await?;
        let mut stream = reader
            .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
            .await?;
        let (mut positions, mut lost) = (Vec::new(), Vec::new());
        loop {
            match stream.next().await {
                Ok(Some(batch)) => positions.extend(batch.entries().map(|(position, _)| position.number())),
                Ok(None) => break,
                Err(NextBatchError::Damaged { from, to }) => lost.push((from.number(), to.number())),
                Err(error) => return Err(error.into()),
            }
        }

        let [first, second, third, fourth] = commits;
        assert_eq!(
            lost,
            [(second.start, second.end - 1)],
            "the positions from the damaged chunk to the end of its block are lost"
        );
        assert_eq!(
            positions,
            first.chain(third).chain(fourth).collect::<Vec<_>>(),
            "the stream hands out the chunk before it, and goes on with the next block"
        );
        Ok(())
    })
}
