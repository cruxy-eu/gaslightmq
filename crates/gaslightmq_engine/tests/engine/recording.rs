//! A recording keeps the sessions, the calls with their frames and the journal entries, from its start to its stop.
//!
//! The recordings of these tests go to the storage in memory of `gaslightmq_recording_conformance`, which keeps
//! every batch as it was written, and hands every entry back as its type.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::Clock;
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_journal::prelude::*;
// The engine names a session event of its own, so its items come by name next to the prelude of a recording.
use gaslightmq_clock_waitable::SteppedClock;
use gaslightmq_engine::{Engine, EngineEvent, RecordingStart, Recordings, StartRecordingError, StopRecordingError};
use gaslightmq_kafka_client_api::{Producer, RawClientConfiguration, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_recording_conformance::{MemoryReader, StoredEntry};
use gaslightmq_wire::{Partition, Record};

use crate::support::{EngineUnderTest, ScriptedBackend};

/// When the recordings of these tests start, as whoever starts them reads it on the wall clock.
const STARTED: &str = "2026-11-21T14:02:11.527Z";

#[tokio::test]
async fn a_recording_holds_the_calls_that_arrive_while_it_runs_with_their_frames() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);

    producer
        .send("orders.eu", Partition::new(3), &[Record::new("placed")])
        .await?;
    let record = proxy.next_record().await?;
    let end = proxy.engine().recorder().stop().await?;

    assert_eq!((end.ending, end.calls), (Ending::Stopped, 1));
    let read = proxy.storage().entries(&recording).await;
    let opened = |read: &StoredEntry| {
        matches!(
            read,
            StoredEntry::Session(SessionEvent {
                change: SessionChange::Opened { .. },
                ..
            })
        )
    };
    assert!(
        opened(&read[0]),
        "the session of the producer opened while the recording ran: {read:?}"
    );
    let calls: Vec<(&Call, [FrameState; 4])> = read
        .iter()
        .filter_map(|read| match read {
            StoredEntry::Call { call, frames } => Some((&**call, frames.clone().map(|frame| frame.state()))),
            _ => None,
        })
        .collect();
    // The request, the truth and the response that the client received were on the wire, and stay whole.
    // No overlay changed an answer of the real cluster, so the slot of that answer holds no frame.
    let request_truth_and_delivered = [
        FrameState::Whole,
        FrameState::None,
        FrameState::Whole,
        FrameState::Whole,
    ];
    assert_eq!(calls, [(&*record, request_truth_and_delivered)]);
    assert_eq!(read.len() as u64, end.entries);
    Ok(())
}

#[tokio::test]
async fn a_call_whose_request_arrived_before_the_recording_started_is_no_part_of_it() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    proxy.backend().hold_answers();
    let asked = client.send_metadata().await?;
    proxy.backend().calls_taken(1).await?;

    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    proxy.engine().recorder().start(start).await?;
    proxy.backend().release_answers();
    EngineUnderTest::soon(client.receive_response(&asked)).await??;
    proxy.next_record().await?;
    let end = proxy.engine().recorder().stop().await?;

    assert_eq!(end.calls, 0);
    Ok(())
}

#[tokio::test]
async fn a_recording_begins_with_the_sessions_that_are_open_and_what_they_learned() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut configuration = RawClientConfiguration::new(proxy.address_of(Endpoint::Bootstrap)?);
    configuration.client_id = ClientId::new("analytics-etl-1")?;
    let member = RawKafkaClient::new(configuration);
    let group: GroupId = "analytics-etl".parse()?;
    let heartbeat = member.send_heartbeat(&group).await?;
    member.receive_response(&heartbeat).await?;

    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;
    proxy.engine().recorder().stop().await?;

    let read = proxy.storage().entries(&recording).await;
    let [StoredEntry::Session(opened)] = read.as_slice() else {
        panic!("the recording holds the session that was open, and nothing else: {read:?}");
    };
    let [session] = proxy.engine().sessions().try_into().expect("one session is open");
    assert_eq!(opened.connection, session.connection());
    assert_eq!(
        opened.at,
        session.opened(),
        "the session opened before the recording started"
    );
    let SessionChange::Opened { client_id, .. } = &opened.change else {
        panic!("a recording begins with an opened event for every session that is open");
    };
    assert_eq!(client_id.as_ref().map(ClientId::as_str), Some("analytics-etl-1"));
    Ok(())
}

#[tokio::test]
async fn a_full_queue_stops_the_recording_and_its_last_entry_says_why() -> anyhow::Result<()> {
    let mut configuration = EngineUnderTest::configuration()?;
    // The queue is full at its first entry, so the recording falls behind with the first event of a session.
    configuration.recorder.queue_entries = 0;
    let mut proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);

    client.send_metadata().await?;

    let stopped = loop {
        if let EngineEvent::RecordingStopped { recording, why } = proxy.next_event().await? {
            break (recording, why);
        }
    };
    assert_eq!(stopped, (recording.clone(), Some(RecordingStopCause::FellBehind)));
    assert_eq!(proxy.engine().recorder().recording(), None);
    let read = proxy.storage().entries(&recording).await;
    let Some(StoredEntry::Journal(last)) = read.last() else {
        panic!("the last entry of the recording says why it ends: {read:?}");
    };
    let fell_behind = Change::RecordingStopped {
        why: RecordingStopCause::FellBehind,
    };
    assert_eq!((&last.change, last.from.surface), (&fell_behind, ChangeSurface::Test));
    assert_eq!(proxy.engine().journal().entries(), std::slice::from_ref(last));
    let stored = proxy.engine().recorder().recordings().open(&recording).await?;
    assert_eq!(stored.extent.end.map(|end| end.ending), Some(Ending::FellBehind));
    Ok(())
}

#[tokio::test]
async fn opening_a_recording_that_runs_commits_what_its_engine_handed_over() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);

    let asked = client.send_metadata().await?;
    client.receive_response(&asked).await?;
    proxy.next_record().await?;
    let stored = proxy.engine().recorder().recordings().open(&recording).await?;

    assert_eq!(stored.description.id(), recording);
    assert_eq!(stored.extent.end, None, "the recording runs");
    let read = proxy.storage().entries(&recording).await;
    assert_eq!(read.len() as u64, stored.extent.entries);
    assert!(
        read.iter().any(|read| matches!(read, StoredEntry::Call { .. })),
        "{read:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_environment_writes_one_recording_at_a_time() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;

    let later = RecordingStart {
        environment: "orders-sim".parse()?,
        started: "2026-11-21T14:05:00.000Z".parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let second = proxy.engine().recorder().start(later).await;

    assert_eq!(
        second,
        Err(StartRecordingError::AlreadyRecording {
            recording: recording.clone()
        })
    );
    assert_eq!(proxy.engine().recorder().recording(), Some(recording));
    proxy.engine().recorder().stop().await?;
    assert_eq!(
        proxy.engine().recorder().stop().await,
        Err(StopRecordingError::NotRecording)
    );
    Ok(())
}

#[tokio::test]
async fn a_recording_ends_with_its_environment() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;
    let recordings = proxy.engine().recorder().recordings().clone();

    proxy.stop().await;

    let stored = recordings.open(&recording).await?;
    assert_eq!(
        stored.extent.end.map(|end| end.ending),
        Some(Ending::EnvironmentStopped)
    );
    assert_eq!(recordings.list().await?, [recording]);
    Ok(())
}

#[tokio::test]
async fn the_description_says_what_was_armed_and_the_last_entry_of_the_journal() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let quiet = gaslightmq_rule::Rule::new(
        "quiet-heartbeats".parse()?,
        gaslightmq_selector::Conditions::from(gaslightmq_selector::Condition::is(ApiKey::HEARTBEAT)),
        gaslightmq_lie::LieDefinition::Silence,
    );
    proxy.arm(&quiet)?;
    let armed = proxy.engine().journal().record(
        Change::RuleArmed {
            rule: "quiet-heartbeats".parse()?,
        },
        ChangeSurface::Test,
        Some(Revision::new(2)),
    );

    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = proxy.engine().recorder().start(start).await?;

    let stored = proxy.engine().recorder().recordings().open(&recording).await?;
    let description = stored.description;
    assert_eq!(description.armed, ["quiet-heartbeats".parse::<RecordedName>()?]);
    assert_eq!(description.journal_after, Some(armed.id));
    assert_eq!(description.mode, RecordingMode::Full);
    assert_eq!(description.clock, proxy.engine().clock().now());
    proxy.engine().recorder().stop().await?;
    Ok(())
}

/// A storage on a disk that is full: a recording starts, and no entry of it is written.
struct FullDisk;

/// The writer of a recording on a full disk.
struct NoRoom;

/// What the system says when the disk is full.
const NO_SPACE: &str = "no space left on the device";

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a full disk has nothing to wait for"
)]
impl RecordingStorage for FullDisk {
    type Writer = NoRoom;
    type Reader = MemoryReader;

    async fn start(&self, _description: Description) -> Result<NoRoom, StartError> {
        Ok(NoRoom)
    }

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListError> {
        Ok(Vec::new())
    }

    async fn open(&self, _recording: &RecordingId) -> Result<MemoryReader, OpenError> {
        Err(OpenError::NotFound)
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a full disk has nothing to wait for"
)]
impl RecordingWriter for NoRoom {
    async fn append(&mut self, _entry: Entry) -> Result<Position, WriteError> {
        Err(WriteError::NotWritten {
            reason: NO_SPACE.to_owned(),
        })
    }

    async fn commit(&mut self) -> Result<(), WriteError> {
        Ok(())
    }

    async fn finish(self, _end: End) -> Result<(), WriteError> {
        Ok(())
    }
}

#[tokio::test]
async fn a_recorder_that_cannot_write_stops_the_recording_and_the_journal_says_why() -> anyhow::Result<()> {
    let engine = Engine::start(
        EngineUnderTest::configuration()?,
        ScriptedBackend::with_three_brokers(),
        SteppedClock::new(),
        Lies::of_every_build(),
        Recordings::new(FullDisk),
    )
    .await?;
    let mut events = engine.every_event();
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = engine.recorder().start(start).await?;
    let client = RawKafkaClient::new(
        engine
            .address_of(Endpoint::Bootstrap)
            .expect("the bootstrap endpoint is open"),
    );

    // The session of the client is the first entry, and the disk takes none.
    client.send_metadata().await?;

    let stopped = EngineUnderTest::soon(async {
        while let Some(event) = events.next().await {
            if let EngineEvent::RecordingStopped { recording, why } = event {
                return Some((recording, why));
            }
        }
        None
    })
    .await?;
    assert_eq!(stopped, Some((recording, Some(RecordingStopCause::CouldNotWrite))));
    assert_eq!(engine.recorder().recording(), None);
    let could_not_write = Change::RecordingStopped {
        why: RecordingStopCause::CouldNotWrite,
    };
    let journal = engine.journal().entries();
    assert_eq!(
        journal
            .iter()
            .map(|entry| (&entry.change, entry.from.surface))
            .collect::<Vec<_>>(),
        [(&could_not_write, ChangeSurface::Test)]
    );
    engine.stop().await;
    Ok(())
}
