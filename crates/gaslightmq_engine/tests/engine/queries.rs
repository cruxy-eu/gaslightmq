//! A recording answers a query with the calls that a selector matches, and its index groups them into facets.
//!
//! The recordings of these tests go to the storage in memory of `gaslightmq_recording_conformance`, which keeps no
//! summary, so every query streams the whole recording and the selector decides every call.

use gaslightmq_call::prelude::*;
use gaslightmq_clock_waitable::SteppedClock;
use gaslightmq_engine::{Engine, QueryRecordingError, ReadingConfiguration, RecordingStart, Recordings};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_index::Facets;
use gaslightmq_journal::prelude::*;
use gaslightmq_kafka_client_api::{Producer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_recording_segments::{MemoryBacking, SegmentConfiguration, SegmentStorage};
use gaslightmq_selector::prelude::*;
use gaslightmq_wire::{Partition, Record};

use crate::support::{EngineUnderTest, ScriptedBackend};

/// When the recordings of these tests start, as whoever starts them reads it on the wall clock.
const STARTED: &str = "2026-11-21T14:02:11.527Z";

/// The ID that a recording of `orders-sim` has which started at [`STARTED`].
const RECORDING_OF_ORDERS_SIM: &str = "orders-sim.2026-11-21T14-02-11.527Z";

#[tokio::test]
async fn a_query_answers_with_the_calls_that_a_selector_matches_and_their_count() -> anyhow::Result<()> {
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
    let mut records = Vec::new();
    for topic in ["orders.eu", "payments.settled", "orders.us"] {
        producer
            .send(topic, Partition::new(0), &[Record::new("placed")])
            .await?;
        records.push(Call::clone(&*proxy.next_record().await?));
    }
    proxy.engine().recorder().stop().await?;
    let recordings = proxy.engine().recorder().recordings();

    let to_orders = EffectiveSelector::from(Condition::parse(Field::Topic, "orders.*")?);
    let queried = recordings.query(&recording, &to_orders, 0).await?;

    assert_eq!(queried.matched, 2);
    assert_eq!(queried.calls, [records[0].clone(), records[2].clone()]);
    assert_eq!(queried.unreadable, 0);
    Ok(())
}

#[tokio::test]
async fn a_query_answers_with_a_page_of_calls_and_counts_them_all() -> anyhow::Result<()> {
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
    let mut records = Vec::new();
    for topic in ["orders.eu", "orders.us", "orders.apac"] {
        producer
            .send(topic, Partition::new(0), &[Record::new("placed")])
            .await?;
        records.push(Call::clone(&*proxy.next_record().await?));
    }
    proxy.engine().recorder().stop().await?;
    let mut reading = ReadingConfiguration::new();
    reading.query_calls = 2;
    let recordings = Recordings::with_configuration(proxy.storage().clone(), reading);
    let every_produce = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));

    let first = recordings.query(&recording, &every_produce, 0).await?;
    let skipped = u64::try_from(first.calls.len())?;
    let rest = recordings.query(&recording, &every_produce, skipped).await?;

    assert_eq!((first.matched, rest.matched), (3, 3));
    assert_eq!(first.calls, records[..2]);
    assert_eq!(rest.calls, records[2..]);
    Ok(())
}

#[tokio::test]
async fn the_index_of_a_recording_counts_its_calls_under_a_filter() -> anyhow::Result<()> {
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
    for topic in ["orders.eu", "orders.eu", "orders.us"] {
        producer
            .send(topic, Partition::new(0), &[Record::new("placed")])
            .await?;
        proxy.next_record().await?;
    }
    proxy.engine().recorder().stop().await?;
    let recordings = proxy.engine().recorder().recordings();

    let to_eu = EffectiveSelector::from(Condition::parse(Field::Topic, "orders.eu")?);
    let indexed = recordings.index(&recording, &to_eu).await?;

    assert_eq!(indexed.facets.calls(), 2);
    // The topics leave their own condition out: they say what another topic would leave.
    let topics = indexed.facets.get(Field::Topic).map(ToString::to_string);
    assert_eq!(topics.as_deref(), Some("orders.eu (2), orders.us (1); orders.* (3)"));
    let apis = indexed.facets.get(Field::Api).map(ToString::to_string);
    assert_eq!(apis.as_deref(), Some("Produce (2)"));
    Ok(())
}

#[tokio::test]
async fn the_index_of_a_recording_that_runs_reads_on_with_every_ask() -> anyhow::Result<()> {
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
    let recordings = proxy.engine().recorder().recordings().clone();
    let every_call = EffectiveSelector::new();

    producer
        .send("orders.eu", Partition::new(0), &[Record::new("placed")])
        .await?;
    proxy.next_record().await?;
    let before = recordings.index(&recording, &every_call).await?;
    producer
        .send("orders.us", Partition::new(0), &[Record::new("placed")])
        .await?;
    proxy.next_record().await?;
    let after = recordings.index(&recording, &every_call).await?;

    let produces_in = |facets: &Facets| {
        facets
            .get(Field::Api)
            .map(|apis| apis.calls_with(&ApiKey::PRODUCE.into()))
    };
    assert_eq!(produces_in(&before.facets), Some(1));
    assert_eq!(produces_in(&after.facets), Some(2));
    Ok(())
}

#[tokio::test]
async fn a_query_of_a_recording_that_is_not_there_says_so() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let recording: RecordingId = RECORDING_OF_ORDERS_SIM.parse()?;

    let error = proxy
        .engine()
        .recorder()
        .recordings()
        .query(&recording, &EffectiveSelector::new(), 0)
        .await
        .expect_err("nobody recorded anything");

    assert_eq!(error, QueryRecordingError::NotFound { recording });
    Ok(())
}

#[tokio::test]
async fn a_query_that_finds_no_room_to_read_is_refused_and_waits_for_nothing() -> anyhow::Result<()> {
    let mut layout = SegmentConfiguration::new();
    // The readers of the process may hold one block together, and a stream sets the room for one block aside.
    layout.all_readers_memory = layout.block_size;
    let storage = SegmentStorage::new(MemoryBacking::new(), layout);
    let engine = Engine::start(
        EngineUnderTest::configuration()?,
        ScriptedBackend::with_three_brokers(),
        SteppedClock::new(),
        Lies::of_every_build(),
        Recordings::new(storage.clone()),
    )
    .await?;
    let start = RecordingStart {
        environment: "orders-sim".parse()?,
        started: STARTED.parse()?,
        mode: RecordingMode::Full,
        backend: RecordedBackend::Upstream,
        from: ChangeSurface::Test.into(),
    };
    let recording = engine.recorder().start(start).await?;
    engine.recorder().stop().await?;
    // Another reader streams the recording, and holds the room of its block for as long as its stream runs.
    let reader = storage.open(&recording).await?;
    let stream = reader
        .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
        .await?;

    let refused = engine
        .recorder()
        .recordings()
        .query(&recording, &EffectiveSelector::new(), 0)
        .await;

    let Err(QueryRecordingError::Busy { reason, .. }) = refused else {
        anyhow::bail!("the query found no room, and was not refused: {refused:?}");
    };
    assert!(reason.contains("`recordings.all_readers_memory`"), "{reason}");
    drop(stream);
    let answered = engine
        .recorder()
        .recordings()
        .query(&recording, &EffectiveSelector::new(), 0)
        .await?;
    assert_eq!(answered.matched, 0);
    engine.stop().await;
    Ok(())
}
