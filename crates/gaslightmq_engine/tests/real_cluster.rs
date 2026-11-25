//! A real client talks through the proxy to real Kafka brokers, and experiences the lies of the armed rules.
//!
//! The tests need Docker.
//! Each test starts the cluster it needs, and puts a proxy in front of it:
//! the backend in front of a real cluster, and an engine whose endpoints each take a free port.
//! The client is the producer and the consumer of `gaslightmq_kafka_client_native`,
//! which knows the bootstrap address of the proxy and nothing else.
//! Whatever sets the cluster up or looks into it goes through the handles of `gaslightmq_test_kafka`.

use anyhow::Context;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::Duration;
use gaslightmq_clock_waitable::WallClock;
use gaslightmq_engine::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_native::{KafkaConsumer, KafkaProducer};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::{Offset, Partition, Record};

/// How long a test waits for the record of a call that the client already got its answer for.
const PATIENCE: Duration = Duration::from_secs(30);

/// When the order of these tests was placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

#[tokio::test]
async fn a_client_produces_and_consumes_through_the_proxy() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let backend = UpstreamBackend::start(UpstreamBackendConfiguration::new(upstream, "127.0.0.1".parse()?)).await?;
    // Every endpoint of the proxy takes a free port: the bootstrap endpoint, and the one of each broker.
    let any: ListenAddress = "127.0.0.1:0".parse()?;
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let configuration = EngineConfiguration::new(Listeners::new(any, brokers, "127.0.0.1".parse()?)?);
    let engine = Engine::start(configuration, backend, WallClock::new(), Lies::of_every_build()).await?;
    let bootstrap = engine
        .address_of(Endpoint::Bootstrap)
        .context("the bootstrap endpoint is open")?;
    let mut events = engine.live_events();
    let producer = KafkaProducer::from(bootstrap);
    let consumer = KafkaConsumer::from(bootstrap);
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);

    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;
    let read = consumer
        .fetch(orders.name().as_str(), Partition::new(0), Offset::new(0))
        .await;

    // No rule is armed, so the client sees the cluster as it is.
    assert_eq!(written, Ok(Offset::new(0)));
    assert_eq!(read, Ok(vec![placed.clone().with_offset(Offset::new(0))]));
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    // The Produce went through the proxy: a session on the endpoint of the leader recorded it.
    // The client learned the address of that endpoint from the Metadata that the proxy answered.
    let leader = Endpoint::Broker(orders.partition(0).leader().await?);
    let produce = loop {
        let event = tokio::time::timeout(PATIENCE, events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            break record;
        }
    };
    let arrived_on = produce.fields().read(Field::Endpoint).present();
    assert_eq!(
        arrived_on.map(|endpoint| endpoint.to_string()),
        Some(leader.to_string())
    );
    assert_eq!(produce.verdict().to_string(), "pass");

    engine.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_client_retries_a_refused_produce_and_then_succeeds() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let backend = UpstreamBackend::start(UpstreamBackendConfiguration::new(upstream, "127.0.0.1".parse()?)).await?;
    let any: ListenAddress = "127.0.0.1:0".parse()?;
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let configuration = EngineConfiguration::new(Listeners::new(any, brokers, "127.0.0.1".parse()?)?);
    let lies = Lies::of_every_build().with(Refusal)?;
    let engine = Engine::start(configuration, backend, WallClock::new(), lies).await?;
    let bootstrap = engine
        .address_of(Endpoint::Bootstrap)
        .context("the bootstrap endpoint is open")?;
    let mut events = engine.live_events();
    // Someone arms a rule: "tell the 1st Produce that its broker no longer leads the partition".
    let flap = Rule::new(
        "leader-flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1)?));
    engine
        .rules()
        .arm(flap.resolve(&SelectorSet::default(), &PresetSet::default())?)?;
    let producer = KafkaProducer::from(bootstrap);
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);

    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    // The client believed the lie: it asked for the leader again, found the same broker, and sent the record again.
    assert_eq!(written, Ok(Offset::new(0)));
    let mut sent = Vec::new();
    while sent.len() < 2 {
        let event = tokio::time::timeout(PATIENCE, events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            sent.push(record);
        }
    }
    assert_eq!(sent[0].verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders/0");
    assert_eq!(sent[1].verdict().to_string(), "pass");
    // The refused Produce never reached the cluster, so the record is in the log once.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );

    engine.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_client_reconnects_after_a_hang_up_and_then_succeeds() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let backend = UpstreamBackend::start(UpstreamBackendConfiguration::new(upstream, "127.0.0.1".parse()?)).await?;
    let any: ListenAddress = "127.0.0.1:0".parse()?;
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let configuration = EngineConfiguration::new(Listeners::new(any, brokers, "127.0.0.1".parse()?)?);
    let engine = Engine::start(configuration, backend, WallClock::new(), Lies::of_every_build()).await?;
    let bootstrap = engine
        .address_of(Endpoint::Bootstrap)
        .context("the bootstrap endpoint is open")?;
    let mut events = engine.live_events();
    // Someone arms a rule: "hang up on the 1st Produce".
    let disconnect = Rule::new(
        "disconnect".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        HangUpLie::new(),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1)?));
    engine
        .rules()
        .arm(disconnect.resolve(&SelectorSet::default(), &PresetSet::default())?)?;
    let producer = KafkaProducer::from(bootstrap);
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);

    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    // The client lost its connection, opened a new one, and sent the record again.
    assert_eq!(written, Ok(Offset::new(0)));
    let mut sent = Vec::new();
    while sent.len() < 2 {
        let event = tokio::time::timeout(PATIENCE, events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            sent.push(record);
        }
    }
    assert_eq!(sent[0].verdict().to_string(), "hang-up");
    assert_eq!(sent[1].verdict().to_string(), "pass");
    assert_ne!(sent[0].connection(), sent[1].connection());
    // The hang-up came before the request went on, so the record is in the log once.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );

    engine.stop().await;
    Ok(())
}
