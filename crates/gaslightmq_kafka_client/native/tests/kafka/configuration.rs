//! The properties of a role whose effect a real broker shows.

use std::time::{Duration, Instant};

use futures::StreamExt;
use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_native::{
    BootstrapServers, ClientError, ConsumerConfiguration, IsolationLevel, KafkaConsumer, KafkaProducer,
    ProducerConfiguration,
};
use gaslightmq_test_kafka::{KafkaCluster, NewTopic, unique_name};
use gaslightmq_wire::Record;

use crate::{
    HOPELESS_BACKOFF, HOPELESS_CALL, bootstrap_servers, consumer, consuming, kafka, offset, partition, producing,
    values,
};

/// The least time a broker takes over a Fetch that it holds for `millis` milliseconds.
/// Its clock is not the clock of the test, so it may seem to answer a little early.
fn held_for(millis: u64) -> Duration {
    Duration::from_millis(millis.saturating_sub(50))
}

/// An address on which nothing listens.
const NOBODY: &str = "127.0.0.1:1";

/// A consumer of `cluster` with `configure` applied to Kafka's defaults.
fn consumer_with(cluster: &KafkaCluster, configure: impl FnOnce(&mut ConsumerConfiguration)) -> KafkaConsumer {
    let mut configuration = consuming(cluster);
    configure(&mut configuration);
    KafkaConsumer::new(configuration)
}

#[tokio::test]
async fn fetch_max_wait_is_how_long_the_broker_holds_a_fetch_of_an_empty_partition() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let topic = orders.name().as_str();
    let fetch_held_for = |millis| async move {
        let consumer = consumer_with(cluster, |configuration| {
            configuration.fetch_max_wait = TimeSpan::from_millis(millis);
        });
        // The first Fetch also finds the leader, so the time of the second one is the time of a Fetch alone.
        consumer.fetch(topic, partition(0), offset(0)).await.unwrap();
        let started = Instant::now();
        let read = consumer.fetch(topic, partition(0), offset(0)).await;
        (read, started.elapsed())
    };

    let (short, short_took) = fetch_held_for(100).await;
    let (long, long_took) = fetch_held_for(1_000).await;

    assert_eq!(short, Ok(vec![]));
    assert_eq!(long, Ok(vec![]));
    assert!(short_took >= held_for(100), "{short_took:?}");
    assert!(short_took < held_for(1_000), "{short_took:?}");
    assert!(long_took >= held_for(1_000), "{long_took:?}");
}

#[tokio::test]
async fn a_fetch_that_the_broker_holds_longer_than_the_request_timeout_brings_no_records() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let consumer = consumer_with(cluster, |configuration| {
        configuration.client.request_timeout = TimeSpan::from_millis(100);
        configuration.fetch_max_wait = TimeSpan::from_millis(400);
    });

    let started = Instant::now();
    let read = consumer.fetch(orders.name().as_str(), partition(0), offset(0)).await;

    assert_eq!(read, Ok(vec![]));
    assert!(started.elapsed() >= held_for(400));
}

#[tokio::test]
async fn a_record_that_arrives_lets_a_held_fetch_go() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let consumer = consumer_with(cluster, |configuration| {
        configuration.fetch_max_wait = TimeSpan::from_secs(20);
    });

    let arrives = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        orders.partition(0).produce(&[Record::new("placed")]).await.unwrap();
    };
    let started = Instant::now();
    let (read, ()) = tokio::join!(consumer.fetch(orders.name().as_str(), partition(0), offset(0)), arrives);

    assert_eq!(values(&read.unwrap()), ["placed"]);
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
}

#[tokio::test]
async fn fetch_min_bytes_makes_the_broker_hold_a_fetch_that_has_fewer_bytes() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let topic = orders.name().as_str();
    orders.partition(0).produce(&[Record::new("placed")]).await.unwrap();
    let wanting = |bytes| async move {
        let consumer = consumer_with(cluster, |configuration| {
            configuration.fetch_min_bytes = bytes;
            configuration.fetch_max_wait = TimeSpan::from_millis(1_000);
        });
        // The consumer learns who leads the partition first, so the time below is the time of a Fetch alone.
        consumer.watermarks(topic, partition(0)).await.unwrap();
        let started = Instant::now();
        let read = consumer.fetch(topic, partition(0), offset(0)).await.unwrap();
        (values(&read), started.elapsed())
    };

    let (any, any_took) = wanting(1).await;
    let (many, many_took) = wanting(100_000).await;

    // Both get the record: the one at once, and the other when the broker gives up waiting for more.
    assert_eq!(any, ["placed"]);
    assert_eq!(many, ["placed"]);
    assert!(any_took < held_for(1_000), "{any_took:?}");
    assert!(many_took >= held_for(1_000), "{many_took:?}");
}

#[tokio::test]
async fn max_partition_fetch_bytes_cuts_a_fetch_off_after_the_batches_that_fit() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let topic = orders.name().as_str();
    for value in ["placed", "paid", "shipped"] {
        // Each batch is a little over a thousand bytes.
        let record = Record::new(value).with_header("padding", vec![0u8; 1_000]);
        orders.partition(0).produce(&[record]).await.unwrap();
    }
    let limited_to = |bytes| {
        consumer_with(cluster, |configuration| {
            configuration.max_partition_fetch_bytes = bytes;
            configuration.fetch_max_wait = TimeSpan::from_millis(100);
        })
    };

    let all = consumer(cluster).fetch(topic, partition(0), offset(0)).await.unwrap();
    let two = limited_to(2_500).fetch(topic, partition(0), offset(0)).await.unwrap();
    // The broker always gives the first batch, however small the limit is.
    let one = limited_to(10).fetch(topic, partition(0), offset(0)).await.unwrap();
    let streamed: Vec<Record> = limited_to(10).stream(topic, partition(0), offset(0)).collect().await;

    assert_eq!(values(&all), ["placed", "paid", "shipped"]);
    assert_eq!(values(&two), ["placed", "paid"]);
    assert_eq!(values(&one), ["placed"]);
    assert_eq!(values(&streamed), ["placed", "paid", "shipped"]);
}

#[tokio::test]
async fn isolation_level_decides_whether_a_consumer_gets_the_records_of_an_open_transaction() {
    let cluster = kafka().await;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await.unwrap();
    let (topic, log) = (payments.name().as_str(), payments.partition(0));
    let reading = |isolation_level| {
        consumer_with(cluster, |configuration| {
            configuration.isolation_level = isolation_level;
            configuration.fetch_max_wait = TimeSpan::from_millis(100);
        })
    };
    let (uncommitted, committed) = (
        reading(IsolationLevel::ReadUncommitted),
        reading(IsolationLevel::ReadCommitted),
    );
    log.produce(&[Record::new("settled")]).await.unwrap();
    let mut transaction = cluster.begin_transaction(&unique_name("payments-tx")).await.unwrap();
    transaction.produce(&log, &[Record::new("pending")]).await.unwrap();
    log.produce(&[Record::new("behind the transaction")]).await.unwrap();

    // While the transaction is open, the committed part of the log ends before its first record.
    let seen_uncommitted = uncommitted.fetch(topic, partition(0), offset(0)).await.unwrap();
    let seen_committed = committed.fetch(topic, partition(0), offset(0)).await.unwrap();
    let end_uncommitted = uncommitted.watermarks(topic, partition(0)).await.unwrap().high;
    let end_committed = committed.watermarks(topic, partition(0)).await.unwrap().high;

    assert_eq!(
        values(&seen_uncommitted),
        ["settled", "pending", "behind the transaction"]
    );
    assert_eq!(values(&seen_committed), ["settled"]);
    assert_eq!((end_uncommitted, end_committed), (offset(3), offset(1)));

    // Once it commits, both read to the end. The marker of the commit is the record at offset 3.
    transaction.commit().await.unwrap();
    let after_commit = committed.fetch(topic, partition(0), offset(0)).await.unwrap();

    assert_eq!(
        values(&after_commit)[..3],
        ["settled", "pending", "behind the transaction"]
    );
    assert_eq!(committed.watermarks(topic, partition(0)).await.unwrap().high, offset(4));
}

#[tokio::test]
async fn bootstrap_servers_that_nobody_listens_on_fail_the_call_when_its_time_runs_out() {
    let nobody: BootstrapServers = NOBODY.parse().unwrap();
    let mut configuration = ProducerConfiguration::new(nobody);
    configuration.delivery_timeout = HOPELESS_CALL;
    configuration.client.retry_backoff = HOPELESS_BACKOFF;
    let producer = KafkaProducer::new(configuration);

    let written = producer.send("orders", partition(0), &[Record::new("placed")]).await;

    assert!(
        matches!(
            written,
            Err(ClientError::CallTimedOut { error: ref last }) if matches!(**last, ClientError::ConnectionFailed(_))
        ),
        "{written:?}"
    );
}

#[tokio::test]
async fn a_bootstrap_server_that_is_down_does_not_keep_a_role_from_the_cluster() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let down_first: BootstrapServers = format!("{NOBODY},{}", bootstrap_servers(cluster)).parse().unwrap();
    let mut configuration = producing(cluster);
    configuration.client.bootstrap_servers = down_first;
    let producer = KafkaProducer::new(configuration);

    let written = producer
        .send(orders.name().as_str(), partition(0), &[Record::new("placed")])
        .await;

    assert_eq!(written, Ok(offset(0)));
}
