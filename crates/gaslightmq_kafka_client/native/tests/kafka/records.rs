//! Writing records and reading them.

use std::time::{SystemTime, UNIX_EPOCH};

use futures::{StreamExt, TryStreamExt};
use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::Watermarks;
use gaslightmq_kafka_client_native::{BootstrapServers, ClientError, KafkaConsumer, KafkaProducer};
use gaslightmq_kernel::BrokerId;
use gaslightmq_test_kafka::NewTopic;
use gaslightmq_wire::{Record, RecordHeader, ResponseError};

use crate::{consumer, consuming, kafka, offset, partition, producer, producing, values};

/// The time of day as Kafka writes it into a record: milliseconds since 1970.
fn now() -> i64 {
    let since_1970 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    i64::try_from(since_1970.as_millis()).unwrap()
}

#[tokio::test]
async fn what_a_producer_writes_is_in_the_log_of_the_partition() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let producer = producer(cluster);
    let placed = Record::new("placed")
        .with_key("o-1")
        .with_header("source", "web")
        .with_timestamp(1_700_000_000_000);
    let binary = Record::new(&b"\x00\x00\x00\x27\x12payload"[..])
        .with_key(&b"\x00\x01key"[..])
        .with_timestamp(1_700_000_000_250);
    let deleted = Record::empty().with_key("o-1").with_timestamp(1_700_000_001_000);

    let first = producer
        .send(orders.name().as_str(), partition(0), &[placed.clone(), binary.clone()])
        .await;
    let second = producer
        .send(orders.name().as_str(), partition(0), std::slice::from_ref(&deleted))
        .await;

    assert_eq!(first, Ok(offset(0)));
    assert_eq!(second, Ok(offset(2)));
    assert_eq!(
        orders.partition(0).records().await?,
        [
            placed.with_offset(offset(0)),
            binary.with_offset(offset(1)),
            deleted.with_offset(offset(2))
        ]
    );
    Ok(())
}

#[tokio::test]
async fn a_record_without_a_timestamp_gets_the_time_it_was_sent_at() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let producer = producer(cluster);

    let before = now();
    producer
        .send(orders.name().as_str(), partition(0), &[Record::new("placed")])
        .await?;
    let after = now();

    // A broker takes the timestamp for the age of the record: one from 1970 is the first that retention deletes.
    let written = orders.partition(0).records().await?[0].timestamp;
    assert!(
        written.is_some_and(|written| (before..=after).contains(&written)),
        "{before} <= {written:?} <= {after}"
    );
    Ok(())
}

#[tokio::test]
async fn a_consumer_reads_what_is_in_the_log_of_the_partition() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let consumer = consumer(cluster);
    let headers = vec![
        RecordHeader {
            key: "trace".to_owned(),
            value: Some("abc".into()),
        },
        RecordHeader {
            key: "retried".to_owned(),
            value: None,
        },
    ];
    let placed = Record::new("placed")
        .with_key("o-1")
        .with_headers(headers)
        .with_timestamp(1_700_000_000_000);
    let checked = Record::new("checked").with_timestamp(1_700_000_000_250);
    let paid = Record::new("paid").with_timestamp(1_700_000_001_000);
    let log = orders.partition(0);
    log.produce(&[placed.clone(), checked.clone()]).await?;
    log.produce(std::slice::from_ref(&paid)).await?;

    let read = consumer.fetch(orders.name().as_str(), partition(0), offset(0)).await;

    assert_eq!(
        read,
        Ok(vec![
            placed.with_offset(offset(0)),
            checked.with_offset(offset(1)),
            paid.with_offset(offset(2))
        ])
    );
    Ok(())
}

#[tokio::test]
async fn a_fetch_starts_at_the_offset_it_asks_for_also_inside_a_batch() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let (producer, consumer) = (producer(cluster), consumer(cluster));
    let topic = orders.name().as_str();
    producer
        .send(topic, partition(0), &["placed", "paid", "shipped"].map(Record::new))
        .await?;

    let from_the_middle = consumer.fetch(topic, partition(0), offset(1)).await?;
    let from_the_end = consumer.fetch(topic, partition(0), offset(3)).await;

    assert_eq!(values(&from_the_middle), ["paid", "shipped"]);
    assert_eq!(from_the_end, Ok(vec![]));
    Ok(())
}

#[tokio::test]
async fn a_fetch_behind_the_end_of_the_log_is_out_of_range() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let consumer = consumer(cluster);

    let read = consumer.fetch(orders.name().as_str(), partition(0), offset(5)).await;

    assert_eq!(
        read,
        Err(ClientError::Refused {
            error: ResponseError::OffsetOutOfRange
        })
    );
    Ok(())
}

#[tokio::test]
async fn every_record_goes_to_the_leader_of_its_partition_and_comes_back_from_there() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let broker = |id: u16| BrokerId::from(id);
    // Each partition has one replica, so only its leader takes a record for it.
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_replica_assignment([[broker(3)], [broker(1)], [broker(2)]]))
        .await?;
    let topic = orders.name().as_str();
    // Both start from the first broker alone, and learn the others from it.
    let first_broker = BootstrapServers::from(cluster.brokers()[0].address());
    let mut producing = producing(cluster);
    producing.client.bootstrap_servers = first_broker.clone();
    let mut consuming = consuming(cluster);
    consuming.client.bootstrap_servers = first_broker;
    let (producer, consumer) = (KafkaProducer::new(producing), KafkaConsumer::new(consuming));

    for number in 0..3 {
        let written = producer
            .send(
                topic,
                partition(number),
                &[Record::new(format!("for partition {number}"))],
            )
            .await;
        assert_eq!(written, Ok(offset(0)), "partition {number}");
    }

    for number in 0..3 {
        let expected = [format!("for partition {number}")];
        let in_the_log = orders.partition(number).records().await?;
        let read = consumer.fetch(topic, partition(number), offset(0)).await?;
        assert_eq!(values(&in_the_log), expected);
        assert_eq!(values(&read), expected);
    }
    Ok(())
}

#[tokio::test]
async fn a_stream_reads_from_its_offset_until_a_fetch_brings_nothing() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let topic = orders.name().as_str();
    for value in ["placed", "paid", "shipped"] {
        orders.partition(0).produce(&[Record::new(value)]).await?;
    }
    let mut configuration = consuming(cluster);
    configuration.fetch_max_wait = TimeSpan::from_millis(100);
    let consumer = KafkaConsumer::new(configuration);

    let read: Vec<Record> = consumer.stream(topic, partition(0), offset(1)).try_collect().await?;

    assert_eq!(values(&read), ["paid", "shipped"]);
    Ok(())
}

#[tokio::test]
async fn a_stream_ends_with_the_failure_of_the_fetch_that_failed() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;

    let read: Vec<Result<Record, ClientError>> = consumer(cluster)
        .stream(orders.name().as_str(), partition(0), offset(5))
        .collect()
        .await;

    assert_eq!(
        read,
        [Err(ClientError::Refused {
            error: ResponseError::OffsetOutOfRange
        })]
    );
    Ok(())
}

#[tokio::test]
async fn the_watermarks_are_the_two_ends_of_the_log() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(2))
        .await?;
    let topic = orders.name().as_str();
    let consumer = consumer(cluster);
    orders
        .partition(1)
        .produce(&["placed", "paid", "shipped"].map(Record::new))
        .await?;

    let empty = consumer.watermarks(topic, partition(0)).await;
    let written = consumer.watermarks(topic, partition(1)).await;

    assert_eq!(
        empty,
        Ok(Watermarks {
            low: offset(0),
            high: offset(0)
        })
    );
    assert_eq!(
        written,
        Ok(Watermarks {
            low: offset(0),
            high: offset(3)
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_record_that_the_topic_does_not_take_is_refused_at_once() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("max.message.bytes", "100"))
        .await?;
    let producer = producer(cluster);

    let written = producer
        .send(orders.name().as_str(), partition(0), &[Record::new(vec![0u8; 1_000])])
        .await;

    assert_eq!(
        written,
        Err(ClientError::Refused {
            error: ResponseError::MessageTooLarge
        })
    );
    assert_eq!(orders.partition(0).records().await?, []);
    Ok(())
}
