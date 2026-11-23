//! Writing records to a partition, and reading back what it holds.

use gaslightmq_test_kafka::{KafkaClusterError, NewTopic};
use gaslightmq_wire::{ApiKey, Offset, Record, RecordHeader, ResponseError};

use crate::kafka;

/// `record` as the log gives it back: at `offset`, with the timestamp of a record that was written without one.
fn at(offset: u64, record: Record) -> Record {
    record.with_offset(Offset::new(offset)).with_timestamp(0)
}

#[tokio::test]
async fn records_come_back_as_they_were_written() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let written = [
        Record::new("placed").with_key("o-1").with_header("source", "web"),
        Record::new(&b"\x00\x00\x00\x27\x12payload"[..]).with_key(&b"\x00\x01key"[..]),
        Record::new("keyless"),
        Record::empty().with_key("o-1"),
    ];

    let first = orders.partition(0).produce(&written).await.unwrap();
    let read = orders.partition(0).records().await.unwrap();

    assert_eq!(first, Offset::new(0));
    let expected: Vec<Record> = (0..).zip(written).map(|(offset, record)| at(offset, record)).collect();
    assert_eq!(read, expected);
}

#[tokio::test]
async fn a_header_keeps_its_place_and_may_have_no_value() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let headers = vec![
        RecordHeader {
            key: "trace".to_owned(),
            value: Some("abc".into()),
        },
        RecordHeader {
            key: "tombstone".to_owned(),
            value: None,
        },
    ];

    orders
        .partition(0)
        .produce(&[Record::new("placed").with_headers(headers.clone())])
        .await
        .unwrap();

    assert_eq!(orders.partition(0).records().await.unwrap()[0].headers, headers);
}

#[tokio::test]
async fn every_batch_continues_where_the_one_before_it_ended() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let log = orders.partition(0);

    let first = log
        .produce(&[Record::new("placed"), Record::new("paid")])
        .await
        .unwrap();
    let second = log.produce(&[Record::new("shipped")]).await.unwrap();

    assert_eq!((first, second), (Offset::new(0), Offset::new(2)));
    assert_eq!(log.offsets().await.unwrap(), Offset::new(0)..Offset::new(3));
    let values: Vec<_> = log
        .records()
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.value.unwrap())
        .collect();
    assert_eq!(values, ["placed", "paid", "shipped"]);
}

#[tokio::test]
async fn a_record_keeps_the_timestamp_it_was_written_with() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let placed = Record::new("placed").with_timestamp(1_700_000_000_000);

    orders
        .partition(0)
        .produce(std::slice::from_ref(&placed))
        .await
        .unwrap();

    assert_eq!(
        orders.partition(0).records().await.unwrap(),
        [placed.with_offset(Offset::new(0))]
    );
}

#[tokio::test]
async fn each_partition_has_a_log_of_its_own() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(3))
        .await
        .unwrap();

    orders.partition(2).produce(&[Record::new("placed")]).await.unwrap();

    assert_eq!(orders.partition(0).records().await.unwrap(), []);
    assert!(orders.partition(1).offsets().await.unwrap().is_empty());
    assert_eq!(orders.partition(2).records().await.unwrap().len(), 1);
    assert_eq!(
        orders.partition(2).offsets().await.unwrap(),
        Offset::new(0)..Offset::new(1)
    );
}

#[tokio::test]
async fn a_partition_that_the_topic_does_not_have_is_not_found() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();

    let error = orders.partition(7).produce(&[Record::new("placed")]).await.unwrap_err();

    assert!(
        matches!(
            error,
            KafkaClusterError::Refused {
                error: ResponseError::UnknownTopicOrPartition,
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(
        error.to_string(),
        format!(
            "Metadata for partition 7 of topic `{}`: the cluster answered UnknownTopicOrPartition: \
             the topic has no such partition",
            orders.name()
        )
    );
}

#[tokio::test]
async fn an_error_of_the_leader_comes_back_as_it_is() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("max.message.bytes", "100"))
        .await
        .unwrap();

    let error = orders
        .partition(0)
        .produce(&[Record::new(vec![0u8; 1_000])])
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            KafkaClusterError::Refused {
                api: ApiKey::Produce,
                error: ResponseError::MessageTooLarge,
                ..
            }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn records_that_make_no_batch_are_not_written() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let years_apart = [
        Record::new("placed").with_timestamp(0),
        Record::new("paid").with_timestamp(1_700_000_000_000),
    ];

    let error = orders.partition(0).produce(&years_apart).await.unwrap_err();

    assert!(matches!(error, KafkaClusterError::InvalidRecords { .. }), "{error}");
    assert_eq!(orders.partition(0).records().await.unwrap(), []);
}
