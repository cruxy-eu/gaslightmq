//! Writing records to a partition, and reading back what it holds.

use gaslightmq_test_kafka::{KafkaClusterError, NewTopic};
use gaslightmq_wire::{ApiKey, Offset, Record, RecordHeader, ResponseError};

use crate::kafka;

/// `record` as the log gives it back: at `offset`, with the timestamp of a record that was written without one.
fn at(offset: u64, record: Record) -> Record {
    record.with_offset(Offset::new(offset)).with_timestamp(0)
}

#[tokio::test]
async fn records_come_back_as_they_were_written() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let written = [
        Record::new("placed").with_key("o-1").with_header("source", "web"),
        Record::new(&b"\x00\x00\x00\x27\x12payload"[..]).with_key(&b"\x00\x01key"[..]),
        Record::new("keyless"),
        Record::empty().with_key("o-1"),
    ];

    let first = orders.partition(0).produce(&written).await?;
    let read = orders.partition(0).records().await?;

    assert_eq!(first, Offset::new(0));
    let expected: Vec<Record> = (0..).zip(written).map(|(offset, record)| at(offset, record)).collect();
    assert_eq!(read, expected);
    Ok(())
}

#[tokio::test]
async fn a_header_keeps_its_place_and_may_have_no_value() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
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
        .await?;

    assert_eq!(orders.partition(0).records().await?[0].headers, headers);
    Ok(())
}

#[tokio::test]
async fn every_batch_continues_where_the_one_before_it_ended() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let log = orders.partition(0);

    let first = log.produce(&[Record::new("placed"), Record::new("paid")]).await?;
    let second = log.produce(&[Record::new("shipped")]).await?;

    assert_eq!((first, second), (Offset::new(0), Offset::new(2)));
    assert_eq!(log.offsets().await?, Offset::new(0)..Offset::new(3));
    let values: Vec<_> = log
        .records()
        .await?
        .into_iter()
        .filter_map(|record| record.value)
        .collect();
    assert_eq!(values, ["placed", "paid", "shipped"]);
    Ok(())
}

#[tokio::test]
async fn a_record_keeps_the_timestamp_it_was_written_with() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let placed = Record::new("placed").with_timestamp(1_700_000_000_000);

    orders.partition(0).produce(std::slice::from_ref(&placed)).await?;

    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    Ok(())
}

#[tokio::test]
async fn each_partition_has_a_log_of_its_own() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(3))
        .await?;

    orders.partition(2).produce(&[Record::new("placed")]).await?;

    assert_eq!(orders.partition(0).records().await?, []);
    assert!(orders.partition(1).offsets().await?.is_empty());
    assert_eq!(orders.partition(2).records().await?.len(), 1);
    assert_eq!(orders.partition(2).offsets().await?, Offset::new(0)..Offset::new(1));
    Ok(())
}

#[tokio::test]
async fn a_partition_that_the_topic_does_not_have_is_not_found() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;

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
    Ok(())
}

#[tokio::test]
async fn an_error_of_the_leader_comes_back_as_it_is() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("max.message.bytes", "100"))
        .await?;

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
    Ok(())
}

#[tokio::test]
async fn records_that_make_no_batch_are_not_written() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await?;
    let years_apart = [
        Record::new("placed").with_timestamp(0),
        Record::new("paid").with_timestamp(1_700_000_000_000),
    ];

    let error = orders.partition(0).produce(&years_apart).await.unwrap_err();

    assert!(matches!(error, KafkaClusterError::InvalidRecords { .. }), "{error}");
    assert_eq!(orders.partition(0).records().await?, []);
    Ok(())
}
