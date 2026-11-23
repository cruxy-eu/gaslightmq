//! Transactions, which decide what a consumer of committed records gets.

use gaslightmq_test_kafka::{KafkaClusterError, NewTopic, unique_name};
use gaslightmq_wire::{Offset, Record};

use crate::kafka;

fn transactional_id() -> String {
    unique_name("payments-tx")
}

#[tokio::test]
async fn the_records_of_an_open_transaction_are_in_the_log_and_not_stable() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await?;
    let log = payments.partition(0);
    log.produce(&[Record::new("settled")]).await?;

    let mut transaction = cluster.begin_transaction(&transactional_id()).await?;
    let pending = transaction.produce(&log, &[Record::new("pending")]).await?;
    let behind = log.produce(&[Record::new("behind the transaction")]).await?;

    assert_eq!((pending, behind), (Offset::new(1), Offset::new(2)));
    assert_eq!(log.offsets().await?.end, Offset::new(3));
    assert_eq!(log.last_stable_offset().await?, pending);
    assert_eq!(log.records().await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn a_committed_transaction_is_stable_and_leaves_its_marker_in_the_log() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await?;
    let log = payments.partition(0);

    let mut transaction = cluster.begin_transaction(&transactional_id()).await?;
    transaction
        .produce(&log, &[Record::new("pending"), Record::new("also pending")])
        .await?;
    transaction.produce(&log, &[Record::new("last")]).await?;
    transaction.commit().await?;

    // Three records, and the marker that says the transaction committed.
    let end = log.offsets().await?.end;
    assert_eq!(end, Offset::new(4));
    assert_eq!(log.last_stable_offset().await?, end);
    let records = log.records().await?;
    assert_eq!(records.len(), 4);
    assert_eq!(records[2].value.as_deref(), Some(&b"last"[..]));
    Ok(())
}

#[tokio::test]
async fn an_aborted_transaction_keeps_its_records_in_the_log() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let payments = cluster
        .topics()
        .create(NewTopic::unique("payments").with_partitions(2))
        .await?;

    let mut transaction = cluster.begin_transaction(&transactional_id()).await?;
    for partition in 0..2 {
        transaction
            .produce(&payments.partition(partition), &[Record::new("pending")])
            .await?;
    }
    transaction.abort().await?;

    for partition in 0..2 {
        let log = payments.partition(partition);
        let end = log.offsets().await?.end;
        assert_eq!(end, Offset::new(2));
        assert_eq!(log.last_stable_offset().await?, end);
        assert_eq!(log.records().await?[0].value.as_deref(), Some(&b"pending"[..]));
    }
    Ok(())
}

#[tokio::test]
async fn a_transaction_that_wrote_nothing_ends_without_a_word() -> anyhow::Result<()> {
    let cluster = kafka().await?;

    let transaction = cluster.begin_transaction(&transactional_id()).await?;

    assert!(transaction.transactional_id().starts_with("payments-tx-"));
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn a_transaction_writes_at_least_one_record_at_a_time() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await?;
    let mut transaction = cluster.begin_transaction(&transactional_id()).await?;

    let error = transaction.produce(&payments.partition(0), &[]).await.unwrap_err();

    assert!(matches!(error, KafkaClusterError::InvalidRecords { .. }), "{error}");
    Ok(())
}
