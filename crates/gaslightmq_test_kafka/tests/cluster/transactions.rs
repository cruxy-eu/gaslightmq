//! Transactions, which decide what a consumer of committed records gets.

use gaslightmq_test_kafka::{KafkaClusterError, NewTopic, unique_name};
use gaslightmq_wire::{Offset, Record};

use crate::kafka;

fn transactional_id() -> String {
    unique_name("payments-tx")
}

#[tokio::test]
async fn the_records_of_an_open_transaction_are_in_the_log_and_not_stable() {
    let cluster = kafka().await;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await.unwrap();
    let log = payments.partition(0);
    log.produce(&[Record::new("settled")]).await.unwrap();

    let mut transaction = cluster.begin_transaction(&transactional_id()).await.unwrap();
    let pending = transaction.produce(&log, &[Record::new("pending")]).await.unwrap();
    let behind = log.produce(&[Record::new("behind the transaction")]).await.unwrap();

    assert_eq!((pending, behind), (Offset::new(1), Offset::new(2)));
    assert_eq!(log.offsets().await.unwrap().end, Offset::new(3));
    assert_eq!(log.last_stable_offset().await.unwrap(), pending);
    assert_eq!(log.records().await.unwrap().len(), 3);
}

#[tokio::test]
async fn a_committed_transaction_is_stable_and_leaves_its_marker_in_the_log() {
    let cluster = kafka().await;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await.unwrap();
    let log = payments.partition(0);

    let mut transaction = cluster.begin_transaction(&transactional_id()).await.unwrap();
    transaction
        .produce(&log, &[Record::new("pending"), Record::new("also pending")])
        .await
        .unwrap();
    transaction.produce(&log, &[Record::new("last")]).await.unwrap();
    transaction.commit().await.unwrap();

    // Three records, and the marker that says the transaction committed.
    let end = log.offsets().await.unwrap().end;
    assert_eq!(end, Offset::new(4));
    assert_eq!(log.last_stable_offset().await.unwrap(), end);
    let records = log.records().await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[2].value.as_deref(), Some(&b"last"[..]));
}

#[tokio::test]
async fn an_aborted_transaction_keeps_its_records_in_the_log() {
    let cluster = kafka().await;
    let payments = cluster
        .topics()
        .create(NewTopic::unique("payments").with_partitions(2))
        .await
        .unwrap();

    let mut transaction = cluster.begin_transaction(&transactional_id()).await.unwrap();
    for partition in 0..2 {
        transaction
            .produce(&payments.partition(partition), &[Record::new("pending")])
            .await
            .unwrap();
    }
    transaction.abort().await.unwrap();

    for partition in 0..2 {
        let log = payments.partition(partition);
        let end = log.offsets().await.unwrap().end;
        assert_eq!(end, Offset::new(2));
        assert_eq!(log.last_stable_offset().await.unwrap(), end);
        assert_eq!(log.records().await.unwrap()[0].value.as_deref(), Some(&b"pending"[..]));
    }
}

#[tokio::test]
async fn a_transaction_that_wrote_nothing_ends_without_a_word() {
    let cluster = kafka().await;

    let transaction = cluster.begin_transaction(&transactional_id()).await.unwrap();

    assert!(transaction.transactional_id().starts_with("payments-tx-"));
    transaction.commit().await.unwrap();
}

#[tokio::test]
async fn a_transaction_writes_at_least_one_record_at_a_time() {
    let cluster = kafka().await;
    let payments = cluster.topics().create(NewTopic::unique("payments")).await.unwrap();
    let mut transaction = cluster.begin_transaction(&transactional_id()).await.unwrap();

    let error = transaction.produce(&payments.partition(0), &[]).await.unwrap_err();

    assert!(matches!(error, KafkaClusterError::InvalidRecords { .. }), "{error}");
}
