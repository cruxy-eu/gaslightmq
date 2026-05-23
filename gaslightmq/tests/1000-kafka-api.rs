use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_kafka_client_api::RawKafkaClient;
use gaslightmq_testing::traits::{Consumer, Producer};
use gaslightmq_testing::{Harness, Record};

#[tokio::test]
async fn smoke_test_produce_and_consume() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("events", Buffer::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let records = vec![
        Record::new("msg-0"),
        Record::new("msg-1"),
        Record::new("msg-2"),
    ];
    let result = harness
        .client
        .send("events", Partition::new(0), &records)
        .await;
    assert!(result.error.is_none(), "produce should succeed");

    let fetched = harness
        .client
        .fetch("events", Partition::new(0), Offset::new(0))
        .await;
    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0].value.as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(fetched[1].value.as_deref(), Some(b"msg-1".as_slice()));
    assert_eq!(fetched[2].value.as_deref(), Some(b"msg-2".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn smoke_test_produce_with_keys_and_headers() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("orders", Buffer::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let records = vec![Record::new("payload")
        .with_key("order-123")
        .with_header("trace-id", "abc-def")];
    let result = harness
        .client
        .send("orders", Partition::new(0), &records)
        .await;
    assert!(result.error.is_none());

    let fetched = harness
        .client
        .fetch("orders", Partition::new(0), Offset::new(0))
        .await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].key.as_deref(), Some(b"order-123".as_slice()));
    assert_eq!(fetched[0].value.as_deref(), Some(b"payload".as_slice()));
    assert_eq!(fetched[0].headers.len(), 1);
    assert_eq!(fetched[0].headers[0].key, "trace-id");

    harness.shutdown().await;
}

#[tokio::test]
async fn smoke_test_multiple_partitions() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("multi", Buffer::new(3)),
        RawKafkaClient::new,
    )
    .await;

    harness
        .client
        .send("multi", Partition::new(0), &[Record::new("p0")])
        .await;
    harness
        .client
        .send("multi", Partition::new(1), &[Record::new("p1")])
        .await;
    harness
        .client
        .send("multi", Partition::new(2), &[Record::new("p2")])
        .await;

    let r0 = harness
        .client
        .fetch("multi", Partition::new(0), Offset::new(0))
        .await;
    let r1 = harness
        .client
        .fetch("multi", Partition::new(1), Offset::new(0))
        .await;
    let r2 = harness
        .client
        .fetch("multi", Partition::new(2), Offset::new(0))
        .await;

    assert_eq!(r0.len(), 1);
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r0[0].value.as_deref(), Some(b"p0".as_slice()));
    assert_eq!(r1[0].value.as_deref(), Some(b"p1".as_slice()));
    assert_eq!(r2[0].value.as_deref(), Some(b"p2".as_slice()));

    harness.shutdown().await;
}
