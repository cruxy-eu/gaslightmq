use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Consumer, Producer};
use gaslightmq_kafka_client_rdkafka::{RdkafkaConsumer, RdkafkaProducer};
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::{Harness, Record};

#[tokio::test]
async fn produce_and_consume() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    let records = vec![
        Record::new("msg-0"),
        Record::new("msg-1"),
        Record::new("msg-2"),
    ];
    let result = producer.send("events", Partition::new(0), &records).await;
    assert!(result.error.is_none(), "produce should succeed");

    let fetched = consumer.fetch("events", Partition::new(0), Offset::new(0)).await;
    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0].value.as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(fetched[1].value.as_deref(), Some(b"msg-1".as_slice()));
    assert_eq!(fetched[2].value.as_deref(), Some(b"msg-2".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_and_consume_with_keys() {
    let harness = Harness::with(BrokerBuilder::new().topic("orders", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    let records = vec![Record::new("payload").with_key("order-123")];
    let result = producer.send("orders", Partition::new(0), &records).await;
    assert!(result.error.is_none());

    let fetched = consumer.fetch("orders", Partition::new(0), Offset::new(0)).await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].key.as_deref(), Some(b"order-123".as_slice()));
    assert_eq!(fetched[0].value.as_deref(), Some(b"payload".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_and_consume_multiple_partitions() {
    let harness = Harness::with(BrokerBuilder::new().topic("multi", Buffer::new(3)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    producer.send("multi", Partition::new(0), &[Record::new("p0")]).await;
    producer.send("multi", Partition::new(1), &[Record::new("p1")]).await;
    producer.send("multi", Partition::new(2), &[Record::new("p2")]).await;

    let r0 = consumer.fetch("multi", Partition::new(0), Offset::new(0)).await;
    let r1 = consumer.fetch("multi", Partition::new(1), Offset::new(0)).await;
    let r2 = consumer.fetch("multi", Partition::new(2), Offset::new(0)).await;

    assert_eq!(r0.len(), 1);
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r0[0].value.as_deref(), Some(b"p0".as_slice()));
    assert_eq!(r1[0].value.as_deref(), Some(b"p1".as_slice()));
    assert_eq!(r2[0].value.as_deref(), Some(b"p2".as_slice()));

    harness.shutdown().await;
}
