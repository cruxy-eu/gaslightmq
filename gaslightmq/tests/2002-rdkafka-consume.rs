use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Consumer, Producer};
use gaslightmq_kafka_client_rdkafka::{RdkafkaConsumer, RdkafkaProducer};
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::{Harness, Record};

#[tokio::test]
async fn consume_after_produce() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    let records = vec![
        Record::new("msg-0"),
        Record::new("msg-1"),
        Record::new("msg-2"),
    ];
    producer.send("events", Partition::new(0), &records).await;

    let fetched = consumer.fetch("events", Partition::new(0), Offset::new(0)).await;
    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0].value.as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(fetched[1].value.as_deref(), Some(b"msg-1".as_slice()));
    assert_eq!(fetched[2].value.as_deref(), Some(b"msg-2".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn consume_with_offset() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    let records = vec![
        Record::new("a"),
        Record::new("b"),
        Record::new("c"),
    ];
    producer.send("events", Partition::new(0), &records).await;

    let fetched = consumer.fetch("events", Partition::new(0), Offset::new(2)).await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].value.as_deref(), Some(b"c".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn consume_from_empty_topic() {
    let harness = Harness::with(BrokerBuilder::new().topic("empty", Buffer::new(1)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let fetched = consumer.fetch("empty", Partition::new(0), Offset::new(0)).await;
    assert!(fetched.is_empty());

    harness.shutdown().await;
}

#[tokio::test]
async fn consume_from_multiple_partitions() {
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
