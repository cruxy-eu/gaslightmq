use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::Producer;
use gaslightmq_kafka_client_rdkafka::RdkafkaProducer;
use gaslightmq_kernel::Partition;
use gaslightmq_testing::{Harness, Record};

#[tokio::test]
async fn produce_single_record() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);

    let result = producer.send("events", Partition::new(0), &[Record::new("hello")]).await;
    assert!(result.error.is_none());

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_multiple_records() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);

    let records = vec![
        Record::new("msg-0"),
        Record::new("msg-1"),
        Record::new("msg-2"),
    ];
    let result = producer.send("events", Partition::new(0), &records).await;
    assert!(result.error.is_none());

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_with_key() {
    let harness = Harness::with(BrokerBuilder::new().topic("orders", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);

    let records = vec![Record::new("payload").with_key("order-123")];
    let result = producer.send("orders", Partition::new(0), &records).await;
    assert!(result.error.is_none());

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_multiple_partitions() {
    let harness = Harness::with(BrokerBuilder::new().topic("multi", Buffer::new(3)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);

    let r0 = producer.send("multi", Partition::new(0), &[Record::new("p0")]).await;
    let r1 = producer.send("multi", Partition::new(1), &[Record::new("p1")]).await;
    let r2 = producer.send("multi", Partition::new(2), &[Record::new("p2")]).await;

    assert!(r0.error.is_none());
    assert!(r1.error.is_none());
    assert!(r2.error.is_none());

    harness.shutdown().await;
}
