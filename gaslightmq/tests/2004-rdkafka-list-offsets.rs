use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Consumer, Producer};
use gaslightmq_kafka_client_rdkafka::{RdkafkaConsumer, RdkafkaProducer};
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::{Harness, Record};

#[tokio::test(flavor = "multi_thread")]
async fn watermarks_empty_topic() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let wm = consumer.watermarks("events", Partition::new(0)).await;
    assert_eq!(wm.low, Offset::new(0));
    assert_eq!(wm.high, Offset::new(0));

    consumer.assert_no_errors();
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watermarks_after_produce() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let producer = RdkafkaProducer::new(addr);
    let consumer = RdkafkaConsumer::new(addr);

    let records = vec![Record::new("a"), Record::new("b"), Record::new("c")];
    producer.send("events", Partition::new(0), &records).await;

    let wm = consumer.watermarks("events", Partition::new(0)).await;
    assert_eq!(wm.low, Offset::new(0));
    assert_eq!(wm.high, Offset::new(3));

    producer.assert_no_errors();
    consumer.assert_no_errors();
    harness.shutdown().await;
}
