use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::Consumer;
use gaslightmq_kafka_client_rdkafka::RdkafkaConsumer;
use gaslightmq_testing::Harness;

#[tokio::test(flavor = "multi_thread")]
async fn describe_single_topic() {
    let harness = Harness::with(BrokerBuilder::new().topic("orders", Buffer::new(1)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let topic = consumer.metadata_for_topic("orders").await;
    assert_eq!(topic.name, "orders");
    assert_eq!(topic.partitions.len(), 1);

    consumer.assert_no_errors();
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_multi_partition_topic() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(5)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let topic = consumer.metadata_for_topic("events").await;
    assert_eq!(topic.name, "events");
    assert_eq!(topic.partitions.len(), 5);

    consumer.assert_no_errors();
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_multiple_topics() {
    let harness = Harness::with(
        BrokerBuilder::new()
            .topic("orders", Buffer::new(1))
            .topic("events", Buffer::new(3)),
    );
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let topics = consumer.metadata_for_all_topics().await;
    assert!(topics.len() >= 2);

    let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"orders"));
    assert!(names.contains(&"events"));

    consumer.assert_no_errors();
    harness.shutdown().await;
}
