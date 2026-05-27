use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::Consumer;
use gaslightmq_kafka_client_rdkafka::RdkafkaConsumer;
use gaslightmq_testing::Harness;

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_topics_metadata() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(1)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let topics = consumer.metadata_for_all_topics().await;
    assert!(!topics.is_empty());

    consumer.assert_no_errors();
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_topic_metadata() {
    let harness = Harness::with(BrokerBuilder::new().topic("events", Buffer::new(3)));
    let addr = harness.addr().await;
    let consumer = RdkafkaConsumer::new(addr);

    let topic = consumer.metadata_for_topic("events").await;
    assert_eq!(topic.name, "events");
    assert_eq!(topic.partitions.len(), 3);

    consumer.assert_no_errors();
    harness.shutdown().await;
}
