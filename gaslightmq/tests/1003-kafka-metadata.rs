use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Metadata, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::metadata_request::MetadataRequest;

#[tokio::test]
async fn metadata_returns_all_topics() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("orders", Buffer::new(3))
            .topic("events", BlackHole::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    assert_eq!(response.topics.len(), 2);

    let topic_names: Vec<&str> = response
        .topics
        .iter()
        .filter_map(|t| t.name.as_ref().map(|n| n.0.as_str()))
        .collect();
    assert!(topic_names.contains(&"orders"));
    assert!(topic_names.contains(&"events"));

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_correct_partition_counts() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("three", Buffer::new(3))
            .topic("five", Buffer::new(5)),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    let three = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("three"))
        .unwrap();
    assert_eq!(three.partitions.len(), 3);

    let five = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("five"))
        .unwrap();
    assert_eq!(five.partitions.len(), 5);

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_broker_info() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("test", BlackHole::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    assert_eq!(response.brokers.len(), 1);
    assert_eq!(
        response.brokers[0].port,
        harness.broker.addr().port() as i32
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_all_partitions_led_by_single_broker() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("test", Buffer::new(4)),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    let topic = &response.topics[0];
    for partition in &topic.partitions {
        assert_eq!(partition.error_code, 0);
        assert_eq!(
            partition.leader_id.0, 0,
            "all partitions should have leader 0"
        );
    }

    harness.shutdown().await;
}
