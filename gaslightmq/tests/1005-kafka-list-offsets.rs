use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{ListOffsets, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

fn make_list_offsets_request(topic: &str, partition: i32, timestamp: i64) -> ListOffsetsRequest {
    ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(partition)
            .with_timestamp(timestamp)])])
}

#[tokio::test]
async fn earliest_offset_returns_zero() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", BlackHole::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let request = make_list_offsets_request("test", 0, -2);
    let response = client.list_offsets(request).await;

    let partition_resp = &response.topics[0].partitions[0];
    assert_eq!(partition_resp.error_code, 0);
    assert_eq!(partition_resp.offset, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn latest_offset_returns_zero() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", BlackHole::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let request = make_list_offsets_request("test", 0, -1);
    let response = client.list_offsets(request).await;

    let partition_resp = &response.topics[0].partitions[0];
    assert_eq!(partition_resp.error_code, 0);
    assert_eq!(partition_resp.offset, 0);

    harness.shutdown().await;
}
