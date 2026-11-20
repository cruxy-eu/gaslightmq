use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{CreateTopics, DeleteTopics, Produce, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::error::ResponseError;
use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

fn make_record(value: &[u8]) -> KafkaRecord {
    KafkaRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: 0,
        timestamp: 0,
        key: None,
        value: Some(Bytes::copy_from_slice(value)),
        headers: IndexMap::new(),
    }
}

fn encode_records(records: &[KafkaRecord]) -> Bytes {
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, records, &options).unwrap();
    Bytes::from(buf)
}

#[tokio::test]
async fn delete_existing_topic() {
    let harness = Harness::with(
        BrokerBuilder::new().topic("orders", Buffer::new(1)),
    );
    let client: RawKafkaClient = harness.client().await;

    let delete_req = DeleteTopicsRequest::default()
        .with_topic_names(vec![TopicName(StrBytes::from_string("orders".to_string()))])
        .with_timeout_ms(5000);
    let delete_resp = client.delete_topics(delete_req).await;
    assert_eq!(delete_resp.responses.len(), 1);
    assert_eq!(delete_resp.responses[0].error_code, 0);

    let records_bytes = encode_records(&[make_record(b"fail")]);
    let produce_req = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string("orders".to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_bytes))])]);
    let resp = client.produce(produce_req).await;
    assert_eq!(
        resp.responses[0].partition_responses[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn delete_nonexistent_topic_returns_error() {
    let harness = Harness::with(BrokerBuilder::new());
    let client: RawKafkaClient = harness.client().await;

    let delete_req = DeleteTopicsRequest::default()
        .with_topic_names(vec![TopicName(StrBytes::from_string("nope".to_string()))])
        .with_timeout_ms(5000);
    let delete_resp = client.delete_topics(delete_req).await;
    assert_eq!(
        delete_resp.responses[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn create_then_delete_then_recreate() {
    let harness = Harness::with(BrokerBuilder::new());
    let client: RawKafkaClient = harness.client().await;

    let create_req = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_string("ephemeral".to_string())))
            .with_num_partitions(1)
            .with_replication_factor(1)])
        .with_timeout_ms(5000);
    let create_resp = client.create_topics(create_req).await;
    assert_eq!(create_resp.topics[0].error_code, 0);

    let delete_req = DeleteTopicsRequest::default()
        .with_topic_names(vec![TopicName(StrBytes::from_string("ephemeral".to_string()))])
        .with_timeout_ms(5000);
    let delete_resp = client.delete_topics(delete_req).await;
    assert_eq!(delete_resp.responses[0].error_code, 0);

    let create_req2 = CreateTopicsRequest::default()
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_string("ephemeral".to_string())))
            .with_num_partitions(2)
            .with_replication_factor(1)])
        .with_timeout_ms(5000);
    let create_resp2 = client.create_topics(create_req2).await;
    assert_eq!(create_resp2.topics[0].error_code, 0);

    harness.shutdown().await;
}
