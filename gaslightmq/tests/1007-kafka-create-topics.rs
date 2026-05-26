use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{CreateTopics, Produce, Fetch, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::error::ResponseError;
use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions,
    RecordBatchDecoder, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID,
};

fn make_create_request(topics: &[(&str, i32)]) -> CreateTopicsRequest {
    let creatable: Vec<CreatableTopic> = topics
        .iter()
        .map(|(name, partitions)| {
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_num_partitions(*partitions)
                .with_replication_factor(1)
        })
        .collect();
    CreateTopicsRequest::default()
        .with_topics(creatable)
        .with_timeout_ms(5000)
}

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
async fn create_topic_then_produce_and_fetch() {
    let harness = Harness::with(BrokerBuilder::new());
    let client: RawKafkaClient = harness.client().await;

    let create_resp = client.create_topics(make_create_request(&[("orders", 3)])).await;
    assert_eq!(create_resp.topics.len(), 1);
    assert_eq!(create_resp.topics[0].error_code, 0);

    let records_bytes = encode_records(&[make_record(b"hello")]);
    let produce_req = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string("orders".to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(records_bytes))])]);
    let produce_resp = client.produce(produce_req).await;
    assert_eq!(produce_resp.responses[0].partition_responses[0].error_code, 0);

    let fetch_req = FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_string("orders".to_string())))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)
                .with_partition_max_bytes(1024 * 1024)])]);
    let fetch_resp = client.fetch(fetch_req).await;
    let partition_data = &fetch_resp.responses[0].partitions[0];
    assert_eq!(partition_data.error_code, 0);
    assert!(partition_data.records.is_some());

    let mut records_buf = partition_data.records.clone().unwrap();
    let batch = RecordBatchDecoder::decode(&mut records_buf).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].value.as_deref(), Some(b"hello".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn create_duplicate_topic_returns_error() {
    let harness = Harness::with(
        BrokerBuilder::new().topic("orders", Buffer::new(1)),
    );
    let client: RawKafkaClient = harness.client().await;

    let resp = client.create_topics(make_create_request(&[("orders", 1)])).await;
    assert_eq!(resp.topics[0].error_code, ResponseError::TopicAlreadyExists.code());

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_empty_broker_returns_unknown_topic() {
    let harness = Harness::with(BrokerBuilder::new());
    let client: RawKafkaClient = harness.client().await;

    let records_bytes = encode_records(&[make_record(b"fail")]);
    let produce_req = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string("nope".to_string())))
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
