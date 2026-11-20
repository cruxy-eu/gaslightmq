use bytes::{Bytes, BytesMut};
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Fetch, Produce, RawKafkaClient};
use gaslightmq_testing::Harness;
use gaslightmq_kafka_client_api::ProtocolConsumer;
use indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID,
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

fn make_produce_request(topic: &str, partition: i32, records: &[KafkaRecord]) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(encode_records(records)))])])
}

fn make_fetch_request(topic: &str, partition: i32, offset: i64) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(1024 * 1024)])])
}

fn decode_fetch_values(
    response: &kafka_protocol::messages::fetch_response::FetchResponse,
) -> Vec<Option<Bytes>> {
    let mut values = Vec::new();
    for topic_resp in &response.responses {
        for partition_resp in &topic_resp.partitions {
            if let Some(ref raw) = partition_resp.records {
                let mut buf = raw.clone();
                if let Ok(batch) = RecordBatchDecoder::decode(&mut buf) {
                    for r in &batch.records {
                        values.push(r.value.clone());
                    }
                }
            }
        }
    }
    values
}

#[tokio::test]
async fn fetch_records_after_produce() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", Buffer::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let produce_req =
        make_produce_request("test", 0, &[make_record(b"msg-0"), make_record(b"msg-1")]);
    client.produce(produce_req).await;

    let fetch_req = make_fetch_request("test", 0, 0);
    let response = Fetch::fetch(&client, fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(values[1].as_deref(), Some(b"msg-1".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_with_offset() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", Buffer::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let produce_req = make_produce_request(
        "test",
        0,
        &[make_record(b"a"), make_record(b"b"), make_record(b"c")],
    );
    client.produce(produce_req).await;

    let fetch_req = make_fetch_request("test", 0, 2);
    let response = Fetch::fetch(&client, fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].as_deref(), Some(b"c".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_from_empty_topic() {
    let harness = Harness::with(BrokerBuilder::new().topic("empty", Buffer::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let fetch_req = make_fetch_request("empty", 0, 0);
    let response = Fetch::fetch(&client, fetch_req).await;

    let values = decode_fetch_values(&response);
    assert!(values.is_empty());

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_from_generator_topic() {
    use gaslightmq::RandomBytes;

    let harness = Harness::with(
        BrokerBuilder::new()
            .topic("generated", BlackHole::new(1))
            .generator(RandomBytes::new(64)),
    );
    let client: RawKafkaClient = harness.client().await;

    let fetch_req = make_fetch_request("generated", 0, 0);
    let response = Fetch::fetch(&client, fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].as_ref().unwrap().len(), 64);

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_with_chaos_error_injection() {
    use gaslightmq::{Api, ErrorResponse};
    use kafka_protocol::error::ResponseError;

    let harness = Harness::with(
        BrokerBuilder::new()
            .topic("chaos", Buffer::new(1))
            .chaos(Api::Fetch, ErrorResponse::new(ResponseError::OffsetOutOfRange)),
    );
    let client: RawKafkaClient = harness.client().await;

    let fetch_req = make_fetch_request("chaos", 0, 0);
    let response = Fetch::fetch(&client, fetch_req).await;

    let error_code = response.responses[0].partitions[0].error_code;
    assert_eq!(error_code, ResponseError::OffsetOutOfRange.code());

    harness.shutdown().await;
}

#[tokio::test]
async fn consumer_group_lifecycle() {
    let harness = Harness::with(BrokerBuilder::new().topic("group-test", Buffer::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let join = client.join_group("test-group").await;
    assert!(join.error.is_none(), "join_group should succeed");
    assert!(!join.member_id.is_empty());
    assert!(!join.leader.is_empty());

    let sync = client.sync_group("test-group").await;
    assert!(sync.error.is_none(), "sync_group should succeed");

    let hb = client.heartbeat("test-group").await;
    assert!(hb.error.is_none(), "heartbeat should succeed");

    harness.shutdown().await;
}
