use bytes::{Bytes, BytesMut};
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Produce, RawKafkaClient};
use gaslightmq_testing::Harness;
use indexmap::IndexMap;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

fn make_record(key: Option<&[u8]>, value: Option<&[u8]>) -> KafkaRecord {
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
        key: key.map(|k| Bytes::copy_from_slice(k)),
        value: value.map(|v| Bytes::copy_from_slice(v)),
        headers: IndexMap::new(),
    }
}

fn make_record_with_headers(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: Vec<(&str, &[u8])>,
) -> KafkaRecord {
    let mut h = IndexMap::new();
    for (k, v) in headers {
        h.insert(
            StrBytes::from_string(k.to_string()),
            Some(Bytes::copy_from_slice(v)),
        );
    }
    let mut rec = make_record(key, value);
    rec.headers = h;
    rec
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
    let records_bytes = encode_records(records);
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(records_bytes))])])
}

#[tokio::test]
async fn produce_single_record_to_buffer() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("test", Buffer::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("test", 0, &[make_record(None, Some(b"hello"))]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_multiple_records_to_multiple_partitions() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("multi", Buffer::new(3)),
        RawKafkaClient::new,
    )
    .await;

    let req0 = make_produce_request("multi", 0, &[make_record(None, Some(b"p0-msg"))]);
    let resp0 = harness.client.produce(req0).await;
    assert_eq!(resp0.responses[0].partition_responses[0].error_code, 0);

    let req1 = make_produce_request("multi", 1, &[make_record(None, Some(b"p1-msg"))]);
    let resp1 = harness.client.produce(req1).await;
    assert_eq!(resp1.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_with_keys_and_headers() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("keyed", Buffer::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let record = make_record_with_headers(
        Some(b"my-key"),
        Some(b"my-value"),
        vec![("trace-id", b"abc-123")],
    );
    let request = make_produce_request("keyed", 0, &[record]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_blackhole_topic() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("void", BlackHole::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("void", 0, &[make_record(None, Some(b"gone"))]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_nonexistent_topic() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("exists", BlackHole::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("nope", 0, &[make_record(None, Some(b"fail"))]);
    let response = harness.client.produce(request).await;

    let error_code = response.responses[0].partition_responses[0].error_code;
    assert_ne!(error_code, 0, "should return error for unknown topic");

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_with_chaos_error_injection() {
    use gaslightmq::{Api, ErrorResponse};
    use kafka_protocol::error::ResponseError;

    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("chaos", Buffer::new(1))
            .chaos(Api::Produce, ErrorResponse::new(ResponseError::NotLeaderOrFollower)),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("chaos", 0, &[make_record(None, Some(b"fail"))]);
    let response = harness.client.produce(request).await;

    let error_code = response.responses[0].partition_responses[0].error_code;
    assert_eq!(
        error_code,
        ResponseError::NotLeaderOrFollower.code(),
        "chaos layer should inject NotLeaderOrFollower error"
    );

    harness.shutdown().await;
}
