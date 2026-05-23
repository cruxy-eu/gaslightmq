use std::collections::HashMap;

use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::ApiVersionsResponse;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use gaslightmq_kernel::{Partition, Offset};
use crate::record::{FetchResult, ProduceResult, Record};
use crate::topic::Topic;

// ---------------------------------------------------------------------------
// Header version helpers
// ---------------------------------------------------------------------------

/// Returns the header version to use when decoding a request for the given
/// API key and API version. ApiVersions v3+ uses header v2, everything else v1.
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    if let Ok(key) = ApiKey::try_from(api_key) {
        key.request_header_version(api_version)
    } else {
        1 // default to v1 for unknown keys
    }
}

/// Returns the header version to use when encoding a response for the given
/// API key and API version. ApiVersions v3+ uses header v1, everything else v0.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if let Ok(key) = ApiKey::try_from(api_key) {
        key.response_header_version(api_version)
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// ParsedRequest
// ---------------------------------------------------------------------------

/// A parsed Kafka request frame containing the decoded header, the API key
/// and version extracted from it, and the remaining body bytes.
pub struct ParsedRequest {
    pub header: RequestHeader,
    pub api_key: i16,
    pub api_version: i16,
    pub body: Bytes,
}

// ---------------------------------------------------------------------------
// Sync frame parsing (for easy unit testing)
// ---------------------------------------------------------------------------

/// Parse a single Kafka request frame from raw bytes (without the 4-byte
/// length prefix). Returns a `ParsedRequest`.
pub fn parse_request(data: &[u8]) -> Result<ParsedRequest> {
    let mut buf = Bytes::copy_from_slice(data);
    let header = kafka_protocol::protocol::decode_request_header_from_buffer(&mut buf)?;
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;
    let body = buf;
    Ok(ParsedRequest {
        header,
        api_key,
        api_version,
        body,
    })
}

// ---------------------------------------------------------------------------
// Async framing: read_request / write_response
// ---------------------------------------------------------------------------

/// Read a length-prefixed Kafka request frame from an async reader.
/// Returns `None` on clean EOF (connection closed).
pub async fn read_request<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Option<ParsedRequest>> {
    // Read 4-byte big-endian length prefix.
    let len = match reader.read_u32().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    if len == 0 {
        bail!("received zero-length frame");
    }

    let mut frame = vec![0u8; len];
    reader.read_exact(&mut frame).await?;

    parse_request(&frame).map(Some)
}

/// Write a length-prefixed Kafka response frame.
///
/// Encodes the `ResponseHeader` followed by the `body` (which should already
/// be the encoded response message), prepends a 4-byte length, and writes
/// everything to the writer.
pub async fn write_response<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<()> {
    let resp_header_version = response_header_version(api_key, api_version);

    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    let header_size = header.compute_size(resp_header_version)?;
    let total = header_size + body.len();

    let mut frame = BytesMut::with_capacity(4 + total);
    // Length prefix
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    // Header
    header.encode(&mut frame, resp_header_version)?;
    // Body
    frame.extend_from_slice(body);

    writer.write_all(&frame).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Protocol handlers
// ---------------------------------------------------------------------------

/// Handle an ApiVersions request. Returns the supported API keys/versions.
pub fn handle_api_versions(_api_version: i16) -> ApiVersionsResponse {
    // We support a subset of APIs at specific versions.
    let supported: Vec<(i16, i16, i16)> = vec![
        (ApiKey::Produce as i16, 3, 9),
        (ApiKey::Fetch as i16, 4, 12),
        (ApiKey::ListOffsets as i16, 1, 5),
        (ApiKey::Metadata as i16, 1, 9),
        (ApiKey::FindCoordinator as i16, 0, 3),
        (ApiKey::JoinGroup as i16, 0, 5),
        (ApiKey::SyncGroup as i16, 0, 3),
        (ApiKey::Heartbeat as i16, 0, 3),
        (ApiKey::ApiVersions as i16, 0, 3),
    ];

    let api_keys: Vec<ApiVersion> = supported
        .into_iter()
        .map(|(key, min, max)| {
            ApiVersion::default()
                .with_api_key(key)
                .with_min_version(min)
                .with_max_version(max)
        })
        .collect();

    ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(api_keys)
}

/// Handle a Metadata request. Returns single broker, all registered topics
/// with their partitions.
pub fn handle_metadata(
    body: &mut Bytes,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
    host: &str,
    port: i32,
) -> Result<MetadataResponse> {
    // Decode the request body.
    let _request = MetadataRequest::decode(body, api_version)?;

    let broker = MetadataResponseBroker::default()
        .with_node_id(BrokerId(0))
        .with_host(StrBytes::from_string(host.to_string()))
        .with_port(port);

    let response_topics: Vec<MetadataResponseTopic> = topics
        .iter()
        .map(|(name, topic)| {
            let partitions: Vec<MetadataResponsePartition> = (0..topic.partitions())
                .map(|i| {
                    MetadataResponsePartition::default()
                        .with_error_code(0)
                        .with_partition_index(i)
                        .with_leader_id(BrokerId(0))
                        .with_replica_nodes(vec![BrokerId(0)])
                        .with_isr_nodes(vec![BrokerId(0)])
                })
                .collect();

            MetadataResponseTopic::default()
                .with_error_code(0)
                .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                .with_is_internal(false)
                .with_partitions(partitions)
        })
        .collect();

    Ok(MetadataResponse::default()
        .with_brokers(vec![broker])
        .with_cluster_id(Some(StrBytes::from_static_str("gaslightmq")))
        .with_controller_id(BrokerId(0))
        .with_topics(response_topics))
}

/// Convert kafka-protocol records to our internal Record type.
fn kafka_records_to_internal(kafka_records: &[KafkaRecord]) -> Vec<Record> {
    kafka_records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| crate::record::Header {
                    key: k.as_str().to_string(),
                    value: v.clone(),
                })
                .collect();
            Record {
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

/// Convert our internal Records to kafka-protocol Records for encoding.
fn internal_to_kafka_records(records: &[Record], base_offset: i64) -> Vec<KafkaRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers = IndexMap::new();
            for h in &r.headers {
                headers.insert(
                    StrBytes::from_string(h.key.clone()),
                    h.value.clone(),
                );
            }
            KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: base_offset + i as i64,
                sequence: NO_SEQUENCE,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

/// Handle a Produce request. Decodes record batches, calls topic.on_produce(),
/// and builds a response.
pub async fn handle_produce(
    body: &mut Bytes,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> Result<ProduceResponse> {
    let request = ProduceRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for topic_data in &request.topic_data {
        let topic_name = topic_data.name.0.as_str().to_string();

        let mut partition_responses = Vec::new();

        for partition_data in &topic_data.partition_data {
            let partition_idx = partition_data.index;

            let result = if let Some(topic) = topics.get(&topic_name) {
                // Decode records from the raw bytes
                if let Some(ref raw_records) = partition_data.records {
                    let mut records_buf = raw_records.clone();
                    match RecordBatchDecoder::decode(&mut records_buf) {
                        Ok(record_set) => {
                            let internal_records = kafka_records_to_internal(&record_set.records);
                            topic.on_produce(Partition::from(partition_idx), &internal_records).await
                        }
                        Err(_) => {
                            // If we can't decode the records, still accept them
                            // (some clients send empty batches)
                            ProduceResult::Success
                        }
                    }
                } else {
                    ProduceResult::Success
                }
            } else {
                ProduceResult::Error(kafka_protocol::error::ResponseError::UnknownTopicOrPartition)
            };

            let error_code = match &result {
                ProduceResult::Success => 0,
                ProduceResult::Error(e) => e.code(),
            };

            partition_responses.push(
                PartitionProduceResponse::default()
                    .with_index(partition_idx)
                    .with_error_code(error_code),
            );
        }

        response_topics.push(
            TopicProduceResponse::default()
                .with_name(topic_data.name.clone())
                .with_partition_responses(partition_responses),
        );
    }

    Ok(ProduceResponse::default().with_responses(response_topics))
}

/// Handle a Fetch request. Calls topic.on_fetch() and encodes records into
/// the response.
pub async fn handle_fetch(
    body: &mut Bytes,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> Result<FetchResponse> {
    let request = FetchRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for fetch_topic in &request.topics {
        let topic_name = fetch_topic.topic.0.as_str().to_string();

        let mut partition_responses = Vec::new();

        for fetch_partition in &fetch_topic.partitions {
            let partition_idx = fetch_partition.partition;
            let fetch_offset = fetch_partition.fetch_offset;

            let (error_code, records_bytes) = if let Some(topic) = topics.get(&topic_name) {
                let result = topic.on_fetch(Partition::from(partition_idx), Offset::from(fetch_offset)).await;
                match result {
                    FetchResult::Empty => (0i16, None),
                    FetchResult::Records(records) => {
                        // Encode records into kafka-protocol format
                        let kafka_records = internal_to_kafka_records(&records, fetch_offset);
                        let options = RecordEncodeOptions {
                            version: 2,
                            compression: Compression::None,
                        };
                        let mut buf = BytesMut::new();
                        RecordBatchEncoder::encode(&mut buf, &kafka_records, &options)?;
                        (0, Some(Bytes::from(buf)))
                    }
                    FetchResult::Error(e) => (e.code(), None),
                }
            } else {
                (
                    kafka_protocol::error::ResponseError::UnknownTopicOrPartition.code(),
                    None,
                )
            };

            partition_responses.push(
                PartitionData::default()
                    .with_partition_index(partition_idx)
                    .with_error_code(error_code)
                    .with_records(records_bytes),
            );
        }

        response_topics.push(
            FetchableTopicResponse::default()
                .with_topic(fetch_topic.topic.clone())
                .with_partitions(partition_responses),
        );
    }

    Ok(FetchResponse::default().with_responses(response_topics))
}

/// Handle a ListOffsets request. Returns offset 0 for earliest (-2 timestamp),
/// current end for latest (-1).
pub async fn handle_list_offsets(
    body: &mut Bytes,
    api_version: i16,
    _topics: &HashMap<String, Box<dyn Topic>>,
) -> Result<ListOffsetsResponse> {
    let request = ListOffsetsRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for topic in &request.topics {

        let mut partitions = Vec::new();

        for partition in &topic.partitions {
            let offset = match partition.timestamp {
                -2 => 0i64, // earliest
                -1 => {
                    // latest: we don't track offsets, so return 0
                    // (In a real impl, we'd query the buffer's length)
                    0
                }
                ts => ts, // specific timestamp; just return 0
            };

            partitions.push(
                ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(0)
                    .with_offset(offset)
                    .with_timestamp(partition.timestamp),
            );
        }

        response_topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }

    Ok(ListOffsetsResponse::default().with_topics(response_topics))
}

/// Handle a FindCoordinator request. Returns self as the coordinator.
pub fn handle_find_coordinator(host: &str, port: i32) -> FindCoordinatorResponse {
    FindCoordinatorResponse::default()
        .with_error_code(0)
        .with_node_id(BrokerId(0))
        .with_host(StrBytes::from_string(host.to_string()))
        .with_port(port)
}

/// Handle a JoinGroup request. Returns a minimal success stub.
pub fn handle_join_group(_api_version: i16) -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_error_code(0)
        .with_generation_id(1)
        .with_protocol_name(Some(StrBytes::from_static_str("range")))
        .with_leader(StrBytes::from_static_str("gaslightmq-member"))
        .with_member_id(StrBytes::from_static_str("gaslightmq-member"))
}

/// Handle a SyncGroup request. Returns a minimal success stub.
pub fn handle_sync_group(_api_version: i16) -> SyncGroupResponse {
    SyncGroupResponse::default().with_error_code(0)
}

/// Handle a Heartbeat request. Returns a minimal success stub.
pub fn handle_heartbeat(_api_version: i16) -> HeartbeatResponse {
    HeartbeatResponse::default().with_error_code(0)
}

/// Encode a response body using the Encodable trait for the given api_version.
pub fn encode_response_body<E: Encodable>(response: &E, api_version: i16) -> Result<Vec<u8>> {
    let size = response.compute_size(api_version)?;
    let mut buf = BytesMut::with_capacity(size);
    response.encode(&mut buf, api_version)?;
    Ok(buf.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;

    #[test]
    fn request_header_version_api_versions_v3_returns_2() {
        assert_eq!(request_header_version(ApiKey::ApiVersions as i16, 3), 2);
    }

    #[test]
    fn request_header_version_api_versions_v2_returns_1() {
        assert_eq!(request_header_version(ApiKey::ApiVersions as i16, 2), 1);
    }

    #[test]
    fn request_header_version_metadata_v1_returns_1() {
        assert_eq!(request_header_version(ApiKey::Metadata as i16, 1), 1);
    }

    #[test]
    fn request_header_version_metadata_v9_returns_2() {
        assert_eq!(request_header_version(ApiKey::Metadata as i16, 9), 2);
    }

    #[test]
    fn response_header_version_api_versions_always_0() {
        // ApiVersionsResponse.header_version always returns 0
        assert_eq!(response_header_version(ApiKey::ApiVersions as i16, 0), 0);
        assert_eq!(response_header_version(ApiKey::ApiVersions as i16, 3), 0);
    }

    #[test]
    fn response_header_version_metadata_v9_returns_1() {
        assert_eq!(response_header_version(ApiKey::Metadata as i16, 9), 1);
    }

    #[test]
    fn response_header_version_metadata_v1_returns_0() {
        assert_eq!(response_header_version(ApiKey::Metadata as i16, 1), 0);
    }

    #[test]
    fn parse_request_decodes_header() {
        // Build a raw frame: RequestHeader (v1) for Metadata v1
        let api_key: i16 = ApiKey::Metadata as i16; // 3
        let api_version: i16 = 1;
        let correlation_id: i32 = 42;
        let client_id = Some(StrBytes::from_static_str("test-client"));

        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(client_id);

        let header_version = request_header_version(api_key, api_version);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        // Add some body bytes
        buf.extend_from_slice(&[0xDE, 0xAD]);

        let parsed = parse_request(&buf).unwrap();
        assert_eq!(parsed.api_key, api_key);
        assert_eq!(parsed.api_version, api_version);
        assert_eq!(parsed.header.correlation_id, 42);
        assert_eq!(
            parsed.header.client_id.as_deref(),
            Some("test-client")
        );
        assert_eq!(parsed.body.as_ref(), &[0xDE, 0xAD]);
    }

    #[tokio::test]
    async fn read_request_parses_length_prefixed_frame() {
        // Build a complete frame with length prefix
        let api_key: i16 = ApiKey::ApiVersions as i16; // 18
        let api_version: i16 = 0;
        let correlation_id: i32 = 7;

        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(None);

        let header_version = request_header_version(api_key, api_version);
        let mut payload = BytesMut::new();
        header.encode(&mut payload, header_version).unwrap();

        // Build the length-prefixed wire format
        let len = payload.len() as u32;
        let mut wire = Vec::new();
        wire.extend_from_slice(&len.to_be_bytes());
        wire.extend_from_slice(&payload);

        // Use a tokio duplex channel as our async reader
        let (mut client, mut server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            client.write_all(&wire).await.unwrap();
            // Dropping client closes the write half
        });

        let parsed = read_request(&mut server).await.unwrap().unwrap();
        assert_eq!(parsed.api_key, api_key);
        assert_eq!(parsed.api_version, api_version);
        assert_eq!(parsed.header.correlation_id, 7);
    }

    #[tokio::test]
    async fn read_request_returns_none_on_eof() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        drop(client); // immediately close
        let result = read_request(&mut server).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn handle_api_versions_returns_supported_keys() {
        let response = handle_api_versions(0);
        assert_eq!(response.error_code, 0);
        assert!(!response.api_keys.is_empty());

        // Check that ApiVersions itself is in the list
        let api_versions_entry = response
            .api_keys
            .iter()
            .find(|k| k.api_key == ApiKey::ApiVersions as i16);
        assert!(api_versions_entry.is_some());

        // Check Produce is present
        let produce_entry = response
            .api_keys
            .iter()
            .find(|k| k.api_key == ApiKey::Produce as i16);
        assert!(produce_entry.is_some());

        // Check Metadata is present
        let metadata_entry = response
            .api_keys
            .iter()
            .find(|k| k.api_key == ApiKey::Metadata as i16);
        assert!(metadata_entry.is_some());
    }

    #[test]
    fn handle_api_versions_response_can_be_encoded() {
        // Verify we can encode the response at version 0 (pre-flexible)
        let response = handle_api_versions(0);
        let body = encode_response_body(&response, 0).unwrap();
        assert!(!body.is_empty());
    }

    #[test]
    fn handle_metadata_returns_topics_and_broker() {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();
        topics.insert(
            "orders".to_string(),
            Box::new(crate::topic::BlackHole::new(3)),
        );
        topics.insert(
            "events".to_string(),
            Box::new(crate::topic::BlackHole::new(1)),
        );

        // Build a MetadataRequest (version 1) body
        let metadata_req = MetadataRequest::default().with_topics(None);
        let mut body_buf = BytesMut::new();
        metadata_req.encode(&mut body_buf, 1).unwrap();
        let mut body = Bytes::from(body_buf);

        let response = handle_metadata(&mut body, 1, &topics, "localhost", 9092).unwrap();

        // Should have 1 broker
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].host.as_str(), "localhost");
        assert_eq!(response.brokers[0].port, 9092);

        // Should have 2 topics
        assert_eq!(response.topics.len(), 2);

        // Find "orders" topic
        let orders = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("orders"))
            .unwrap();
        assert_eq!(orders.partitions.len(), 3);
        assert_eq!(orders.error_code, 0);

        // Find "events" topic
        let events = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("events"))
            .unwrap();
        assert_eq!(events.partitions.len(), 1);

        // Verify the response can be encoded at version 1
        let body = encode_response_body(&response, 1).unwrap();
        assert!(!body.is_empty());
    }

    #[test]
    fn handle_find_coordinator_returns_self() {
        let response = handle_find_coordinator("localhost", 9092);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.host.as_str(), "localhost");
        assert_eq!(response.port, 9092);
    }
}
