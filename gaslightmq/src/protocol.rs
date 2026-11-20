use anyhow::Result;
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
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};

use gaslightmq_kernel::{Partition, Offset};
use crate::cluster::ClusterView;
use crate::record::{FetchResult, ProduceResult, Record};
use crate::registry::{CreateResult, DeleteResult, TopicRegistryHandle};
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::{CreatableTopicResult, CreateTopicsResponse};
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use kafka_protocol::messages::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse};

pub use gaslightmq_wire::{
    ParsedRequest, encode_response_body, parse_request, read_request, request_header_version,
    response_header_version, write_response,
};

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
        (ApiKey::CreateTopics as i16, 2, 7),
        (ApiKey::DeleteTopics as i16, 1, 5),
        (ApiKey::DescribeConfigs as i16, 1, 4),
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

/// Handle a Metadata request. Returns the cluster's brokers and the requested
/// topics (all of them if none are listed). Unknown topics get
/// UNKNOWN_TOPIC_OR_PARTITION.
pub async fn handle_metadata(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
    cluster: &ClusterView,
) -> Result<MetadataResponse> {
    // Decode the request body.
    let request = MetadataRequest::decode(body, api_version)?;

    let brokers: Vec<MetadataResponseBroker> = cluster
        .nodes
        .iter()
        .map(|n| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(n.node_id))
                .with_host(StrBytes::from_string(n.host.clone()))
                .with_port(n.port)
        })
        .collect();

    let topics = registry.list_all().await;
    let requested: Option<Vec<String>> = request
        .topics
        .map(|ts| ts.into_iter().filter_map(|t| t.name.map(|n| n.0.to_string())).collect());
    let unknown: Vec<MetadataResponseTopic> = requested
        .iter()
        .flatten()
        .filter(|name| !topics.contains_key(*name))
        .map(|name| {
            MetadataResponseTopic::default()
                .with_error_code(kafka_protocol::error::ResponseError::UnknownTopicOrPartition.code())
                .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
        })
        .collect();
    let mut response_topics: Vec<MetadataResponseTopic> = topics
        .iter()
        .filter(|(name, _)| requested.as_ref().is_none_or(|r| r.contains(name)))
        .map(|(name, topic)| {
            let partitions: Vec<MetadataResponsePartition> = (0..topic.partitions())
                .map(|i| {
                    let leader = BrokerId(cluster.leader_for(i));
                    MetadataResponsePartition::default()
                        .with_error_code(0)
                        .with_partition_index(i)
                        .with_leader_id(leader)
                        .with_replica_nodes(vec![leader])
                        .with_isr_nodes(vec![leader])
                })
                .collect();

            MetadataResponseTopic::default()
                .with_error_code(0)
                .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                .with_is_internal(false)
                .with_partitions(partitions)
        })
        .collect();
    response_topics.extend(unknown);

    Ok(MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from_static_str("gaslightmq")))
        .with_controller_id(BrokerId(cluster.controller_id()))
        .with_topics(response_topics))
}

fn kafka_records_to_internal(kafka_records: &[KafkaRecord]) -> Vec<Record> {
    kafka_records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| gaslightmq_kernel::Header {
                    key: k.as_str().to_string(),
                    value: v.clone(),
                })
                .collect();
            let mut record = Record::empty()
                .with_headers(headers)
                .with_offset(Offset::from(r.offset))
                .with_timestamp(r.timestamp);
            record.key = r.key.clone();
            record.value = r.value.clone();
            record
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
                sequence: i as i32,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

/// Handle a Produce request. Decodes record batches, calls topic.on_produce(),
/// and builds a response. Returns `None` for acks=0, which gets no response.
pub async fn handle_produce(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
) -> Result<Option<ProduceResponse>> {
    let request = ProduceRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for topic_data in &request.topic_data {
        let topic_name = topic_data.name.0.as_str().to_string();

        let mut partition_responses = Vec::new();

        for partition_data in &topic_data.partition_data {
            let partition_idx = partition_data.index;

            let result = if let Some(topic) = registry.lookup(&topic_name).await {
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

            tracing::debug!(topic = %topic_name, partition = partition_idx, result = ?result, "produce");
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

    // With acks=0 the client expects no response at all.
    if request.acks == 0 {
        return Ok(None);
    }
    Ok(Some(ProduceResponse::default().with_responses(response_topics)))
}

/// Handle a Fetch request. Calls topic.on_fetch() and encodes records into
/// the response.
pub async fn handle_fetch(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
) -> Result<FetchResponse> {
    let request = FetchRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for fetch_topic in &request.topics {
        let topic_name = fetch_topic.topic.0.as_str().to_string();

        let mut partition_responses = Vec::new();

        for fetch_partition in &fetch_topic.partitions {
            let partition_idx = fetch_partition.partition;
            let fetch_offset = fetch_partition.fetch_offset;

            let (error_code, records_bytes, hw) = if let Some(topic) = registry.lookup(&topic_name).await {
                let partition = Partition::from(partition_idx);
                let hw = i64::from(topic.high_watermark(partition).await);
                let result = topic.on_fetch(partition, Offset::from(fetch_offset)).await;
                tracing::debug!(
                    topic = %topic_name,
                    partition = partition_idx,
                    offset = fetch_offset,
                    result = match &result {
                        FetchResult::Empty => "empty",
                        FetchResult::Records(_) => "records",
                        FetchResult::Error(_) => "error",
                    },
                    count = match &result {
                        FetchResult::Records(r) => r.len(),
                        _ => 0,
                    },
                    "fetch",
                );
                match result {
                    FetchResult::Empty => (0i16, Some(Bytes::new()), hw),
                    FetchResult::Records(records) => {
                        let kafka_records = internal_to_kafka_records(&records, fetch_offset);
                        let options = RecordEncodeOptions {
                            version: 2,
                            compression: Compression::None,
                        };
                        let mut buf = BytesMut::new();
                        RecordBatchEncoder::encode(&mut buf, &kafka_records, &options)?;
                        (0, Some(Bytes::from(buf)), hw)
                    }
                    FetchResult::Error(e) => (e.code(), Some(Bytes::new()), hw),
                }
            } else {
                (
                    kafka_protocol::error::ResponseError::UnknownTopicOrPartition.code(),
                    Some(Bytes::new()),
                    0,
                )
            };

            partition_responses.push(
                PartitionData::default()
                    .with_partition_index(partition_idx)
                    .with_error_code(error_code)
                    .with_high_watermark(hw)
                    .with_last_stable_offset(hw)
                    .with_log_start_offset(0)
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
    registry: &TopicRegistryHandle,
) -> Result<ListOffsetsResponse> {
    let request = ListOffsetsRequest::decode(body, api_version)?;

    let mut response_topics = Vec::new();

    for topic in &request.topics {
        let topic_name = topic.name.0.as_str().to_string();
        let mut partitions = Vec::new();

        for partition in &topic.partitions {
            let offset = match partition.timestamp {
                -2 => 0i64,
                -1 => {
                    if let Some(t) = registry.lookup(&topic_name).await {
                        i64::from(t.high_watermark(Partition::from(partition.partition_index)).await)
                    } else {
                        0
                    }
                }
                _ => 0,
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
pub fn handle_find_coordinator(cluster: &ClusterView) -> FindCoordinatorResponse {
    let node = cluster.this_node();
    FindCoordinatorResponse::default()
        .with_error_code(0)
        .with_node_id(BrokerId(node.node_id))
        .with_host(StrBytes::from_string(node.host.clone()))
        .with_port(node.port)
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

/// Handle a CreateTopics request. Creates topics via the registry and returns results.
pub async fn handle_create_topics(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
) -> Result<CreateTopicsResponse> {
    let request = CreateTopicsRequest::decode(body, api_version)?;

    let mut results = Vec::new();
    for creatable in &request.topics {
        let name = creatable.name.0.as_str().to_string();
        let partitions = creatable.num_partitions.max(1);

        let error_code = match registry.create(&name, partitions).await {
            CreateResult::Success => 0,
            CreateResult::TopicAlreadyExists => {
                kafka_protocol::error::ResponseError::TopicAlreadyExists.code()
            }
        };

        results.push(
            CreatableTopicResult::default()
                .with_name(creatable.name.clone())
                .with_error_code(error_code)
                .with_num_partitions(partitions)
                .with_replication_factor(1),
        );
    }

    Ok(CreateTopicsResponse::default().with_topics(results))
}

/// Topic configs reported by DescribeConfigs: Kafka's defaults for the
/// settings clients usually look at.
const TOPIC_CONFIG_DEFAULTS: &[(&str, &str)] = &[
    ("cleanup.policy", "delete"),
    ("max.message.bytes", "1048588"),
    ("retention.bytes", "-1"),
    ("retention.ms", "604800000"),
];

/// Resource type of a topic in DescribeConfigs.
const RESOURCE_TOPIC: i8 = 2;

/// Handle a DescribeConfigs request. Topics report Kafka's default configs;
/// unknown topics get UNKNOWN_TOPIC_OR_PARTITION, other resource types
/// INVALID_REQUEST.
pub async fn handle_describe_configs(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
) -> Result<DescribeConfigsResponse> {
    use kafka_protocol::error::ResponseError;
    let request = DescribeConfigsRequest::decode(body, api_version)?;
    let mut results = Vec::new();
    for resource in request.resources {
        let result = DescribeConfigsResult::default()
            .with_resource_type(resource.resource_type)
            .with_resource_name(resource.resource_name.clone());
        let result = if resource.resource_type != RESOURCE_TOPIC {
            result.with_error_code(ResponseError::InvalidRequest.code())
        } else if registry.lookup(resource.resource_name.as_str()).await.is_none() {
            result.with_error_code(ResponseError::UnknownTopicOrPartition.code())
        } else {
            let wanted = resource.configuration_keys.as_ref();
            result.with_configs(
                TOPIC_CONFIG_DEFAULTS
                    .iter()
                    .filter(|(k, _)| wanted.is_none_or(|w| w.iter().any(|key| key.as_str() == *k)))
                    .map(|(k, v)| {
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str(k))
                            .with_value(Some(StrBytes::from_static_str(v)))
                            .with_config_source(5) // DEFAULT_CONFIG
                    })
                    .collect(),
            )
        };
        results.push(result);
    }
    Ok(DescribeConfigsResponse::default().with_results(results))
}

/// Handle a DeleteTopics request. Deletes topics via the registry and returns results.
pub async fn handle_delete_topics(
    body: &mut Bytes,
    api_version: i16,
    registry: &TopicRegistryHandle,
) -> Result<DeleteTopicsResponse> {
    let request = DeleteTopicsRequest::decode(body, api_version)?;

    let mut results = Vec::new();
    for topic_name in &request.topic_names {
        let name = topic_name.0.as_str().to_string();

        let error_code = match registry.delete(&name).await {
            DeleteResult::Success => 0,
            DeleteResult::UnknownTopicOrPartition => {
                kafka_protocol::error::ResponseError::UnknownTopicOrPartition.code()
            }
        };

        results.push(
            DeletableTopicResult::default()
                .with_name(Some(topic_name.clone()))
                .with_error_code(error_code),
        );
    }

    Ok(DeleteTopicsResponse::default().with_responses(results))
}

/// Encode a response body using the Encodable trait for the given api_version.
#[cfg(test)]
mod tests {
    use super::*;

    fn localhost_cluster() -> ClusterView {
        ClusterView {
            node_id: 0,
            nodes: vec![crate::cluster::Node { node_id: 0, host: "localhost".into(), port: 9092 }],
        }
    }
    use kafka_protocol::protocol::Encodable;
    use std::collections::HashMap;
    use std::sync::Arc;
    use kafka_protocol::messages::ApiKey;

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

    #[tokio::test]
    async fn handle_metadata_returns_topics_and_broker() {
        use crate::registry::TopicRegistry;
        use gaslightmq_scenario::TopicMode;

        let mut seed: HashMap<String, Arc<dyn crate::topic::Topic>> = HashMap::new();
        seed.insert("orders".to_string(), Arc::new(crate::topic::BlackHole::new(3)));
        seed.insert("events".to_string(), Arc::new(crate::topic::BlackHole::new(1)));
        let registry = TopicRegistry::spawn(seed, TopicMode::Buffer);

        let metadata_req = MetadataRequest::default().with_topics(None);
        let mut body_buf = BytesMut::new();
        metadata_req.encode(&mut body_buf, 1).unwrap();
        let mut body = Bytes::from(body_buf);

        let response = handle_metadata(&mut body, 1, &registry, &localhost_cluster()).await.unwrap();

        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].host.as_str(), "localhost");
        assert_eq!(response.brokers[0].port, 9092);
        assert_eq!(response.topics.len(), 2);

        let orders = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("orders"))
            .unwrap();
        assert_eq!(orders.partitions.len(), 3);
        assert_eq!(orders.error_code, 0);

        let events = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("events"))
            .unwrap();
        assert_eq!(events.partitions.len(), 1);

        let body = encode_response_body(&response, 1).unwrap();
        assert!(!body.is_empty());
    }

    #[test]
    fn handle_find_coordinator_returns_self() {
        let response = handle_find_coordinator(&localhost_cluster());
        assert_eq!(response.error_code, 0);
        assert_eq!(response.host.as_str(), "localhost");
        assert_eq!(response.port, 9092);
    }
}
