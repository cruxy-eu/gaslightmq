use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use crate::traits::{
    Admin, Consumer, PartitionMetadata, ProduceResult, Producer, TopicConfigs, TopicMetadata,
    Watermarks,
};
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Offset, Partition, Record, RecordBatch};
use gaslightmq_wire::messages::describe_configs_request::DescribeConfigsResource;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, DescribeConfigsRequest,
    DescribeConfigsResponse, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, TopicName,
};
use gaslightmq_wire::protocol::StrBytes;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tracing::debug;

use crate::connection::Connection;
use crate::error::ClientError;

/// Resource type of a topic in DescribeConfigs.
const RESOURCE_TOPIC: i8 = 2;

/// Versions this client speaks, per API. Produce and Fetch stop before
/// topic ids replaced topic names.
const SUPPORTED: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::DescribeConfigs, 1, 4),
];

/// Evaluate `$body` (an async block result) until it succeeds, fails with a
/// non-retriable error, or the delivery timeout runs out. `$timeout` is bound
/// to the time the attempt may take.
macro_rules! retry {
    ($client:expr, |$timeout:ident| $body:expr) => {{
        let config = &$client.inner.config;
        let deadline = Instant::now() + config.delivery_timeout;
        loop {
            let $timeout = config.request_timeout.min(deadline.saturating_duration_since(Instant::now()));
            let err = match $body {
                Ok(v) => break Ok(v),
                Err(e) if e.is_retriable() => e,
                Err(e) => break Err(e),
            };
            debug!(error = %err, "retrying");
            if Instant::now() + config.retry_backoff >= deadline {
                break Err(ClientError::DeliveryTimeout(Box::new(err)));
            }
            tokio::time::sleep(config.retry_backoff).await;
        }
    }};
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    /// How long one request may take.
    pub request_timeout: Duration,
    /// How long an operation keeps retrying retriable errors.
    pub delivery_timeout: Duration,
    pub retry_backoff: Duration,
    /// `-1` (all in-sync replicas) or `1` (leader only).
    pub acks: i16,
}

impl ClientConfig {
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: vec![bootstrap.into()],
            client_id: "gaslightmq-client".to_string(),
            request_timeout: Duration::from_secs(5),
            delivery_timeout: Duration::from_secs(10),
            retry_backoff: Duration::from_millis(50),
            acks: -1,
        }
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    pub fn acks(mut self, acks: i16) -> Self {
        self.acks = acks;
        self
    }
}

#[derive(Default)]
struct ClusterState {
    brokers: BTreeMap<i32, String>,
    leaders: HashMap<(String, i32), i32>,
}

/// A Kafka client that follows the cluster: it learns brokers and partition
/// leaders from Metadata, sends requests to the right broker, and retries
/// retriable errors until the delivery timeout runs out.
///
/// Cheap to clone.
#[derive(Clone)]
pub struct KafkaClient {
    inner: Arc<Inner>,
}

struct Inner {
    config: ClientConfig,
    bootstrap: Vec<Arc<Connection>>,
    nodes: Mutex<HashMap<i32, Arc<Connection>>>,
    cluster: Mutex<ClusterState>,
    versions: OnceCell<HashMap<i16, i16>>,
}

impl From<SocketAddr> for KafkaClient {
    fn from(addr: SocketAddr) -> Self {
        Self::new(ClientConfig::new(addr.to_string()))
    }
}

impl KafkaClient {
    pub fn new(config: ClientConfig) -> Self {
        let bootstrap = config
            .bootstrap
            .iter()
            .map(|addr| Arc::new(Connection::new(addr.clone(), &config.client_id)))
            .collect();
        Self {
            inner: Arc::new(Inner {
                config,
                bootstrap,
                nodes: Mutex::default(),
                cluster: Mutex::default(),
                versions: OnceCell::new(),
            }),
        }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    // -----------------------------------------------------------------
    // Plumbing
    // -----------------------------------------------------------------

    /// Connections to try for cluster-wide requests: known brokers first,
    /// then the bootstrap addresses.
    fn any_connections(&self) -> Vec<Arc<Connection>> {
        let brokers: Vec<i32> = self.inner.cluster.lock().unwrap().brokers.keys().copied().collect();
        let mut conns: Vec<Arc<Connection>> = brokers.into_iter().filter_map(|id| self.node(id).ok()).collect();
        conns.extend(self.inner.bootstrap.iter().cloned());
        conns
    }

    fn node(&self, node_id: i32) -> Result<Arc<Connection>, ClientError> {
        let addr = self
            .inner
            .cluster
            .lock()
            .unwrap()
            .brokers
            .get(&node_id)
            .cloned()
            .ok_or_else(|| ClientError::Protocol(format!("unknown broker {node_id}")))?;
        let mut nodes = self.inner.nodes.lock().unwrap();
        match nodes.get(&node_id) {
            Some(conn) if conn.addr() == addr => Ok(conn.clone()),
            _ => {
                let conn = Arc::new(Connection::new(addr, &self.inner.config.client_id));
                nodes.insert(node_id, conn.clone());
                Ok(conn)
            }
        }
    }

    /// Negotiated version of `key`, asking the cluster once.
    async fn version(&self, key: ApiKey, timeout: Duration) -> Result<i16, ClientError> {
        let versions = self
            .inner
            .versions
            .get_or_try_init(|| async {
                let resp: ApiVersionsResponse = self.on_any(0, &ApiVersionsRequest::default(), timeout).await?;
                if let Some(e) = ClientError::broker(resp.error_code) {
                    return Err(e);
                }
                Ok(SUPPORTED
                    .iter()
                    .filter_map(|&(k, min, max)| {
                        let theirs = resp.api_keys.iter().find(|a| a.api_key == k as i16)?;
                        let v = max.min(theirs.max_version);
                        (v >= min.max(theirs.min_version)).then_some((k as i16, v))
                    })
                    .collect())
            })
            .await?;
        versions.get(&(key as i16)).copied().ok_or(ClientError::UnsupportedVersion { api_key: key as i16 })
    }

    /// Send a request to whichever broker answers first.
    async fn on_any<R: KafkaRequest>(&self, version: i16, req: &R, timeout: Duration) -> Result<R::Response, ClientError> {
        let mut last = ClientError::Io("no brokers to connect to".into());
        for conn in self.any_connections() {
            match conn.request(version, req, timeout).await {
                Ok(resp) => return Ok(resp),
                Err(e) if e.is_retriable() => last = e,
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }

    /// Fetch metadata for `topics` (all topics if `None`) and update the
    /// broker and leader caches.
    async fn refresh(&self, topics: Option<&[&str]>, timeout: Duration) -> Result<MetadataResponse, ClientError> {
        let version = self.version(ApiKey::Metadata, timeout).await?;
        let req = MetadataRequest::default().with_topics(topics.map(|ts| {
            ts.iter().map(|t| MetadataRequestTopic::default().with_name(Some(topic_name(t)))).collect()
        }));
        let resp: MetadataResponse = self.on_any(version, &req, timeout).await?;
        let mut cluster = self.inner.cluster.lock().unwrap();
        cluster.brokers = resp.brokers.iter().map(|b| (b.node_id.0, format!("{}:{}", b.host, b.port))).collect();
        for t in &resp.topics {
            let Some(name) = &t.name else { continue };
            cluster.leaders.retain(|(topic, _), _| topic != name.as_str());
            for p in &t.partitions {
                if p.error_code == 0 && p.leader_id.0 >= 0 {
                    cluster.leaders.insert((name.to_string(), p.partition_index), p.leader_id.0);
                }
            }
        }
        Ok(resp)
    }

    /// Connection to the leader of a partition, refreshing metadata if it
    /// isn't known.
    async fn leader(&self, topic: &str, partition: i32, timeout: Duration) -> Result<Arc<Connection>, ClientError> {
        let cached = self.inner.cluster.lock().unwrap().leaders.get(&(topic.to_string(), partition)).copied();
        let leader = match cached {
            Some(id) => id,
            None => {
                let resp = self.refresh(Some(&[topic]), timeout).await?;
                if let Some(t) = resp.topics.iter().find(|t| t.name.as_ref().is_some_and(|n| n.as_str() == topic))
                    && let Some(e) = ClientError::broker(t.error_code)
                {
                    return Err(e);
                }
                self.inner
                    .cluster
                    .lock()
                    .unwrap()
                    .leaders
                    .get(&(topic.to_string(), partition))
                    .copied()
                    .ok_or_else(|| ClientError::NoLeader { topic: topic.to_string(), partition })?
            }
        };
        self.node(leader)
    }

    /// Forget a partition's leader so the next attempt asks again.
    fn forget_leader(&self, topic: &str, partition: i32) {
        self.inner.cluster.lock().unwrap().leaders.remove(&(topic.to_string(), partition));
    }

    // -----------------------------------------------------------------
    // Operations
    // -----------------------------------------------------------------

    /// Write `records` to a partition. Returns the offset of the first one.
    pub async fn produce(&self, topic: &str, partition: i32, records: &[Record]) -> Result<Offset, ClientError> {
        let batch = RecordBatch::new(records).encode()?;
        retry!(self, |timeout| {
            let result = self.produce_once(topic, partition, batch.clone(), timeout).await;
            if matches!(&result, Err(e) if e.is_retriable()) {
                self.forget_leader(topic, partition);
            }
            result
        })
    }

    async fn produce_once(&self, topic: &str, partition: i32, batch: Bytes, timeout: Duration) -> Result<Offset, ClientError> {
        let version = self.version(ApiKey::Produce, timeout).await?;
        let conn = self.leader(topic, partition, timeout).await?;
        let req = ProduceRequest::default()
            .with_acks(self.inner.config.acks)
            .with_timeout_ms(timeout.as_millis() as i32)
            .with_topic_data(vec![TopicProduceData::default().with_name(topic_name(topic)).with_partition_data(vec![
                PartitionProduceData::default().with_index(partition).with_records(Some(batch)),
            ])]);
        let resp: ProduceResponse = conn.request(version, &req, timeout).await?;
        let p = resp
            .responses
            .iter()
            .filter(|t| t.name.as_str() == topic)
            .flat_map(|t| &t.partition_responses)
            .find(|p| p.index == partition)
            .ok_or_else(|| ClientError::Protocol(format!("no produce result for {topic}-{partition}")))?;
        match ClientError::broker(p.error_code) {
            Some(e) => Err(e),
            None => Ok(Offset::from(p.base_offset)),
        }
    }

    /// Read records of a partition starting at `offset`.
    pub async fn fetch(&self, topic: &str, partition: i32, offset: i64) -> Result<Vec<Record>, ClientError> {
        retry!(self, |timeout| {
            let result = self.fetch_once(topic, partition, offset, timeout).await;
            if matches!(&result, Err(e) if e.is_retriable()) {
                self.forget_leader(topic, partition);
            }
            result
        })
    }

    async fn fetch_once(&self, topic: &str, partition: i32, offset: i64, timeout: Duration) -> Result<Vec<Record>, ClientError> {
        let version = self.version(ApiKey::Fetch, timeout).await?;
        let conn = self.leader(topic, partition, timeout).await?;
        let req = FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(0)
            .with_max_bytes(16 << 20)
            .with_topics(vec![FetchTopic::default().with_topic(topic_name(topic)).with_partitions(vec![
                FetchPartition::default().with_partition(partition).with_fetch_offset(offset).with_partition_max_bytes(16 << 20),
            ])]);
        let resp: FetchResponse = conn.request(version, &req, timeout).await?;
        if let Some(e) = ClientError::broker(resp.error_code) {
            return Err(e);
        }
        let mut records = Vec::new();
        for p in resp.responses.iter().flat_map(|t| &t.partitions).filter(|p| p.partition_index == partition) {
            if let Some(e) = ClientError::broker(p.error_code) {
                return Err(e);
            }
            if let Some(raw) = &p.records {
                records.extend(RecordBatch::decode_all(raw)?.into_iter().flat_map(|batch| batch.records));
            }
        }
        Ok(records)
    }

    /// Partition count and ids of a topic.
    pub async fn topic_metadata(&self, topic: Option<&str>) -> Result<Vec<TopicMetadata>, ClientError> {
        retry!(self, |timeout| async {
            let topics = topic.map(|t| [t]);
            let resp = self.refresh(topics.as_ref().map(|t| t.as_slice()), timeout).await?;
            resp.topics
                .iter()
                .map(|t| {
                    if let Some(e) = ClientError::broker(t.error_code) {
                        return Err(e);
                    }
                    let mut partitions: Vec<PartitionMetadata> =
                        t.partitions.iter().map(|p| PartitionMetadata { id: Partition::from(p.partition_index) }).collect();
                    partitions.sort_by_key(|p| i32::from(p.id));
                    Ok(TopicMetadata { name: t.name.as_ref().map(|n| n.to_string()).unwrap_or_default(), partitions })
                })
                .collect()
        }
        .await)
    }

    /// Earliest and latest offsets of a partition.
    pub async fn watermarks(&self, topic: &str, partition: i32) -> Result<Watermarks, ClientError> {
        let low = self.list_offset(topic, partition, -2).await?;
        let high = self.list_offset(topic, partition, -1).await?;
        Ok(Watermarks { low: Offset::from(low), high: Offset::from(high) })
    }

    async fn list_offset(&self, topic: &str, partition: i32, timestamp: i64) -> Result<i64, ClientError> {
        retry!(self, |timeout| async {
            let version = self.version(ApiKey::ListOffsets, timeout).await?;
            let conn = self.leader(topic, partition, timeout).await?;
            let req = ListOffsetsRequest::default().with_replica_id(BrokerId(-1)).with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name(topic))
                    .with_partitions(vec![ListOffsetsPartition::default().with_partition_index(partition).with_timestamp(timestamp)]),
            ]);
            let resp: ListOffsetsResponse = conn.request(version, &req, timeout).await?;
            let p = resp
                .topics
                .iter()
                .flat_map(|t| &t.partitions)
                .find(|p| p.partition_index == partition)
                .ok_or_else(|| ClientError::Protocol(format!("no offsets for {topic}-{partition}")))?;
            match ClientError::broker(p.error_code) {
                Some(e) => {
                    self.forget_leader(topic, partition);
                    Err(e)
                }
                None => Ok(p.offset),
            }
        }
        .await)
    }

    /// Configuration of a topic. `keys` limits the result; empty means all.
    pub async fn describe_topic_configs(&self, topic: &str, keys: &[&str]) -> Result<BTreeMap<String, Option<String>>, ClientError> {
        retry!(self, |timeout| async {
            let version = self.version(ApiKey::DescribeConfigs, timeout).await?;
            let keys = (!keys.is_empty()).then(|| keys.iter().map(|k| StrBytes::from_string(k.to_string())).collect());
            let req = DescribeConfigsRequest::default().with_resources(vec![
                DescribeConfigsResource::default()
                    .with_resource_type(RESOURCE_TOPIC)
                    .with_resource_name(StrBytes::from_string(topic.to_string()))
                    .with_configuration_keys(keys),
            ]);
            let resp: DescribeConfigsResponse = self.on_any(version, &req, timeout).await?;
            let result = resp
                .results
                .iter()
                .find(|r| r.resource_type == RESOURCE_TOPIC && r.resource_name.as_str() == topic)
                .ok_or_else(|| ClientError::Protocol(format!("no configs for topic {topic}")))?;
            if let Some(e) = ClientError::broker(result.error_code) {
                return Err(e);
            }
            Ok(result.configs.iter().map(|c| (c.name.to_string(), c.value.as_ref().map(|v| v.to_string()))).collect())
        }
        .await)
    }
}

fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

// ---------------------------------------------------------------------
// Facade
// ---------------------------------------------------------------------

impl Producer for KafkaClient {
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> ProduceResult {
        match self.produce(topic, i32::from(partition), records).await {
            Ok(offset) => ProduceResult { error: None, partition, offset },
            Err(e) => ProduceResult { error: Some(e.to_response_error()), partition, offset: Offset::from(-1i64) },
        }
    }
}

impl Consumer for KafkaClient {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        KafkaClient::fetch(self, topic, i32::from(partition), i64::from(offset))
            .await
            .unwrap_or_else(|e| panic!("fetch {topic}-{partition} failed: {e}"))
    }

    async fn metadata_for_topic(&self, topic: &str) -> TopicMetadata {
        self.topic_metadata(Some(topic))
            .await
            .unwrap_or_else(|e| panic!("metadata for {topic} failed: {e}"))
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no metadata for {topic}"))
    }

    async fn metadata_for_all_topics(&self) -> Vec<TopicMetadata> {
        self.topic_metadata(None).await.unwrap_or_else(|e| panic!("metadata failed: {e}"))
    }

    async fn watermarks(&self, topic: &str, partition: Partition) -> Watermarks {
        KafkaClient::watermarks(self, topic, i32::from(partition))
            .await
            .unwrap_or_else(|e| panic!("watermarks of {topic}-{partition} failed: {e}"))
    }
}

impl Admin for KafkaClient {
    async fn describe_topic_configs(&self, topic: &str, keys: &[&str]) -> TopicConfigs {
        match KafkaClient::describe_topic_configs(self, topic, keys).await {
            Ok(configs) => TopicConfigs { error: None, configs },
            Err(e) => TopicConfigs { error: Some(e.to_response_error()), configs: BTreeMap::new() },
        }
    }
}
