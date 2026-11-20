//! Proxy test helpers: an in-process fake cluster, a proxy in front of it,
//! and a raw Kafka client that reports errors and timeouts instead of
//! panicking.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use gaslightmq::{Broker, BrokerBuilder, Buffer, ClusterView, Node, Proxy, ProxyBuilder};
use gaslightmq_wire::{decode_response, encode_request, peek_response_correlation_id, read_frame, write_frame};
use indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse, ProduceRequest,
    ProduceResponse, RequestHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::net::TcpStream;

pub const TIMEOUT: Duration = Duration::from_secs(5);

/// Several fake brokers posing as one cluster with node ids 0..n. Every
/// broker has every topic; partition `p` is led by node `p % n`.
pub struct FakeCluster {
    pub brokers: Vec<Broker>,
}

impl FakeCluster {
    pub async fn start(nodes: usize, topics: &[(&str, i32)]) -> Self {
        gaslightmq_logging::init();
        let mut brokers = Vec::new();
        for _ in 0..nodes {
            let mut config = BrokerBuilder::new().bind("127.0.0.1:0").build_config();
            for (name, partitions) in topics {
                config.topics.insert(name.to_string(), Box::new(Buffer::new(*partitions)));
            }
            brokers.push(Broker::start(config).await.unwrap());
        }
        let all: Vec<Node> = brokers.iter().enumerate().map(|(i, b)| Node::new(i as i32, b.addr())).collect();
        for (i, b) in brokers.iter().enumerate() {
            b.set_cluster(ClusterView { node_id: i as i32, nodes: all.clone() });
        }
        Self { brokers }
    }

    pub fn addr(&self, node: usize) -> SocketAddr {
        self.brokers[node].addr()
    }

    /// Start a proxy bootstrapping from node 0.
    pub async fn proxy(&self) -> Proxy {
        self.proxy_with(|c| c).await
    }

    pub async fn proxy_with(&self, f: impl FnOnce(ProxyBuilder) -> ProxyBuilder) -> Proxy {
        f(Proxy::builder().upstream(self.addr(0).to_string())).start().await.unwrap()
    }

    /// Values stored on `node` for a partition, read directly (not via the proxy).
    pub async fn values(&self, node: usize, topic: &str, partition: i32) -> Vec<String> {
        let mut client = TestClient::connect(self.addr(node)).await.unwrap();
        fetch_values(&mut client, topic, partition).await.unwrap()
    }
}

pub fn name(s: &str) -> TopicName {
    TopicName(StrBytes::from_string(s.to_string()))
}

/// A bare Kafka connection: one frame out, one frame in.
pub struct TestClient {
    stream: TcpStream,
    next_cid: i32,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        Ok(Self { stream: TcpStream::connect(addr).await?, next_cid: 1 })
    }

    /// Send a request without waiting; returns its correlation id.
    pub async fn send<R: Encodable>(&mut self, key: ApiKey, version: i16, req: &R) -> Result<i32> {
        let cid = self.next_cid;
        self.next_cid += 1;
        let header = RequestHeader::default()
            .with_request_api_key(key as i16)
            .with_request_api_version(version)
            .with_correlation_id(cid)
            .with_client_id(Some(StrBytes::from_static_str("proxy-test")));
        write_frame(&mut self.stream, &encode_request(&header, req)?).await?;
        Ok(cid)
    }

    /// Read the next response; returns its correlation id and body.
    pub async fn recv<R: Decodable>(&mut self, key: ApiKey, version: i16) -> Result<(i32, R)> {
        let frame = read_frame(&mut self.stream).await?.context("connection closed")?;
        let cid = peek_response_correlation_id(&frame)?;
        let (_, body) = decode_response(frame, key as i16, version)?;
        Ok((cid, body))
    }

    /// Read the next response, or `None` if nothing arrives within `wait`.
    pub async fn recv_within<R: Decodable>(&mut self, key: ApiKey, version: i16, wait: Duration) -> Result<Option<(i32, R)>> {
        match tokio::time::timeout(wait, self.recv(key, version)).await {
            Ok(r) => r.map(Some),
            Err(_) => Ok(None),
        }
    }

    pub async fn call<Req: Encodable, Resp: Decodable>(&mut self, key: ApiKey, version: i16, req: &Req) -> Result<Resp> {
        let cid = self.send(key, version, req).await?;
        let (got, resp) = tokio::time::timeout(TIMEOUT, self.recv(key, version)).await.context("timed out")??;
        anyhow::ensure!(got == cid, "correlation id {got}, expected {cid}");
        Ok(resp)
    }

    /// Whether the peer closed the connection within `wait`.
    pub async fn closed_within(&mut self, wait: Duration) -> bool {
        matches!(tokio::time::timeout(wait, read_frame(&mut self.stream)).await, Ok(Ok(None)) | Ok(Err(_)))
    }
}

pub const METADATA_V: i16 = 9;
pub const PRODUCE_V: i16 = 7;
pub const FETCH_V: i16 = 4;

pub fn metadata_request(topics: Option<&[&str]>) -> MetadataRequest {
    MetadataRequest::default().with_topics(
        topics.map(|ts| ts.iter().map(|t| MetadataRequestTopic::default().with_name(Some(name(t)))).collect()),
    )
}

pub async fn metadata(client: &mut TestClient, topics: Option<&[&str]>) -> Result<MetadataResponse> {
    client.call(ApiKey::Metadata, METADATA_V, &metadata_request(topics)).await
}

fn batch(values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: i as i32,
            timestamp: 0,
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &records, &RecordEncodeOptions { version: 2, compression: Compression::None }).unwrap();
    buf.freeze()
}

/// A produce request writing `values` to each `(topic, partition)`.
pub fn produce_request(acks: i16, parts: &[(&str, i32, &[&str])]) -> ProduceRequest {
    let mut topics: Vec<TopicProduceData> = Vec::new();
    for (topic, partition, values) in parts {
        let data = PartitionProduceData::default().with_index(*partition).with_records(Some(batch(values)));
        match topics.iter_mut().find(|t| t.name.as_str() == *topic) {
            Some(t) => t.partition_data.push(data),
            None => topics.push(TopicProduceData::default().with_name(name(topic)).with_partition_data(vec![data])),
        }
    }
    ProduceRequest::default().with_acks(acks).with_timeout_ms(5000).with_topic_data(topics)
}

pub async fn produce(client: &mut TestClient, parts: &[(&str, i32, &[&str])]) -> Result<ProduceResponse> {
    client.call(ApiKey::Produce, PRODUCE_V, &produce_request(-1, parts)).await
}

/// `(topic, partition, error_code)` for every partition in a produce response.
pub fn produce_codes(resp: &ProduceResponse) -> Vec<(String, i32, i16)> {
    let mut codes: Vec<_> = resp
        .responses
        .iter()
        .flat_map(|t| t.partition_responses.iter().map(|p| (t.name.to_string(), p.index, p.error_code)))
        .collect();
    codes.sort();
    codes
}

pub async fn fetch_values(client: &mut TestClient, topic: &str, partition: i32) -> Result<Vec<String>> {
    let req = FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(1 << 20)
        .with_topics(vec![FetchTopic::default().with_topic(name(topic)).with_partitions(vec![
            FetchPartition::default().with_partition(partition).with_fetch_offset(0).with_partition_max_bytes(1 << 20),
        ])]);
    let resp: FetchResponse = client.call(ApiKey::Fetch, FETCH_V, &req).await?;
    let mut values = Vec::new();
    for t in &resp.responses {
        for p in &t.partitions {
            if let Some(raw) = &p.records {
                let mut buf = raw.clone();
                while !buf.is_empty() {
                    let Ok(set) = RecordBatchDecoder::decode(&mut buf) else { break };
                    for r in set.records {
                        values.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned());
                    }
                }
            }
        }
    }
    Ok(values)
}
