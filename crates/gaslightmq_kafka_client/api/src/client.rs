//! A client that sends one request to one address, at a fixed version.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use futures::StreamExt;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, JoinGroupRequest, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse, SyncGroupRequest, TopicName,
};
use gaslightmq_wire::protocol::{Request as KafkaRequest, StrBytes};
use gaslightmq_wire::{
    FrameDecoder, Offset, Partition, Record, RecordBatch, Request, Response, ResponseError, WireError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::application::{
    HeartbeatResult, JoinGroupResult, ProduceResult, Producer, ProtocolConsumer, RecordStream, SyncGroupResult,
};
use crate::{ApiVersions, CreateTopics, DeleteTopics, Fetch, FindCoordinator, ListOffsets, Metadata, Produce};

/// Sends each request as it is given, to one address, at a fixed version.
///
/// It follows no leaders, negotiates no versions and retries nothing, so a
/// test sees exactly what one request gets back.
///
/// # Panics
///
/// Every method that sends a request panics when the request does not
/// encode, when the connection fails, or when the response does not decode.
/// The traits it implements return a response and no error, so a test that
/// gets a response knows the exchange took place.
pub struct RawKafkaClient {
    addr: SocketAddr,
    socket: Mutex<Option<Socket>>,
    correlation_id: AtomicI32,
}

/// A connected stream, and the bytes read from it that no frame has taken yet.
struct Socket {
    stream: TcpStream,
    frames: FrameDecoder,
}

type Failure = Box<dyn std::error::Error + Send + Sync>;

impl Socket {
    async fn connect(addr: SocketAddr) -> Result<Self, Failure> {
        Ok(Self {
            stream: TcpStream::connect(addr).await?,
            frames: FrameDecoder::new(),
        })
    }

    /// Send `request` and read its response.
    async fn exchange<R: KafkaRequest>(&mut self, request: &Request) -> Result<R::Response, Failure> {
        self.stream.write_all(&request.frame().to_bytes()).await?;
        let frame = loop {
            if let Some(frame) = self.frames.next_frame()? {
                break frame;
            }
            if self.stream.read_buf(self.frames.buffer_mut()).await? == 0 {
                return Err("connection closed by broker".into());
            }
        };
        Ok(Response::decode(request.api(), frame)?.body()?)
    }
}

impl From<SocketAddr> for RawKafkaClient {
    fn from(addr: SocketAddr) -> Self {
        Self::new(addr)
    }
}

impl RawKafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            socket: Mutex::new(None),
            correlation_id: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Send `body` at `version` and return its response.
    ///
    /// # Panics
    ///
    /// When `body` does not encode at `version`, when the connection fails,
    /// or when the response does not decode.
    #[expect(
        clippy::panic,
        reason = "the API traits return a response and no error, so a failed exchange panics, as the type documents"
    )]
    async fn request<R: KafkaRequest>(&self, version: i16, body: &R) -> R::Response {
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(version, correlation_id, None, body)
            .unwrap_or_else(|error| panic!("the request does not encode at version {version}: {error}"));
        let mut slot = self.socket.lock().await;
        let socket = match slot.as_mut() {
            Some(socket) => socket,
            None => slot.insert(
                Socket::connect(self.addr)
                    .await
                    .unwrap_or_else(|error| panic!("connecting to {} failed: {error}", self.addr)),
            ),
        };
        socket
            .exchange::<R>(&request)
            .await
            .unwrap_or_else(|error| panic!("the exchange with {} failed: {error}", self.addr))
    }
}

/// One Fetch for one partition on a connection of its own.
async fn fetch_once(addr: SocketAddr, request: &FetchRequest) -> Result<Vec<Record>, Failure> {
    let request = Request::new(4, 0, None, request)?;
    let response = Socket::connect(addr).await?.exchange::<FetchRequest>(&request).await?;
    Ok(records_of(&response)?)
}

fn fetch_request(topic: &str, partition: Partition, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(max_wait_ms)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name(topic))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(i32::from(partition))
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ])
}

/// Every record a Fetch response carries, over all its partitions.
fn records_of(response: &FetchResponse) -> Result<Vec<Record>, WireError> {
    let mut records = Vec::new();
    for partition in response.responses.iter().flat_map(|topic| &topic.partitions) {
        if let Some(field) = &partition.records {
            records.extend(
                RecordBatch::decode_all(field)?
                    .into_iter()
                    .flat_map(|batch| batch.records),
            );
        }
    }
    Ok(records)
}

fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

fn group_id(group: &str) -> GroupId {
    GroupId(StrBytes::from_string(group.to_string()))
}

impl Produce for RawKafkaClient {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse {
        self.request(3, &request).await
    }
}

impl Fetch for RawKafkaClient {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse {
        self.request(4, &request).await
    }
}

impl Metadata for RawKafkaClient {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        self.request(1, &request).await
    }
}

impl ApiVersions for RawKafkaClient {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        self.request(0, &request).await
    }
}

impl ListOffsets for RawKafkaClient {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse {
        self.request(1, &request).await
    }
}

impl FindCoordinator for RawKafkaClient {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse {
        self.request(0, &request).await
    }
}

impl CreateTopics for RawKafkaClient {
    async fn create_topics(&self, request: CreateTopicsRequest) -> CreateTopicsResponse {
        self.request(2, &request).await
    }
}

impl DeleteTopics for RawKafkaClient {
    async fn delete_topics(&self, request: DeleteTopicsRequest) -> DeleteTopicsResponse {
        self.request(1, &request).await
    }
}

impl Producer for RawKafkaClient {
    /// # Panics
    ///
    /// When `records` do not encode, when the exchange fails, or when the
    /// response holds no result for the partition.
    #[expect(
        clippy::panic,
        reason = "`Producer::send` returns a result and no error, so records that do not encode panic, as the type documents"
    )]
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> ProduceResult {
        let batch = RecordBatch::new(records)
            .encode()
            .unwrap_or_else(|error| panic!("the records do not encode: {error}"));
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name(topic))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(i32::from(partition))
                            .with_records(Some(batch)),
                    ]),
            ]);

        let response = self.request(3, &request).await;

        let partition_response = &response.responses[0].partition_responses[0];
        ProduceResult {
            error: ResponseError::try_from_code(partition_response.error_code),
            partition,
            offset: Offset::from(partition_response.base_offset),
        }
    }
}

impl ProtocolConsumer for RawKafkaClient {
    /// # Panics
    ///
    /// When the exchange fails, or when the records of the response do not
    /// decode.
    #[expect(
        clippy::panic,
        reason = "`ProtocolConsumer::fetch` returns records and no error, so records that do not decode panic, as the type documents"
    )]
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        let response = self
            .request(4, &fetch_request(topic, partition, i64::from(offset), 1000))
            .await;
        records_of(&response).unwrap_or_else(|error| panic!("the fetched records do not decode: {error}"))
    }

    /// Records from `offset` on, fetched one request at a time, each on a
    /// connection of its own. The stream ends at the first Fetch that brings
    /// no records or fails.
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        let addr = self.addr;
        let topic = topic.to_string();

        Box::pin(
            futures::stream::unfold(i64::from(offset), move |next_offset| {
                let request = fetch_request(&topic, partition, next_offset, 100);
                async move {
                    let records = fetch_once(addr, &request).await.ok()?;
                    if records.is_empty() {
                        return None;
                    }
                    let fetched = i64::try_from(records.len()).expect("a record count that fits an i64");
                    let next_offset = next_offset + fetched;
                    Some((records, next_offset))
                }
            })
            .flat_map(futures::stream::iter),
        )
    }

    async fn join_group(&self, group: &str) -> JoinGroupResult {
        let request = JoinGroupRequest::default()
            .with_group_id(group_id(group))
            .with_session_timeout_ms(30000)
            .with_protocol_type(StrBytes::from_static_str("consumer"));

        let response = self.request(0, &request).await;

        JoinGroupResult {
            error: ResponseError::try_from_code(response.error_code),
            member_id: response.member_id.as_str().to_string(),
            leader: response.leader.as_str().to_string(),
        }
    }

    async fn sync_group(&self, group: &str) -> SyncGroupResult {
        let response = self
            .request(0, &SyncGroupRequest::default().with_group_id(group_id(group)))
            .await;
        SyncGroupResult {
            error: ResponseError::try_from_code(response.error_code),
        }
    }

    async fn heartbeat(&self, group: &str) -> HeartbeatResult {
        let response = self
            .request(0, &HeartbeatRequest::default().with_group_id(group_id(group)))
            .await;
        HeartbeatResult {
            error: ResponseError::try_from_code(response.error_code),
        }
    }
}
