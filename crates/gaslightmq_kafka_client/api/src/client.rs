//! A client that sends one request to one address, at a fixed version.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use futures::StreamExt;
use gaslightmq_kernel::MemberId;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{
    ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, JoinGroupRequest, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse, SyncGroupRequest, TopicName,
};
use gaslightmq_wire::protocol::{Request as KafkaRequest, StrBytes};
use gaslightmq_wire::{Frame, FrameDecoder, Offset, Partition, Record, RecordBatch, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::application::{JoinGroupResult, ProduceResult, Producer, ProtocolConsumer, RecordStream};
use crate::{
    ApiVersions, ClientError, CreateTopics, DeleteTopics, Fetch, FindCoordinator, ListOffsets, Metadata, Produce,
};

/// Sends each request as it is given, to one address, at a fixed version.
///
/// It follows no leaders, negotiates no versions and retries nothing, so a
/// test sees exactly what one request gets back.
/// It waits for a response as long as the response takes: a test that expects none bounds the wait itself.
///
/// # Error codes
///
/// The traits named after an API return the response as the broker wrote it, with every error code in it.
/// [`Producer`] and [`ProtocolConsumer`] ask for one partition or one group,
/// so an error code of the broker is the failure of the call: [`ClientError::Refused`].
///
/// # Failures
///
/// No method panics. A call fails with the [`ClientError`] that says what happened:
///
/// | Failure | When |
/// |---|---|
/// | [`ClientError::UnwritableRequest`] | The request does not encode at its version. |
/// | [`ClientError::Unreachable`] | No connection to the address opens. |
/// | [`ClientError::ConnectionLost`] | The connection ends before the response arrived. The next request opens a new one. |
/// | [`ClientError::UnreadableAnswer`] | What arrives is no response, answers another request, or does not hold what was asked for. |
///
/// # A request without its response
///
/// [`send_request`](Self::send_request) sends a request and returns at once,
/// and [`receive_response`](Self::receive_response) waits for the response later.
/// A client can therefore send a second request before the first one is answered:
///
/// ```no_run
/// # use gaslightmq_kafka_client_api::{ClientError, RawKafkaClient};
/// # use gaslightmq_wire::messages::{ApiVersionsRequest, MetadataRequest};
/// # async fn two_requests_on_their_way(client: RawKafkaClient) -> Result<(), ClientError> {
/// let versions = client.send_request(0, &ApiVersionsRequest::default()).await?;
/// let metadata = client.send_request(1, &MetadataRequest::default()).await?;
///
/// let versions = client.receive_response(&versions).await?;
/// let metadata = client.receive_response(&metadata).await?;
/// # Ok(())
/// # }
/// ```
///
/// A broker answers the requests of a connection in the order they arrived in.
/// The client reads one response at a time, so the responses are received in the order of their requests,
/// and before any other method of the client, which waits for the response to its own request.
pub struct RawKafkaClient {
    addr: SocketAddr,
    connection: Mutex<Connection>,
    correlation_id: AtomicI32,
}

/// A request that a [`RawKafkaClient`] sent, and whose response is still to come.
///
/// [`RawKafkaClient::receive_response`] reads the response with it.
#[derive(Debug)]
pub struct SentRequest<R> {
    request: Request,
    body: PhantomData<fn() -> R>,
}

/// A connected stream, and the bytes read from it that no frame has taken yet.
struct Socket {
    stream: TcpStream,
    frames: FrameDecoder,
}

impl Socket {
    async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|error| ClientError::unreachable(format_args!("{addr}: {error}")))?;
        Ok(Self {
            stream,
            frames: FrameDecoder::new(),
        })
    }

    async fn send(&mut self, request: &Request) -> Result<(), ClientError> {
        self.stream
            .write_all(&request.frame().to_bytes())
            .await
            .map_err(ClientError::connection_lost)
    }

    /// The next frame that arrives.
    async fn receive(&mut self) -> Result<Frame, ClientError> {
        loop {
            if let Some(frame) = self.frames.next_frame().map_err(ClientError::unreadable_answer)? {
                return Ok(frame);
            }
            let read = self
                .stream
                .read_buf(self.frames.buffer_mut())
                .await
                .map_err(ClientError::connection_lost)?;
            if read == 0 {
                return Err(ClientError::connection_lost("closed by the broker"));
            }
        }
    }
}

/// The connection to one address.
///
/// It opens when the first request needs it.
/// A connection that fails closes, and the next request opens a new one.
struct Connection {
    addr: SocketAddr,
    socket: Option<Socket>,
}

impl Connection {
    /// A connection to `addr`, which is not open yet.
    fn to(addr: SocketAddr) -> Self {
        Self { addr, socket: None }
    }

    /// Send `request`, on a new socket when none is open.
    async fn send(&mut self, request: &Request) -> Result<(), ClientError> {
        let socket = match self.socket.as_mut() {
            Some(socket) => socket,
            None => self.socket.insert(Socket::connect(self.addr).await?),
        };
        let sent = socket.send(request).await;
        if sent.is_err() {
            self.socket = None;
        }
        sent
    }

    /// The next response that arrives, which has to answer `request`.
    async fn receive(&mut self, request: &Request) -> Result<Response, ClientError> {
        let Some(socket) = self.socket.as_mut() else {
            return Err(ClientError::connection_lost(
                "closed before the response to the request arrived",
            ));
        };
        let frame = match socket.receive().await {
            Ok(frame) => frame,
            Err(error) => {
                self.socket = None;
                return Err(error);
            }
        };
        let response = Response::decode(request.api(), frame).map_err(ClientError::unreadable_answer)?;
        if response.correlation_id() != request.correlation_id() {
            return Err(ClientError::unreadable_answer(format_args!(
                "the response to request {} arrived where the response to request {} is next",
                response.correlation_id(),
                request.correlation_id()
            )));
        }
        Ok(response)
    }

    /// Send `request`, and wait for its response.
    async fn exchange(&mut self, request: &Request) -> Result<Response, ClientError> {
        self.send(request).await?;
        self.receive(request).await
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
            connection: Mutex::new(Connection::to(addr)),
            correlation_id: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Send `body` at `version`, and return without waiting for its response.
    ///
    /// # Errors
    ///
    /// [`ClientError::UnwritableRequest`] when `body` does not encode at `version`,
    /// [`ClientError::Unreachable`] when no connection opens,
    /// and [`ClientError::ConnectionLost`] when the connection ends while the request goes out.
    pub async fn send_request<R: KafkaRequest>(&self, version: i16, body: &R) -> Result<SentRequest<R>, ClientError> {
        let request = self.next_request(version, body)?;
        self.connection.lock().await.send(&request).await?;
        Ok(SentRequest {
            request,
            body: PhantomData,
        })
    }

    /// Wait for the next response on the connection, which has to answer `sent`.
    ///
    /// Giving up on the wait loses nothing: the bytes that arrived stay with the client,
    /// and a later call for the same request reads its response.
    ///
    /// # Errors
    ///
    /// [`ClientError::ConnectionLost`] when the connection ends before a response arrived,
    /// and [`ClientError::UnreadableAnswer`] when what arrives is no response to `sent`.
    pub async fn receive_response<R: KafkaRequest>(&self, sent: &SentRequest<R>) -> Result<R::Response, ClientError> {
        let response = self.connection.lock().await.receive(&sent.request).await?;
        response.body().map_err(ClientError::unreadable_answer)
    }

    /// Send `body` at `version`, and wait for its response. No other request goes out in between.
    async fn request<R: KafkaRequest>(&self, version: i16, body: &R) -> Result<R::Response, ClientError> {
        let request = self.next_request(version, body)?;
        let response = self.connection.lock().await.exchange(&request).await?;
        response.body().map_err(ClientError::unreadable_answer)
    }

    /// `body` at `version`, as the next request of the client.
    fn next_request<R: KafkaRequest>(&self, version: i16, body: &R) -> Result<Request, ClientError> {
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::Relaxed);
        Request::new(version, correlation_id, None, body).map_err(ClientError::unwritable_request)
    }
}

/// One Fetch for one partition on a connection of its own.
async fn fetch_once(addr: SocketAddr, request: &FetchRequest) -> Result<Vec<Record>, ClientError> {
    let request = Request::new(4, 0, None, request).map_err(ClientError::unwritable_request)?;
    let response = Connection::to(addr).exchange(&request).await?;
    records_of(&response.body().map_err(ClientError::unreadable_answer)?)
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
///
/// # Errors
///
/// [`ClientError::Refused`] when a partition has an error code,
/// and [`ClientError::UnreadableAnswer`] when its records do not decode.
fn records_of(response: &FetchResponse) -> Result<Vec<Record>, ClientError> {
    let mut records = Vec::new();
    for partition in response.responses.iter().flat_map(|topic| &topic.partitions) {
        if let Some(refused) = ClientError::refusal(partition.error_code) {
            return Err(refused);
        }
        if let Some(field) = &partition.records {
            records.extend(
                RecordBatch::decode_all(field)
                    .map_err(ClientError::unreadable_answer)?
                    .into_iter()
                    .flat_map(|batch| batch.records),
            );
        }
    }
    Ok(records)
}

/// `Ok` when `error_code` is no error, and the refusal it stands for otherwise.
fn accepted(error_code: i16) -> Result<(), ClientError> {
    ClientError::refusal(error_code).map_or(Ok(()), Err)
}

fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

fn group_id(group: &str) -> GroupId {
    GroupId(StrBytes::from_string(group.to_string()))
}

impl Produce for RawKafkaClient {
    async fn produce(&self, request: ProduceRequest) -> Result<ProduceResponse, ClientError> {
        self.request(3, &request).await
    }
}

impl Fetch for RawKafkaClient {
    async fn fetch(&self, request: FetchRequest) -> Result<FetchResponse, ClientError> {
        self.request(4, &request).await
    }
}

impl Metadata for RawKafkaClient {
    async fn metadata(&self, request: MetadataRequest) -> Result<MetadataResponse, ClientError> {
        self.request(1, &request).await
    }
}

impl ApiVersions for RawKafkaClient {
    async fn api_versions(&self, request: ApiVersionsRequest) -> Result<ApiVersionsResponse, ClientError> {
        self.request(0, &request).await
    }
}

impl ListOffsets for RawKafkaClient {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> Result<ListOffsetsResponse, ClientError> {
        self.request(1, &request).await
    }
}

impl FindCoordinator for RawKafkaClient {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> Result<FindCoordinatorResponse, ClientError> {
        self.request(0, &request).await
    }
}

impl CreateTopics for RawKafkaClient {
    async fn create_topics(&self, request: CreateTopicsRequest) -> Result<CreateTopicsResponse, ClientError> {
        self.request(2, &request).await
    }
}

impl DeleteTopics for RawKafkaClient {
    async fn delete_topics(&self, request: DeleteTopicsRequest) -> Result<DeleteTopicsResponse, ClientError> {
        self.request(1, &request).await
    }
}

impl Producer for RawKafkaClient {
    /// # Errors
    ///
    /// Also [`ClientError::UnwritableRequest`] when `records` do not encode,
    /// and [`ClientError::UnreadableAnswer`] when the response holds no result for the partition.
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> Result<ProduceResult, ClientError> {
        let batch = RecordBatch::new(records)
            .encode()
            .map_err(ClientError::unwritable_request)?;
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

        let response = self.request(3, &request).await?;

        let written = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .ok_or_else(|| ClientError::unreadable_answer(format_args!("no result for {topic}-{partition}")))?;
        accepted(written.error_code)?;
        Ok(ProduceResult {
            partition,
            offset: Offset::from(written.base_offset),
        })
    }
}

impl ProtocolConsumer for RawKafkaClient {
    /// # Errors
    ///
    /// Also [`ClientError::UnreadableAnswer`] when the records of the response do not decode.
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Result<Vec<Record>, ClientError> {
        let response = self
            .request(4, &fetch_request(topic, partition, i64::from(offset), 1000))
            .await?;
        records_of(&response)
    }

    /// Records from `offset` on, fetched one request at a time, each on a
    /// connection of its own. The stream ends after the first Fetch that brings
    /// no records, and after the first one that fails, which is its last item.
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        let addr = self.addr;
        let topic = topic.to_string();

        Box::pin(
            futures::stream::unfold(Some(i64::from(offset)), move |next_offset| {
                let request = next_offset.map(|offset| (offset, fetch_request(&topic, partition, offset, 100)));
                async move {
                    let (offset, request) = request?;
                    match fetch_once(addr, &request).await {
                        Ok(records) if records.is_empty() => None,
                        Ok(records) => {
                            let fetched = i64::try_from(records.len()).unwrap_or(i64::MAX);
                            let records: Vec<_> = records.into_iter().map(Ok).collect();
                            Some((records, Some(offset.saturating_add(fetched))))
                        }
                        Err(error) => Some((vec![Err(error)], None)),
                    }
                }
            })
            .flat_map(futures::stream::iter),
        )
    }

    async fn join_group(&self, group: &str) -> Result<JoinGroupResult, ClientError> {
        let request = JoinGroupRequest::default()
            .with_group_id(group_id(group))
            .with_session_timeout_ms(30000)
            .with_protocol_type(StrBytes::from_static_str("consumer"));

        let response = self.request(0, &request).await?;

        accepted(response.error_code)?;
        Ok(JoinGroupResult {
            member_id: MemberId::new(response.member_id.as_str()).map_err(ClientError::unreadable_answer)?,
            leader: MemberId::new(response.leader.as_str()).map_err(ClientError::unreadable_answer)?,
        })
    }

    async fn sync_group(&self, group: &str) -> Result<(), ClientError> {
        let response = self
            .request(0, &SyncGroupRequest::default().with_group_id(group_id(group)))
            .await?;
        accepted(response.error_code)
    }

    async fn heartbeat(&self, group: &str) -> Result<(), ClientError> {
        let response = self
            .request(0, &HeartbeatRequest::default().with_group_id(group_id(group)))
            .await?;
        accepted(response.error_code)
    }
}
