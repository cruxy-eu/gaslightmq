//! Peers that stand in for brokers.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaslightmq_wire::messages::api_versions_response::ApiVersion;
use gaslightmq_wire::messages::describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult};
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::join_group_response::JoinGroupResponseMember;
use gaslightmq_wire::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
use gaslightmq_wire::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    ApiVersionsResponse, BrokerId, DescribeConfigsRequest, DescribeConfigsResponse, FetchRequest, FetchResponse,
    FindCoordinatorResponse, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, ListOffsetsRequest,
    ListOffsetsResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, FrameDecoder, Offset, Record, RecordBatch, Request, Response, ResponseError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// The topic every fake cluster has, with one partition.
pub const TOPIC: &str = "orders";

/// One request that reached a fake broker.
#[derive(Debug, Clone)]
pub struct Arrival {
    /// Which connection it came on: 0 for the first one the broker accepted, 1 for the next.
    pub connection: usize,
    pub request: Request,
}

impl Arrival {
    pub fn api(&self) -> ApiKey {
        self.request.api().known().unwrap()
    }

    pub fn client_id(&self) -> &str {
        self.request.client_id().unwrap()
    }
}

/// Every request that reached a fake broker, in the order of arrival.
struct Arrivals {
    log: Mutex<Vec<Arrival>>,
    count: watch::Sender<usize>,
}

/// How a fake broker writes a response.
#[derive(Clone, Copy)]
enum Writing {
    Whole,
    /// In two writes, split inside the frame, so the client has to put the response back together.
    InPieces,
}

/// A listener that answers each connection as a broker does: one request at a time, in order.
///
/// It answers `ApiVersions` itself, with every API at every version.
/// The test decides the answer to every other request.
pub struct FakeBroker {
    address: SocketAddr,
    listener: Mutex<Option<TcpListener>>,
    arrivals: Arc<Arrivals>,
}

impl FakeBroker {
    /// Listen on a free port. The broker accepts connections once it [serves](Self::serve).
    pub async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self {
            address: listener.local_addr().unwrap(),
            listener: Mutex::new(Some(listener)),
            arrivals: Arc::new(Arrivals {
                log: Mutex::default(),
                count: watch::Sender::new(0),
            }),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Answer every request with what `answer` returns, whenever it returns.
    pub fn serve<F, Fut>(&self, answer: F)
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        self.serve_writing(Writing::Whole, answer);
    }

    /// As [`serve`](Self::serve), with every response in two writes.
    pub fn serve_in_pieces<F, Fut>(&self, answer: F)
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        self.serve_writing(Writing::InPieces, answer);
    }

    fn serve_writing<F, Fut>(&self, writing: Writing, answer: F)
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        let listener = self.listener.lock().unwrap().take().expect("a broker serves once");
        let arrivals = Arc::clone(&self.arrivals);
        let answer = Arc::new(answer);
        tokio::spawn(async move {
            let mut connection = 0;
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(converse(
                    stream,
                    connection,
                    Arc::clone(&arrivals),
                    Arc::clone(&answer),
                    writing,
                ));
                connection += 1;
            }
        });
    }

    /// Every request that arrived, in the order of arrival.
    pub fn arrivals(&self) -> Vec<Arrival> {
        self.arrivals.log.lock().unwrap().clone()
    }

    /// Every request of `api` that arrived, in the order of arrival.
    pub fn arrivals_of(&self, api: ApiKey) -> Vec<Arrival> {
        let mut arrivals = self.arrivals();
        arrivals.retain(|arrival| arrival.api() == api);
        arrivals
    }

    /// Wait until `count` requests of `api` have arrived.
    pub async fn receives(&self, count: usize, api: ApiKey) {
        self.arrivals
            .count
            .subscribe()
            .wait_for(|_| self.arrivals_of(api).len() >= count)
            .await
            .unwrap();
    }
}

/// Serve one connection until the client closes it.
async fn converse<F, Fut>(
    mut stream: TcpStream,
    connection: usize,
    arrivals: Arc<Arrivals>,
    answer: Arc<F>,
    writing: Writing,
) where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    let mut frames = FrameDecoder::new();
    loop {
        let frame = loop {
            if let Some(frame) = frames.next_frame().unwrap() {
                break frame;
            }
            if stream.read_buf(frames.buffer_mut()).await.unwrap_or(0) == 0 {
                return;
            }
        };
        let request = Request::decode(frame).unwrap();
        arrivals.log.lock().unwrap().push(Arrival {
            connection,
            request: request.clone(),
        });
        arrivals.count.send_modify(|count| *count += 1);
        let response = if request.api().known() == Some(ApiKey::ApiVersions) {
            every_api(&request)
        } else {
            answer(request).await
        };
        let bytes = response.frame().to_bytes();
        let written = match writing {
            Writing::Whole => stream.write_all(&bytes).await,
            Writing::InPieces => {
                let (head, tail) = bytes.split_at(bytes.len() / 2);
                let head_written = stream.write_all(head).await;
                let _ = stream.flush().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                head_written.and(stream.write_all(tail).await)
            }
        };
        if written.is_err() {
            return;
        }
    }
}

/// The answer of a broker that speaks every API at every version.
fn every_api(request: &Request) -> Response {
    let api_keys = ApiKey::iter()
        .map(|key| {
            let versions = key.valid_versions();
            ApiVersion::default()
                .with_api_key(key as i16)
                .with_min_version(versions.min)
                .with_max_version(versions.max)
        })
        .collect();
    request
        .response(&ApiVersionsResponse::default().with_api_keys(api_keys))
        .unwrap()
}

fn text(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

/// A healthy cluster of fake brokers.
///
/// It has the topic [`TOPIC`] with one partition, which keeps what is produced to it.
/// The first broker leads the partition and coordinates every group.
#[derive(Clone)]
pub struct Cluster {
    brokers: Vec<(i32, SocketAddr)>,
    log: Arc<Mutex<Vec<Record>>>,
    members: Arc<AtomicUsize>,
}

impl Cluster {
    /// A cluster of `brokers`, each with its ID.
    pub fn of(brokers: &[(i32, &FakeBroker)]) -> Self {
        Self {
            brokers: brokers.iter().map(|(id, broker)| (*id, broker.address())).collect(),
            log: Arc::default(),
            members: Arc::default(),
        }
    }

    /// A cluster of one broker, with ID 1, that serves every request the healthy way.
    pub async fn of_one() -> (Self, FakeBroker) {
        let broker = FakeBroker::bind().await;
        let cluster = Self::of(&[(1, &broker)]);
        let serving = cluster.clone();
        broker.serve(move |request| {
            let cluster = serving.clone();
            async move { cluster.answer(&request) }
        });
        (cluster, broker)
    }

    /// The records the partition holds.
    pub fn log(&self) -> Vec<Record> {
        self.log.lock().unwrap().clone()
    }

    fn first_broker(&self) -> (i32, SocketAddr) {
        self.brokers[0]
    }

    /// Answer `request` the healthy way.
    pub fn answer(&self, request: &Request) -> Response {
        match request.api().known().unwrap() {
            ApiKey::Metadata => self.metadata(request, self.first_broker().0),
            ApiKey::Produce => self.produce(request),
            ApiKey::Fetch => self.fetch(request),
            ApiKey::ListOffsets => self.list_offsets(request),
            ApiKey::FindCoordinator => self.find_coordinator(request),
            ApiKey::JoinGroup => self.join_group(request),
            ApiKey::SyncGroup => Self::sync_group(request),
            ApiKey::Heartbeat => request.response(&HeartbeatResponse::default()).unwrap(),
            ApiKey::DescribeConfigs => Self::describe_configs(request),
            other => panic!("the fake cluster has no answer to {other:?}"),
        }
    }

    /// The Metadata of the cluster, with `leader` as the leader of the partition.
    pub fn metadata(&self, request: &Request, leader: i32) -> Response {
        let asked: MetadataRequest = request.body().unwrap();
        let brokers = self
            .brokers
            .iter()
            .map(|(id, address)| {
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(*id))
                    .with_host(text(&address.ip().to_string()))
                    .with_port(i32::from(address.port()))
            })
            .collect();
        let names = asked.topics.map_or_else(
            || vec![TopicName(text(TOPIC))],
            |topics| topics.into_iter().filter_map(|topic| topic.name).collect(),
        );
        let topics = names
            .into_iter()
            .map(|name| {
                if name.as_str() == TOPIC {
                    MetadataResponseTopic::default()
                        .with_name(Some(name))
                        .with_partitions(vec![
                            MetadataResponsePartition::default()
                                .with_partition_index(0)
                                .with_leader_id(BrokerId(leader)),
                        ])
                } else {
                    MetadataResponseTopic::default()
                        .with_name(Some(name))
                        .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                }
            })
            .collect();
        request
            .response(&MetadataResponse::default().with_brokers(brokers).with_topics(topics))
            .unwrap()
    }

    fn produce(&self, request: &Request) -> Response {
        let produced: ProduceRequest = request.body().unwrap();
        let data = &produced.topic_data[0].partition_data[0];
        let mut log = self.log.lock().unwrap();
        let base_offset = i64::try_from(log.len()).unwrap();
        for batch in RecordBatch::decode_all(data.records.as_ref().unwrap()).unwrap() {
            for record in batch.records {
                let offset = Offset::from(i64::try_from(log.len()).unwrap());
                log.push(record.with_offset(offset));
            }
        }
        let written = PartitionProduceResponse::default()
            .with_index(data.index)
            .with_base_offset(base_offset);
        request
            .response(&ProduceResponse::default().with_responses(vec![
                TopicProduceResponse::default()
                    .with_name(produced.topic_data[0].name.clone())
                    .with_partition_responses(vec![written]),
            ]))
            .unwrap()
    }

    /// Every record from the offset the request asks for. An offset at the end of the log gets none.
    pub fn fetch(&self, request: &Request) -> Response {
        let asked: FetchRequest = request.body().unwrap();
        let partition = &asked.topics[0].partitions[0];
        let log = self.log.lock().unwrap();
        let from = usize::try_from(partition.fetch_offset).unwrap().min(log.len());
        let records = RecordBatch::new(log[from..].to_vec()).encode().unwrap();
        let fetched = PartitionData::default()
            .with_partition_index(partition.partition)
            .with_high_watermark(i64::try_from(log.len()).unwrap())
            .with_records((!records.is_empty()).then_some(records));
        request
            .response(&FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic(asked.topics[0].topic.clone())
                    .with_partitions(vec![fetched]),
            ]))
            .unwrap()
    }

    fn list_offsets(&self, request: &Request) -> Response {
        let asked: ListOffsetsRequest = request.body().unwrap();
        let partition = &asked.topics[0].partitions[0];
        let offset = match partition.timestamp {
            -2 => 0,
            -1 => i64::try_from(self.log.lock().unwrap().len()).unwrap(),
            other => panic!("the fake cluster knows no offset for timestamp {other}"),
        };
        let listed = ListOffsetsPartitionResponse::default()
            .with_partition_index(partition.partition_index)
            .with_offset(offset);
        request
            .response(&ListOffsetsResponse::default().with_topics(vec![
                ListOffsetsTopicResponse::default()
                    .with_name(asked.topics[0].name.clone())
                    .with_partitions(vec![listed]),
            ]))
            .unwrap()
    }

    fn find_coordinator(&self, request: &Request) -> Response {
        let (id, address) = self.first_broker();
        request
            .response(
                &FindCoordinatorResponse::default()
                    .with_node_id(BrokerId(id))
                    .with_host(text(&address.ip().to_string()))
                    .with_port(i32::from(address.port())),
            )
            .unwrap()
    }

    /// A member without an ID gets one, and has to join again under it, as with a real coordinator.
    /// A member with an ID is let into generation 1, as the leader of a group of one.
    fn join_group(&self, request: &Request) -> Response {
        let joining: JoinGroupRequest = request.body().unwrap();
        let response = if joining.member_id.is_empty() {
            let number = self.members.fetch_add(1, Ordering::Relaxed) + 1;
            JoinGroupResponse::default()
                .with_error_code(ResponseError::MemberIdRequired.code())
                .with_member_id(text(&format!("member-{number}")))
        } else {
            JoinGroupResponse::default()
                .with_generation_id(1)
                .with_protocol_name(Some(joining.protocols[0].name.clone()))
                .with_leader(joining.member_id.clone())
                .with_member_id(joining.member_id.clone())
                .with_members(vec![
                    JoinGroupResponseMember::default()
                        .with_member_id(joining.member_id.clone())
                        .with_metadata(joining.protocols[0].metadata.clone()),
                ])
        };
        request.response(&response).unwrap()
    }

    /// The member gets the assignment it handed out for itself.
    fn sync_group(request: &Request) -> Response {
        let syncing: SyncGroupRequest = request.body().unwrap();
        let assignment = syncing
            .assignments
            .iter()
            .find(|assignment| assignment.member_id == syncing.member_id)
            .map_or_default(|assignment| assignment.assignment.clone());
        request
            .response(&SyncGroupResponse::default().with_assignment(assignment))
            .unwrap()
    }

    fn describe_configs(request: &Request) -> Response {
        let asked: DescribeConfigsRequest = request.body().unwrap();
        let resource = &asked.resources[0];
        let described = DescribeConfigsResult::default()
            .with_resource_type(resource.resource_type)
            .with_resource_name(resource.resource_name.clone())
            .with_configs(vec![
                DescribeConfigsResourceResult::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_value(Some(StrBytes::from_static_str("604800000"))),
            ]);
        request
            .response(&DescribeConfigsResponse::default().with_results(vec![described]))
            .unwrap()
    }
}
