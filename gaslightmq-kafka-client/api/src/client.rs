use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::*;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType, NO_PARTITION_LEADER_EPOCH,
    NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use futures::StreamExt;
use gaslightmq_kernel::{Offset, Partition};
use crate::traits::{
    HeartbeatResult, JoinGroupResult, Producer, ProtocolConsumer, ProduceResult,
    RecordStream, SyncGroupResult,
};
use gaslightmq_kernel::{Header, Record};

use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::CreateTopicsResponse;
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::delete_topics_response::DeleteTopicsResponse;
use crate::{ApiVersions, CreateTopics, DeleteTopics, Fetch, FindCoordinator, ListOffsets, Metadata, Produce};

pub struct RawKafkaClient {
    addr: SocketAddr,
    stream: Mutex<Option<TcpStream>>,
    correlation_id: AtomicI32,
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
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn request<Req: Encodable, Resp: Decodable>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Resp {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            *guard = Some(TcpStream::connect(self.addr).await.unwrap());
        }
        let stream = guard.as_mut().unwrap();
        let cid = self.correlation_id.fetch_add(1, Ordering::Relaxed);

        let header_version = api_key.request_header_version(api_version);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = cid;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        request.encode(&mut buf, api_version).unwrap();

        stream.write_u32(buf.len() as u32).await.unwrap();
        stream.write_all(&buf).await.unwrap();

        let len = stream.read_u32().await.unwrap() as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).await.unwrap();

        let mut resp_buf = Bytes::from(frame);
        let resp_header_version = api_key.response_header_version(api_version);
        let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_version).unwrap();
        Resp::decode(&mut resp_buf, api_version).unwrap()
    }
}


impl Produce for RawKafkaClient {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse {
        self.request(ApiKey::Produce, 3, &request).await
    }
}


impl Fetch for RawKafkaClient {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse {
        self.request(ApiKey::Fetch, 4, &request).await
    }
}


impl Metadata for RawKafkaClient {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        self.request(ApiKey::Metadata, 1, &request).await
    }
}


impl ApiVersions for RawKafkaClient {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        self.request(ApiKey::ApiVersions, 0, &request).await
    }
}


impl ListOffsets for RawKafkaClient {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse {
        self.request(ApiKey::ListOffsets, 1, &request).await
    }
}


impl FindCoordinator for RawKafkaClient {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse {
        self.request(ApiKey::FindCoordinator, 0, &request).await
    }
}


impl CreateTopics for RawKafkaClient {
    async fn create_topics(&self, request: CreateTopicsRequest) -> CreateTopicsResponse {
        self.request(ApiKey::CreateTopics, 2, &request).await
    }
}


impl DeleteTopics for RawKafkaClient {
    async fn delete_topics(&self, request: DeleteTopicsRequest) -> DeleteTopicsResponse {
        self.request(ApiKey::DeleteTopics, 1, &request).await
    }
}

fn testing_records_to_kafka(records: &[Record]) -> Vec<KafkaRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers = IndexMap::new();
            for h in &r.headers {
                headers.insert(StrBytes::from_string(h.key.clone()), h.value.clone());
            }
            KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: i as i32,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

fn kafka_records_to_testing(batch: &kafka_protocol::records::RecordSet) -> Vec<Record> {
    batch
        .records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| Header {
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

fn encode_records_batch(records: &[Record]) -> Bytes {
    let kafka_records = testing_records_to_kafka(records);
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &kafka_records, &options).unwrap();
    Bytes::from(buf)
}


impl Producer for RawKafkaClient {
    async fn send(
        &self,
        topic: &str,
        partition: Partition,
        records: &[Record],
    ) -> ProduceResult {
        let records_bytes = encode_records_batch(records);
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(i32::from(partition))
                    .with_records(Some(records_bytes))])]);

        let response: ProduceResponse = self.request(ApiKey::Produce, 3, &request).await;

        let partition_resp = &response.responses[0].partition_responses[0];
        let error = kafka_protocol::error::ResponseError::try_from_code(partition_resp.error_code);

        ProduceResult {
            error,
            partition,
            offset: Offset::from(partition_resp.base_offset),
        }
    }
}


impl ProtocolConsumer for RawKafkaClient {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        let request = FetchRequest::default()
            .with_max_wait_ms(1000)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(i32::from(partition))
                    .with_fetch_offset(i64::from(offset))
                    .with_partition_max_bytes(1024 * 1024)])]);

        let response: FetchResponse = self.request(ApiKey::Fetch, 4, &request).await;

        let mut records = Vec::new();
        for topic_resp in &response.responses {
            for partition_resp in &topic_resp.partitions {
                if let Some(ref raw) = partition_resp.records {
                    let mut buf = raw.clone();
                    if let Ok(batch) = RecordBatchDecoder::decode(&mut buf) {
                        records.extend(kafka_records_to_testing(&batch));
                    }
                }
            }
        }
        records
    }

    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        let addr = self.addr;
        let topic = topic.to_string();
        let partition_val = i32::from(partition);
        let start = i64::from(offset);

        Box::pin(
            futures::stream::unfold(start, move |current_offset| {
                let topic = topic.clone();
                async move {
                    let mut tcp = TcpStream::connect(addr).await.ok()?;
                    let header_version = ApiKey::Fetch.request_header_version(4);
                    let mut header = RequestHeader::default();
                    header.request_api_key = ApiKey::Fetch as i16;
                    header.request_api_version = 4;
                    header.correlation_id = 0;

                    let request = FetchRequest::default()
                        .with_max_wait_ms(100)
                        .with_min_bytes(1)
                        .with_max_bytes(1024 * 1024)
                        .with_topics(vec![FetchTopic::default()
                            .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                            .with_partitions(vec![FetchPartition::default()
                                .with_partition(partition_val)
                                .with_fetch_offset(current_offset)
                                .with_partition_max_bytes(1024 * 1024)])]);

                    let mut buf = BytesMut::new();
                    header.encode(&mut buf, header_version).unwrap();
                    request.encode(&mut buf, 4).unwrap();
                    tcp.write_u32(buf.len() as u32).await.ok()?;
                    tcp.write_all(&buf).await.ok()?;

                    let len = tcp.read_u32().await.ok()? as usize;
                    let mut frame = vec![0u8; len];
                    tcp.read_exact(&mut frame).await.ok()?;
                    let mut resp_buf = Bytes::from(frame);
                    let resp_header_version = ApiKey::Fetch.response_header_version(4);
                    let _ = ResponseHeader::decode(&mut resp_buf, resp_header_version).ok()?;
                    let response = FetchResponse::decode(&mut resp_buf, 4).ok()?;

                    let mut records = Vec::new();
                    for topic_resp in &response.responses {
                        for partition_resp in &topic_resp.partitions {
                            if let Some(ref raw) = partition_resp.records {
                                let mut rbuf = raw.clone();
                                if let Ok(batch) = RecordBatchDecoder::decode(&mut rbuf) {
                                    records.extend(kafka_records_to_testing(&batch));
                                }
                            }
                        }
                    }

                    if records.is_empty() {
                        None
                    } else {
                        let next = current_offset + records.len() as i64;
                        Some((records, next))
                    }
                }
            })
            .flat_map(futures::stream::iter),
        )
    }

    async fn join_group(&self, group_id: &str) -> JoinGroupResult {
        let request = JoinGroupRequest::default()
            .with_group_id(kafka_protocol::messages::GroupId(StrBytes::from_string(
                group_id.to_string(),
            )))
            .with_session_timeout_ms(30000)
            .with_protocol_type(StrBytes::from_static_str("consumer"));

        let response: JoinGroupResponse = self.request(ApiKey::JoinGroup, 0, &request).await;

        JoinGroupResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
            member_id: response.member_id.as_str().to_string(),
            leader: response.leader.as_str().to_string(),
        }
    }

    async fn sync_group(&self, group_id: &str) -> SyncGroupResult {
        let request = SyncGroupRequest::default().with_group_id(
            kafka_protocol::messages::GroupId(StrBytes::from_string(group_id.to_string())),
        );

        let response: SyncGroupResponse = self.request(ApiKey::SyncGroup, 0, &request).await;

        SyncGroupResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
        }
    }

    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult {
        let request = HeartbeatRequest::default().with_group_id(
            kafka_protocol::messages::GroupId(StrBytes::from_string(group_id.to_string())),
        );

        let response: HeartbeatResponse = self.request(ApiKey::Heartbeat, 0, &request).await;

        HeartbeatResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
        }
    }
}
