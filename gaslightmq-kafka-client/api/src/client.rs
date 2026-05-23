use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{ApiVersions, Fetch, FindCoordinator, ListOffsets, Metadata, Produce};

pub struct RawKafkaClient {
    addr: SocketAddr,
    stream: Mutex<Option<TcpStream>>,
    correlation_id: AtomicI32,
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

#[async_trait]
impl Produce for RawKafkaClient {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse {
        self.request(ApiKey::Produce, 3, &request).await
    }
}

#[async_trait]
impl Fetch for RawKafkaClient {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse {
        self.request(ApiKey::Fetch, 4, &request).await
    }
}

#[async_trait]
impl Metadata for RawKafkaClient {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        self.request(ApiKey::Metadata, 1, &request).await
    }
}

#[async_trait]
impl ApiVersions for RawKafkaClient {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        self.request(ApiKey::ApiVersions, 0, &request).await
    }
}

#[async_trait]
impl ListOffsets for RawKafkaClient {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse {
        self.request(ApiKey::ListOffsets, 1, &request).await
    }
}

#[async_trait]
impl FindCoordinator for RawKafkaClient {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse {
        self.request(ApiKey::FindCoordinator, 0, &request).await
    }
}
