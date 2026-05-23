use bytes::BytesMut;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader};
use kafka_protocol::messages::metadata_response::MetadataResponse;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use gaslightmq::{BlackHole, BrokerBuilder, Buffer};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Send a Kafka request over TCP. Encodes `RequestHeader` + `body` with a
/// 4-byte big-endian length prefix.
async fn send_request(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    body: &(impl Encodable + ?Sized),
) {
    let header_version = api_key.request_header_version(api_version);

    let mut header = RequestHeader::default();
    header.request_api_key = api_key as i16;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;

    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version).unwrap();
    body.encode(&mut buf, api_version).unwrap();

    stream.write_u32(buf.len() as u32).await.unwrap();
    stream.write_all(&buf).await.unwrap();
}

/// Read a Kafka response from TCP. Reads the 4-byte length prefix, decodes the
/// `ResponseHeader`, then decodes the response body of type `T`.
async fn read_response<T: Decodable>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
) -> T {
    let len = stream.read_u32().await.unwrap() as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.unwrap();

    let mut buf = bytes::Bytes::from(frame);

    let resp_header_version = api_key.response_header_version(api_version);
    let _header = ResponseHeader::decode(&mut buf, resp_header_version).unwrap();

    T::decode(&mut buf, api_version).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn api_versions_handshake() {
    // Start broker with a single BlackHole topic.
    let broker = BrokerBuilder::new()
        .topic("test", BlackHole::new(1))
        .bind("127.0.0.1:0")
        .start()
        .await
        .unwrap();

    let mut stream = TcpStream::connect(broker.addr()).await.unwrap();

    // Send ApiVersions v0 request.
    let request = ApiVersionsRequest::default();
    send_request(&mut stream, ApiKey::ApiVersions, 0, 1, &request).await;

    // Read response.
    let response: ApiVersionsResponse =
        read_response(&mut stream, ApiKey::ApiVersions, 0).await;

    assert_eq!(response.error_code, 0, "error_code should be 0 (no error)");
    assert!(
        !response.api_keys.is_empty(),
        "api_keys list should not be empty"
    );

    // Verify some expected API keys are present.
    let api_key_ids: Vec<i16> = response.api_keys.iter().map(|k| k.api_key).collect();
    assert!(
        api_key_ids.contains(&(ApiKey::ApiVersions as i16)),
        "ApiVersions should be in supported APIs"
    );
    assert!(
        api_key_ids.contains(&(ApiKey::Produce as i16)),
        "Produce should be in supported APIs"
    );
    assert!(
        api_key_ids.contains(&(ApiKey::Fetch as i16)),
        "Fetch should be in supported APIs"
    );
    assert!(
        api_key_ids.contains(&(ApiKey::Metadata as i16)),
        "Metadata should be in supported APIs"
    );

    broker.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_topics() {
    // Start broker with two topics.
    let broker = BrokerBuilder::new()
        .topic("orders", Buffer::new(3))
        .topic("events", BlackHole::new(1))
        .bind("127.0.0.1:0")
        .start()
        .await
        .unwrap();

    let mut stream = TcpStream::connect(broker.addr()).await.unwrap();

    // Step 1: ApiVersions handshake (required before other requests).
    let api_versions_req = ApiVersionsRequest::default();
    send_request(&mut stream, ApiKey::ApiVersions, 0, 1, &api_versions_req).await;
    let _: ApiVersionsResponse = read_response(&mut stream, ApiKey::ApiVersions, 0).await;

    // Step 2: Metadata request (v1) — ask for all topics (topics = None).
    let metadata_req = MetadataRequest::default().with_topics(None);
    send_request(&mut stream, ApiKey::Metadata, 1, 2, &metadata_req).await;

    let response: MetadataResponse = read_response(&mut stream, ApiKey::Metadata, 1).await;

    // Verify 1 broker is reported.
    assert_eq!(response.brokers.len(), 1, "should have exactly 1 broker");
    assert_eq!(response.brokers[0].port, broker.addr().port() as i32);

    // Verify 2 topics are reported.
    assert_eq!(response.topics.len(), 2, "should have exactly 2 topics");

    // Find "orders" topic and verify partition count.
    let orders = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("orders"))
        .expect("orders topic should be present");
    assert_eq!(orders.error_code, 0);
    assert_eq!(orders.partitions.len(), 3, "orders should have 3 partitions");

    // Find "events" topic and verify partition count.
    let events = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("events"))
        .expect("events topic should be present");
    assert_eq!(events.error_code, 0);
    assert_eq!(events.partitions.len(), 1, "events should have 1 partition");

    broker.shutdown().await;
}
