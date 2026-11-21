//! The client against a hand-written peer that speaks just enough Kafka.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use gmq_kafka_client::{ClientConfig, ClientError, KafkaClient};
use gmq_wire::{decode_frame, encode_frame, encode_response, peek_request};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsResponse, MetadataResponse, ResponseHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Answer ApiVersions and Metadata on one connection. Every response leaves
/// in two writes, split inside the frame, so the client has to put it back
/// together.
async fn serve(mut stream: TcpStream) {
    let mut unread = BytesMut::new();
    loop {
        let request = loop {
            if let Some(frame) = decode_frame(&mut unread).unwrap() {
                break frame;
            }
            if stream.read_buf(&mut unread).await.unwrap() == 0 {
                return;
            }
        };
        let peek = peek_request(&request).unwrap();
        let header = ResponseHeader::default().with_correlation_id(peek.correlation_id);
        let payload = if peek.api_key == ApiKey::ApiVersions as i16 {
            let body = ApiVersionsResponse::default().with_api_keys(vec![
                ApiVersion::default().with_api_key(ApiKey::Metadata as i16).with_min_version(1).with_max_version(12),
            ]);
            encode_response(&header, peek.api_key, peek.api_version, &body).unwrap()
        } else {
            encode_response(&header, peek.api_key, peek.api_version, &MetadataResponse::default()).unwrap()
        };
        let frame = encode_frame(&payload);
        let (head, tail) = frame.split_at(frame.len() / 2);
        stream.write_all(head).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        stream.write_all(tail).await.unwrap();
    }
}

async fn peer() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve(stream));
        }
    });
    addr
}

#[tokio::test]
async fn reads_responses_that_arrive_in_pieces() {
    let client = KafkaClient::from(peer().await);

    // ApiVersions, then Metadata, on the same connection.
    assert_eq!(client.topic_metadata(None).await.unwrap(), vec![]);
    assert_eq!(client.topic_metadata(None).await.unwrap(), vec![]);
}

#[tokio::test]
async fn a_peer_that_hangs_up_is_a_delivery_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let client = KafkaClient::new(ClientConfig::new(addr.to_string()).delivery_timeout(Duration::from_millis(200)));

    let err = client.topic_metadata(None).await.unwrap_err();

    assert!(matches!(err, ClientError::DeliveryTimeout(ref last) if matches!(**last, ClientError::Io(_))), "{err}");
}
