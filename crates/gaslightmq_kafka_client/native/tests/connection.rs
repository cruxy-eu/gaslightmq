//! The client against a hand-written peer that speaks just enough Kafka.

use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_kafka_client_native::{ClientConfig, ClientError, KafkaClient};
use gaslightmq_wire::messages::api_versions_response::ApiVersion;
use gaslightmq_wire::messages::{ApiVersionsResponse, MetadataResponse};
use gaslightmq_wire::{ApiKey, FrameDecoder, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Answer ApiVersions and Metadata on one connection. Every response leaves
/// in two writes, split inside the frame, so the client has to put it back
/// together.
async fn serve(mut stream: TcpStream) {
    let mut frames = FrameDecoder::new();
    loop {
        let frame = loop {
            if let Some(frame) = frames.next_frame().unwrap() {
                break frame;
            }
            if stream.read_buf(frames.buffer_mut()).await.unwrap() == 0 {
                return;
            }
        };
        let request = Request::decode(frame).unwrap();
        let response = if request.api().known() == Some(ApiKey::ApiVersions) {
            let metadata = ApiVersion::default().with_api_key(ApiKey::Metadata as i16).with_min_version(1).with_max_version(12);
            request.response(&ApiVersionsResponse::default().with_api_keys(vec![metadata])).unwrap()
        } else {
            request.response(&MetadataResponse::default()).unwrap()
        };
        let bytes = response.frame().to_bytes();
        let (head, tail) = bytes.split_at(bytes.len() / 2);
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
