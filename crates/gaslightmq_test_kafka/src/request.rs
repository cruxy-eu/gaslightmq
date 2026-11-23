//! One request to one broker, at one version.

use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Api, FrameDecoder, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::KafkaClusterError;

/// The `client.id` of every request this crate sends, so a broker's log tells them from those of the client under test.
const CLIENT_ID: &str = "gaslightmq-test-kafka";

/// Send `body` to `address` at `version`, on a connection of its own, and return the response.
///
/// Nothing is negotiated, nothing is retried and no leader is followed:
/// what sets a cluster up and looks into it shares no decision with the client that a test is about.
pub(crate) async fn send<R: KafkaRequest>(
    address: SocketAddr,
    version: i16,
    body: &R,
    within: Duration,
) -> Result<R::Response, KafkaClusterError> {
    let api = Api::of::<R>(version);
    let unanswered = |reason: String| KafkaClusterError::Unanswered {
        request: format!("{api} to {address}"),
        reason,
    };
    let request = Request::new(version, 0, Some(CLIENT_ID), body).map_err(|error| unanswered(error.to_string()))?;
    match tokio::time::timeout(within, exchange::<R>(address, &request)).await {
        Ok(answered) => answered.map_err(unanswered),
        Err(_) => Err(unanswered(format!("no answer within {} ms", within.as_millis()))),
    }
}

/// The response to `request`, or the reason there is none.
async fn exchange<R: KafkaRequest>(address: SocketAddr, request: &Request) -> Result<R::Response, String> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| format!("no connection: {error}"))?;
    let broken = |error| format!("the connection broke: {error}");
    stream.write_all(&request.frame().to_bytes()).await.map_err(broken)?;
    let mut frames = FrameDecoder::new();
    let frame = loop {
        if let Some(frame) = frames.next_frame().map_err(|error| error.to_string())? {
            break frame;
        }
        if stream.read_buf(frames.buffer_mut()).await.map_err(broken)? == 0 {
            // A broker does this for a version it does not speak.
            return Err("the broker closed the connection without an answer".to_owned());
        }
    };
    Response::decode(request.api(), frame)
        .and_then(|response| response.body())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::MetadataRequest;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn a_peer_that_hangs_up_is_a_broker_that_closed_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                // Take the request, so the hang-up is a clean close and not a reset.
                let _ = stream.read(&mut [0; 1024]).await;
            }
        });

        let error = send(address, 9, &MetadataRequest::default(), Duration::from_secs(5))
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaClusterError::Unanswered { .. }), "{error}");
        assert_eq!(
            error.to_string(),
            format!("Metadata v9 to {address}: the broker closed the connection without an answer")
        );
    }

    #[tokio::test]
    async fn a_peer_that_stays_silent_runs_into_the_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let error = send(address, 9, &MetadataRequest::default(), Duration::from_millis(50))
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("Metadata v9 to {address}: no answer within 50 ms")
        );
    }

    #[tokio::test]
    async fn a_request_names_the_version_it_does_not_encode_at() {
        let address = "127.0.0.1:1".parse().unwrap();

        let error = send(address, 99, &MetadataRequest::default(), Duration::from_secs(5))
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaClusterError::Unanswered { .. }), "{error}");
        assert!(
            error
                .to_string()
                .starts_with("Metadata v99 to 127.0.0.1:1: cannot encode"),
            "{error}"
        );
    }
}
