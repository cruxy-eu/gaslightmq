//! A Kafka client gets its answer.

use gaslightmq_kafka_client_api::{ApiVersions, RawKafkaClient};
use gaslightmq_listener::prelude::*;
use gaslightmq_wire::messages::api_versions_response::ApiVersion;
use gaslightmq_wire::messages::{ApiVersionsRequest, ApiVersionsResponse};
use gaslightmq_wire::{ApiKey, Request};

use crate::support::{listen, soon};

/// A broker that knows one API: it answers `ApiVersions`, and closes the connection on anything else.
struct OneApiBroker;

impl OneApiBroker {
    /// The only API the broker offers, with its versions.
    fn offered() -> ApiVersion {
        ApiVersion::default()
            .with_api_key(ApiKey::ApiVersions as i16)
            .with_min_version(0)
            .with_max_version(3)
    }
}

impl ConnectionHandler for OneApiBroker {
    async fn handle(&self, mut transport: Transport) {
        while let Ok(frame) = transport.receive().await {
            let Ok(request) = Request::decode(frame) else {
                return;
            };
            if request.api().known() != Some(ApiKey::ApiVersions) {
                return;
            }
            let body = ApiVersionsResponse::default().with_api_keys(vec![Self::offered()]);
            let Ok(response) = request.response(&body) else {
                return;
            };
            if transport.send(&response.frame().to_bytes()).await.is_err() {
                return;
            }
        }
    }
}

#[tokio::test]
async fn a_kafka_client_gets_its_answer() {
    let listener = listen(OneApiBroker).await;
    let client = RawKafkaClient::new(listener.address());

    // Two requests on one connection: the client keeps its socket open.
    let first = soon(client.api_versions(ApiVersionsRequest::default())).await.unwrap();
    let second = soon(client.api_versions(ApiVersionsRequest::default())).await.unwrap();

    assert_eq!(first.api_keys, vec![OneApiBroker::offered()]);
    assert_eq!(second, first);
    soon(listener.close()).await;
}
