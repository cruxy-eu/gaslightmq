use async_trait::async_trait;
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;

#[async_trait]
pub trait ApiVersions {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse;
}
