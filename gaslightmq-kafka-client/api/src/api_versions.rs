use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;

pub trait ApiVersions {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse;
}
