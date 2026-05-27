use std::future::Future;

use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;

pub trait ApiVersions {
    fn api_versions(&self, request: ApiVersionsRequest) -> impl Future<Output = ApiVersionsResponse> + Send;
}
