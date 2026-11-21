use std::future::Future;

use gmq_wire::messages::ApiVersionsRequest;
use gmq_wire::messages::ApiVersionsResponse;

pub trait ApiVersions {
    fn api_versions(&self, request: ApiVersionsRequest) -> impl Future<Output = ApiVersionsResponse> + Send;
}
