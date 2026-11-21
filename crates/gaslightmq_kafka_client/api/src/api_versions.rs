use std::future::Future;

use gaslightmq_wire::messages::ApiVersionsRequest;
use gaslightmq_wire::messages::ApiVersionsResponse;

pub trait ApiVersions {
    fn api_versions(&self, request: ApiVersionsRequest) -> impl Future<Output = ApiVersionsResponse> + Send;
}
