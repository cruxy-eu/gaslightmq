use std::future::Future;

use gaslightmq_wire::messages::ApiVersionsRequest;
use gaslightmq_wire::messages::ApiVersionsResponse;

use crate::ClientError;

/// One `ApiVersions` request, and its response.
///
/// The response comes back as the broker wrote it. An error code in it is a part of the response, and no `Err`:
/// a response may answer for several things, and each of them has an error code of its own.
pub trait ApiVersions {
    /// Send `request`, and return its response.
    ///
    /// # Errors
    ///
    /// When the request does not go out, or when no response comes back that the client can read.
    /// No error code of the broker is an error here.
    fn api_versions(
        &self,
        request: ApiVersionsRequest,
    ) -> impl Future<Output = Result<ApiVersionsResponse, ClientError>> + Send;
}
