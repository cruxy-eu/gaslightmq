use std::future::Future;

use gaslightmq_wire::messages::metadata_request::MetadataRequest;
use gaslightmq_wire::messages::metadata_response::MetadataResponse;

use crate::ClientError;

/// One `Metadata` request, and its response.
///
/// The response comes back as the broker wrote it. An error code in it is a part of the response, and no `Err`:
/// a response may answer for several things, and each of them has an error code of its own.
pub trait Metadata {
    /// Send `request`, and return its response.
    ///
    /// # Errors
    ///
    /// When the request does not go out, or when no response comes back that the client can read.
    /// No error code of the broker is an error here.
    fn metadata(&self, request: MetadataRequest) -> impl Future<Output = Result<MetadataResponse, ClientError>> + Send;
}
