use std::future::Future;

use gaslightmq_wire::messages::list_offsets_request::ListOffsetsRequest;
use gaslightmq_wire::messages::list_offsets_response::ListOffsetsResponse;

use crate::ClientError;

/// One `ListOffsets` request, and its response.
///
/// The response comes back as the broker wrote it. An error code in it is a part of the response, and no `Err`:
/// a response may answer for several things, and each of them has an error code of its own.
pub trait ListOffsets {
    /// Send `request`, and return its response.
    ///
    /// # Errors
    ///
    /// When the request does not go out, or when no response comes back that the client can read.
    /// No error code of the broker is an error here.
    fn list_offsets(
        &self,
        request: ListOffsetsRequest,
    ) -> impl Future<Output = Result<ListOffsetsResponse, ClientError>> + Send;
}
