use std::future::Future;

use gaslightmq_wire::messages::delete_topics_request::DeleteTopicsRequest;
use gaslightmq_wire::messages::delete_topics_response::DeleteTopicsResponse;

use crate::ClientError;

/// One `DeleteTopics` request, and its response.
///
/// The response comes back as the broker wrote it. An error code in it is a part of the response, and no `Err`:
/// a response may answer for several things, and each of them has an error code of its own.
pub trait DeleteTopics {
    /// Send `request`, and return its response.
    ///
    /// # Errors
    ///
    /// When the request does not go out, or when no response comes back that the client can read.
    /// No error code of the broker is an error here.
    fn delete_topics(
        &self,
        request: DeleteTopicsRequest,
    ) -> impl Future<Output = Result<DeleteTopicsResponse, ClientError>> + Send;
}
