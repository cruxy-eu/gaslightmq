use std::future::Future;

use gaslightmq_wire::messages::list_offsets_request::ListOffsetsRequest;
use gaslightmq_wire::messages::list_offsets_response::ListOffsetsResponse;

pub trait ListOffsets {
    fn list_offsets(&self, request: ListOffsetsRequest) -> impl Future<Output = ListOffsetsResponse> + Send;
}
