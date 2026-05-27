use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;

pub trait ListOffsets {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse;
}
