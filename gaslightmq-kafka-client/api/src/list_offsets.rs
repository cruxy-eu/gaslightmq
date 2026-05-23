use async_trait::async_trait;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;

#[async_trait]
pub trait ListOffsets {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse;
}
