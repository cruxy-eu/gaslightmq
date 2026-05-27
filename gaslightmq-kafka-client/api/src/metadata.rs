use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;

pub trait Metadata {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse;
}
