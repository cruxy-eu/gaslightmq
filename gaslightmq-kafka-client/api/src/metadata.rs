use async_trait::async_trait;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;

#[async_trait]
pub trait Metadata {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse;
}
