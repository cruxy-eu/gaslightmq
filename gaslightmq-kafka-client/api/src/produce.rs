use async_trait::async_trait;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;

#[async_trait]
pub trait Produce {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse;
}
