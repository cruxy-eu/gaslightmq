use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;

pub trait Produce {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse;
}
