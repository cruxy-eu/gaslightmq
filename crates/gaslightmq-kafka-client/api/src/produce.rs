use std::future::Future;

use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;

pub trait Produce {
    fn produce(&self, request: ProduceRequest) -> impl Future<Output = ProduceResponse> + Send;
}
