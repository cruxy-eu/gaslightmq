use std::future::Future;

use gaslightmq_wire::messages::produce_request::ProduceRequest;
use gaslightmq_wire::messages::produce_response::ProduceResponse;

pub trait Produce {
    fn produce(&self, request: ProduceRequest) -> impl Future<Output = ProduceResponse> + Send;
}
