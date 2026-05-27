use std::future::Future;

use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::fetch_response::FetchResponse;

pub trait Fetch {
    fn fetch(&self, request: FetchRequest) -> impl Future<Output = FetchResponse> + Send;
}
