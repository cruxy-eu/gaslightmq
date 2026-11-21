use std::future::Future;

use gaslightmq_wire::messages::FetchRequest;
use gaslightmq_wire::messages::fetch_response::FetchResponse;

pub trait Fetch {
    fn fetch(&self, request: FetchRequest) -> impl Future<Output = FetchResponse> + Send;
}
