use std::future::Future;

use gmq_wire::messages::FetchRequest;
use gmq_wire::messages::fetch_response::FetchResponse;

pub trait Fetch {
    fn fetch(&self, request: FetchRequest) -> impl Future<Output = FetchResponse> + Send;
}
