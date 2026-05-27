use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::fetch_response::FetchResponse;

pub trait Fetch {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse;
}
