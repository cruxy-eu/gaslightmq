use async_trait::async_trait;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::fetch_response::FetchResponse;

#[async_trait]
pub trait Fetch {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse;
}
