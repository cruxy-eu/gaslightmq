use async_trait::async_trait;
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::CreateTopicsResponse;

#[async_trait]
pub trait CreateTopics {
    async fn create_topics(&self, request: CreateTopicsRequest) -> CreateTopicsResponse;
}
