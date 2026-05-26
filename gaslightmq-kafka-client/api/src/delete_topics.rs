use async_trait::async_trait;
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::delete_topics_response::DeleteTopicsResponse;

#[async_trait]
pub trait DeleteTopics {
    async fn delete_topics(&self, request: DeleteTopicsRequest) -> DeleteTopicsResponse;
}
