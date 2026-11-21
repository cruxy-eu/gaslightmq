use std::future::Future;

use gmq_wire::messages::create_topics_request::CreateTopicsRequest;
use gmq_wire::messages::create_topics_response::CreateTopicsResponse;

pub trait CreateTopics {
    fn create_topics(&self, request: CreateTopicsRequest) -> impl Future<Output = CreateTopicsResponse> + Send;
}
