use std::future::Future;

use gmq_wire::messages::delete_topics_request::DeleteTopicsRequest;
use gmq_wire::messages::delete_topics_response::DeleteTopicsResponse;

pub trait DeleteTopics {
    fn delete_topics(&self, request: DeleteTopicsRequest) -> impl Future<Output = DeleteTopicsResponse> + Send;
}
