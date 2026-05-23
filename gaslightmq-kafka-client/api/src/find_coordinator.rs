use async_trait::async_trait;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

#[async_trait]
pub trait FindCoordinator {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse;
}
