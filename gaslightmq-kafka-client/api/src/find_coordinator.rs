use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

pub trait FindCoordinator {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse;
}
