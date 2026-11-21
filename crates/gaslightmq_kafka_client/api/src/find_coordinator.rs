use std::future::Future;

use gaslightmq_wire::messages::find_coordinator_request::FindCoordinatorRequest;
use gaslightmq_wire::messages::find_coordinator_response::FindCoordinatorResponse;

pub trait FindCoordinator {
    fn find_coordinator(&self, request: FindCoordinatorRequest)
    -> impl Future<Output = FindCoordinatorResponse> + Send;
}
