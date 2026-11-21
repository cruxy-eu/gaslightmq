use std::future::Future;

use gmq_wire::messages::metadata_request::MetadataRequest;
use gmq_wire::messages::metadata_response::MetadataResponse;

pub trait Metadata {
    fn metadata(&self, request: MetadataRequest) -> impl Future<Output = MetadataResponse> + Send;
}
