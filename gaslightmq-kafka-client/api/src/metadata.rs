use std::future::Future;

use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;

pub trait Metadata {
    fn metadata(&self, request: MetadataRequest) -> impl Future<Output = MetadataResponse> + Send;
}
