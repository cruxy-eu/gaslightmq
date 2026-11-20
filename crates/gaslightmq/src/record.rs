use futures::Stream;
use kafka_protocol::error::ResponseError;
use std::future::Future;
use std::pin::Pin;

pub use gaslightmq_kernel::Record;

#[derive(Debug, Clone, PartialEq)]
pub enum ProduceResult {
    Success,
    Error(ResponseError),
}

pub type RecordStream = Pin<Box<dyn Stream<Item = Record> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub enum FetchResult {
    Empty,
    Records(Vec<Record>),
    Error(ResponseError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    FindCoordinator,
    JoinGroup,
    SyncGroup,
    Heartbeat,
    ApiVersions,
}

pub type TopicFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
