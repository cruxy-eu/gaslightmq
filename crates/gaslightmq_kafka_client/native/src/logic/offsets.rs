//! Asking where the log of a partition starts and ends.

use std::time::Duration;

use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use gaslightmq_wire::messages::{BrokerId, ListOffsetsRequest, ListOffsetsResponse};

use crate::configuration::ConsumerConfiguration;
use crate::error::ClientError;
use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::{Route, TopicPartition};
use crate::logic::versions::Versions;

/// The `replica_id` of a request that comes from a client, and not from another broker.
const NOT_A_REPLICA: BrokerId = BrokerId(-1);

/// The end of a partition's log that a [`ListOffset`] asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogEnd {
    /// The offset of the oldest record the log still has.
    Earliest,
    /// The offset the next record gets.
    Latest,
}

impl LogEnd {
    /// The timestamp that stands for this end in a `ListOffsets` request.
    fn to_wire(self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
        }
    }
}

/// Ask the leader of a partition for the offset at one end of its log.
pub(crate) struct ListOffset<'a> {
    pub configuration: &'a ConsumerConfiguration,
    pub partition: TopicPartition,
    pub end: LogEnd,
}

impl Operation for ListOffset<'_> {
    type Request = ListOffsetsRequest;
    type Output = i64;

    /// From version 1 on, a partition has one offset in the response.
    const VERSIONS: Versions = Versions::new(1, 7);

    fn route(&self) -> Route {
        Route::Leader(self.partition.clone())
    }

    fn allow_auto_create_topics(&self) -> bool {
        self.configuration.allow_auto_create_topics
    }

    fn request(&self, _within: Duration) -> ListOffsetsRequest {
        ListOffsetsRequest::default()
            .with_replica_id(NOT_A_REPLICA)
            .with_isolation_level(self.configuration.isolation_level.to_wire())
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(self.partition.topic.clone())
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(self.partition.partition)
                            .with_timestamp(self.end.to_wire()),
                    ]),
            ])
    }

    fn read(&mut self, response: ListOffsetsResponse) -> Result<Reading<i64>, ClientError> {
        let listed = response
            .topics
            .iter()
            .filter(|topic| topic.name == self.partition.topic)
            .flat_map(|topic| &topic.partitions)
            .find(|listed| listed.partition_index == self.partition.partition)
            .ok_or_else(|| ClientError::Protocol(format!("no offset for {}", self.partition)))?;
        match ClientError::broker(listed.error_code) {
            Some(error) => Err(error),
            None => Ok(Reading::Done(listed.offset)),
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};

    use super::*;
    use crate::address::BootstrapServers;
    use crate::configuration::IsolationLevel;
    use crate::logic::cluster::tests::partition;
    use crate::logic::cluster::topic_name;

    fn configuration() -> ConsumerConfiguration {
        let mut configuration = ConsumerConfiguration::new("localhost:9092".parse::<BootstrapServers>().unwrap());
        configuration.isolation_level = IsolationLevel::ReadCommitted;
        configuration.allow_auto_create_topics = false;
        configuration
    }

    fn list(configuration: &ConsumerConfiguration, end: LogEnd) -> ListOffset<'_> {
        ListOffset {
            configuration,
            partition: partition("orders", 3),
            end,
        }
    }

    fn response(listed: ListOffsetsPartitionResponse) -> ListOffsetsResponse {
        ListOffsetsResponse::default().with_topics(vec![
            ListOffsetsTopicResponse::default()
                .with_name(topic_name("orders"))
                .with_partitions(vec![listed.with_partition_index(3)]),
        ])
    }

    #[test]
    fn the_request_names_the_end_of_the_log_as_kafka_writes_it() {
        let earliest = list(&configuration(), LogEnd::Earliest).request(Duration::ZERO);
        let latest = list(&configuration(), LogEnd::Latest).request(Duration::ZERO);

        assert_eq!(earliest.replica_id, BrokerId(-1));
        assert_eq!(earliest.isolation_level, 1);
        assert_eq!(earliest.topics[0].name, topic_name("orders"));
        assert_eq!(earliest.topics[0].partitions[0].partition_index, 3);
        assert_eq!(earliest.topics[0].partitions[0].timestamp, -2);
        assert_eq!(latest.topics[0].partitions[0].timestamp, -1);
        assert_eq!(
            list(&configuration(), LogEnd::Latest).route(),
            Route::Leader(partition("orders", 3))
        );
        assert!(!list(&configuration(), LogEnd::Latest).allow_auto_create_topics());
    }

    #[test]
    fn the_offset_of_the_partition_comes_back() {
        let listed = ListOffsetsPartitionResponse::default().with_offset(42);

        assert_eq!(
            list(&configuration(), LogEnd::Latest).read(response(listed)),
            Ok(Reading::Done(42))
        );
    }

    #[test]
    fn the_error_of_the_partition_is_the_failure_of_the_call() {
        let refused =
            ListOffsetsPartitionResponse::default().with_error_code(ResponseError::NotLeaderOrFollower.code());

        assert_eq!(
            list(&configuration(), LogEnd::Latest).read(response(refused)),
            Err(ClientError::Broker {
                error: ResponseError::NotLeaderOrFollower
            })
        );
    }

    #[test]
    fn a_response_without_the_partition_makes_no_sense() {
        assert_eq!(
            list(&configuration(), LogEnd::Latest).read(ListOffsetsResponse::default()),
            Err(ClientError::Protocol("no offset for orders-3".into()))
        );
    }
}
