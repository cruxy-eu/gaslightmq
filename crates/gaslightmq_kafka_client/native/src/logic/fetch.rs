//! Reading records from a partition.

use std::time::Duration;

use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::{FetchRequest, FetchResponse};
use gaslightmq_wire::{Record, RecordBatch};

use crate::configuration::ConsumerConfiguration;
use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::{Route, TopicPartition};
use crate::logic::versions::Versions;
use crate::logic::{wire_bytes, wire_millis};

/// Read the records of a partition from `offset` on, from its leader.
///
/// A broker answers with whole batches, so the first one may start before `offset`.
/// The records before `offset` are left out, as a Kafka consumer leaves them out.
pub(crate) struct FetchRecords<'a> {
    pub configuration: &'a ConsumerConfiguration,
    pub partition: TopicPartition,
    pub offset: i64,
}

impl Operation for FetchRecords<'_> {
    type Request = FetchRequest;
    type Output = Vec<Record>;

    /// Fetch stops before topic IDs replaced topic names.
    const VERSIONS: Versions = Versions::new(4, 12);

    fn route(&self) -> Route {
        Route::Leader(self.partition.clone())
    }

    fn allow_auto_create_topics(&self) -> bool {
        self.configuration.allow_auto_create_topics
    }

    /// The broker holds a Fetch for as long as the request tells it to.
    /// The response then still needs the time of a request to arrive.
    fn answer_within(&self, request_timeout: Duration) -> Duration {
        self.configuration
            .fetch_max_wait
            .duration()
            .saturating_add(request_timeout)
    }

    fn request(&self, _within: Duration) -> FetchRequest {
        let configuration = self.configuration;
        FetchRequest::default()
            .with_max_wait_ms(wire_millis(configuration.fetch_max_wait.duration()))
            .with_min_bytes(wire_bytes(configuration.fetch_min_bytes))
            .with_max_bytes(wire_bytes(configuration.fetch_max_bytes))
            .with_isolation_level(configuration.isolation_level.to_wire())
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(self.partition.topic.clone())
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(self.partition.partition)
                            .with_fetch_offset(self.offset)
                            .with_partition_max_bytes(wire_bytes(configuration.max_partition_fetch_bytes)),
                    ]),
            ])
    }

    fn read(&mut self, response: FetchResponse) -> Result<Reading<Vec<Record>>, ClientError> {
        if let Some(error) = ClientError::refusal(response.error_code) {
            return Err(error);
        }
        let mut records = Vec::new();
        for fetched in response
            .responses
            .iter()
            .flat_map(|topic| &topic.partitions)
            .filter(|fetched| fetched.partition_index == self.partition.partition)
        {
            if let Some(error) = ClientError::refusal(fetched.error_code) {
                return Err(error);
            }
            if let Some(batches) = &fetched.records {
                records.extend(
                    RecordBatch::decode_all(batches)
                        .map_err(ClientError::unreadable_answer)?
                        .into_iter()
                        .flat_map(|batch| batch.records)
                        .filter(|record| record.offset.is_none_or(|offset| i64::from(offset) >= self.offset)),
                );
            }
        }
        Ok(Reading::Done(records))
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};

    use super::*;
    use crate::address::BootstrapServers;
    use crate::configuration::IsolationLevel;
    use crate::logic::cluster::tests::partition;
    use crate::logic::cluster::topic_name;

    fn configuration() -> ConsumerConfiguration {
        ConsumerConfiguration::new("localhost:9092".parse::<BootstrapServers>().unwrap())
    }

    fn fetch(configuration: &ConsumerConfiguration) -> FetchRecords<'_> {
        FetchRecords {
            configuration,
            partition: partition("orders", 3),
            offset: 42,
        }
    }

    fn response(fetched: PartitionData) -> FetchResponse {
        FetchResponse::default().with_responses(vec![
            FetchableTopicResponse::default()
                .with_topic(topic_name("orders"))
                .with_partitions(vec![fetched.with_partition_index(3)]),
        ])
    }

    #[test]
    fn the_request_carries_what_the_consumer_is_configured_with() {
        let mut configuration = configuration();
        configuration.fetch_max_wait = TimeSpan::from_secs(2);
        configuration.fetch_min_bytes = 16;
        configuration.fetch_max_bytes = 4_096;
        configuration.max_partition_fetch_bytes = 1_024;
        configuration.isolation_level = IsolationLevel::ReadCommitted;

        let request = fetch(&configuration).request(Duration::from_secs(30));

        assert_eq!(request.max_wait_ms, 2_000);
        assert_eq!(request.min_bytes, 16);
        assert_eq!(request.max_bytes, 4_096);
        assert_eq!(request.isolation_level, 1);
        assert_eq!(request.topics[0].topic, topic_name("orders"));
        let asked = &request.topics[0].partitions[0];
        assert_eq!(asked.partition, 3);
        assert_eq!(asked.fetch_offset, 42);
        assert_eq!(asked.partition_max_bytes, 1_024);
        assert_eq!(fetch(&configuration).route(), Route::Leader(partition("orders", 3)));
    }

    #[test]
    fn a_fetch_waits_as_long_as_the_broker_may_hold_it_and_then_for_the_response() {
        let mut configuration = configuration();
        configuration.fetch_max_wait = TimeSpan::from_secs(2);

        let fetch = fetch(&configuration);

        assert_eq!(
            fetch.answer_within(Duration::from_millis(100)),
            Duration::from_millis(2_100)
        );
    }

    #[test]
    fn the_consumer_decides_whether_asking_for_the_leader_may_create_the_topic() {
        let mut configuration = configuration();
        assert!(fetch(&configuration).allow_auto_create_topics());

        configuration.allow_auto_create_topics = false;

        assert!(!fetch(&configuration).allow_auto_create_topics());
    }

    #[test]
    fn the_request_carries_kafkas_defaults_when_nothing_is_configured() {
        let configuration = configuration();

        let request = fetch(&configuration).request(Duration::from_secs(30));

        assert_eq!(request.max_wait_ms, 500);
        assert_eq!(request.min_bytes, 1);
        assert_eq!(request.max_bytes, 52_428_800);
        assert_eq!(request.isolation_level, 0);
        assert_eq!(request.topics[0].partitions[0].partition_max_bytes, 1_048_576);
    }

    #[test]
    fn the_records_before_the_offset_are_left_out() {
        let configuration = configuration();
        let batch = RecordBatch::new(vec![
            Record::new("a").with_offset(40i64),
            Record::new("b"),
            Record::new("c"),
            Record::new("d"),
        ])
        .encode()
        .unwrap();

        let read = fetch(&configuration).read(response(PartitionData::default().with_records(Some(batch))));

        let Ok(Reading::Done(records)) = read else {
            panic!("the call is over: {read:?}")
        };
        let offsets: Vec<_> = records.iter().map(|record| record.offset.map(i64::from)).collect();
        assert_eq!(offsets, [Some(42), Some(43)]);
    }

    #[test]
    fn the_records_of_every_batch_come_back() {
        let configuration = configuration();
        let batch = RecordBatch::new(vec![Record::new("a").with_offset(42i64), Record::new("b")])
            .encode()
            .unwrap();

        let read = fetch(&configuration).read(response(PartitionData::default().with_records(Some(batch))));

        let Ok(Reading::Done(records)) = read else {
            panic!("the call is over: {read:?}")
        };
        let values: Vec<_> = records.iter().map(|record| record.value.clone()).collect();
        assert_eq!(values, [Some("a".into()), Some("b".into())]);
    }

    #[test]
    fn a_partition_without_records_gives_none() {
        let configuration = configuration();

        assert_eq!(
            fetch(&configuration).read(response(PartitionData::default())),
            Ok(Reading::Done(vec![]))
        );
    }

    #[test]
    fn the_error_of_the_response_or_of_the_partition_is_the_failure_of_the_call() {
        let configuration = configuration();
        let refused = PartitionData::default().with_error_code(ResponseError::OffsetOutOfRange.code());
        let unavailable = FetchResponse::default().with_error_code(ResponseError::NotLeaderOrFollower.code());

        assert_eq!(
            fetch(&configuration).read(response(refused)),
            Err(ClientError::Refused {
                error: ResponseError::OffsetOutOfRange
            })
        );
        assert_eq!(
            fetch(&configuration).read(unavailable),
            Err(ClientError::Refused {
                error: ResponseError::NotLeaderOrFollower
            })
        );
    }
}
