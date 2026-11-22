//! Writing records to a partition.

use std::time::Duration;

use bytes::Bytes;
use gaslightmq_wire::Offset;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{ProduceRequest, ProduceResponse};

use crate::configuration::Acks;
use crate::error::ClientError;
use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::{Route, TopicPartition};
use crate::logic::versions::Versions;
use crate::logic::wire_millis;

/// Write one batch of records to the leader of a partition.
/// The output is the offset of the first record.
pub(crate) struct ProduceRecords {
    pub partition: TopicPartition,
    /// The records, as an encoded record batch.
    pub batch: Bytes,
    pub acks: Acks,
}

impl Operation for ProduceRecords {
    type Request = ProduceRequest;
    type Output = Offset;

    /// Produce stops before topic IDs replaced topic names.
    const VERSIONS: Versions = Versions::new(3, 9);

    fn route(&self) -> Route {
        Route::Leader(self.partition.clone())
    }

    /// The broker gets as long to collect the acks as the try waits for the response.
    fn request(&self, within: Duration) -> ProduceRequest {
        ProduceRequest::default()
            .with_acks(self.acks.to_wire())
            .with_timeout_ms(wire_millis(within))
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(self.partition.topic.clone())
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(self.partition.partition)
                            .with_records(Some(self.batch.clone())),
                    ]),
            ])
    }

    fn read(&mut self, response: ProduceResponse) -> Result<Reading<Offset>, ClientError> {
        let written = response
            .responses
            .iter()
            .filter(|topic| topic.name == self.partition.topic)
            .flat_map(|topic| &topic.partition_responses)
            .find(|partition| partition.index == self.partition.partition)
            .ok_or_else(|| ClientError::Protocol(format!("no produce result for {}", self.partition)))?;
        match ClientError::broker(written.error_code) {
            Some(error) => Err(error),
            None => Ok(Reading::Done(Offset::from(written.base_offset))),
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};

    use super::*;
    use crate::logic::cluster::tests::partition;
    use crate::logic::cluster::topic_name;

    fn produce() -> ProduceRecords {
        ProduceRecords {
            partition: partition("orders", 3),
            batch: Bytes::from_static(b"batch"),
            acks: Acks::Leader,
        }
    }

    fn response(topic: &str, partition: i32, written: PartitionProduceResponse) -> ProduceResponse {
        ProduceResponse::default().with_responses(vec![
            TopicProduceResponse::default()
                .with_name(topic_name(topic))
                .with_partition_responses(vec![written.with_index(partition)]),
        ])
    }

    #[test]
    fn the_request_carries_the_batch_the_acks_and_the_time_of_the_try() {
        let request = produce().request(Duration::from_millis(1_500));

        assert_eq!(request.acks, 1);
        assert_eq!(request.timeout_ms, 1_500);
        assert_eq!(request.topic_data[0].name, topic_name("orders"));
        let data = &request.topic_data[0].partition_data[0];
        assert_eq!(data.index, 3);
        assert_eq!(data.records, Some(Bytes::from_static(b"batch")));
        assert_eq!(produce().route(), Route::Leader(partition("orders", 3)));
    }

    #[test]
    fn the_offset_of_the_first_record_comes_back() {
        let written = PartitionProduceResponse::default().with_base_offset(42);

        assert_eq!(
            produce().read(response("orders", 3, written)),
            Ok(Reading::Done(Offset::from(42i64)))
        );
    }

    #[test]
    fn the_error_of_the_partition_is_the_failure_of_the_call() {
        let refused = PartitionProduceResponse::default().with_error_code(ResponseError::NotLeaderOrFollower.code());

        assert_eq!(
            produce().read(response("orders", 3, refused)),
            Err(ClientError::Broker {
                error: ResponseError::NotLeaderOrFollower
            })
        );
    }

    #[test]
    fn a_response_without_the_partition_makes_no_sense() {
        let other = PartitionProduceResponse::default().with_base_offset(42);

        assert_eq!(
            produce().read(response("orders", 4, other)),
            Err(ClientError::Protocol("no produce result for orders-3".into()))
        );
    }
}
