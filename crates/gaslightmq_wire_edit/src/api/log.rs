//! The APIs that write and read the log of a partition: Produce, Fetch, `ListOffsets`, `OffsetForLeaderEpoch` and `DeleteRecords`.
//! Their parts are partitions.

use gaslightmq_wire::messages::delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult};
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
use gaslightmq_wire::messages::offset_for_leader_epoch_response::{EpochEndOffset, OffsetForLeaderTopicResult};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    DeleteRecordsRequest, DeleteRecordsResponse, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse,
};

use crate::body::{Editable, Keep, TopicKey, Verdict};
use crate::entry::{partition, response, response_in_parts, topic};

/// No offset: what a broker writes where a response has no offset to give.
const NO_OFFSET: i64 = -1;

impl Editable for ProduceRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topic_data.retain_mut(|topic| {
            let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
            keep.partitions(asked, &mut topic.partition_data, |partition| partition.index)
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> ProduceResponse {
        let topics = self.topic_data.iter().filter_map(|topic| {
            let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
            let indexes = topic.partition_data.iter().map(|partition| partition.index);
            let partitions = verdict.partitions(asked, indexes, |index, error_code| {
                PartitionProduceResponse::default()
                    .with_index(index)
                    .with_error_code(error_code)
                    .with_base_offset(NO_OFFSET)
            })?;
            Some(
                TopicProduceResponse::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partition_responses(partitions),
            )
        });
        ProduceResponse::default().with_responses(topics.collect())
    }

    response_in_parts!();
}

response!(ProduceResponse, responses);
topic!(
    TopicProduceResponse,
    |topic| TopicKey::new(&topic.name, topic.topic_id.into_bytes()),
    partition_responses
);
partition!(PartitionProduceResponse, index, error_code);

impl Editable for FetchRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            let asked = TopicKey::new(&topic.topic, topic.topic_id.into_bytes());
            keep.partitions(asked, &mut topic.partitions, |partition| partition.partition)
        });
    }

    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> FetchResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let asked = TopicKey::new(&topic.topic, topic.topic_id.into_bytes());
            let indexes = topic.partitions.iter().map(|partition| partition.partition);
            let partitions = verdict.partitions(asked, indexes, |index, error_code| {
                PartitionData::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
                    .with_high_watermark(NO_OFFSET)
            })?;
            Some(
                FetchableTopicResponse::default()
                    .with_topic(topic.topic.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            )
        });
        // The response itself has an error code since version 7.
        let of_call = if version >= 7 { verdict.call() } else { 0 };
        FetchResponse::default()
            .with_error_code(of_call)
            .with_responses(topics.collect())
    }

    response_in_parts!();
}

response!(FetchResponse, responses);
topic!(
    FetchableTopicResponse,
    |topic| TopicKey::new(&topic.topic, topic.topic_id.into_bytes()),
    partitions
);
partition!(PartitionData, partition_index, error_code);

impl Editable for ListOffsetsRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            keep.partitions(TopicKey::named(&topic.name), &mut topic.partitions, |partition| {
                partition.partition_index
            })
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> ListOffsetsResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let indexes = topic.partitions.iter().map(|partition| partition.partition_index);
            let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
                ListOffsetsPartitionResponse::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
            })?;
            Some(
                ListOffsetsTopicResponse::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            )
        });
        ListOffsetsResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(ListOffsetsResponse, topics);
topic!(
    ListOffsetsTopicResponse,
    |topic| TopicKey::named(&topic.name),
    partitions
);
partition!(ListOffsetsPartitionResponse, partition_index, error_code);

impl Editable for OffsetForLeaderEpochRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            keep.partitions(TopicKey::named(&topic.topic), &mut topic.partitions, |partition| {
                partition.partition
            })
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> OffsetForLeaderEpochResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let indexes = topic.partitions.iter().map(|partition| partition.partition);
            let partitions = verdict.partitions(TopicKey::named(&topic.topic), indexes, |index, error_code| {
                EpochEndOffset::default()
                    .with_partition(index)
                    .with_error_code(error_code)
            })?;
            Some(
                OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic.clone())
                    .with_partitions(partitions),
            )
        });
        OffsetForLeaderEpochResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(OffsetForLeaderEpochResponse, topics);
topic!(
    OffsetForLeaderTopicResult,
    |topic| TopicKey::named(&topic.topic),
    partitions
);
partition!(EpochEndOffset, partition, error_code);

impl Editable for DeleteRecordsRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            keep.partitions(TopicKey::named(&topic.name), &mut topic.partitions, |partition| {
                partition.partition_index
            })
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DeleteRecordsResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let indexes = topic.partitions.iter().map(|partition| partition.partition_index);
            let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
                DeleteRecordsPartitionResult::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
                    .with_low_watermark(NO_OFFSET)
            })?;
            Some(
                DeleteRecordsTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            )
        });
        DeleteRecordsResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(DeleteRecordsResponse, topics);
topic!(
    DeleteRecordsTopicResult,
    |topic| TopicKey::named(&topic.name),
    partitions
);
partition!(DeleteRecordsPartitionResult, partition_index, error_code);
