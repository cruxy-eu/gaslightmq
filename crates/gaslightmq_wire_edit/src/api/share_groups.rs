//! The APIs of share groups and of the state a share coordinator keeps. They have no parts, and only fail as a whole.

use gaslightmq_wire::messages::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup;
use gaslightmq_wire::messages::{
    AlterShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequest, DeleteShareGroupStateRequest,
    DeleteShareGroupStateResponse, DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse,
    InitializeShareGroupStateRequest, InitializeShareGroupStateResponse, ReadShareGroupStateRequest,
    ReadShareGroupStateResponse, ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse,
    ShareAcknowledgeRequest, ShareFetchRequest, ShareGroupDescribeRequest, ShareGroupDescribeResponse,
    ShareGroupHeartbeatRequest, WriteShareGroupStateRequest, WriteShareGroupStateResponse,
    delete_share_group_state_response, initialize_share_group_state_response, read_share_group_state_response,
    read_share_group_state_summary_response, share_group_describe_response, write_share_group_state_response,
};

use super::fails_as_a_whole;
use crate::body::{Editable, Verdict};

fails_as_a_whole!(
    AlterShareGroupOffsetsRequest,
    DeleteShareGroupOffsetsRequest,
    ShareAcknowledgeRequest,
    ShareFetchRequest,
    ShareGroupHeartbeatRequest,
);

impl Editable for ShareGroupDescribeRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> ShareGroupDescribeResponse {
        let groups = self.group_ids.iter().map(|group| {
            share_group_describe_response::DescribedGroup::default()
                .with_group_id(group.clone())
                .with_error_code(verdict.call())
        });
        ShareGroupDescribeResponse::default().with_groups(groups.collect())
    }
}

impl Editable for DescribeShareGroupOffsetsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeShareGroupOffsetsResponse {
        let groups = self.groups.iter().map(|group| {
            DescribeShareGroupOffsetsResponseGroup::default()
                .with_group_id(group.group_id.clone())
                .with_error_code(verdict.call())
        });
        DescribeShareGroupOffsetsResponse::default().with_groups(groups.collect())
    }
}

/// The APIs on the state of a share group. Their response has no error code of its own:
/// every partition the request names fails, under its topic ID.
macro_rules! every_partition_fails {
    ($($request:ty => $response:ty, $topic:ty, $partition:ty;)+) => {
        $(
            impl Editable for $request {
                fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> $response {
                    let topics = self.topics.iter().map(|topic| {
                        let partitions = topic.partitions.iter().map(|partition| {
                            <$partition>::default()
                                .with_partition(partition.partition)
                                .with_error_code(verdict.call())
                        });
                        <$topic>::default()
                            .with_topic_id(topic.topic_id)
                            .with_partitions(partitions.collect())
                    });
                    <$response>::default().with_results(topics.collect())
                }
            }
        )+
    };
}

every_partition_fails!(
    InitializeShareGroupStateRequest => InitializeShareGroupStateResponse,
        initialize_share_group_state_response::InitializeStateResult,
        initialize_share_group_state_response::PartitionResult;
    ReadShareGroupStateRequest => ReadShareGroupStateResponse,
        read_share_group_state_response::ReadStateResult,
        read_share_group_state_response::PartitionResult;
    WriteShareGroupStateRequest => WriteShareGroupStateResponse,
        write_share_group_state_response::WriteStateResult,
        write_share_group_state_response::PartitionResult;
    DeleteShareGroupStateRequest => DeleteShareGroupStateResponse,
        delete_share_group_state_response::DeleteStateResult,
        delete_share_group_state_response::PartitionResult;
    ReadShareGroupStateSummaryRequest => ReadShareGroupStateSummaryResponse,
        read_share_group_state_summary_response::ReadStateSummaryResult,
        read_share_group_state_summary_response::PartitionResult;
);
