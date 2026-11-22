//! The APIs of consumer groups and their coordinator. They have no parts, and only fail as a whole.

use gaslightmq_wire::messages::delete_groups_response::DeletableGroupResult;
use gaslightmq_wire::messages::find_coordinator_response::Coordinator;
use gaslightmq_wire::messages::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatRequest, DeleteGroupsRequest,
    DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
    SyncGroupRequest, consumer_group_describe_response, describe_groups_response,
};

use super::fails_as_a_whole;
use crate::body::{Editable, Verdict};

fails_as_a_whole!(
    ConsumerGroupHeartbeatRequest,
    HeartbeatRequest,
    JoinGroupRequest,
    LeaveGroupRequest,
    ListGroupsRequest,
    SyncGroupRequest,
);

/// No broker and no port: what a broker writes where a response has no coordinator to give.
const NOWHERE: i32 = -1;

/// Up to version 3 a `FindCoordinator` asks for one key, and the response itself fails.
/// Since version 4 it asks for several keys, and each of them fails.
impl Editable for FindCoordinatorRequest {
    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> FindCoordinatorResponse {
        if version >= 4 {
            let coordinators = self.coordinator_keys.iter().map(|key| {
                Coordinator::default()
                    .with_key(key.clone())
                    .with_node_id(NOWHERE.into())
                    .with_port(NOWHERE)
                    .with_error_code(verdict.call())
            });
            FindCoordinatorResponse::default().with_coordinators(coordinators.collect())
        } else {
            FindCoordinatorResponse::default()
                .with_node_id(NOWHERE.into())
                .with_port(NOWHERE)
                .with_error_code(verdict.call())
        }
    }
}

impl Editable for DescribeGroupsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeGroupsResponse {
        let groups = self.groups.iter().map(|group| {
            describe_groups_response::DescribedGroup::default()
                .with_group_id(group.clone())
                .with_error_code(verdict.call())
        });
        DescribeGroupsResponse::default().with_groups(groups.collect())
    }
}

impl Editable for DeleteGroupsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DeleteGroupsResponse {
        let groups = self.groups_names.iter().map(|group| {
            DeletableGroupResult::default()
                .with_group_id(group.clone())
                .with_error_code(verdict.call())
        });
        DeleteGroupsResponse::default().with_results(groups.collect())
    }
}

impl Editable for ConsumerGroupDescribeRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> ConsumerGroupDescribeResponse {
        let groups = self.group_ids.iter().map(|group| {
            consumer_group_describe_response::DescribedGroup::default()
                .with_group_id(group.clone())
                .with_error_code(verdict.call())
        });
        ConsumerGroupDescribeResponse::default().with_groups(groups.collect())
    }
}
