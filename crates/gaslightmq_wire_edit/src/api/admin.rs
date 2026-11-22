//! The APIs of cluster administration, and the handshake on versions.
//! They have no parts, and only fail as a whole.

use gaslightmq_wire::messages::alter_partition_reassignments_response::{
    ReassignablePartitionResponse, ReassignableTopicResponse,
};
use gaslightmq_wire::messages::alter_replica_log_dirs_response::{
    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
};
use gaslightmq_wire::messages::describe_configs_response::DescribeConfigsResult;
use gaslightmq_wire::messages::describe_log_dirs_response::DescribeLogDirsResult;
use gaslightmq_wire::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use gaslightmq_wire::messages::elect_leaders_response::{PartitionResult, ReplicaElectionResult};
use gaslightmq_wire::messages::update_features_response::UpdatableFeatureResult;
use gaslightmq_wire::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterConfigsRequest, AlterConfigsResponse,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, ApiVersionsRequest, DescribeClientQuotasRequest, DescribeClusterRequest,
    DescribeConfigsRequest, DescribeConfigsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, ElectLeadersRequest, ElectLeadersResponse,
    GetTelemetrySubscriptionsRequest, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    ListConfigResourcesRequest, ListPartitionReassignmentsRequest, PushTelemetryRequest, UpdateFeaturesRequest,
    UpdateFeaturesResponse, alter_client_quotas_response, alter_configs_response, incremental_alter_configs_response,
};

use super::fails_as_a_whole;
use crate::body::{Editable, Verdict};

fails_as_a_whole!(
    ApiVersionsRequest,
    DescribeClientQuotasRequest,
    DescribeClusterRequest,
    GetTelemetrySubscriptionsRequest,
    ListConfigResourcesRequest,
    ListPartitionReassignmentsRequest,
    PushTelemetryRequest,
);

impl Editable for DescribeConfigsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeConfigsResponse {
        let resources = self.resources.iter().map(|resource| {
            DescribeConfigsResult::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone())
                .with_error_code(verdict.call())
        });
        DescribeConfigsResponse::default().with_results(resources.collect())
    }
}

impl Editable for AlterConfigsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> AlterConfigsResponse {
        let resources = self.resources.iter().map(|resource| {
            alter_configs_response::AlterConfigsResourceResponse::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone())
                .with_error_code(verdict.call())
        });
        AlterConfigsResponse::default().with_responses(resources.collect())
    }
}

impl Editable for IncrementalAlterConfigsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> IncrementalAlterConfigsResponse {
        let resources = self.resources.iter().map(|resource| {
            incremental_alter_configs_response::AlterConfigsResourceResponse::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone())
                .with_error_code(verdict.call())
        });
        IncrementalAlterConfigsResponse::default().with_responses(resources.collect())
    }
}

impl Editable for AlterClientQuotasRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> AlterClientQuotasResponse {
        let entries = self.entries.iter().map(|entry| {
            let entity = entry.entity.iter().map(|entity| {
                alter_client_quotas_response::EntityData::default()
                    .with_entity_type(entity.entity_type.clone())
                    .with_entity_name(entity.entity_name.clone())
            });
            alter_client_quotas_response::EntryData::default()
                .with_entity(entity.collect())
                .with_error_code(verdict.call())
        });
        AlterClientQuotasResponse::default().with_entries(entries.collect())
    }
}

/// The response names each partition once, whichever log directory the request moves it to.
impl Editable for AlterReplicaLogDirsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> AlterReplicaLogDirsResponse {
        let mut topics: Vec<AlterReplicaLogDirTopicResult> = Vec::new();
        for topic in self.dirs.iter().flat_map(|dir| &dir.topics) {
            let partitions = topic.partitions.iter().map(|index| {
                AlterReplicaLogDirPartitionResult::default()
                    .with_partition_index(*index)
                    .with_error_code(verdict.call())
            });
            match topics.iter_mut().find(|known| known.topic_name == topic.name) {
                Some(known) => {
                    for partition in partitions {
                        if !known.partitions.contains(&partition) {
                            known.partitions.push(partition);
                        }
                    }
                }
                None => topics.push(
                    AlterReplicaLogDirTopicResult::default()
                        .with_topic_name(topic.name.clone())
                        .with_partitions(partitions.collect()),
                ),
            }
        }
        AlterReplicaLogDirsResponse::default().with_results(topics)
    }
}

/// Since version 3 the response itself has an error code.
/// Before that only a log directory has one, so the response names one directory without a path, which failed.
impl Editable for DescribeLogDirsRequest {
    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> DescribeLogDirsResponse {
        if version >= 3 {
            DescribeLogDirsResponse::default().with_error_code(verdict.call())
        } else {
            let unnamed = DescribeLogDirsResult::default().with_error_code(verdict.call());
            DescribeLogDirsResponse::default().with_results(vec![unnamed])
        }
    }
}

impl Editable for ElectLeadersRequest {
    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> ElectLeadersResponse {
        let topics = self.topic_partitions.iter().flatten().map(|topic| {
            let partitions = topic.partitions.iter().map(|index| {
                PartitionResult::default()
                    .with_partition_id(*index)
                    .with_error_code(verdict.call())
            });
            ReplicaElectionResult::default()
                .with_topic(topic.topic.clone())
                .with_partition_result(partitions.collect())
        });
        // The response itself has an error code since version 1.
        let of_call = if version >= 1 { verdict.call() } else { 0 };
        ElectLeadersResponse::default()
            .with_error_code(of_call)
            .with_replica_election_results(topics.collect())
    }
}

impl Editable for AlterPartitionReassignmentsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> AlterPartitionReassignmentsResponse {
        let topics = self.topics.iter().map(|topic| {
            let partitions = topic.partitions.iter().map(|partition| {
                ReassignablePartitionResponse::default()
                    .with_partition_index(partition.partition_index)
                    .with_error_code(verdict.call())
            });
            ReassignableTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions.collect())
        });
        AlterPartitionReassignmentsResponse::default()
            .with_error_code(verdict.call())
            .with_responses(topics.collect())
    }
}

impl Editable for DescribeTopicPartitionsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeTopicPartitionsResponse {
        let topics = self.topics.iter().map(|topic| {
            DescribeTopicPartitionsResponseTopic::default()
                .with_name(Some(topic.name.clone()))
                .with_error_code(verdict.call())
        });
        DescribeTopicPartitionsResponse::default().with_topics(topics.collect())
    }
}

/// Up to version 1 the response also names every feature the request updates, each with the error code.
impl Editable for UpdateFeaturesRequest {
    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> UpdateFeaturesResponse {
        let features = self.feature_updates.iter().filter(|_| version <= 1).map(|update| {
            UpdatableFeatureResult::default()
                .with_feature(update.feature.clone())
                .with_error_code(verdict.call())
        });
        UpdateFeaturesResponse::default()
            .with_error_code(verdict.call())
            .with_results(features.collect())
    }
}
