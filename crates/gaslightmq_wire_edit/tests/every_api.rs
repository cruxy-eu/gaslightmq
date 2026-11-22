//! Every API that `gaslightmq_wire` has messages for, at every version: the whole call fails, and the response says so.
//!
//! The test builds one request of each API, with something in it to ask for where the API asks for things by name.
//! It fails when an API is missing here, so a new API of `kafka-protocol` cannot go unnoticed.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use common::{ORDERS, text, topic_names, wire_id, wire_name};
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::alter_client_quotas_request::{EntityData, EntryData};
use gaslightmq_wire::messages::alter_replica_log_dirs_request::{AlterReplicaLogDir, AlterReplicaLogDirTopic};
use gaslightmq_wire::messages::alter_user_scram_credentials_request::ScramCredentialDeletion;
use gaslightmq_wire::messages::create_acls_request::AclCreation;
use gaslightmq_wire::messages::delete_acls_request::DeleteAclsFilter;
use gaslightmq_wire::messages::describe_configs_request::DescribeConfigsResource;
use gaslightmq_wire::messages::describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestGroup;
use gaslightmq_wire::messages::elect_leaders_request::TopicPartitions;
use gaslightmq_wire::messages::write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic};
use gaslightmq_wire::messages::{
    AddOffsetsToTxnRequest, AddRaftVoterRequest, AllocateProducerIdsRequest, AlterClientQuotasRequest,
    AlterConfigsRequest, AlterPartitionReassignmentsRequest, AlterPartitionRequest, AlterReplicaLogDirsRequest,
    AlterShareGroupOffsetsRequest, AlterUserScramCredentialsRequest, ApiVersionsRequest, AssignReplicasToDirsRequest,
    BeginQuorumEpochRequest, BrokerHeartbeatRequest, BrokerRegistrationRequest, ConsumerGroupDescribeRequest,
    ConsumerGroupHeartbeatRequest, ControllerRegistrationRequest, CreateAclsRequest, CreateDelegationTokenRequest,
    DeleteAclsRequest, DeleteGroupsRequest, DeleteShareGroupOffsetsRequest, DeleteShareGroupStateRequest,
    DescribeAclsRequest, DescribeClientQuotasRequest, DescribeClusterRequest, DescribeConfigsRequest,
    DescribeDelegationTokenRequest, DescribeGroupsRequest, DescribeLogDirsRequest, DescribeProducersRequest,
    DescribeQuorumRequest, DescribeShareGroupOffsetsRequest, DescribeTopicPartitionsRequest,
    DescribeTransactionsRequest, DescribeUserScramCredentialsRequest, ElectLeadersRequest, EndQuorumEpochRequest,
    EndTxnRequest, EnvelopeRequest, ExpireDelegationTokenRequest, FetchSnapshotRequest, FindCoordinatorRequest,
    GetTelemetrySubscriptionsRequest, HeartbeatRequest, IncrementalAlterConfigsRequest, InitProducerIdRequest,
    InitializeShareGroupStateRequest, JoinGroupRequest, LeaveGroupRequest, ListConfigResourcesRequest,
    ListGroupsRequest, ListPartitionReassignmentsRequest, ListTransactionsRequest, PushTelemetryRequest,
    ReadShareGroupStateRequest, ReadShareGroupStateSummaryRequest, RemoveRaftVoterRequest, RenewDelegationTokenRequest,
    SaslAuthenticateRequest, SaslHandshakeRequest, ShareAcknowledgeRequest, ShareFetchRequest,
    ShareGroupDescribeRequest, ShareGroupHeartbeatRequest, SyncGroupRequest, UnregisterBrokerRequest,
    UpdateFeaturesRequest, UpdateRaftVoterRequest, VoteRequest, WriteShareGroupStateRequest, WriteTxnMarkersRequest,
    alter_configs_request, delete_share_group_state_request, describe_producers_request,
    describe_topic_partitions_request, incremental_alter_configs_request, initialize_share_group_state_request,
    read_share_group_state_request, read_share_group_state_summary_request, write_share_group_state_request,
};
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{ApiKey, Request, Response};
use gaslightmq_wire_edit::prelude::*;

/// An error code whose two bytes are easy to find in a response: `ZZ`. No text of the requests holds them.
const MARKED: i16 = 0x5A5A;

/// Checks that a request of `api` fails as a whole at every version of the API, and notes the API as checked.
fn check<R>(checked: &mut Vec<ApiKey>, api: ApiKey, sample: impl Fn(i16) -> R)
where
    R: KafkaRequest,
    R::Response: PartialEq + std::fmt::Debug,
{
    assert_eq!(R::KEY, api as i16, "{api:?} is checked with the message of another API");
    let error = ErrorCode::new(MARKED).unwrap();

    for version in R::VERSIONS.min..=R::VERSIONS.max {
        let request = Request::new(version, 7, Some("order-ingest"), &sample(version))
            .unwrap_or_else(|error| panic!("the example of {api:?} v{version}: {error}"));
        let asked = EditableRequest::decode(&request, &topic_names())
            .unwrap_or_else(|error| panic!("{api:?} v{version}: {error}"));

        let failed = asked
            .error_response(&Failures::whole_call(error))
            .unwrap_or_else(|error| panic!("{api:?} v{version}: {error}"));

        // The response belongs to the request, and it is a valid response of the API at that version.
        assert_eq!(failed.api(), request.api());
        assert_eq!(failed.correlation_id(), 7, "{api:?} v{version}");
        let body: R::Response = failed
            .body()
            .unwrap_or_else(|error| panic!("{api:?} v{version}: {error}"));
        let again = Response::new(failed.api(), 7, &body).unwrap();
        assert_eq!(
            again.frame(),
            failed.frame(),
            "{api:?} v{version} reads back as another response"
        );

        // The response says that something failed, and with which error.
        assert!(
            failed.body_bytes().windows(2).any(|pair| pair == MARKED.to_be_bytes()),
            "the response of {api:?} v{version} has no error code: {body:?}"
        );

        // Nothing else of surgery goes wrong on a request of this API.
        assert!(matches!(asked.without(std::iter::empty()), Ok(Remainder::Whole)));
        assert_eq!(asked.merge(&failed, &Failures::none()).unwrap().frame(), failed.frame());
        asked.parts_of(&failed).unwrap();

        // The response itself says that error, or it has no error code of its own.
        let of_response = asked.error_of(&failed).unwrap();
        assert!(
            of_response.is_none() || of_response == Some(error),
            "the response of {api:?} v{version} reads as {of_response:?}"
        );
    }
    checked.push(api);
}

/// Checks the APIs whose request needs nothing in it for the response to fail.
macro_rules! check_the_empty_request {
    ($checked:expr, $($api:ident => $request:ty,)+) => {
        $(check($checked, ApiKey::$api, |_| <$request>::default());)+
    };
}

/// Checks the APIs on the state of a share group, whose request names partitions of a topic by its ID.
macro_rules! check_share_group_state {
    ($checked:expr, $($api:ident => $request:ty, $topic:ty, $partition:ty;)+) => {
        $(check($checked, ApiKey::$api, |_| {
            let partition = <$partition>::default().with_partition(0);
            let topic = <$topic>::default().with_topic_id(wire_id(ORDERS)).with_partitions(vec![partition]);
            <$request>::default().with_topics(vec![topic])
        });)+
    };
}

/// The APIs with parts.
fn check_the_apis_with_parts(checked: &mut Vec<ApiKey>) {
    check(checked, ApiKey::Produce, common::produce);
    check(checked, ApiKey::Fetch, common::fetch);
    check(checked, ApiKey::ListOffsets, common::list_offsets);
    check(checked, ApiKey::Metadata, common::metadata);
    check(checked, ApiKey::OffsetCommit, common::offset_commit);
    check(checked, ApiKey::OffsetFetch, common::offset_fetch);
    check(checked, ApiKey::CreateTopics, common::create_topics);
    check(checked, ApiKey::DeleteTopics, common::delete_topics);
    check(checked, ApiKey::DeleteRecords, common::delete_records);
    check(checked, ApiKey::OffsetForLeaderEpoch, common::offset_for_leader_epoch);
    check(checked, ApiKey::AddPartitionsToTxn, common::add_partitions_to_txn);
    check(checked, ApiKey::TxnOffsetCommit, common::txn_offset_commit);
    check(checked, ApiKey::CreatePartitions, common::create_partitions);
    check(checked, ApiKey::OffsetDelete, common::offset_delete);
}

/// The APIs of groups and transactions whose response names what the request asked for, each with the error code.
fn check_the_apis_that_ask_for_groups_and_transactions(checked: &mut Vec<ApiKey>) {
    let group = || text("analytics-etl").into();

    check(checked, ApiKey::FindCoordinator, |version| {
        if version >= 4 {
            FindCoordinatorRequest::default().with_coordinator_keys(vec![text("analytics-etl")])
        } else {
            FindCoordinatorRequest::default().with_key(text("analytics-etl"))
        }
    });
    check(checked, ApiKey::DescribeGroups, |_| {
        DescribeGroupsRequest::default().with_groups(vec![group()])
    });
    check(checked, ApiKey::DeleteGroups, |_| {
        DeleteGroupsRequest::default().with_groups_names(vec![group()])
    });
    check(checked, ApiKey::ConsumerGroupDescribe, |_| {
        ConsumerGroupDescribeRequest::default().with_group_ids(vec![group()])
    });
    check(checked, ApiKey::ShareGroupDescribe, |_| {
        ShareGroupDescribeRequest::default().with_group_ids(vec![group()])
    });
    check(checked, ApiKey::DescribeShareGroupOffsets, |_| {
        let asked = DescribeShareGroupOffsetsRequestGroup::default().with_group_id(group());
        DescribeShareGroupOffsetsRequest::default().with_groups(vec![asked])
    });
    check(checked, ApiKey::WriteTxnMarkers, |_| {
        let topic = WritableTxnMarkerTopic::default()
            .with_name(wire_name(ORDERS))
            .with_partition_indexes(vec![0]);
        let marker = WritableTxnMarker::default()
            .with_producer_id(4000.into())
            .with_topics(vec![topic]);
        WriteTxnMarkersRequest::default().with_markers(vec![marker])
    });
    check(checked, ApiKey::DescribeTransactions, |_| {
        DescribeTransactionsRequest::default().with_transactional_ids(vec![text("payments-tx-1").into()])
    });
    check(checked, ApiKey::DescribeProducers, |_| {
        let topic = describe_producers_request::TopicRequest::default()
            .with_name(wire_name(ORDERS))
            .with_partition_indexes(vec![0]);
        DescribeProducersRequest::default().with_topics(vec![topic])
    });
}

/// The APIs of administration whose response names what the request asked for, each with the error code.
fn check_the_apis_that_ask_for_resources(checked: &mut Vec<ApiKey>) {
    check(checked, ApiKey::CreateAcls, |_| {
        CreateAclsRequest::default().with_creations(vec![AclCreation::default()])
    });
    check(checked, ApiKey::DeleteAcls, |_| {
        DeleteAclsRequest::default().with_filters(vec![DeleteAclsFilter::default()])
    });
    check(checked, ApiKey::AlterUserScramCredentials, |_| {
        let deletion = ScramCredentialDeletion::default().with_name(text("alice"));
        AlterUserScramCredentialsRequest::default().with_deletions(vec![deletion])
    });
    check(checked, ApiKey::DescribeConfigs, |_| {
        let resource = DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(text(ORDERS));
        DescribeConfigsRequest::default().with_resources(vec![resource])
    });
    check(checked, ApiKey::AlterConfigs, |_| {
        let resource = alter_configs_request::AlterConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(text(ORDERS));
        AlterConfigsRequest::default().with_resources(vec![resource])
    });
    check(checked, ApiKey::IncrementalAlterConfigs, |_| {
        let resource = incremental_alter_configs_request::AlterConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(text(ORDERS));
        IncrementalAlterConfigsRequest::default().with_resources(vec![resource])
    });
    check(checked, ApiKey::AlterClientQuotas, |_| {
        let entity = EntityData::default().with_entity_type(text("user"));
        let entry = EntryData::default().with_entity(vec![entity]);
        AlterClientQuotasRequest::default().with_entries(vec![entry])
    });
    check(checked, ApiKey::AlterReplicaLogDirs, |_| {
        let topic = AlterReplicaLogDirTopic::default()
            .with_name(wire_name(ORDERS))
            .with_partitions(vec![0]);
        let dir = AlterReplicaLogDir::default()
            .with_path(text("/data"))
            .with_topics(vec![topic]);
        AlterReplicaLogDirsRequest::default().with_dirs(vec![dir])
    });
    check(checked, ApiKey::ElectLeaders, |_| {
        let topic = TopicPartitions::default()
            .with_topic(wire_name(ORDERS))
            .with_partitions(vec![0]);
        ElectLeadersRequest::default().with_topic_partitions(Some(vec![topic]))
    });
    check(checked, ApiKey::DescribeTopicPartitions, |_| {
        let topic = describe_topic_partitions_request::TopicRequest::default().with_name(wire_name(ORDERS));
        DescribeTopicPartitionsRequest::default().with_topics(vec![topic])
    });
    check_share_group_state!(
        checked,
        InitializeShareGroupState => InitializeShareGroupStateRequest,
            initialize_share_group_state_request::InitializeStateData,
            initialize_share_group_state_request::PartitionData;
        ReadShareGroupState => ReadShareGroupStateRequest,
            read_share_group_state_request::ReadStateData,
            read_share_group_state_request::PartitionData;
        WriteShareGroupState => WriteShareGroupStateRequest,
            write_share_group_state_request::WriteStateData,
            write_share_group_state_request::PartitionData;
        DeleteShareGroupState => DeleteShareGroupStateRequest,
            delete_share_group_state_request::DeleteStateData,
            delete_share_group_state_request::PartitionData;
        ReadShareGroupStateSummary => ReadShareGroupStateSummaryRequest,
            read_share_group_state_summary_request::ReadStateSummaryData,
            read_share_group_state_summary_request::PartitionData;
    );
}

/// The APIs whose response has an error code of its own.
fn check_the_apis_that_fail_in_the_response_itself(checked: &mut Vec<ApiKey>) {
    check_the_empty_request!(
        checked,
        JoinGroup => JoinGroupRequest,
        Heartbeat => HeartbeatRequest,
        LeaveGroup => LeaveGroupRequest,
        SyncGroup => SyncGroupRequest,
        ListGroups => ListGroupsRequest,
        SaslHandshake => SaslHandshakeRequest,
        ApiVersions => ApiVersionsRequest,
        InitProducerId => InitProducerIdRequest,
        AddOffsetsToTxn => AddOffsetsToTxnRequest,
        EndTxn => EndTxnRequest,
        DescribeAcls => DescribeAclsRequest,
        DescribeLogDirs => DescribeLogDirsRequest,
        SaslAuthenticate => SaslAuthenticateRequest,
        CreateDelegationToken => CreateDelegationTokenRequest,
        RenewDelegationToken => RenewDelegationTokenRequest,
        ExpireDelegationToken => ExpireDelegationTokenRequest,
        DescribeDelegationToken => DescribeDelegationTokenRequest,
        AlterPartitionReassignments => AlterPartitionReassignmentsRequest,
        ListPartitionReassignments => ListPartitionReassignmentsRequest,
        DescribeClientQuotas => DescribeClientQuotasRequest,
        DescribeUserScramCredentials => DescribeUserScramCredentialsRequest,
        Vote => VoteRequest,
        BeginQuorumEpoch => BeginQuorumEpochRequest,
        EndQuorumEpoch => EndQuorumEpochRequest,
        DescribeQuorum => DescribeQuorumRequest,
        AlterPartition => AlterPartitionRequest,
        UpdateFeatures => UpdateFeaturesRequest,
        Envelope => EnvelopeRequest,
        FetchSnapshot => FetchSnapshotRequest,
        DescribeCluster => DescribeClusterRequest,
        BrokerRegistration => BrokerRegistrationRequest,
        BrokerHeartbeat => BrokerHeartbeatRequest,
        UnregisterBroker => UnregisterBrokerRequest,
        ListTransactions => ListTransactionsRequest,
        AllocateProducerIds => AllocateProducerIdsRequest,
        ConsumerGroupHeartbeat => ConsumerGroupHeartbeatRequest,
        ControllerRegistration => ControllerRegistrationRequest,
        GetTelemetrySubscriptions => GetTelemetrySubscriptionsRequest,
        PushTelemetry => PushTelemetryRequest,
        AssignReplicasToDirs => AssignReplicasToDirsRequest,
        ListConfigResources => ListConfigResourcesRequest,
        ShareGroupHeartbeat => ShareGroupHeartbeatRequest,
        ShareFetch => ShareFetchRequest,
        ShareAcknowledge => ShareAcknowledgeRequest,
        AddRaftVoter => AddRaftVoterRequest,
        RemoveRaftVoter => RemoveRaftVoterRequest,
        UpdateRaftVoter => UpdateRaftVoterRequest,
        AlterShareGroupOffsets => AlterShareGroupOffsetsRequest,
        DeleteShareGroupOffsets => DeleteShareGroupOffsetsRequest,
    );
}

#[test]
fn the_error_code_of_a_response_itself_reads_back() {
    let timed_out = ErrorCode::REQUEST_TIMED_OUT;
    let error_of = |request: &Request| {
        let asked = EditableRequest::decode(request, &topic_names()).unwrap();
        let failed = asked.error_response(&Failures::whole_call(timed_out)).unwrap();
        asked.error_of(&failed).unwrap()
    };
    let request = |version: i16, body: &dyn Fn(i16) -> Request| body(version);
    let heartbeat = |version| Request::new(version, 7, None, &HeartbeatRequest::default()).unwrap();
    let produce = |version| Request::new(version, 7, None, &common::produce(version)).unwrap();
    let fetch = |version| Request::new(version, 7, None, &common::fetch(version)).unwrap();
    let find_coordinator = |version| {
        let body = if version >= 4 {
            FindCoordinatorRequest::default().with_coordinator_keys(vec![text("analytics-etl")])
        } else {
            FindCoordinatorRequest::default().with_key(text("analytics-etl"))
        };
        Request::new(version, 7, None, &body).unwrap()
    };

    // A Heartbeat response has an error code of its own.
    assert_eq!(error_of(&request(4, &heartbeat)), Some(timed_out));
    // A Produce response answers each partition, and has none.
    assert_eq!(error_of(&request(9, &produce)), None);
    // A Fetch response has one since version 7.
    assert_eq!(error_of(&request(4, &fetch)), None);
    assert_eq!(error_of(&request(13, &fetch)), Some(timed_out));
    // A FindCoordinator response has one up to version 3. Since version 4 each key fails, and a key is no part.
    assert_eq!(error_of(&request(3, &find_coordinator)), Some(timed_out));
    assert_eq!(error_of(&request(4, &find_coordinator)), None);

    // A response without an error reads as none.
    let asked = EditableRequest::decode(&heartbeat(4), &topic_names()).unwrap();
    let truth = heartbeat(4)
        .response(&gaslightmq_wire::messages::HeartbeatResponse::default())
        .unwrap();
    assert_eq!(asked.error_of(&truth), Ok(None));

    // A response to another request is not read.
    let other = heartbeat(4)
        .response(&gaslightmq_wire::messages::HeartbeatResponse::default())
        .unwrap();
    let asked = EditableRequest::decode(&produce(9), &topic_names()).unwrap();
    assert!(matches!(
        asked.error_of(&other),
        Err(EditableRequestError::ResponseOfAnotherCall { .. })
    ));
}

#[test]
fn the_whole_call_fails_on_every_api_at_every_version() {
    let mut checked = Vec::new();
    check_the_apis_with_parts(&mut checked);
    check_the_apis_that_ask_for_groups_and_transactions(&mut checked);
    check_the_apis_that_ask_for_resources(&mut checked);
    check_the_apis_that_fail_in_the_response_itself(&mut checked);

    // No API of `gaslightmq_wire` went unchecked.
    checked.sort_by_key(|api| *api as i16);
    let every_api: Vec<ApiKey> = ApiKey::iter().collect();
    assert_eq!(checked, every_api);
}
