//! What every API has to say for itself, and the decoded body of a request of any API.
//!
//! [`Editable`] is the one place that knows a Kafka API: each request message implements it in [`crate::api`].
//! [`Body`] hides which message a request holds, so [`EditableRequest`](crate::EditableRequest) works on any API.

use std::fmt;
use std::sync::Arc;

use gaslightmq_wire::messages::ApiKey as KnownApi;
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Request, Response, WireError};

/// A topic as an entry of a request names it: by name, by ID, or by both.
/// The one it does not use is empty, or all zeros.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TopicKey<'a> {
    pub name: &'a str,
    pub id: [u8; 16],
}

impl<'a> TopicKey<'a> {
    pub fn new(name: &'a str, id: [u8; 16]) -> Self {
        Self { name, id }
    }

    pub fn named(name: &'a str) -> Self {
        Self { name, id: [0; 16] }
    }
}

/// Whether two entries name the same topic: by the same name, or by the same ID.
pub(crate) fn same_topic(one: TopicKey<'_>, other: TopicKey<'_>) -> bool {
    (!one.name.is_empty() && one.name == other.name) || (one.id != [0; 16] && one.id == other.id)
}

/// Says for each part of a request whether it stays, and counts what stays and what goes.
pub(crate) struct Keep<'a> {
    decide: &'a mut dyn FnMut(TopicKey<'_>, Option<i32>) -> bool,
    kept: usize,
    gone: usize,
}

impl<'a> Keep<'a> {
    pub fn new(decide: &'a mut dyn FnMut(TopicKey<'_>, Option<i32>) -> bool) -> Self {
        Self {
            decide,
            kept: 0,
            gone: 0,
        }
    }

    /// Whether a topic stays, on a request whose parts are topics.
    pub fn topic(&mut self, topic: TopicKey<'_>) -> bool {
        self.count(topic, None)
    }

    /// Whether a partition stays.
    pub fn partition(&mut self, topic: TopicKey<'_>, index: i32) -> bool {
        self.count(topic, Some(index))
    }

    /// Keeps the partitions that stay, and says whether their topic stays.
    /// A topic goes when it lost its last partition.
    pub fn partitions<P>(&mut self, topic: TopicKey<'_>, partitions: &mut Vec<P>, index: impl Fn(&P) -> i32) -> bool {
        let before = partitions.len();
        partitions.retain(|partition| self.partition(topic, index(partition)));
        partitions.len() == before || !partitions.is_empty()
    }

    /// Notes that the request asks for something that is no part, such as every topic of a group.
    /// It stays, so the request still asks for something when every part is gone.
    pub fn something_else(&mut self) {
        self.kept += 1;
    }

    fn count(&mut self, topic: TopicKey<'_>, partition: Option<i32>) -> bool {
        let stays = (self.decide)(topic, partition);
        if stays {
            self.kept += 1;
        } else {
            self.gone += 1;
        }
        stays
    }
}

/// What is left of a request after parts were removed.
pub(crate) enum Left {
    /// Nothing was removed.
    Everything,
    /// The request without the removed parts.
    Rest(Request),
    /// The request asks for nothing any more.
    Nothing,
}

/// Says what fails: the error code of the whole call, and the error code of each part.
pub(crate) struct Verdict<'a> {
    call: i16,
    part: &'a dyn Fn(TopicKey<'_>, Option<i32>) -> Option<i16>,
}

impl<'a> Verdict<'a> {
    pub fn new(call: i16, part: &'a dyn Fn(TopicKey<'_>, Option<i32>) -> Option<i16>) -> Self {
        Self { call, part }
    }

    /// The error code of the whole call, or zero when only parts fail.
    pub fn call(&self) -> i16 {
        self.call
    }

    /// The error code of a topic, on a request whose parts are topics. `None` leaves the topic out of the response.
    pub fn topic(&self, topic: TopicKey<'_>) -> Option<i16> {
        (self.part)(topic, None)
    }

    /// The error code of a partition. `None` leaves the partition out of the response.
    pub fn partition(&self, topic: TopicKey<'_>, index: i32) -> Option<i16> {
        (self.part)(topic, Some(index))
    }

    /// The failed partitions of one topic, each built by `failed` from its index and its error code.
    /// `None` leaves the topic out of the response, because none of its partitions fails.
    pub fn partitions<P>(
        &self,
        topic: TopicKey<'_>,
        indexes: impl IntoIterator<Item = i32>,
        failed: impl Fn(i32, i16) -> P,
    ) -> Option<Vec<P>> {
        let partitions: Vec<P> = indexes
            .into_iter()
            .filter_map(|index| Some(failed(index, self.partition(topic, index)?)))
            .collect();
        (!partitions.is_empty()).then_some(partitions)
    }
}

/// What a request message knows about its API, so that surgery works on it.
///
/// Every request message that `gaslightmq_wire` decodes implements it.
/// An API without [parts](gaslightmq_field::Part) only says how its response fails.
/// An API with parts also removes them, and merges two responses into one.
pub(crate) trait Editable: KafkaRequest {
    /// Removes the parts that `keep` does not keep. A request without parts keeps everything.
    fn retain_parts(&mut self, _version: i16, _keep: &mut Keep<'_>) {}

    /// The response to this request in which what `verdict` names failed.
    ///
    /// The whole call fails with [`Verdict::call`], wherever the response has a place for it.
    /// A part is in the response when the verdict has an error code for it.
    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> Self::Response;

    /// Puts the parts of `failed` into `truth`: a part of both is replaced, and another one is added.
    fn merge(_truth: &mut Self::Response, _failed: Self::Response) {}

    /// Puts the parts of `response` in the order that `asked` has them.
    fn order(_response: &mut Self::Response, _asked: &Self::Response) {}

    /// Shows every part of `response` with its error code. A response without parts shows nothing.
    fn each_answered_part(_response: &Self::Response, _see: &mut dyn FnMut(TopicKey<'_>, Option<i32>, i16)) {}

    /// The error code of `response` itself, or zero when it has none.
    ///
    /// An API whose response has an error code of its own says so with
    /// [`response_has_error_code!`](crate::api::response_has_error_code).
    /// The field is zero on a version that does not write it.
    fn error_of_response(_response: &Self::Response) -> i16 {
        0
    }
}

/// Why a request and a response did not merge.
pub(crate) enum MergeFlaw {
    UnreadableResponse(WireError),
    UnwritableResponse(WireError),
}

/// The decoded body of a request, of whichever API.
pub(crate) trait Body: fmt::Debug + Send + Sync {
    /// Shows every part of the request, in order.
    fn each_part(&self, version: i16, see: &mut dyn FnMut(TopicKey<'_>, Option<i32>));

    /// `request` without the parts of which `stays` says that they go.
    fn without(
        &self,
        request: &Request,
        stays: &mut dyn FnMut(TopicKey<'_>, Option<i32>) -> bool,
    ) -> Result<Left, WireError>;

    /// The response to `request` in which what `verdict` names failed.
    fn failed(&self, request: &Request, verdict: &Verdict<'_>) -> Result<Response, WireError>;

    /// `truth` with the parts that `verdict` names failed, in the order of the request.
    fn merged(&self, request: &Request, truth: &Response, verdict: &Verdict<'_>) -> Result<Response, MergeFlaw>;

    /// Shows every part of `response`, a response to the request, with its error code.
    fn each_answered_part(
        &self,
        response: &Response,
        see: &mut dyn FnMut(TopicKey<'_>, Option<i32>, i16),
    ) -> Result<(), WireError>;

    /// The error code of `response` itself, a response to the request, or zero when it has none.
    fn error_of_response(&self, response: &Response) -> Result<i16, WireError>;
}

impl<R> Body for R
where
    R: Editable + Clone + fmt::Debug + Send + Sync,
{
    fn each_part(&self, version: i16, see: &mut dyn FnMut(TopicKey<'_>, Option<i32>)) {
        let mut see_and_keep = |topic: TopicKey<'_>, partition: Option<i32>| {
            see(topic, partition);
            true
        };
        self.clone().retain_parts(version, &mut Keep::new(&mut see_and_keep));
    }

    fn without(
        &self,
        request: &Request,
        stays: &mut dyn FnMut(TopicKey<'_>, Option<i32>) -> bool,
    ) -> Result<Left, WireError> {
        let api = request.api();
        let mut rest = self.clone();
        let mut keep = Keep::new(stays);
        rest.retain_parts(api.version, &mut keep);
        match (keep.gone, keep.kept) {
            (0, _) => Ok(Left::Everything),
            (_, 0) => Ok(Left::Nothing),
            _ => Request::new(api.version, request.correlation_id(), request.client_id(), &rest).map(Left::Rest),
        }
    }

    fn failed(&self, request: &Request, verdict: &Verdict<'_>) -> Result<Response, WireError> {
        request.response(&Editable::failed(self, request.api().version, verdict))
    }

    fn merged(&self, request: &Request, truth: &Response, verdict: &Verdict<'_>) -> Result<Response, MergeFlaw> {
        let version = request.api().version;
        let mut response: R::Response = truth.body().map_err(MergeFlaw::UnreadableResponse)?;
        R::merge(&mut response, Editable::failed(self, version, verdict));
        // The request with every part failed has every part in the order the client asked for it.
        let every_part = |_: TopicKey<'_>, _: Option<i32>| Some(0);
        R::order(
            &mut response,
            &Editable::failed(self, version, &Verdict::new(0, &every_part)),
        );
        Response::new(truth.api(), truth.correlation_id(), &response).map_err(MergeFlaw::UnwritableResponse)
    }

    fn each_answered_part(
        &self,
        response: &Response,
        see: &mut dyn FnMut(TopicKey<'_>, Option<i32>, i16),
    ) -> Result<(), WireError> {
        R::each_answered_part(&response.body()?, see);
        Ok(())
    }

    fn error_of_response(&self, response: &Response) -> Result<i16, WireError> {
        Ok(R::error_of_response(&response.body()?))
    }
}

/// Lists every API with its request message, and decodes the body of a request by its API key.
///
/// The `match` has no wildcard arm.
/// When `gaslightmq_wire` learns a new API, this stops compiling until the API is on the list,
/// and its message stops compiling until it implements [`Editable`].
macro_rules! decode_by_api {
    ($($api:ident => $request:ident,)*) => {
        /// Decodes the body of `request` as the request message of `api`.
        pub(crate) fn decode(api: KnownApi, request: &Request) -> Result<Arc<dyn Body>, WireError> {
            use gaslightmq_wire::messages::{$($request,)*};
            Ok(match api {
                $(KnownApi::$api => Arc::new(request.body::<$request>()?),)*
            })
        }
    };
}

decode_by_api! {
    Produce => ProduceRequest,
    Fetch => FetchRequest,
    ListOffsets => ListOffsetsRequest,
    Metadata => MetadataRequest,
    OffsetCommit => OffsetCommitRequest,
    OffsetFetch => OffsetFetchRequest,
    FindCoordinator => FindCoordinatorRequest,
    JoinGroup => JoinGroupRequest,
    Heartbeat => HeartbeatRequest,
    LeaveGroup => LeaveGroupRequest,
    SyncGroup => SyncGroupRequest,
    DescribeGroups => DescribeGroupsRequest,
    ListGroups => ListGroupsRequest,
    SaslHandshake => SaslHandshakeRequest,
    ApiVersions => ApiVersionsRequest,
    CreateTopics => CreateTopicsRequest,
    DeleteTopics => DeleteTopicsRequest,
    DeleteRecords => DeleteRecordsRequest,
    InitProducerId => InitProducerIdRequest,
    OffsetForLeaderEpoch => OffsetForLeaderEpochRequest,
    AddPartitionsToTxn => AddPartitionsToTxnRequest,
    AddOffsetsToTxn => AddOffsetsToTxnRequest,
    EndTxn => EndTxnRequest,
    WriteTxnMarkers => WriteTxnMarkersRequest,
    TxnOffsetCommit => TxnOffsetCommitRequest,
    DescribeAcls => DescribeAclsRequest,
    CreateAcls => CreateAclsRequest,
    DeleteAcls => DeleteAclsRequest,
    DescribeConfigs => DescribeConfigsRequest,
    AlterConfigs => AlterConfigsRequest,
    AlterReplicaLogDirs => AlterReplicaLogDirsRequest,
    DescribeLogDirs => DescribeLogDirsRequest,
    SaslAuthenticate => SaslAuthenticateRequest,
    CreatePartitions => CreatePartitionsRequest,
    CreateDelegationToken => CreateDelegationTokenRequest,
    RenewDelegationToken => RenewDelegationTokenRequest,
    ExpireDelegationToken => ExpireDelegationTokenRequest,
    DescribeDelegationToken => DescribeDelegationTokenRequest,
    DeleteGroups => DeleteGroupsRequest,
    ElectLeaders => ElectLeadersRequest,
    IncrementalAlterConfigs => IncrementalAlterConfigsRequest,
    AlterPartitionReassignments => AlterPartitionReassignmentsRequest,
    ListPartitionReassignments => ListPartitionReassignmentsRequest,
    OffsetDelete => OffsetDeleteRequest,
    DescribeClientQuotas => DescribeClientQuotasRequest,
    AlterClientQuotas => AlterClientQuotasRequest,
    DescribeUserScramCredentials => DescribeUserScramCredentialsRequest,
    AlterUserScramCredentials => AlterUserScramCredentialsRequest,
    Vote => VoteRequest,
    BeginQuorumEpoch => BeginQuorumEpochRequest,
    EndQuorumEpoch => EndQuorumEpochRequest,
    DescribeQuorum => DescribeQuorumRequest,
    AlterPartition => AlterPartitionRequest,
    UpdateFeatures => UpdateFeaturesRequest,
    Envelope => EnvelopeRequest,
    FetchSnapshot => FetchSnapshotRequest,
    DescribeCluster => DescribeClusterRequest,
    DescribeProducers => DescribeProducersRequest,
    BrokerRegistration => BrokerRegistrationRequest,
    BrokerHeartbeat => BrokerHeartbeatRequest,
    UnregisterBroker => UnregisterBrokerRequest,
    DescribeTransactions => DescribeTransactionsRequest,
    ListTransactions => ListTransactionsRequest,
    AllocateProducerIds => AllocateProducerIdsRequest,
    ConsumerGroupHeartbeat => ConsumerGroupHeartbeatRequest,
    ConsumerGroupDescribe => ConsumerGroupDescribeRequest,
    ControllerRegistration => ControllerRegistrationRequest,
    GetTelemetrySubscriptions => GetTelemetrySubscriptionsRequest,
    PushTelemetry => PushTelemetryRequest,
    AssignReplicasToDirs => AssignReplicasToDirsRequest,
    ListConfigResources => ListConfigResourcesRequest,
    DescribeTopicPartitions => DescribeTopicPartitionsRequest,
    ShareGroupHeartbeat => ShareGroupHeartbeatRequest,
    ShareGroupDescribe => ShareGroupDescribeRequest,
    ShareFetch => ShareFetchRequest,
    ShareAcknowledge => ShareAcknowledgeRequest,
    AddRaftVoter => AddRaftVoterRequest,
    RemoveRaftVoter => RemoveRaftVoterRequest,
    UpdateRaftVoter => UpdateRaftVoterRequest,
    InitializeShareGroupState => InitializeShareGroupStateRequest,
    ReadShareGroupState => ReadShareGroupStateRequest,
    WriteShareGroupState => WriteShareGroupStateRequest,
    DeleteShareGroupState => DeleteShareGroupStateRequest,
    ReadShareGroupStateSummary => ReadShareGroupStateSummaryRequest,
    DescribeShareGroupOffsets => DescribeShareGroupOffsetsRequest,
    AlterShareGroupOffsets => AlterShareGroupOffsetsRequest,
    DeleteShareGroupOffsets => DeleteShareGroupOffsetsRequest,
}
