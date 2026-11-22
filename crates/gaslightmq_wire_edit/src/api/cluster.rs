//! The APIs that brokers and controllers speak among themselves. They have no parts, and only fail as a whole.

use gaslightmq_wire::messages::{
    AddRaftVoterRequest, AlterPartitionRequest, AssignReplicasToDirsRequest, BeginQuorumEpochRequest,
    BrokerHeartbeatRequest, BrokerRegistrationRequest, ControllerRegistrationRequest, DescribeQuorumRequest,
    EndQuorumEpochRequest, EnvelopeRequest, FetchSnapshotRequest, RemoveRaftVoterRequest, UnregisterBrokerRequest,
    UpdateRaftVoterRequest, VoteRequest,
};

use super::fails_as_a_whole;

fails_as_a_whole!(
    AddRaftVoterRequest,
    AlterPartitionRequest,
    AssignReplicasToDirsRequest,
    BeginQuorumEpochRequest,
    BrokerHeartbeatRequest,
    BrokerRegistrationRequest,
    ControllerRegistrationRequest,
    DescribeQuorumRequest,
    EndQuorumEpochRequest,
    EnvelopeRequest,
    FetchSnapshotRequest,
    RemoveRaftVoterRequest,
    UnregisterBrokerRequest,
    UpdateRaftVoterRequest,
    VoteRequest,
);
