//! Being a member of a consumer group.
//!
//! A consumer joins a group, takes its assignment, and then sends heartbeats.
//! All three go to the coordinator of the group.
//! A consumer joins without a subscription, so the assignment of every member is empty.

use std::time::Duration;

use bytes::Bytes;
use gaslightmq_wire::ResponseError;
use gaslightmq_wire::messages::join_group_request::JoinGroupRequestProtocol;
use gaslightmq_wire::messages::sync_group_request::SyncGroupRequestAssignment;
use gaslightmq_wire::messages::{
    GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, SyncGroupRequest,
    SyncGroupResponse,
};
use gaslightmq_wire::protocol::StrBytes;

use crate::configuration::ConsumerConfiguration;
use crate::error::ClientError;
use crate::logic::attempts::Limits;
use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::Route;
use crate::logic::versions::Versions;
use crate::logic::wire_millis;

/// The `protocol_type` of a group of consumers.
const CONSUMER: &str = "consumer";

/// The generation of a member that is in none.
const NO_GENERATION: i32 = -1;

/// A subscription to no topic, as a consumer writes it into `JoinGroup`:
/// version 0, an array of no topics, and no user data.
const SUBSCRIBED_TO_NOTHING: &[u8] = &[0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

/// An assignment of no partition, as the leader of a group writes it into `SyncGroup`:
/// version 0, an array of no topics, and no user data.
const NOTHING_ASSIGNED: &[u8] = &[0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

/// What a consumer knows about its membership of one group.
///
/// It starts as no member at all.
/// Each answer of the coordinator changes it, and each request to the coordinator carries what it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Membership {
    /// The ID the coordinator gave the consumer. Empty until it gave one.
    member_id: StrBytes,
    generation: i32,
    /// The member that leads the group in this generation.
    leader: StrBytes,
    /// Every member of the group, when this consumer leads it. Empty otherwise.
    members: Vec<StrBytes>,
    /// What the leader assigned to this consumer, as it arrived.
    assignment: Bytes,
}

impl Default for Membership {
    fn default() -> Self {
        Self {
            member_id: StrBytes::default(),
            generation: NO_GENERATION,
            leader: StrBytes::default(),
            members: Vec::new(),
            assignment: Bytes::new(),
        }
    }
}

impl Membership {
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }

    pub fn leader(&self) -> &str {
        &self.leader
    }

    /// What the leader of the group assigned to this consumer, as it arrived. Empty until the consumer took it.
    pub fn assignment(&self) -> &Bytes {
        &self.assignment
    }

    /// The request to join `group`, or to join it again under the member ID the consumer has.
    pub fn join<'a>(&self, configuration: &'a ConsumerConfiguration, group: GroupId) -> JoinGroup<'a> {
        JoinGroup {
            configuration,
            group,
            member_id: self.member_id.clone(),
        }
    }

    /// The coordinator let the consumer into a generation of the group.
    pub fn joined(&mut self, joined: Joined) {
        *self = Self {
            member_id: joined.member_id,
            generation: joined.generation,
            leader: joined.leader,
            members: joined.members,
            assignment: Bytes::new(),
        };
    }

    /// The request for the assignment of this generation.
    /// The leader of the group sends the assignment of every member with it: nothing, for each of them.
    pub fn sync(&self, group: GroupId) -> SyncGroup {
        SyncGroup {
            group,
            generation: self.generation,
            member_id: self.member_id.clone(),
            members: self.members.clone(),
        }
    }

    /// The coordinator handed out the assignment of this generation.
    pub fn synced(&mut self, assignment: Bytes) {
        self.assignment = assignment;
    }

    /// The request that tells the coordinator the consumer is alive.
    pub fn heartbeat(&self, group: GroupId) -> Heartbeat {
        Heartbeat {
            group,
            generation: self.generation,
            member_id: self.member_id.clone(),
        }
    }

    /// A join failed with `error`, after the coordinator may have handed the consumer a member ID.
    ///
    /// The consumer keeps that ID and joins under it the next time.
    /// Without it, every further join would leave one more member behind at the coordinator.
    pub fn join_failed(&mut self, join: &JoinGroup<'_>, error: &ClientError) {
        if !join.member_id.is_empty() {
            self.member_id = join.member_id.clone();
        }
        self.refused(error);
    }

    /// The coordinator, or the way to it, failed a request with `error`.
    ///
    /// A coordinator that does not know the member ends the membership: the consumer joins as a new member.
    /// A coordinator that is in another generation ends the generation: the consumer joins again under its ID.
    /// Every other failure leaves the membership as it is.
    pub fn refused(&mut self, error: &ClientError) {
        match error {
            ClientError::Broker {
                error: ResponseError::UnknownMemberId | ResponseError::FencedInstanceId,
            } => {
                *self = Self::default();
            }
            ClientError::Broker {
                error: ResponseError::IllegalGeneration,
            } => {
                *self = Self {
                    member_id: std::mem::take(&mut self.member_id),
                    ..Self::default()
                };
            }
            _ => {}
        }
    }
}

/// What the coordinator answers a member that joined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Joined {
    pub member_id: StrBytes,
    pub generation: i32,
    pub leader: StrBytes,
    /// Every member of the group, when the coordinator made this member the leader. Empty otherwise.
    pub members: Vec<StrBytes>,
}

/// Join a group, or join it again.
pub(crate) struct JoinGroup<'a> {
    configuration: &'a ConsumerConfiguration,
    group: GroupId,
    member_id: StrBytes,
}

impl Operation for JoinGroup<'_> {
    type Request = JoinGroupRequest;
    type Output = Joined;

    /// `JoinGroup` stops before its flexible versions.
    const VERSIONS: Versions = Versions::new(0, 5);

    fn route(&self) -> Route {
        Route::Coordinator(self.group.clone())
    }

    fn request(&self, _within: Duration) -> JoinGroupRequest {
        let configuration = self.configuration;
        let protocols = configuration
            .partition_assignment_strategy
            .iter()
            .map(|assignor| {
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_string(assignor.as_str().to_owned()))
                    .with_metadata(Bytes::from_static(SUBSCRIBED_TO_NOTHING))
            })
            .collect();
        JoinGroupRequest::default()
            .with_group_id(self.group.clone())
            .with_session_timeout_ms(wire_millis(configuration.session_timeout.duration()))
            .with_rebalance_timeout_ms(wire_millis(configuration.max_poll_interval.duration()))
            .with_member_id(self.member_id.clone())
            .with_protocol_type(StrBytes::from_static_str(CONSUMER))
            .with_protocols(protocols)
    }

    /// The coordinator holds a `JoinGroup` until every member joined, or until the rebalance timeout,
    /// which is the max poll interval of the consumer.
    /// The response then still needs the time of a request to arrive.
    fn answer_within(&self, request_timeout: Duration) -> Duration {
        self.configuration
            .max_poll_interval
            .duration()
            .saturating_add(request_timeout)
    }

    /// One try of a join may take longer than the other calls of a consumer, and the call has time for it.
    /// Kafka's own consumer does not bound a join by `default.api.timeout.ms` either.
    fn call_within(&self, limits: &Limits) -> Duration {
        limits.call_timeout.max(self.answer_within(limits.request_timeout))
    }

    /// A coordinator that wants a member ID first hands one out, and the consumer joins again under it.
    fn read(&mut self, response: JoinGroupResponse) -> Result<Reading<Joined>, ClientError> {
        match ClientError::broker(response.error_code) {
            Some(ClientError::Broker {
                error: ResponseError::MemberIdRequired,
            }) if self.member_id.is_empty() && !response.member_id.is_empty() => {
                self.member_id = response.member_id;
                Ok(Reading::Again)
            }
            Some(error) => Err(error),
            None => Ok(Reading::Done(Joined {
                member_id: response.member_id,
                generation: response.generation_id,
                leader: response.leader,
                members: response.members.into_iter().map(|member| member.member_id).collect(),
            })),
        }
    }
}

/// Take the assignment of a generation, and hand out everyone's when this member leads the group.
pub(crate) struct SyncGroup {
    group: GroupId,
    generation: i32,
    member_id: StrBytes,
    members: Vec<StrBytes>,
}

impl Operation for SyncGroup {
    type Request = SyncGroupRequest;
    type Output = Bytes;

    /// `SyncGroup` stops before its flexible versions.
    const VERSIONS: Versions = Versions::new(0, 3);

    fn route(&self) -> Route {
        Route::Coordinator(self.group.clone())
    }

    fn request(&self, _within: Duration) -> SyncGroupRequest {
        let assignments = self
            .members
            .iter()
            .map(|member| {
                SyncGroupRequestAssignment::default()
                    .with_member_id(member.clone())
                    .with_assignment(Bytes::from_static(NOTHING_ASSIGNED))
            })
            .collect();
        SyncGroupRequest::default()
            .with_group_id(self.group.clone())
            .with_generation_id(self.generation)
            .with_member_id(self.member_id.clone())
            .with_assignments(assignments)
    }

    fn read(&mut self, response: SyncGroupResponse) -> Result<Reading<Bytes>, ClientError> {
        match ClientError::broker(response.error_code) {
            Some(error) => Err(error),
            None => Ok(Reading::Done(response.assignment)),
        }
    }
}

/// Tell the coordinator that this member is alive.
pub(crate) struct Heartbeat {
    group: GroupId,
    generation: i32,
    member_id: StrBytes,
}

impl Operation for Heartbeat {
    type Request = HeartbeatRequest;
    type Output = ();

    /// Heartbeat stops before its flexible versions.
    const VERSIONS: Versions = Versions::new(0, 3);

    fn route(&self) -> Route {
        Route::Coordinator(self.group.clone())
    }

    fn request(&self, _within: Duration) -> HeartbeatRequest {
        HeartbeatRequest::default()
            .with_group_id(self.group.clone())
            .with_generation_id(self.generation)
            .with_member_id(self.member_id.clone())
    }

    fn read(&mut self, response: HeartbeatResponse) -> Result<Reading<()>, ClientError> {
        match ClientError::broker(response.error_code) {
            Some(error) => Err(error),
            None => Ok(Reading::Done(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use gaslightmq_clock::{Instant, TimeSpan};
    use gaslightmq_wire::messages::join_group_response::JoinGroupResponseMember;
    use gaslightmq_wire::messages::{BrokerId, ConsumerProtocolAssignment, ConsumerProtocolSubscription};
    use gaslightmq_wire::protocol::Encodable;

    use super::*;
    use crate::address::BootstrapServers;
    use crate::logic::call::tests::limits;
    use crate::logic::call::{Call, Input, Step};
    use crate::logic::cluster::group_id;
    use crate::logic::cluster::tests::view;

    fn configuration() -> ConsumerConfiguration {
        ConsumerConfiguration::new("localhost:9092".parse::<BootstrapServers>().unwrap())
    }

    fn id(text: &'static str) -> StrBytes {
        StrBytes::from_static_str(text)
    }

    fn group() -> GroupId {
        group_id("analytics")
    }

    /// The coordinator's answer to a member it let into generation 7, led by `leader`.
    fn let_in(member: &'static str, leader: &'static str, members: &[&'static str]) -> JoinGroupResponse {
        JoinGroupResponse::default()
            .with_generation_id(7)
            .with_member_id(id(member))
            .with_leader(id(leader))
            .with_members(
                members
                    .iter()
                    .map(|member| JoinGroupResponseMember::default().with_member_id(id(member)))
                    .collect(),
            )
    }

    fn refusal(error: ResponseError) -> ClientError {
        ClientError::Broker { error }
    }

    /// A membership of generation 7 that leads a group of two.
    fn leading() -> Membership {
        let mut membership = Membership::default();
        let read = membership.join(&configuration(), group()).read(let_in(
            "consumer-1",
            "consumer-1",
            &["consumer-1", "consumer-2"],
        ));
        let Ok(Reading::Done(joined)) = read else {
            panic!("the member is in: {read:?}")
        };
        membership.joined(joined);
        membership
    }

    #[test]
    fn a_consumer_writes_an_empty_subscription_and_assignment_as_kafka_does() {
        let mut subscription = BytesMut::from(&0i16.to_be_bytes()[..]);
        ConsumerProtocolSubscription::default()
            .encode(&mut subscription, 0)
            .unwrap();
        let mut assignment = BytesMut::from(&0i16.to_be_bytes()[..]);
        ConsumerProtocolAssignment::default()
            .encode(&mut assignment, 0)
            .unwrap();

        assert_eq!(SUBSCRIBED_TO_NOTHING, &subscription[..]);
        assert_eq!(NOTHING_ASSIGNED, &assignment[..]);
    }

    #[test]
    fn a_new_consumer_joins_without_a_member_id() {
        let mut configuration = configuration();
        configuration.session_timeout = TimeSpan::from_secs(10);
        configuration.max_poll_interval = TimeSpan::from_secs(60);
        configuration.partition_assignment_strategy = "roundrobin,range".parse().unwrap();

        let join = Membership::default().join(&configuration, group());
        let request = join.request(Duration::ZERO);

        assert_eq!(join.route(), Route::Coordinator(group()));
        assert_eq!(request.group_id, group());
        assert_eq!(request.member_id.as_str(), "");
        assert_eq!(request.session_timeout_ms, 10_000);
        assert_eq!(request.rebalance_timeout_ms, 60_000);
        assert_eq!(request.protocol_type.as_str(), "consumer");
        let offered: Vec<_> = request
            .protocols
            .iter()
            .map(|protocol| (protocol.name.as_str(), &protocol.metadata[..]))
            .collect();
        assert_eq!(
            offered,
            [("roundrobin", SUBSCRIBED_TO_NOTHING), ("range", SUBSCRIBED_TO_NOTHING)]
        );
    }

    #[test]
    fn a_consumer_offers_the_assignors_of_kafkas_own_consumer_when_nothing_is_configured() {
        let configuration = configuration();

        let request = Membership::default()
            .join(&configuration, group())
            .request(Duration::ZERO);

        let offered: Vec<_> = request
            .protocols
            .iter()
            .map(|protocol| protocol.name.as_str())
            .collect();
        assert_eq!(offered, ["range", "cooperative-sticky"]);
        assert_eq!(request.session_timeout_ms, 45_000);
        assert_eq!(request.rebalance_timeout_ms, 300_000);
    }

    #[test]
    fn a_join_waits_for_the_rebalance_and_then_for_the_response() {
        let mut configuration = configuration();
        configuration.max_poll_interval = TimeSpan::from_secs(60);

        let join = Membership::default().join(&configuration, group());

        assert_eq!(join.answer_within(Duration::from_secs(30)), Duration::from_secs(90));
    }

    #[test]
    fn a_coordinator_that_wants_a_member_id_first_gets_the_join_again_under_it() {
        let configuration = configuration();
        let mut join = Membership::default().join(&configuration, group());
        let wants_an_id = JoinGroupResponse::default()
            .with_error_code(ResponseError::MemberIdRequired.code())
            .with_member_id(id("consumer-1"));

        assert_eq!(join.read(wants_an_id.clone()), Ok(Reading::Again));
        assert_eq!(join.request(Duration::ZERO).member_id.as_str(), "consumer-1");
        assert_eq!(
            join.read(wants_an_id),
            Err(refusal(ResponseError::MemberIdRequired)),
            "the join asks again once, and never without end"
        );
    }

    #[test]
    fn a_member_id_that_was_handed_out_survives_a_join_that_fails() {
        let configuration = configuration();
        let mut membership = Membership::default();
        let mut join = membership.join(&configuration, group());
        let wants_an_id = JoinGroupResponse::default()
            .with_error_code(ResponseError::MemberIdRequired.code())
            .with_member_id(id("consumer-1"));
        assert_eq!(join.read(wants_an_id), Ok(Reading::Again));

        membership.join_failed(&join, &ClientError::RequestTimedOut);

        assert_eq!(membership.member_id(), "consumer-1");
        assert_eq!(membership.generation(), -1);
        let again = membership.join(&configuration, group()).request(Duration::ZERO);
        assert_eq!(again.member_id.as_str(), "consumer-1");
    }

    #[test]
    fn a_join_that_fails_because_the_member_is_unknown_drops_the_member_id() {
        let configuration = configuration();
        let mut membership = leading();
        let join = membership.join(&configuration, group());

        membership.join_failed(&join, &refusal(ResponseError::UnknownMemberId));

        assert_eq!(membership, Membership::default());
    }

    #[test]
    fn a_join_has_the_time_of_a_rebalance_also_when_other_calls_have_less() {
        let mut configuration = configuration();
        configuration.max_poll_interval = TimeSpan::from_secs(300);
        let mut cluster = view();
        cluster.learn_coordinator(&group(), BrokerId(1), "broker-1", 9092);
        let limits = Limits {
            call_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(30),
            ..limits()
        };
        let join = Membership::default().join(&configuration, group());
        assert_eq!(join.call_within(&limits), Duration::from_secs(330));

        let mut call = Call::new(join, limits, Instant::ZERO);
        let Step::Exchange(joining) = call.handle(Input::Started, &mut cluster, Instant::ZERO) else {
            panic!("the join goes to the coordinator")
        };

        assert_eq!(joining.answer_within, Duration::from_secs(330));
        assert_eq!(joining.not_after, Instant::ZERO + Duration::from_secs(330));
    }

    #[test]
    fn a_join_never_has_less_time_than_the_other_calls() {
        let mut configuration = configuration();
        configuration.max_poll_interval = TimeSpan::from_secs(1);
        let limits = Limits {
            call_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(30),
            ..limits()
        };

        let join = Membership::default().join(&configuration, group());

        assert_eq!(join.call_within(&limits), Duration::from_secs(60));
    }

    #[test]
    fn a_member_remembers_what_the_coordinator_answered() {
        let membership = leading();

        assert_eq!(membership.member_id(), "consumer-1");
        assert_eq!(membership.generation(), 7);
        assert_eq!(membership.leader(), "consumer-1");
    }

    #[test]
    fn a_member_joins_again_under_its_member_id() {
        let configuration = configuration();

        let request = leading().join(&configuration, group()).request(Duration::ZERO);

        assert_eq!(request.member_id.as_str(), "consumer-1");
    }

    #[test]
    fn a_join_the_coordinator_refuses_is_the_failure_of_the_call() {
        let configuration = configuration();
        let mut join = Membership::default().join(&configuration, group());
        let refused = JoinGroupResponse::default().with_error_code(ResponseError::GroupAuthorizationFailed.code());

        assert_eq!(
            join.read(refused),
            Err(refusal(ResponseError::GroupAuthorizationFailed))
        );
    }

    #[test]
    fn the_leader_hands_out_an_empty_assignment_to_every_member() {
        let sync = leading().sync(group());
        let request = sync.request(Duration::ZERO);

        assert_eq!(sync.route(), Route::Coordinator(group()));
        assert_eq!(request.group_id, group());
        assert_eq!(request.generation_id, 7);
        assert_eq!(request.member_id.as_str(), "consumer-1");
        let assigned: Vec<_> = request
            .assignments
            .iter()
            .map(|assignment| (assignment.member_id.as_str(), &assignment.assignment[..]))
            .collect();
        assert_eq!(
            assigned,
            [("consumer-1", NOTHING_ASSIGNED), ("consumer-2", NOTHING_ASSIGNED)]
        );
    }

    #[test]
    fn another_member_hands_out_nothing() {
        let configuration = configuration();
        let mut membership = Membership::default();
        let read = membership
            .join(&configuration, group())
            .read(let_in("consumer-2", "consumer-1", &[]));
        let Ok(Reading::Done(joined)) = read else {
            panic!("the member is in: {read:?}")
        };
        membership.joined(joined);

        let request = membership.sync(group()).request(Duration::ZERO);

        assert_eq!(request.member_id.as_str(), "consumer-2");
        assert_eq!(request.assignments, []);
    }

    #[test]
    fn a_member_keeps_the_assignment_it_was_given() {
        let mut membership = leading();
        let response = SyncGroupResponse::default().with_assignment(Bytes::from_static(NOTHING_ASSIGNED));

        let read = membership.sync(group()).read(response);
        let Ok(Reading::Done(assignment)) = read else {
            panic!("the assignment is there: {read:?}")
        };
        membership.synced(assignment);

        assert_eq!(&membership.assignment()[..], NOTHING_ASSIGNED);
    }

    #[test]
    fn a_heartbeat_carries_the_member_and_its_generation() {
        let heartbeat = leading().heartbeat(group());
        let request = heartbeat.request(Duration::ZERO);

        assert_eq!(heartbeat.route(), Route::Coordinator(group()));
        assert_eq!(request.group_id, group());
        assert_eq!(request.generation_id, 7);
        assert_eq!(request.member_id.as_str(), "consumer-1");
    }

    #[test]
    fn a_consumer_that_never_joined_sends_a_heartbeat_of_no_member() {
        let request = Membership::default().heartbeat(group()).request(Duration::ZERO);

        assert_eq!(request.generation_id, -1);
        assert_eq!(request.member_id.as_str(), "");
    }

    #[test]
    fn the_error_of_a_heartbeat_or_a_sync_is_the_failure_of_the_call() {
        let rebalancing = ResponseError::RebalanceInProgress;

        assert_eq!(
            leading()
                .heartbeat(group())
                .read(HeartbeatResponse::default().with_error_code(rebalancing.code())),
            Err(refusal(rebalancing))
        );
        assert_eq!(
            leading()
                .sync(group())
                .read(SyncGroupResponse::default().with_error_code(rebalancing.code())),
            Err(refusal(rebalancing))
        );
        assert_eq!(
            leading().heartbeat(group()).read(HeartbeatResponse::default()),
            Ok(Reading::Done(()))
        );
    }

    #[test]
    fn a_coordinator_that_does_not_know_the_member_ends_the_membership() {
        for error in [ResponseError::UnknownMemberId, ResponseError::FencedInstanceId] {
            let mut membership = leading();

            membership.refused(&refusal(error));

            assert_eq!(membership, Membership::default());
        }
    }

    #[test]
    fn a_coordinator_in_another_generation_ends_the_generation_and_keeps_the_member() {
        let mut membership = leading();

        membership.refused(&refusal(ResponseError::IllegalGeneration));

        assert_eq!(membership.member_id(), "consumer-1");
        assert_eq!(membership.generation(), -1);
        assert_eq!(membership.sync(group()).request(Duration::ZERO).assignments, []);
    }

    #[test]
    fn a_rebalance_or_a_lost_connection_leaves_the_membership_as_it_is() {
        let mut membership = leading();

        membership.refused(&refusal(ResponseError::RebalanceInProgress));
        membership.refused(&ClientError::ConnectionFailed("reset".into()));
        membership.refused(&ClientError::CallTimedOut {
            error: Box::new(refusal(ResponseError::UnknownMemberId)),
        });

        assert_eq!(membership, leading());
    }
}
