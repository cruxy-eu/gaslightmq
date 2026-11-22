//! A consumer as a member of consumer groups.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::ProtocolConsumer;
use gaslightmq_kafka_client_native::{ClientError, ConsumerConfiguration, JoinedGroup, KafkaConsumer};
use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, SyncGroupRequest};
use gaslightmq_wire::{ApiKey, ResponseError};

use crate::support::{Arrival, Cluster, FakeBroker};

fn heartbeat_of(arrival: &Arrival) -> (String, String, i32) {
    let heartbeat: HeartbeatRequest = arrival.request.body().unwrap();
    (
        heartbeat.group_id.as_str().to_owned(),
        heartbeat.member_id.as_str().to_owned(),
        heartbeat.generation_id,
    )
}

#[tokio::test]
async fn a_consumer_tells_its_coordinator_what_the_coordinator_told_it() {
    let (_, broker) = Cluster::of_one().await;
    let mut configuration = ConsumerConfiguration::new(broker.address());
    configuration.session_timeout = TimeSpan::from_secs(10);
    configuration.max_poll_interval = TimeSpan::from_secs(20);
    configuration.partition_assignment_strategy = "roundrobin,range".parse().unwrap();
    let consumer = KafkaConsumer::new(configuration);

    let joined = consumer.join_group("analytics").await.unwrap();
    let assignment = consumer.sync_group("analytics").await.unwrap();
    consumer.heartbeat("analytics").await.unwrap();

    assert_eq!(
        joined,
        JoinedGroup {
            member_id: "member-1".into(),
            generation: 1,
            leader: "member-1".into()
        }
    );
    // The coordinator hands out a member ID first, and the consumer joins again under it.
    let joins: Vec<JoinGroupRequest> = broker
        .arrivals_of(ApiKey::JoinGroup)
        .iter()
        .map(|arrival| arrival.request.body().unwrap())
        .collect();
    let member_ids: Vec<&str> = joins.iter().map(|join| join.member_id.as_str()).collect();
    assert_eq!(member_ids, ["", "member-1"]);
    assert_eq!(joins[0].session_timeout_ms, 10_000);
    assert_eq!(joins[0].rebalance_timeout_ms, 20_000);
    assert_eq!(joins[0].protocol_type.as_str(), "consumer");
    let offered: Vec<&str> = joins[0]
        .protocols
        .iter()
        .map(|protocol| protocol.name.as_str())
        .collect();
    assert_eq!(offered, ["roundrobin", "range"]);
    // As the leader of the group, it hands out the assignment of its one member.
    let sync: SyncGroupRequest = broker.arrivals_of(ApiKey::SyncGroup)[0].request.body().unwrap();
    assert_eq!(sync.member_id.as_str(), "member-1");
    assert_eq!(sync.generation_id, 1);
    assert_eq!(sync.assignments.len(), 1);
    assert_eq!(sync.assignments[0].member_id.as_str(), "member-1");
    assert_eq!(assignment, sync.assignments[0].assignment);
    assert!(!assignment.is_empty());
    assert_eq!(
        heartbeat_of(&broker.arrivals_of(ApiKey::Heartbeat)[0]),
        ("analytics".to_owned(), "member-1".to_owned(), 1)
    );
}

#[tokio::test]
async fn two_consumers_in_one_group_are_two_members() {
    let (_, broker) = Cluster::of_one().await;
    let (one, other) = (
        KafkaConsumer::from(broker.address()),
        KafkaConsumer::from(broker.address()),
    );

    let first = one.join_group("analytics").await.unwrap();
    let second = other.join_group("analytics").await.unwrap();
    one.heartbeat("analytics").await.unwrap();
    other.heartbeat("analytics").await.unwrap();

    assert_eq!(first.member_id, "member-1");
    assert_eq!(second.member_id, "member-2");
    let heartbeats = broker.arrivals_of(ApiKey::Heartbeat);
    assert_eq!(heartbeat_of(&heartbeats[0]).1, "member-1");
    assert_eq!(heartbeat_of(&heartbeats[1]).1, "member-2");
    // Each member talks to the coordinator on its own connection, and found the coordinator itself.
    assert_ne!(heartbeats[0].connection, heartbeats[1].connection);
    assert_eq!(broker.arrivals_of(ApiKey::FindCoordinator).len(), 2);
}

#[tokio::test]
async fn a_consumer_is_a_member_of_each_group_it_joins() {
    let (_, broker) = Cluster::of_one().await;
    let consumer = KafkaConsumer::from(broker.address());

    let analytics = consumer.join_group("analytics").await.unwrap();
    let billing = consumer.join_group("billing").await.unwrap();
    consumer.heartbeat("billing").await.unwrap();
    consumer.heartbeat("analytics").await.unwrap();

    assert_ne!(analytics.member_id, billing.member_id);
    let heartbeats: Vec<_> = broker.arrivals_of(ApiKey::Heartbeat).iter().map(heartbeat_of).collect();
    assert_eq!(
        heartbeats,
        [
            ("billing".to_owned(), billing.member_id, 1),
            ("analytics".to_owned(), analytics.member_id, 1)
        ]
    );
}

#[tokio::test]
async fn a_coordinator_that_forgot_the_member_makes_the_consumer_join_as_a_new_one() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let forgot_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let forgot_once = Arc::clone(&forgot_once);
        async move {
            if request.api().known() == Some(ApiKey::Heartbeat) && forgot_once.swap(false, Ordering::Relaxed) {
                let unknown = HeartbeatResponse::default().with_error_code(ResponseError::UnknownMemberId.code());
                return request.response(&unknown).unwrap();
            }
            cluster.answer(&request)
        }
    });
    let consumer = KafkaConsumer::from(broker.address());
    consumer.join_group("analytics").await.unwrap();

    let forgotten = consumer.heartbeat("analytics").await;
    let joined_again = consumer.join_group("analytics").await.unwrap();

    assert_eq!(
        forgotten,
        Err(ClientError::Broker {
            error: ResponseError::UnknownMemberId
        })
    );
    assert_eq!(joined_again.member_id, "member-2");
    let joins = broker.arrivals_of(ApiKey::JoinGroup);
    let third: JoinGroupRequest = joins[2].request.body().unwrap();
    assert_eq!(
        third.member_id.as_str(),
        "",
        "the consumer joins without its old member ID"
    );
}

#[tokio::test]
async fn the_group_methods_of_the_trait_carry_what_the_coordinator_answered() {
    let (_, broker) = Cluster::of_one().await;
    let consumer = KafkaConsumer::from(broker.address());

    let joined = ProtocolConsumer::join_group(&consumer, "analytics").await;
    let synced = ProtocolConsumer::sync_group(&consumer, "analytics").await;
    let alive = ProtocolConsumer::heartbeat(&consumer, "analytics").await;

    assert_eq!(joined.error, None);
    assert_eq!(joined.member_id, "member-1");
    assert_eq!(joined.leader, "member-1");
    assert_eq!(synced.error, None);
    assert_eq!(alive.error, None);
}

#[tokio::test]
async fn a_heartbeat_of_a_consumer_that_never_joined_names_no_member() {
    let (_, broker) = Cluster::of_one().await;
    let consumer = KafkaConsumer::from(broker.address());

    consumer.heartbeat("analytics").await.unwrap();

    assert_eq!(
        heartbeat_of(&broker.arrivals_of(ApiKey::Heartbeat)[0]),
        ("analytics".to_owned(), String::new(), -1)
    );
}
