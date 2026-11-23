//! A consumer as a member of consumer groups, where the coordinator has to be held still or made to refuse.
//!
//! Joining, syncing and heartbeating with a real coordinator is in `tests/kafka`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gaslightmq_kafka_client_native::{ClientError, KafkaConsumer};
use gaslightmq_wire::messages::{HeartbeatRequest, JoinGroupRequest, JoinGroupResponse};
use gaslightmq_wire::{ApiKey, ResponseError};

use crate::support::{Arrival, Cluster, FakeBroker};

/// The member ID a heartbeat names.
fn member_of(arrival: &Arrival) -> String {
    let heartbeat: HeartbeatRequest = arrival.request.body().unwrap();
    heartbeat.member_id.as_str().to_owned()
}

#[tokio::test]
async fn two_members_of_one_group_talk_to_the_coordinator_on_connections_of_their_own() {
    let (_, broker) = Cluster::of_one().await;
    let (one, other) = (
        KafkaConsumer::from(broker.address()),
        KafkaConsumer::from(broker.address()),
    );

    one.join_group("analytics").await.unwrap();
    other.join_group("analytics").await.unwrap();
    one.heartbeat("analytics").await.unwrap();
    other.heartbeat("analytics").await.unwrap();

    let heartbeats = broker.arrivals_of(ApiKey::Heartbeat);
    assert_eq!(member_of(&heartbeats[0]), "member-1");
    assert_eq!(member_of(&heartbeats[1]), "member-2");
    assert_ne!(heartbeats[0].connection, heartbeats[1].connection);
    // Each member found the coordinator itself.
    assert_eq!(broker.arrivals_of(ApiKey::FindCoordinator).len(), 2);
}

#[tokio::test]
async fn a_member_id_that_was_handed_out_is_used_again_after_a_join_that_failed() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let refuse_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let refuse_once = Arc::clone(&refuse_once);
        async move {
            if request.api().known() == Some(ApiKey::JoinGroup) {
                let joining: JoinGroupRequest = request.body().unwrap();
                if !joining.member_id.is_empty() && refuse_once.swap(false, Ordering::Relaxed) {
                    let refused =
                        JoinGroupResponse::default().with_error_code(ResponseError::GroupAuthorizationFailed.code());
                    return request.response(&refused).unwrap();
                }
            }
            cluster.answer(&request)
        }
    });
    let consumer = KafkaConsumer::from(broker.address());

    let refused = consumer.join_group("analytics").await;
    let joined = consumer.join_group("analytics").await.unwrap();

    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::GroupAuthorizationFailed
        })
    );
    assert_eq!(joined.member_id, "member-1");
    let member_ids: Vec<String> = broker
        .arrivals_of(ApiKey::JoinGroup)
        .iter()
        .map(|arrival| {
            let joining: JoinGroupRequest = arrival.request.body().unwrap();
            joining.member_id.as_str().to_owned()
        })
        .collect();
    assert_eq!(member_ids, ["", "member-1", "member-1"]);
}
