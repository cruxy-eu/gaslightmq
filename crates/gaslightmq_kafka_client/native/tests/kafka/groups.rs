//! A consumer as a member of consumer groups, with a real coordinator.

use std::time::Duration;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::ProtocolConsumer;
use gaslightmq_kafka_client_native::{ClientError, KafkaConsumer};
use gaslightmq_test_kafka::unique_name;
use gaslightmq_wire::ResponseError;
use tokio::time::timeout;

use crate::{SHORTEST_SESSION_MS, consumer, consuming, kafka};

/// How long a test waits for a group to do what it should do anyway.
const PATIENCE: Duration = Duration::from_secs(30);

fn refused_with(error: ResponseError) -> ClientError {
    ClientError::Broker { error }
}

/// Send heartbeats until the coordinator answers one with an error, and return that error.
async fn first_refused_heartbeat(consumer: &KafkaConsumer, group: &str) -> ClientError {
    let refused = async {
        loop {
            if let Err(error) = consumer.heartbeat(group).await {
                return error;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    timeout(PATIENCE, refused).await.unwrap()
}

#[tokio::test]
async fn a_consumer_joins_a_group_takes_its_assignment_and_stays_alive() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let consumer = consumer(cluster);

    let joined = consumer.join_group(&group).await?;
    let assignment = consumer.sync_group(&group).await?;
    let alive = consumer.heartbeat(&group).await;

    // It is the only member, so it leads the first generation of the group.
    assert_ne!(joined.member_id, "");
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(joined.generation, 1);
    // As the leader it handed out its own assignment, and the coordinator gave it back.
    assert!(!assignment.is_empty());
    // The coordinator only takes a heartbeat that names the member and the generation it knows.
    assert_eq!(alive, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_second_member_makes_the_group_rebalance_into_a_generation_of_two() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let (one, other) = (consumer(cluster), consumer(cluster));
    let alone = one.join_group(&group).await?;
    one.sync_group(&group).await?;

    // The coordinator holds the join of the second member until the first one has joined again.
    let joining = other.join_group(&group);
    tokio::pin!(joining);
    let told = tokio::select! {
        joined = &mut joining => panic!("the coordinator waits for the first member: {joined:?}"),
        told = first_refused_heartbeat(&one, &group) => told,
    };
    let (again, second) = tokio::join!(one.join_group(&group), joining);
    let (again, second) = (again?, second?);

    assert_eq!(told, refused_with(ResponseError::RebalanceInProgress));
    assert_eq!(again.member_id, alone.member_id);
    assert_ne!(second.member_id, alone.member_id);
    assert_eq!((again.generation, second.generation), (2, 2));
    assert_eq!(again.leader, second.leader);
    // Both take their assignment, and both stay alive in the new generation.
    let (first_assignment, second_assignment) = tokio::join!(one.sync_group(&group), other.sync_group(&group));
    assert!(first_assignment.is_ok(), "{first_assignment:?}");
    assert!(second_assignment.is_ok(), "{second_assignment:?}");
    assert_eq!(one.heartbeat(&group).await, Ok(()));
    assert_eq!(other.heartbeat(&group).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_join_waits_for_the_other_members_longer_than_any_other_call_may_take() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let one = consumer(cluster);
    one.join_group(&group).await?;
    one.sync_group(&group).await?;
    let mut impatient = consuming(cluster);
    impatient.client.request_timeout = TimeSpan::from_millis(400);
    impatient.default_api_timeout = TimeSpan::from_millis(400);
    let other = KafkaConsumer::new(impatient);

    // The first member takes its time before it joins again, and the coordinator holds the second join that long.
    let rejoin = async {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        one.join_group(&group).await
    };
    let (again, second) = tokio::join!(rejoin, other.join_group(&group));

    assert_eq!(again.map(|joined| joined.generation), Ok(2));
    assert_eq!(second.map(|joined| joined.generation), Ok(2));
    Ok(())
}

#[tokio::test]
async fn a_consumer_is_a_member_of_each_group_it_joins() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let (analytics, billing) = (unique_name("analytics"), unique_name("billing"));
    let consumer = consumer(cluster);

    let in_analytics = consumer.join_group(&analytics).await?;
    let in_billing = consumer.join_group(&billing).await?;
    consumer.sync_group(&analytics).await?;
    consumer.sync_group(&billing).await?;

    assert_ne!(in_analytics.member_id, in_billing.member_id);
    // Each coordinator knows the consumer under the member ID of its own group.
    assert_eq!(consumer.heartbeat(&billing).await, Ok(()));
    assert_eq!(consumer.heartbeat(&analytics).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_member_whose_session_ran_out_is_forgotten_and_joins_as_a_new_one() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let mut configuration = consuming(cluster);
    configuration.session_timeout = TimeSpan::from_millis(SHORTEST_SESSION_MS);
    let consumer = KafkaConsumer::new(configuration);
    let joined = consumer.join_group(&group).await?;
    consumer.sync_group(&group).await?;

    // No heartbeat for much longer than the session lasts.
    tokio::time::sleep(Duration::from_millis(SHORTEST_SESSION_MS * 5 / 2)).await;
    let forgotten = consumer.heartbeat(&group).await;
    let joined_again = consumer.join_group(&group).await?;

    assert_eq!(forgotten, Err(refused_with(ResponseError::UnknownMemberId)));
    assert_ne!(joined_again.member_id, joined.member_id);
    assert_eq!(consumer.sync_group(&group).await.map(drop), Ok(()));
    assert_eq!(consumer.heartbeat(&group).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_consumer_that_never_joined_is_no_member_for_the_coordinator() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let consumer = consumer(cluster);

    let alive = consumer.heartbeat(&group).await;
    let assignment = consumer.sync_group(&group).await;

    assert_eq!(alive, Err(refused_with(ResponseError::UnknownMemberId)));
    assert_eq!(assignment, Err(refused_with(ResponseError::UnknownMemberId)));
    Ok(())
}

#[tokio::test]
async fn a_consumer_that_offers_no_assignor_of_the_group_is_not_let_in() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let offering = |assignors| {
        let mut configuration = consuming(cluster);
        configuration.partition_assignment_strategy = assignors;
        KafkaConsumer::new(configuration)
    };
    let (member, stranger) = (offering("roundrobin,range".parse()?), offering("sticky".parse()?));
    member.join_group(&group).await?;

    let refused = stranger.join_group(&group).await;

    assert_eq!(refused, Err(refused_with(ResponseError::InconsistentGroupProtocol)));
    Ok(())
}

#[tokio::test]
async fn the_group_methods_of_the_trait_carry_what_the_coordinator_answered() -> anyhow::Result<()> {
    let cluster = kafka().await?;
    let group = unique_name("analytics");
    let (consumer, stranger) = (consumer(cluster), consumer(cluster));

    let joined = ProtocolConsumer::join_group(&consumer, &group).await;
    let synced = ProtocolConsumer::sync_group(&consumer, &group).await;
    let alive = ProtocolConsumer::heartbeat(&consumer, &group).await;
    let not_alive = ProtocolConsumer::heartbeat(&stranger, &group).await;

    assert_eq!(joined.error, None);
    assert_ne!(joined.member_id, "");
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(synced.error, None);
    assert_eq!(alive.error, None);
    assert_eq!(not_alive.error, Some(ResponseError::UnknownMemberId));
    Ok(())
}
