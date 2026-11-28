//! The tests of the orders service, with GaslightMQ in the process of each test.
//!
//! Each test starts a real cluster and GaslightMQ in front of it, hands the service the bootstrap address of the
//! proxy, arms a rule through the control plane, and asserts what the service did and what the cluster holds.
//! The tests run side by side: each proxy and each control plane listens on free ports of its own.

use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
use gaslightmq_control_api::lie::{RefusalLie, Stage};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::Rule;
use gaslightmq_control_api::selector::{Condition, Conditions, SelectorRef};
use gaslightmq_control_api::trigger::{Count, TriggerParts};
use gaslightmq_test::GaslightMq;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::Offset;
use orders::{Order, Orders, OrdersConfiguration, PlaceOrderError};

/// When the orders of these tests were placed, in milliseconds since 1970.
const PLACED_AT: i64 = 1_700_000_000_000;

/// A service that reports the first answer it gets, and does not try again.
const NO_RETRIES: u32 = 0;

/// The Produce calls of an order whose first try is refused: that try, and the one after it.
const TRIES_OF_THE_ORDER: u64 = 2;

#[tokio::test]
async fn an_order_that_the_cluster_does_not_let_the_service_write_is_not_allowed() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let topic = cluster.topics().create("orders").await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let orders = Orders::new(OrdersConfiguration::new(gaslight.bootstrap()));
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };

    // The proxy refuses every Produce, as a cluster does that the service may not write to.
    gaslight
        .control()
        .send(CreateLiveRule {
            environment: gaslight.environment().clone(),
            rule: Rule::new(
                "orders-are-not-allowed".parse()?,
                Conditions::from(Condition::is(ApiKey::PRODUCE)),
                RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
            ),
            armed: true,
        })
        .await??;
    let placed = orders.place(&order).await;

    assert_eq!(
        placed,
        Err(PlaceOrderError::NotAllowed {
            topic: "orders".to_owned()
        })
    );
    // The refusal came before the cluster, so it holds nothing.
    assert_eq!(topic.partition(0).records().await?, []);
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_order_whose_acknowledgement_got_lost_is_unconfirmed_though_the_cluster_holds_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let topic = cluster.topics().create("orders").await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let mut configuration = OrdersConfiguration::new(gaslight.bootstrap());
    configuration.producer.retries = NO_RETRIES;
    let orders = Orders::new(configuration);
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };

    // The cluster writes the order, and the proxy tells the service that the Produce timed out.
    gaslight
        .control()
        .send(CreateLiveRule {
            environment: gaslight.environment().clone(),
            rule: Rule::new(
                "order-acknowledgements-get-lost".parse()?,
                Conditions::from(Condition::is(ApiKey::PRODUCE)),
                RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse),
            ),
            armed: true,
        })
        .await??;
    let placed = orders.place(&order).await;

    assert!(
        matches!(placed, Err(PlaceOrderError::Unconfirmed { ref order, .. }) if order == "o-1"),
        "{placed:?}"
    );
    // The service was right not to call the order lost: the cluster holds it.
    assert_eq!(
        topic.partition(0).records().await?,
        [order.record().with_offset(Offset::new(0))]
    );
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_order_placed_while_the_leader_moves_is_placed_on_its_second_try() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let topic = cluster.topics().create("orders").await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let control = gaslight.control();
    let orders = Orders::new(OrdersConfiguration::new(gaslight.bootstrap()));
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };
    let every_produce = SelectorRef::Inline(Conditions::from(Condition::is(ApiKey::PRODUCE)));
    // The test follows the Produce calls, so it knows when the proxy handed each one to the recording.
    let mut stream = control.live_stream().await?;
    stream
        .follow(Subscription::Calls {
            environment: gaslight.environment().clone(),
            selector: every_produce.clone(),
        })
        .await;
    while !matches!(stream.next().await?, StreamMessage::Following { .. }) {}
    let recording = control
        .send(StartRecording {
            environment: gaslight.environment().clone(),
        })
        .await??;

    // The first Produce is told that its broker no longer leads the partition, as while a leader moves.
    let leader_moves = Rule::new(
        "the-leader-moves".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_trigger(TriggerParts::new().with_first(Count::ONE));
    control
        .send(CreateLiveRule {
            environment: gaslight.environment().clone(),
            rule: leader_moves.clone(),
            armed: true,
        })
        .await??;
    let placed = orders.place(&order).await;
    let mut followed = 0;
    while followed < TRIES_OF_THE_ORDER {
        if let StreamMessage::Call { .. } = stream.next().await? {
            followed += 1;
        }
    }
    control
        .send(StopRecording {
            environment: gaslight.environment().clone(),
        })
        .await??;
    let recorded = control
        .send(QueryRecording {
            recording,
            selector: every_produce,
            draft: None,
            skip: 0,
        })
        .await??;

    // The service saw no failure: its producer tried again.
    assert_eq!(placed, Ok(Offset::new(0)));
    assert_eq!(
        topic.partition(0).records().await?,
        [order.record().with_offset(Offset::new(0))]
    );
    // The recording shows what the service did not: the rule refused the first try, and let the second one through.
    assert_eq!(recorded.matched, TRIES_OF_THE_ORDER);
    let applied: Vec<bool> = recorded
        .calls
        .iter()
        .map(|call| call.explanation().applied(leader_moves.name().as_str()))
        .collect();
    assert_eq!(applied, [true, false]);
    assert!(recorded.calls[1].verdict().is_pass());
    stream.close().await;
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn two_services_side_by_side_each_meet_the_lies_of_their_own_proxy_alone() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let topic = cluster.topics().create("orders").await?;
    // Two proxies in front of one cluster. Each listens on free ports of its own, and has a control plane of its own.
    let refusing = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let truthful = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let refused_orders = Orders::new(OrdersConfiguration::new(refusing.bootstrap()));
    let placed_orders = Orders::new(OrdersConfiguration::new(truthful.bootstrap()));
    let refused = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };
    let placed = Order {
        id: "o-2".to_owned(),
        item: "milk frother".to_owned(),
        placed_at: PLACED_AT,
    };

    // The rule is armed on one proxy, and the other one knows nothing of it.
    refusing
        .control()
        .send(CreateLiveRule {
            environment: refusing.environment().clone(),
            rule: Rule::new(
                "orders-are-not-allowed".parse()?,
                Conditions::from(Condition::is(ApiKey::PRODUCE)),
                RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
            ),
            armed: true,
        })
        .await??;
    let (first, second) = tokio::join!(refused_orders.place(&refused), placed_orders.place(&placed));

    assert_eq!(
        first,
        Err(PlaceOrderError::NotAllowed {
            topic: "orders".to_owned()
        })
    );
    assert_eq!(second, Ok(Offset::new(0)));
    assert_eq!(
        topic.partition(0).records().await?,
        [placed.record().with_offset(Offset::new(0))]
    );
    refusing.stop().await;
    truthful.stop().await;
    Ok(())
}
