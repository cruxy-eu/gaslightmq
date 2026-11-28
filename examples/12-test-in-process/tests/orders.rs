//! The tests of the orders service, with GaslightMQ in the process of each test.
//!
//! Each test starts a real cluster and GaslightMQ in front of it, hands the service the bootstrap address of the
//! proxy, arms a rule written as a rule file writes it, and asserts what the service did and what the cluster holds.
//! The tests run side by side: each proxy and each control plane listens on free ports of its own.

use gaslightmq_test::{GaslightMq, RecordingMode, rule, scenario, selector};
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::Offset;
use orders::{Order, Orders, OrdersConfiguration, PlaceOrderError};

/// When the orders of these tests were placed, in milliseconds since 1970.
const PLACED_AT: i64 = 1_700_000_000_000;

/// A service that reports the first answer it gets, and does not try again.
const NO_RETRIES: u32 = 0;

/// The Produce calls of an order whose first try is refused: that try, and the one after it.
const TRIES_OF_THE_ORDER: u64 = 2;

/// The tries of the order that the leader churns under, which the scenario refuses before its leader stays.
const TRIES_REFUSED: u64 = 2;

/// The Produce calls of the three orders of the churn: one each, and the two tries that the churn refused.
const PRODUCES_THROUGH_THE_CHURN: u64 = 5;

/// The seed of the dice of the scenario of the churn, whose rule rolls none.
const SEED: u64 = 7;

#[tokio::test]
async fn an_order_that_the_cluster_does_not_let_the_service_write_is_not_allowed() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let configuration = OrdersConfiguration::new(gaslight.bootstrap());
    let topic = cluster.topics().create(configuration.topic.as_str()).await?;
    let orders = Orders::new(configuration.clone());
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };

    // The proxy refuses every Produce, as a cluster does that the service may not write to.
    gaslight
        .rules()
        .arm(rule! {
            selector: { api: Produce },
            action: { lie: refusal, error: TOPIC_AUTHORIZATION_FAILED },
        })
        .await?;
    let placed = orders.place(&order).await;

    assert_eq!(
        placed,
        Err(PlaceOrderError::NotAllowed {
            topic: configuration.topic.clone()
        })
    );
    // The refusal came before the cluster, so it holds nothing.
    assert_eq!(topic.partition(configuration.partition).records().await?, []);
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_order_whose_acknowledgement_got_lost_is_unconfirmed_though_the_cluster_holds_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let mut configuration = OrdersConfiguration::new(gaslight.bootstrap());
    configuration.producer.retries = NO_RETRIES;
    let topic = cluster.topics().create(configuration.topic.as_str()).await?;
    let orders = Orders::new(configuration.clone());
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };

    // The cluster writes the order, and the proxy tells the service that the Produce timed out.
    gaslight
        .rules()
        .arm(rule! {
            selector: { api: Produce },
            action: { lie: refusal, error: REQUEST_TIMED_OUT, stage: after },
        })
        .await?;
    let placed = orders.place(&order).await;

    assert!(
        matches!(placed, Err(PlaceOrderError::Unconfirmed { ref order, .. }) if order == "o-1"),
        "{placed:?}"
    );
    // The service was right not to call the order lost: the cluster holds it.
    assert_eq!(
        topic.partition(configuration.partition).records().await?,
        [order.record().with_offset(Offset::new(0))]
    );
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_order_placed_while_the_leader_moves_is_placed_on_its_second_try() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .recording_from_the_start()
        .start()
        .await?;
    let configuration = OrdersConfiguration::new(gaslight.bootstrap());
    let topic = cluster.topics().create(configuration.topic.as_str()).await?;
    let orders = Orders::new(configuration.clone());
    let order = Order {
        id: "o-1".to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    };
    // The test follows the Produce calls, so it knows when the proxy handed each one to the recording.
    let mut produces = gaslight.follow(selector! { api: Produce }).await?;

    // The first Produce is told that its broker no longer leads the partition, as while a leader moves.
    let leader_moves = gaslight
        .rules()
        .arm(rule! {
            selector: { api: Produce },
            action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER },
            trigger: { first: 1 },
        })
        .await?;
    let placed = orders.place(&order).await;
    produces.wait_for(TRIES_OF_THE_ORDER).await?;
    let recording = gaslight.recordings().current().await?;
    recording.stop().await?;
    let recorded = recording.query(selector! { api: Produce }).await?;

    // The service saw no failure: its producer tried again.
    assert_eq!(placed, Ok(Offset::new(0)));
    assert_eq!(
        topic.partition(configuration.partition).records().await?,
        [order.record().with_offset(Offset::new(0))]
    );
    // The recording shows what the service did not: the rule refused the first try, and let the second one through.
    assert_eq!(recorded.matched, TRIES_OF_THE_ORDER);
    let applied: Vec<bool> = recorded
        .calls
        .iter()
        .map(|call| call.explanation().applied(&leader_moves))
        .collect();
    assert_eq!(applied, [true, false]);
    assert!(recorded.calls[1].verdict().is_pass());
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn orders_placed_while_the_leader_churns_are_each_written_once() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    // The first order goes through, the next tries are told that the leader moved, and then the leader stays.
    // The recording keeps the records of the orders too, which it leaves out in front of a real cluster otherwise.
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .recording(RecordingMode::Full)
        .recording_from_the_start()
        .activating(scenario! {
            mode: sequence,
            seed: (SEED),
            steps: [
                { until: { calls: 1, selector: { api: Produce } } },
                {
                    title: "The leader churns",
                    arm: [{ selector: { api: Produce }, action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER } }],
                    until: { firings: (TRIES_REFUSED) },
                },
                { title: Recovery },
            ],
        })
        .start()
        .await?;
    let configuration = OrdersConfiguration::new(gaslight.bootstrap());
    let topic = cluster.topics().create(configuration.topic.as_str()).await?;
    let orders = Orders::new(configuration.clone());
    let to_place = ["o-1", "o-2", "o-3"].map(|id| Order {
        id: id.to_owned(),
        item: "espresso machine".to_owned(),
        placed_at: PLACED_AT,
    });
    let mut produces = gaslight.follow(selector! { api: Produce }).await?;

    let mut placed = Vec::new();
    for order in &to_place {
        placed.push(orders.place(order).await);
    }
    produces.wait_for(PRODUCES_THROUGH_THE_CHURN).await?;
    let recording = gaslight.recordings().current().await?;
    recording.stop().await?;
    let recorded = recording.query(selector! { api: Produce }).await?;

    // The service saw no failure, and the cluster holds every order once, in the order they were placed.
    assert_eq!(placed, [Ok(Offset::new(0)), Ok(Offset::new(1)), Ok(Offset::new(2))]);
    assert_eq!(
        topic.partition(configuration.partition).records().await?,
        [
            to_place[0].record().with_offset(Offset::new(0)),
            to_place[1].record().with_offset(Offset::new(1)),
            to_place[2].record().with_offset(Offset::new(2)),
        ]
    );
    // The churn ended by itself once its rule had refused twice, and the scenario stands in its last step.
    let status = gaslight.scenario_status().await?;
    assert!(
        status
            .active
            .and_then(|at| at.step)
            .is_some_and(|step| step == "step-3"),
        "the scenario stands in its last step"
    );
    // The recording shows which tries the churn refused: the two first tries of the second order, and no other.
    let passed: Vec<bool> = recorded.calls.iter().map(|call| call.verdict().is_pass()).collect();
    assert_eq!(passed, [true, false, false, true, true]);
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn two_services_side_by_side_each_meet_the_lies_of_their_own_proxy_alone() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    // Two proxies in front of one cluster. Each listens on free ports of its own, and has a control plane of its own.
    let refusing = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let truthful = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let configuration = OrdersConfiguration::new(refusing.bootstrap());
    let topic = cluster.topics().create(configuration.topic.as_str()).await?;
    // Both services run with the defaults, so they write to the same topic, each through its own proxy.
    let refused_orders = Orders::new(configuration.clone());
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
        .rules()
        .arm(rule! {
            selector: { api: Produce },
            action: { lie: refusal, error: TOPIC_AUTHORIZATION_FAILED },
        })
        .await?;
    let (first, second) = tokio::join!(refused_orders.place(&refused), placed_orders.place(&placed));

    assert_eq!(
        first,
        Err(PlaceOrderError::NotAllowed {
            topic: configuration.topic.clone()
        })
    );
    assert_eq!(second, Ok(Offset::new(0)));
    assert_eq!(
        topic.partition(configuration.partition).records().await?,
        [placed.record().with_offset(Offset::new(0))]
    );
    refusing.stop().await;
    truthful.stop().await;
    Ok(())
}
