//! The tests of the orders service, with GaslightMQ in a container next to each test.
//!
//! Each test puts a real cluster and the image of GaslightMQ on a network of containers of its own: the container
//! reaches the brokers under the alias of the cluster there, and the service reaches the proxy where the container
//! publishes it. The test arms a rule through the control plane of the container, and asserts what the service did
//! and what the cluster holds. The tests run side by side: each container publishes its ports on free ports of the
//! machine, and each network has a name of its own.

use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
use gaslightmq_control_api::lie::RefusalLie;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName};
use gaslightmq_control_api::selector::{Condition, Conditions};
use gaslightmq_test_kafka::{ContainerNetwork, KafkaCluster, NetworkListener};
use gaslightmq_testcontainers::GaslightMqContainer;
use gaslightmq_wire::Offset;
use orders::{Order, Orders, OrdersConfiguration, PlaceOrderError};

/// The workspace of the tests: the rules in `gaslight/rules/`, which a test arms by name.
const WORKSPACE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/gaslight");

/// The rule of the workspace that tells the service a Produce timed out, after the cluster wrote the order.
const LOST_ACKNOWLEDGEMENTS: &str = "order-acknowledgements-get-lost";

/// The port of the first broker for the containers of the network of a test.
const PORT_ON_THE_NETWORK: u16 = 19_092;

/// When the orders of these tests were placed, in milliseconds since 1970.
const PLACED_AT: i64 = 1_700_000_000_000;

/// A service that reports the first answer it gets, and does not try again.
const NO_RETRIES: u32 = 0;

#[tokio::test]
async fn an_order_that_the_cluster_does_not_let_the_service_write_is_not_allowed() -> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach the cluster under this alias.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    let gaslight = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
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
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach the cluster under this alias.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    // The files of the workspace are copied into the container, so the rules of the project are there.
    let gaslight = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
        .with_workspace(WORKSPACE)
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
    let lost: RuleName = LOST_ACKNOWLEDGEMENTS.parse()?;
    gaslight
        .control()
        .send(ArmRule {
            environment: gaslight.environment().clone(),
            rule: lost,
            expects: None,
        })
        .await??;
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
async fn two_services_side_by_side_each_meet_the_lies_of_their_own_container_alone() -> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach the cluster under this alias.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    // Two containers in front of one cluster. Each publishes its ports on free ports of the machine.
    let refusing = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
        .start()
        .await?;
    let truthful = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
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

    // The rule is armed in one container, and the other one knows nothing of it.
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
