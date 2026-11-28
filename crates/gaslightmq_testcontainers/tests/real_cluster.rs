//! A test of a Kafka client starts GaslightMQ in a container next to it, in front of a real cluster, and arms a rule
//! through the control plane of the container that makes the client's produce fail.
//!
//! The test puts the cluster and the container on one network of containers: the container reaches the brokers under
//! the alias of the cluster there, and the client of the test reaches the proxy where the container publishes it.
//!
//! This is how a user of GaslightMQ writes such a test with the image of a release.
//! The cluster is the one of `gaslightmq_test_kafka`, so the tests need Docker, and the image of GaslightMQ:
//! `just test-image` builds it, and names it in `GASLIGHTMQ_IMAGE`.
//! The client under test is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address of
//! the proxy and nothing else.

use std::path::PathBuf;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
use gaslightmq_control_api::lie::RefusalLie;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName};
use gaslightmq_control_api::selector::{Condition, Conditions};
use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kafka_client_native::{KafkaProducer, ProducerConfiguration};
use gaslightmq_test_kafka::{ContainerNetwork, KafkaCluster, NetworkListener, NewTopic};
use gaslightmq_testcontainers::{GaslightMqContainer, GaslightMqContainerStartError, GaslightMqImage};
use gaslightmq_wire::{Offset, Partition, Record, ResponseError};

/// Example 09: a rule that loses one in ten acknowledgements of the order topics.
const EXAMPLE_09: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/09-proxy-in-front-of-real-kafka"
);

/// The rule of example 09: the real cluster writes the records, and the client is told that the Produce timed out.
const LOST_ACKNOWLEDGEMENTS: &str = "order-acknowledgements-get-lost";

/// The port of the first broker for the containers of the network of a test.
const PORT_ON_THE_NETWORK: u16 = 19_092;

/// A cluster that nothing reaches: the container never gets that far.
const NO_CLUSTER: &str = "127.0.0.1:9";

/// When the orders of these tests were placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

#[tokio::test]
async fn a_rule_armed_through_the_control_plane_of_the_container_refuses_the_produce_of_a_real_client()
-> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach broker 1 of the cluster under its alias, and the others on the next ports.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    let orders = cluster.topics().create("orders").await?;
    let gaslight = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
        .start()
        .await?;
    let producer = KafkaProducer::from(gaslight.bootstrap());
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);

    // The proxy refuses every Produce: the client may not write to the topic.
    let refuse = Rule::new(
        "refuse-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
    );
    gaslight
        .control()
        .send(CreateLiveRule {
            environment: gaslight.environment().clone(),
            rule: refuse.clone(),
            armed: true,
        })
        .await??;
    let refused = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    // Once the rule is disarmed, the same client writes the record.
    gaslight
        .control()
        .send(DisarmRule {
            environment: gaslight.environment().clone(),
            rule: refuse.name().clone(),
            expects: None,
        })
        .await??;
    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::TopicAuthorizationFailed
        })
    );
    assert_eq!(written, Ok(Offset::new(0)));
    // The broker holds the record once: the refused Produce never reached it.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_of_the_workspace_that_the_test_hands_over_tells_a_real_client_that_a_record_it_wrote_was_lost()
-> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach broker 1 of the cluster under its alias, and the others on the next ports.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    let orders = cluster.topics().create("orders.eu").await?;
    // The files of example 09 are copied into the container, so whatever the process writes stays there.
    let gaslight = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
        .with_workspace(EXAMPLE_09)
        .start()
        .await?;

    // The rule of the workspace loses one in ten acknowledgements. A firing skips the chance for the next Produce.
    let lost: RuleName = LOST_ACKNOWLEDGEMENTS.parse()?;
    gaslight
        .control()
        .send(ArmRule {
            environment: gaslight.environment().clone(),
            rule: lost.clone(),
            expects: None,
        })
        .await??;
    gaslight
        .control()
        .send(FireRule {
            environment: gaslight.environment().clone(),
            rule: lost,
        })
        .await??;
    // The producer does not try again, so it reports the first answer it gets.
    let mut producer = ProducerConfiguration::new(gaslight.bootstrap());
    producer.retries = 0;
    let producer = KafkaProducer::new(producer);
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);
    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    assert_eq!(
        written,
        Err(ClientError::Refused {
            error: ResponseError::RequestTimedOut
        })
    );
    // The real cluster holds the record that the client was told it lost.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_container_in_front_of_several_brokers_runs_its_environment_in_the_run_it_says() -> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    // The containers of the network reach broker 1 of the cluster under its alias, and the others on the next ports.
    let kafka = NetworkListener::unique("kafka", PORT_ON_THE_NETWORK);
    let cluster = KafkaCluster::plaintext()
        .with_brokers(3)
        .with_network(&network, kafka.clone())
        .start()
        .await?;
    let orders = cluster
        .topics()
        .create(NewTopic::named("orders").with_partitions(3))
        .await?;
    // The proxy gives each broker that it finds in the cluster a published port of its own: the test names none.
    let gaslight = GaslightMqContainer::proxy_in_front_of(kafka.to_string().parse()?)
        .with_network(network.name())
        .named("orders-proxy".parse()?)
        .start()
        .await?;
    let producer = KafkaProducer::from(gaslight.bootstrap());

    // The control plane knows the environment of the proxy, which runs, in the run of the process.
    let mut stream = gaslight.control().live_stream().await?;
    let StreamMessage::Snapshot(state) = stream.next().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    // Each partition has its leader on another broker, and the client writes to each through the proxy.
    let mut written = Vec::new();
    for partition in 0..orders.partitions() {
        let placed = Record::new("placed").with_key(format!("o-{partition}"));
        written.push(
            producer
                .send(orders.name().as_str(), Partition::from(partition), &[placed])
                .await,
        );
    }

    assert_eq!(state.run, gaslight.run());
    let running: Vec<&EnvironmentName> = state.environments.iter().map(|running| &running.environment).collect();
    assert_eq!(running, [gaslight.environment()]);
    assert_eq!(gaslight.environment().as_str(), "orders-proxy");
    assert_eq!(written, [Ok(Offset::new(0)), Ok(Offset::new(0)), Ok(Offset::new(0))]);
    stream.close().await;
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_image_that_no_registry_holds_starts_no_container() -> anyhow::Result<()> {
    let unreleased = GaslightMqImage::version("no-such-release")?;

    let started = GaslightMqContainer::proxy_in_front_of(NO_CLUSTER.parse()?)
        .with_image(unreleased.clone())
        .start()
        .await;

    match started {
        Err(GaslightMqContainerStartError::ImageMissing { image, .. }) => assert_eq!(image, unreleased),
        other => anyhow::bail!("an image that no registry holds did not go missing: {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn a_workspace_that_is_not_there_starts_no_container() -> anyhow::Result<()> {
    let missing = PathBuf::from(EXAMPLE_09).join("no-such-directory");

    // The workspace is read before any container starts, so this needs no image either.
    let started = GaslightMqContainer::proxy_in_front_of(NO_CLUSTER.parse()?)
        .with_workspace(&missing)
        .start()
        .await;

    match started {
        Err(GaslightMqContainerStartError::WorkspaceUnreadable { directory, .. }) => assert_eq!(directory, missing),
        other => anyhow::bail!("a workspace that is not there was read: {other:?}"),
    }
    Ok(())
}
