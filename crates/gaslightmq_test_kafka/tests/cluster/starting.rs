//! Starting a cluster, and what it says about itself.

use std::collections::BTreeSet;

use gaslightmq_kernel::BrokerId;
use gaslightmq_test_kafka::{Broker, ContainerNetwork, KafkaCluster, KafkaClusterError, KafkaImage, NetworkListener};

use crate::kafka;

#[tokio::test]
async fn a_cluster_runs_the_tested_version_of_kafka_unless_it_is_told_otherwise() -> anyhow::Result<()> {
    let cluster = kafka().await?;

    let expected = match std::env::var(KafkaImage::VARIABLE) {
        Ok(image) => image.parse()?,
        Err(_) => KafkaImage::default(),
    };
    assert_eq!(cluster.image(), &expected);
    Ok(())
}

#[tokio::test]
async fn every_broker_has_an_id_and_an_address_of_its_own() -> anyhow::Result<()> {
    let cluster = kafka().await?;

    let ids: Vec<BrokerId> = cluster.brokers().iter().map(Broker::id).collect();
    let addresses: BTreeSet<_> = cluster.brokers().iter().map(Broker::address).collect();

    assert_eq!(ids, [BrokerId::from(1), BrokerId::from(2), BrokerId::from(3)]);
    assert_eq!(addresses.len(), 3);
    assert!(addresses.iter().all(|address| address.ip().is_loopback()));
    let bootstrap: Vec<String> = cluster
        .brokers()
        .iter()
        .map(|broker| broker.address().to_string())
        .collect();
    assert_eq!(cluster.bootstrap_servers(), bootstrap.join(","));
    assert_eq!(cluster.broker(BrokerId::from(2)), Some(&cluster.brokers()[1]));
    assert_eq!(cluster.broker(BrokerId::from(4)), None);
    Ok(())
}

#[tokio::test]
async fn a_cluster_of_one_broker_takes_the_properties_of_the_test() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext()
        .with_broker_property("num.partitions", "4")
        .start()
        .await?;

    let orders = cluster.topics().create("orders").await?;

    assert_eq!(cluster.brokers().len(), 1);
    assert_eq!(orders.partitions(), 4);
    Ok(())
}

#[tokio::test]
async fn a_cluster_without_brokers_does_not_start() {
    let error = KafkaCluster::plaintext().with_brokers(0).start().await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "the Kafka cluster did not start: a cluster has at least one broker, and the configuration asks for none"
    );
}

#[tokio::test]
async fn a_property_that_would_break_the_configuration_of_a_broker_is_refused() {
    let error = KafkaCluster::plaintext()
        .with_broker_property("num.partitions", "1\n# start")
        .start()
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "the Kafka cluster did not start: broker property `num.partitions` is no single line of a properties file"
    );
}

#[tokio::test]
async fn brokers_that_cannot_start_say_why_in_their_log() {
    let error = KafkaCluster::plaintext()
        .with_broker_property("log.retention.hours", "soon")
        .start()
        .await
        .unwrap_err();

    let KafkaClusterError::NotStarted { reason, log } = &error else {
        panic!("{error}");
    };
    assert_eq!(reason, "the brokers stopped before all of them had started");
    assert!(log.contains("log.retention.hours"), "{log}");
    assert!(
        error.to_string().contains("the end of the log of its brokers:\n"),
        "{error}"
    );
}

#[tokio::test]
async fn an_image_that_does_not_exist_starts_no_cluster() -> anyhow::Result<()> {
    let image = KafkaImage::version("0.0.0-no-such-version")?;

    let error = KafkaCluster::plaintext().with_image(image).start().await.unwrap_err();

    assert!(
        error.to_string().starts_with(
            "the Kafka cluster did not start: \
             the image apache/kafka-native:0.0.0-no-such-version is not there and could not be pulled"
        ),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_listener_for_a_network_that_takes_a_port_of_the_brokers_is_refused() {
    // Broker 2 of three listens for the clients of the host on 19002 already.
    let error = KafkaCluster::plaintext()
        .with_brokers(3)
        .with_network(
            &ContainerNetwork::named("orders-test"),
            NetworkListener::new("kafka", 19_001),
        )
        .start()
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "the Kafka cluster did not start: the listener kafka:19001 gives broker 1 port 19001, \
         which the cluster listens on already"
    );
}

#[tokio::test]
async fn a_cluster_on_a_network_names_its_brokers_to_the_clients_there_under_its_alias() -> anyhow::Result<()> {
    let network = ContainerNetwork::unique("orders-test");
    let kafka = NetworkListener::unique("kafka", 19_092);

    let cluster = KafkaCluster::plaintext()
        .with_brokers(2)
        .with_network(&network, kafka.clone())
        .start()
        .await?;

    let alias = &kafka.alias;
    assert_eq!(
        cluster.network_bootstrap_servers(),
        Some(format!("{alias}:19092,{alias}:19093"))
    );
    // The clients of the host reach the brokers as before.
    let orders = cluster.topics().create("orders").await?;
    assert_eq!(orders.partitions(), 1);
    Ok(())
}
