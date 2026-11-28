//! The `server.properties` of each broker of a cluster.
//!
//! Every broker of a cluster runs in the same container, so they share one network:
//! each one listens on ports of its own, and they reach each other on `localhost`.
//! Clients come from outside the container, through a port that Docker publishes on the host.
//! A cluster on a network of containers has a listener more, which its clients on that network reach under the
//! alias of the cluster.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::SocketAddr;

use crate::configuration::NetworkListener;

/// The port the controller listens on, inside the container. The first broker is the controller.
const CONTROLLER_PORT: u16 = 9093;

/// The ID of the broker that is also the controller of the cluster.
const CONTROLLER: u8 = 1;

/// The most replicas Kafka gives its own topics, and the fewest of them that have to be in sync.
const KAFKAS_REPLICAS: u8 = 3;
const KAFKAS_MIN_IN_SYNC: u8 = 2;

/// The port broker `broker` listens on for clients, inside the container.
pub(crate) fn client_port(broker: u8) -> u16 {
    19_000 + u16::from(broker)
}

/// The port broker `broker` listens on for the other brokers, inside the container.
fn inter_broker_port(broker: u8) -> u16 {
    29_000 + u16::from(broker)
}

/// The port broker `broker` listens on for the clients of the network of `listener`, inside the container.
pub(crate) fn network_port(listener: &NetworkListener, broker: u8) -> Option<u16> {
    listener.port.checked_add(u16::from(broker) - 1)
}

/// The ports inside the container that broker `broker` listens on, whatever the test sets.
pub(crate) fn ports_of(broker: u8) -> [u16; 2] {
    [client_port(broker), inter_broker_port(broker)]
}

/// The port of the controller, inside the container.
pub(crate) const fn controller_port() -> u16 {
    CONTROLLER_PORT
}

/// The properties of broker `broker` in a cluster of `brokers`, which clients reach at `advertised`,
/// and the clients of a network under `network`: the alias of the cluster and the port of the broker.
/// A property of `set_by_the_test` replaces the one of the same name.
pub(crate) fn of_broker(
    broker: u8,
    brokers: u8,
    advertised: SocketAddr,
    network: Option<(&str, u16)>,
    set_by_the_test: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let (client, inter_broker) = (client_port(broker), inter_broker_port(broker));
    let is_controller = broker == CONTROLLER;
    let mut listeners = format!("CLIENT://:{client},BROKER://:{inter_broker}");
    let mut advertised_listeners = format!("CLIENT://{advertised},BROKER://localhost:{inter_broker}");
    let mut protocols = "CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT,BROKER:PLAINTEXT".to_owned();
    if let Some((alias, port)) = network {
        let _ = write!(listeners, ",NETWORK://:{port}");
        let _ = write!(advertised_listeners, ",NETWORK://{alias}:{port}");
        protocols.push_str(",NETWORK:PLAINTEXT");
    }
    if is_controller {
        let _ = write!(listeners, ",CONTROLLER://:{CONTROLLER_PORT}");
    }
    let replicas = brokers.min(KAFKAS_REPLICAS).to_string();
    let min_in_sync = brokers.min(KAFKAS_MIN_IN_SYNC).to_string();
    let ours = [
        ("node.id", broker.to_string()),
        (
            "process.roles",
            if is_controller { "broker,controller" } else { "broker" }.to_owned(),
        ),
        (
            "controller.quorum.voters",
            format!("{CONTROLLER}@localhost:{CONTROLLER_PORT}"),
        ),
        ("controller.listener.names", "CONTROLLER".to_owned()),
        ("listeners", listeners),
        ("advertised.listeners", advertised_listeners),
        ("listener.security.protocol.map", protocols),
        ("inter.broker.listener.name", "BROKER".to_owned()),
        ("log.dirs", format!("/tmp/kafka/{broker}/data")),
        ("offsets.topic.replication.factor", replicas.clone()),
        ("transaction.state.log.replication.factor", replicas.clone()),
        ("transaction.state.log.min.isr", min_in_sync.clone()),
        ("share.coordinator.state.topic.replication.factor", replicas),
        ("share.coordinator.state.topic.min.isr", min_in_sync),
        ("group.initial.rebalance.delay.ms", "0".to_owned()),
        ("log.retention.ms", "-1".to_owned()),
    ];
    let mut properties: BTreeMap<String, String> =
        ours.into_iter().map(|(name, value)| (name.to_owned(), value)).collect();
    properties.extend(
        set_by_the_test
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
    );
    properties
}

/// The name of the first property that a properties file cannot hold as one line, if there is one.
///
/// A name is not empty, and neither a name nor a value holds a line break.
/// A name holds no `=`, no space and no `#`, which would make the line mean something else.
pub(crate) fn first_unwritable(properties: &BTreeMap<String, String>) -> Option<&str> {
    let breaks = |text: &str| text.contains(['\n', '\r']);
    properties
        .iter()
        .find(|(name, value)| {
            name.is_empty() || breaks(name) || breaks(value) || name.contains(['=', ' ', '\t', '#', ':'])
        })
        .map(|(name, _)| name.as_str())
}

/// `properties` as the lines of a properties file.
pub(crate) fn lines(properties: &BTreeMap<String, String>) -> String {
    properties.iter().fold(String::new(), |mut lines, (name, value)| {
        let _ = writeln!(lines, "{name}={value}");
        lines
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advertised() -> SocketAddr {
        "127.0.0.1:50123".parse().unwrap()
    }

    #[test]
    fn the_first_broker_is_also_the_controller() {
        let first = of_broker(1, 3, advertised(), None, &BTreeMap::new());
        let second = of_broker(2, 3, advertised(), None, &BTreeMap::new());

        assert_eq!(first["process.roles"], "broker,controller");
        assert_eq!(first["listeners"], "CLIENT://:19001,BROKER://:29001,CONTROLLER://:9093");
        assert_eq!(second["process.roles"], "broker");
        assert_eq!(second["listeners"], "CLIENT://:19002,BROKER://:29002");
        assert_eq!(second["controller.quorum.voters"], "1@localhost:9093");
    }

    #[test]
    fn a_broker_advertises_the_address_clients_reach_it_at() {
        let properties = of_broker(2, 3, advertised(), None, &BTreeMap::new());

        assert_eq!(
            properties["advertised.listeners"],
            "CLIENT://127.0.0.1:50123,BROKER://localhost:29002"
        );
        assert_eq!(properties["node.id"], "2");
        assert_eq!(properties["log.dirs"], "/tmp/kafka/2/data");
    }

    #[test]
    fn a_broker_on_a_network_advertises_the_alias_of_the_cluster_to_its_clients_there() {
        let listener = NetworkListener::new("kafka", 19092);
        let port = network_port(&listener, 2).unwrap();

        let properties = of_broker(2, 3, advertised(), Some(("kafka", port)), &BTreeMap::new());

        assert_eq!(
            properties["listeners"],
            "CLIENT://:19002,BROKER://:29002,NETWORK://:19093"
        );
        assert_eq!(
            properties["advertised.listeners"],
            "CLIENT://127.0.0.1:50123,BROKER://localhost:29002,NETWORK://kafka:19093"
        );
        assert_eq!(
            properties["listener.security.protocol.map"],
            "CONTROLLER:PLAINTEXT,CLIENT:PLAINTEXT,BROKER:PLAINTEXT,NETWORK:PLAINTEXT"
        );
    }

    #[test]
    fn kafkas_own_topics_have_no_more_replicas_than_the_cluster_has_brokers() {
        let alone = of_broker(1, 1, advertised(), None, &BTreeMap::new());
        let of_five = of_broker(1, 5, advertised(), None, &BTreeMap::new());

        assert_eq!(alone["offsets.topic.replication.factor"], "1");
        assert_eq!(alone["transaction.state.log.min.isr"], "1");
        assert_eq!(of_five["offsets.topic.replication.factor"], "3");
        assert_eq!(of_five["transaction.state.log.min.isr"], "2");
    }

    #[test]
    fn a_property_of_the_test_replaces_the_one_of_the_cluster() {
        let set = BTreeMap::from([
            ("group.initial.rebalance.delay.ms".to_owned(), "3000".to_owned()),
            ("auto.create.topics.enable".to_owned(), "false".to_owned()),
        ]);

        let properties = of_broker(1, 1, advertised(), None, &set);

        assert_eq!(properties["group.initial.rebalance.delay.ms"], "3000");
        assert_eq!(properties["log.retention.ms"], "-1");
        assert_eq!(properties["auto.create.topics.enable"], "false");
        assert!(lines(&properties).contains("auto.create.topics.enable=false\n"));
    }

    #[test]
    fn a_property_that_is_no_single_line_is_found() {
        let property = |name: &str, value: &str| BTreeMap::from([(name.to_owned(), value.to_owned())]);

        assert_eq!(first_unwritable(&property("num.partitions", "3")), None);
        assert_eq!(first_unwritable(&property("", "3")), Some(""));
        assert_eq!(
            first_unwritable(&property("num.partitions", "3\n# start")),
            Some("num.partitions")
        );
        assert_eq!(first_unwritable(&property("# start", "")), Some("# start"));
    }
}
