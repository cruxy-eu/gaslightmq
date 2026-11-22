//! The cluster as one role instance knows it, and the connection each request belongs on.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::Duration;

use gaslightmq_clock::Instant;
use gaslightmq_wire::Partition;
use gaslightmq_wire::messages::{BrokerId, GroupId, MetadataResponse, TopicName};
use gaslightmq_wire::protocol::StrBytes;

use crate::address::{BootstrapServers, BrokerAddress};

/// One partition of one topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TopicPartition {
    pub topic: TopicName,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: &str, partition: Partition) -> Self {
        Self {
            topic: topic_name(topic),
            partition: i32::from(partition),
        }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic.as_str(), self.partition)
    }
}

pub(crate) fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_owned()))
}

pub(crate) fn group_id(group: &str) -> GroupId {
    GroupId(StrBytes::from_string(group.to_owned()))
}

/// Where a request has to go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Route {
    /// To any broker: every broker answers it the same way.
    AnyBroker,
    /// To the broker that leads the partition.
    Leader(TopicPartition),
    /// To the broker that coordinates the group.
    Coordinator(GroupId),
}

/// What a connection carries.
///
/// A role instance keeps the two apart: a request of one kind never waits for a request of the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Traffic {
    /// Records, offsets, Metadata and the requests of an admin.
    Data,
    /// Everything a consumer says about a group: asking who coordinates it, and talking to the coordinator.
    /// A Fetch that waits for records therefore never holds up a Heartbeat.
    Group,
}

/// Who a connection leads to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Peer {
    /// A bootstrap address, by its place in the configuration.
    Bootstrap(usize),
    /// A broker the role learned from the cluster.
    Broker(BrokerId),
}

/// One of the connections a role instance keeps.
///
/// A role instance has at most one connection of each ID,
/// and a request waits for the request before it on the same connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ConnectionId {
    pub traffic: Traffic,
    pub peer: Peer,
}

/// A connection, and the address it leads to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Destination {
    pub connection: ConnectionId,
    pub address: BrokerAddress,
}

/// The connections that can take a request: one to try first, and others for when it fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidates {
    pub first: Destination,
    pub others: VecDeque<Destination>,
}

impl From<Destination> for Candidates {
    fn from(only: Destination) -> Self {
        Self {
            first: only,
            others: VecDeque::new(),
        }
    }
}

/// The leader of a partition, and when the view learned it.
#[derive(Debug, Clone, Copy)]
struct Leader {
    broker: BrokerId,
    learned_at: Instant,
}

/// What one role instance knows about the cluster: its brokers, the leader of each partition,
/// and the coordinator of each group.
///
/// It learns from the responses that this instance receives, and from nothing else.
#[derive(Debug)]
pub(crate) struct ClusterView {
    bootstrap: BootstrapServers,
    metadata_max_age: Duration,
    brokers: BTreeMap<BrokerId, BrokerAddress>,
    leaders: HashMap<TopicPartition, Leader>,
    coordinators: HashMap<GroupId, Destination>,
    /// The connections whose last request got no response.
    failing: HashSet<ConnectionId>,
}

impl ClusterView {
    /// A view that knows the bootstrap addresses and nothing else.
    /// It trusts a leader for `metadata_max_age`.
    pub fn new(bootstrap: BootstrapServers, metadata_max_age: Duration) -> Self {
        Self {
            bootstrap,
            metadata_max_age,
            brokers: BTreeMap::new(),
            leaders: HashMap::new(),
            coordinators: HashMap::new(),
            failing: HashSet::new(),
        }
    }

    /// Take over what a Metadata response says.
    ///
    /// The brokers of the response replace the brokers of the view.
    /// For each topic of the response, its leaders replace the leaders the view had for that topic.
    /// A broker without a usable address is left out, and so is a partition with an error or without a leader.
    pub fn learn(&mut self, metadata: &MetadataResponse, now: Instant) {
        self.brokers = metadata
            .brokers
            .iter()
            .filter_map(|broker| Some((broker.node_id, address(&broker.host, broker.port)?)))
            .collect();
        for topic in &metadata.topics {
            let Some(name) = &topic.name else { continue };
            self.leaders.retain(|partition, _| partition.topic != *name);
            for partition in &topic.partitions {
                if partition.error_code == 0 && partition.leader_id.0 >= 0 {
                    self.leaders.insert(
                        TopicPartition {
                            topic: name.clone(),
                            partition: partition.partition_index,
                        },
                        Leader {
                            broker: partition.leader_id,
                            learned_at: now,
                        },
                    );
                }
            }
        }
    }

    /// Take over the coordinator a `FindCoordinator` response names for `group`.
    /// Returns whether the coordinator has a usable address.
    pub fn learn_coordinator(&mut self, group: &GroupId, broker: BrokerId, host: &str, port: i32) -> bool {
        let Some(address) = address(host, port) else {
            return false;
        };
        self.coordinators.insert(
            group.clone(),
            Destination {
                connection: ConnectionId {
                    traffic: Traffic::Group,
                    peer: Peer::Broker(broker),
                },
                address,
            },
        );
        true
    }

    /// The connection to the leader of `partition`.
    ///
    /// `None` when the view never learned the leader, when it learned it longer ago than the max age,
    /// or when it knows no address for the broker. The role then has to ask for Metadata.
    pub fn leader(&self, partition: &TopicPartition, now: Instant) -> Option<Destination> {
        let leader = self.leaders.get(partition)?;
        if now.duration_since(leader.learned_at) > self.metadata_max_age {
            return None;
        }
        Some(Destination {
            connection: ConnectionId {
                traffic: Traffic::Data,
                peer: Peer::Broker(leader.broker),
            },
            address: self.brokers.get(&leader.broker)?.clone(),
        })
    }

    /// The connection to the coordinator of `group`, or `None` when the role has to ask for it.
    pub fn coordinator(&self, group: &GroupId) -> Option<Destination> {
        self.coordinators.get(group).cloned()
    }

    /// The connections of one kind of traffic for a request that any broker answers.
    ///
    /// The brokers the view knows come first, in the order of their IDs, and then the bootstrap addresses.
    /// The bootstrap addresses stay in, because they are the way back when the brokers of the view are wrong.
    /// A connection whose last request got no response moves behind the others,
    /// so a broker that is down costs one wait, and not one for every request.
    pub fn any_broker(&self, traffic: Traffic) -> Candidates {
        let known = self
            .brokers
            .iter()
            .map(|(broker, address)| (Peer::Broker(*broker), address));
        let bootstrap = self
            .bootstrap
            .iter()
            .enumerate()
            .map(|(place, address)| (Peer::Bootstrap(place), address));
        let mut all: VecDeque<Destination> = known
            .chain(bootstrap)
            .map(|(peer, address)| Destination {
                connection: ConnectionId { traffic, peer },
                address: address.clone(),
            })
            .collect();
        all.make_contiguous()
            .sort_by_key(|destination| self.failing.contains(&destination.connection));
        let first = all.pop_front().unwrap_or_else(|| Destination {
            connection: ConnectionId {
                traffic,
                peer: Peer::Bootstrap(0),
            },
            address: self.bootstrap.first().clone(),
        });
        Candidates { first, others: all }
    }

    /// A request on `connection` got no response.
    pub fn failed(&mut self, connection: ConnectionId) {
        self.failing.insert(connection);
    }

    /// A request on `connection` got its response.
    pub fn answered(&mut self, connection: ConnectionId) {
        self.failing.remove(&connection);
    }

    /// Forget who leads `partition`, so the next request for it asks again.
    pub fn forget_leader(&mut self, partition: &TopicPartition) {
        self.leaders.remove(partition);
    }

    /// Forget who coordinates `group`, so the next request for it asks again.
    pub fn forget_coordinator(&mut self, group: &GroupId) {
        self.coordinators.remove(group);
    }
}

/// The address a response names, when it is one a client can connect to.
fn address(host: &str, port: i32) -> Option<BrokerAddress> {
    BrokerAddress::new(host, u16::try_from(port).ok()?).ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };

    use super::*;

    pub const MAX_AGE: Duration = Duration::from_secs(300);

    pub fn bootstrap() -> BootstrapServers {
        "bootstrap-a:9092,bootstrap-b:9092".parse().unwrap()
    }

    pub fn view() -> ClusterView {
        ClusterView::new(bootstrap(), MAX_AGE)
    }

    pub fn broker(id: i32) -> MetadataResponseBroker {
        MetadataResponseBroker::default()
            .with_node_id(BrokerId(id))
            .with_host(StrBytes::from_string(format!("broker-{id}")))
            .with_port(9092)
    }

    /// A topic whose partitions 0, 1, … have `leaders`, in that order.
    pub fn topic(name: &str, leaders: &[i32]) -> MetadataResponseTopic {
        let partitions = (0..)
            .zip(leaders)
            .map(|(index, leader)| {
                MetadataResponsePartition::default()
                    .with_partition_index(index)
                    .with_leader_id(BrokerId(*leader))
            })
            .collect();
        MetadataResponseTopic::default()
            .with_name(Some(topic_name(name)))
            .with_partitions(partitions)
    }

    pub fn metadata(brokers: &[i32], topics: Vec<MetadataResponseTopic>) -> MetadataResponse {
        MetadataResponse::default()
            .with_brokers(brokers.iter().copied().map(broker).collect())
            .with_topics(topics)
    }

    pub fn partition(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, Partition::from(partition))
    }

    pub fn for_data(peer: Peer) -> ConnectionId {
        ConnectionId {
            traffic: Traffic::Data,
            peer,
        }
    }

    pub fn for_groups(peer: Peer) -> ConnectionId {
        ConnectionId {
            traffic: Traffic::Group,
            peer,
        }
    }

    pub fn to_broker(id: i32) -> Destination {
        Destination {
            connection: for_data(Peer::Broker(BrokerId(id))),
            address: BrokerAddress::new(format!("broker-{id}"), 9092).unwrap(),
        }
    }

    fn to_bootstrap(place: usize, host: &str) -> Destination {
        Destination {
            connection: for_data(Peer::Bootstrap(place)),
            address: BrokerAddress::new(host, 9092).unwrap(),
        }
    }

    #[test]
    fn a_new_view_only_knows_the_bootstrap_addresses() {
        let view = view();

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), None);
        assert_eq!(
            view.any_broker(Traffic::Data),
            Candidates {
                first: to_bootstrap(0, "bootstrap-a"),
                others: VecDeque::from([to_bootstrap(1, "bootstrap-b")]),
            }
        );
    }

    #[test]
    fn metadata_names_the_leader_of_each_partition() {
        let mut view = view();

        view.learn(&metadata(&[1, 2], vec![topic("orders", &[2, 1])]), Instant::ZERO);

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), Some(to_broker(2)));
        assert_eq!(view.leader(&partition("orders", 1), Instant::ZERO), Some(to_broker(1)));
        assert_eq!(view.leader(&partition("orders", 2), Instant::ZERO), None);
    }

    #[test]
    fn known_brokers_come_before_the_bootstrap_addresses() {
        let mut view = view();

        view.learn(&metadata(&[2, 1], vec![]), Instant::ZERO);

        let candidates = view.any_broker(Traffic::Data);
        assert_eq!(candidates.first, to_broker(1));
        assert_eq!(
            candidates.others,
            [
                to_broker(2),
                to_bootstrap(0, "bootstrap-a"),
                to_bootstrap(1, "bootstrap-b")
            ]
        );
    }

    #[test]
    fn the_two_kinds_of_traffic_share_no_connection() {
        let mut view = view();
        view.learn(&metadata(&[1], vec![]), Instant::ZERO);

        let data = view.any_broker(Traffic::Data);
        let groups = view.any_broker(Traffic::Group);

        assert_eq!(data.first.connection, for_data(Peer::Broker(BrokerId(1))));
        assert_eq!(groups.first.connection, for_groups(Peer::Broker(BrokerId(1))));
        assert_eq!(data.first.address, groups.first.address);
        let others: Vec<_> = groups.others.iter().map(|other| other.connection).collect();
        assert_eq!(others, [for_groups(Peer::Bootstrap(0)), for_groups(Peer::Bootstrap(1))]);
    }

    #[test]
    fn a_connection_that_got_no_response_moves_behind_the_others() {
        let mut view = view();
        view.learn(&metadata(&[1, 2], vec![]), Instant::ZERO);

        view.failed(for_data(Peer::Broker(BrokerId(1))));

        let candidates = view.any_broker(Traffic::Data);
        assert_eq!(candidates.first, to_broker(2));
        assert_eq!(
            candidates.others,
            [
                to_bootstrap(0, "bootstrap-a"),
                to_bootstrap(1, "bootstrap-b"),
                to_broker(1)
            ]
        );
        assert_eq!(
            view.any_broker(Traffic::Group).first.connection,
            for_groups(Peer::Broker(BrokerId(1))),
            "the other kind of traffic has a connection of its own to that broker"
        );

        view.answered(for_data(Peer::Broker(BrokerId(1))));

        assert_eq!(view.any_broker(Traffic::Data).first, to_broker(1));
    }

    #[test]
    fn a_leader_is_trusted_until_the_max_age() {
        let mut view = view();
        let learned_at = Instant::ZERO + Duration::from_secs(10);
        view.learn(&metadata(&[1], vec![topic("orders", &[1])]), learned_at);

        assert!(view.leader(&partition("orders", 0), learned_at + MAX_AGE).is_some());
        assert_eq!(
            view.leader(&partition("orders", 0), learned_at + MAX_AGE + Duration::from_millis(1)),
            None
        );
    }

    #[test]
    fn newer_metadata_replaces_the_leaders_of_its_topics_only() {
        let mut view = view();
        view.learn(
            &metadata(&[1, 2], vec![topic("orders", &[1, 1]), topic("payments", &[2])]),
            Instant::ZERO,
        );

        view.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), Instant::ZERO);

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), Some(to_broker(2)));
        assert_eq!(view.leader(&partition("orders", 1), Instant::ZERO), None);
        assert_eq!(
            view.leader(&partition("payments", 0), Instant::ZERO),
            Some(to_broker(2))
        );
    }

    #[test]
    fn a_partition_with_an_error_or_without_a_leader_has_no_leader() {
        let mut view = view();
        let mut orders = topic("orders", &[1, -1]);
        orders.partitions[0].error_code = ResponseError::LeaderNotAvailable.code();

        view.learn(&metadata(&[1], vec![orders]), Instant::ZERO);

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), None);
        assert_eq!(view.leader(&partition("orders", 1), Instant::ZERO), None);
    }

    #[test]
    fn a_leader_that_is_no_broker_of_the_cluster_is_no_leader() {
        let mut view = view();

        view.learn(&metadata(&[1], vec![topic("orders", &[9])]), Instant::ZERO);

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), None);
    }

    #[test]
    fn a_broker_without_a_usable_address_is_left_out() {
        let mut view = view();
        let mut response = metadata(&[1, 2], vec![topic("orders", &[1])]);
        response.brokers[0].port = -1;
        response.brokers[1].host = StrBytes::from_static_str("");

        view.learn(&response, Instant::ZERO);

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), None);
        assert_eq!(view.any_broker(Traffic::Data).first, to_bootstrap(0, "bootstrap-a"));
    }

    #[test]
    fn a_forgotten_leader_has_to_be_asked_again() {
        let mut view = view();
        view.learn(&metadata(&[1], vec![topic("orders", &[1, 1])]), Instant::ZERO);

        view.forget_leader(&partition("orders", 0));

        assert_eq!(view.leader(&partition("orders", 0), Instant::ZERO), None);
        assert!(view.leader(&partition("orders", 1), Instant::ZERO).is_some());
    }

    #[test]
    fn a_coordinator_has_a_connection_of_its_own() {
        let mut view = view();
        view.learn(&metadata(&[1], vec![topic("orders", &[1])]), Instant::ZERO);
        let group = group_id("analytics");

        assert_eq!(view.coordinator(&group), None);
        assert!(view.learn_coordinator(&group, BrokerId(1), "broker-1", 9092));

        let coordinator = view.coordinator(&group).unwrap();
        let leader = view.leader(&partition("orders", 0), Instant::ZERO).unwrap();
        assert_eq!(coordinator.connection, for_groups(Peer::Broker(BrokerId(1))));
        assert_eq!(leader.connection, for_data(Peer::Broker(BrokerId(1))));
        assert_eq!(coordinator.address, leader.address);

        view.forget_coordinator(&group);
        assert_eq!(view.coordinator(&group), None);
    }

    #[test]
    fn a_coordinator_without_a_usable_address_is_not_learned() {
        let mut view = view();
        let group = group_id("analytics");

        assert!(!view.learn_coordinator(&group, BrokerId(-1), "", -1));

        assert_eq!(view.coordinator(&group), None);
    }

    #[test]
    fn two_views_learn_nothing_from_each_other() {
        let mut producer = view();
        let mut consumer = view();
        consumer.learn(&metadata(&[1, 2], vec![topic("orders", &[1])]), Instant::ZERO);

        producer.learn(&metadata(&[1, 2], vec![topic("orders", &[2])]), Instant::ZERO);

        assert_eq!(
            producer.leader(&partition("orders", 0), Instant::ZERO),
            Some(to_broker(2))
        );
        assert_eq!(
            consumer.leader(&partition("orders", 0), Instant::ZERO),
            Some(to_broker(1))
        );
    }
}
