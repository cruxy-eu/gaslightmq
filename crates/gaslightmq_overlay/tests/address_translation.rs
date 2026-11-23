//! Address translation, on responses as the real cluster encodes them, at the versions clients speak.
//!
//! Every test reads the truth as a client does: from bytes, at the version of its request.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use bytes::{BufMut, BytesMut};
use common::{
    ADVERTISED_HOST, BROKERS, REAL_PORT, advertised_address, advertised_port, from_a_client, from_the_cluster, overlay,
    read_by_a_client, real_host, text,
};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_overlay::prelude::*;
use gaslightmq_wire::messages::describe_cluster_response::DescribeClusterBroker;
use gaslightmq_wire::messages::find_coordinator_response::Coordinator;
use gaslightmq_wire::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use gaslightmq_wire::messages::produce_response::{
    LeaderIdAndEpoch, NodeEndpoint, PartitionProduceResponse, TopicProduceResponse,
};
use gaslightmq_wire::messages::{
    DescribeClusterRequest, DescribeClusterResponse, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest,
    HeartbeatResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, ShareAcknowledgeRequest,
    ShareAcknowledgeResponse, ShareFetchRequest, ShareFetchResponse, share_acknowledge_response, share_fetch_response,
};
use gaslightmq_wire::{Frame, Request, Response};

const METADATA_VERSIONS: [i16; 7] = [0, 1, 5, 8, 9, 12, 13];
const COORDINATOR_VERSIONS: [i16; 4] = [0, 1, 2, 3];
const COORDINATORS_VERSIONS: [i16; 3] = [4, 5, 6];
const DESCRIBE_CLUSTER_VERSIONS: [i16; 3] = [0, 1, 2];

fn broker(id: u16) -> BrokerId {
    BrokerId::from(id)
}

/// The rack of a real broker, which Metadata tells since version 1.
fn rack(broker: u16) -> String {
    format!("eu-west-1{}", ["a", "b", "c"][usize::from(broker) - 1])
}

/// What the real cluster answers a Metadata request of `version`:
/// its three brokers, and `orders.eu` with one partition on each of them.
fn cluster_metadata(version: i16) -> MetadataResponse {
    let brokers = BROKERS.map(|id| {
        let broker = MetadataResponseBroker::default()
            .with_node_id(i32::from(id).into())
            .with_host(text(&real_host(id)))
            .with_port(REAL_PORT);
        if version >= 1 {
            broker.with_rack(Some(text(&rack(id))))
        } else {
            broker
        }
    });
    let partitions = BROKERS.map(|id| {
        let leader = i32::from(id);
        let partition = MetadataResponsePartition::default()
            .with_partition_index(leader - 1)
            .with_leader_id(leader.into())
            .with_replica_nodes(vec![leader.into(), (leader % 3 + 1).into()])
            .with_isr_nodes(vec![leader.into()]);
        if version >= 7 {
            partition.with_leader_epoch(4)
        } else {
            partition
        }
    });
    let orders = MetadataResponseTopic::default()
        .with_name(Some(text("orders.eu").into()))
        .with_partitions(partitions.to_vec());
    let orders = if version >= 10 {
        orders.with_topic_id([7; 16].to_vec().try_into().unwrap())
    } else {
        orders
    };
    let response = MetadataResponse::default()
        .with_brokers(brokers.to_vec())
        .with_topics(vec![orders]);
    let response = if version >= 1 {
        response.with_controller_id(3.into())
    } else {
        response
    };
    if version >= 2 {
        response.with_cluster_id(Some(text("staging-eu")))
    } else {
        response
    }
}

/// The hosts and ports of the brokers of a Metadata response, in its order.
fn addresses(response: &MetadataResponse) -> Vec<(i32, String, i32)> {
    response
        .brokers
        .iter()
        .map(|broker| (broker.node_id.0, broker.host.to_string(), broker.port))
        .collect()
}

/// The addresses of the three listeners, as a response tells them.
fn listeners() -> Vec<(i32, String, i32)> {
    BROKERS
        .map(|id| {
            (
                i32::from(id),
                ADVERTISED_HOST.to_owned(),
                i32::from(advertised_port(id)),
            )
        })
        .to_vec()
}

#[test]
fn metadata_tells_the_listeners_at_every_version() {
    for version in METADATA_VERSIONS {
        let mut overlay = overlay();
        let request = from_a_client(version, 7, &MetadataRequest::default());
        let cluster = cluster_metadata(version);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();

        assert_eq!(truth.response().api(), request.api(), "v{version}");
        assert_eq!(truth.response().correlation_id(), 7, "v{version}");
        let told: MetadataResponse = read_by_a_client(truth.response());
        assert_eq!(addresses(&told), listeners(), "v{version}");

        // Nothing but the hosts and the ports changed.
        let mut expected = cluster.clone();
        for (broker, id) in expected.brokers.iter_mut().zip(BROKERS) {
            broker.host = text(ADVERTISED_HOST);
            broker.port = i32::from(advertised_port(id));
        }
        assert_eq!(told, expected, "v{version}");

        // The answer of the cluster stands next to the truth.
        let upstream: MetadataResponse = truth.upstream().unwrap().body().unwrap();
        assert_eq!(upstream, cluster, "v{version}");
    }
}

#[test]
fn metadata_teaches_the_overlay_where_the_real_brokers_are() {
    let mut overlay = Overlay::new(AdvertisedAddresses::new());
    let request = from_a_client(12, 7, &MetadataRequest::default());

    overlay
        .answer(&request, from_the_cluster(&request, &cluster_metadata(12)))
        .unwrap();

    let real: Vec<(BrokerId, String)> = overlay
        .real_brokers()
        .map(|(broker, address)| (broker, address.to_string()))
        .collect();
    assert_eq!(
        real,
        [
            (broker(1), "kafka-1.internal:9092".to_owned()),
            (broker(2), "kafka-2.internal:9092".to_owned()),
            (broker(3), "kafka-3.internal:9092".to_owned()),
        ]
    );
    assert_eq!(overlay.real_address(broker(4)), None);
}

#[test]
fn a_broker_without_a_listener_is_left_out_until_it_is_advertised() {
    for version in METADATA_VERSIONS {
        // The backend runs listeners for brokers 1 and 3, and broker 2 just joined the cluster.
        let advertised =
            AdvertisedAddresses::of([(broker(1), advertised_address(1)), (broker(3), advertised_address(3))]).unwrap();
        let mut overlay = Overlay::new(advertised);
        let request = from_a_client(version, 7, &MetadataRequest::default());
        let cluster = from_the_cluster(&request, &cluster_metadata(version));

        let truth = overlay.answer(&request, cluster.clone()).unwrap();
        let told: MetadataResponse = read_by_a_client(truth.response());

        // No address of the real cluster is in the truth, and the partition of broker 2 still names its leader.
        let listeners = listeners();
        assert_eq!(
            addresses(&told),
            [listeners[0].clone(), listeners[2].clone()],
            "v{version}"
        );
        assert_eq!(told.topics[0].partitions[1].leader_id.0, 2, "v{version}");
        assert!(!format!("{told:?}").contains("kafka-"), "v{version}");

        // The overlay names the broker, and knows where it is.
        assert_eq!(overlay.unadvertised().collect::<Vec<_>>(), [broker(2)], "v{version}");
        assert_eq!(
            overlay.real_address(broker(2)),
            Some(&"kafka-2.internal:9092".parse().unwrap())
        );

        // The backend opens a listener, and the next answer has the broker.
        overlay.advertise(broker(2), advertised_address(2)).unwrap();
        let truth = overlay.answer(&request, cluster).unwrap();
        assert_eq!(addresses(&read_by_a_client(truth.response())), listeners, "v{version}");
        assert_eq!(overlay.unadvertised().count(), 0);
    }
}

#[test]
fn a_response_that_tells_the_advertised_addresses_already_passes_byte_for_byte() {
    // The proxy stands in front of brokers that advertise the listeners themselves.
    let mut overlay = overlay();
    let request = from_a_client(12, 7, &MetadataRequest::default());
    let mut cluster = cluster_metadata(12);
    for (broker, id) in cluster.brokers.iter_mut().zip(BROKERS) {
        broker.host = text(ADVERTISED_HOST);
        broker.port = i32::from(advertised_port(id));
    }
    let upstream = from_the_cluster(&request, &cluster);

    let truth = overlay.answer(&request, upstream.clone()).unwrap();

    assert_eq!(truth.response().frame(), upstream.frame());
    assert!(truth.upstream().is_none());
}

/// A `FindCoordinator` request for the group `payments`, in the shape of its version.
fn find_coordinator(version: i16) -> Request {
    let body = if version >= 4 {
        FindCoordinatorRequest::default().with_coordinator_keys(vec![text("payments"), text("analytics-etl")])
    } else {
        FindCoordinatorRequest::default().with_key(text("payments"))
    };
    from_a_client(version, 11, &body)
}

/// The coordinator of one key on a real broker, as versions 4 and later answer.
fn coordinator_on(key: &str, broker: u16) -> Coordinator {
    Coordinator::default()
        .with_key(text(key))
        .with_node_id(i32::from(broker).into())
        .with_host(text(&real_host(broker)))
        .with_port(REAL_PORT)
}

#[test]
fn find_coordinator_tells_the_listener_of_the_coordinator_at_the_versions_with_one_key() {
    for version in COORDINATOR_VERSIONS {
        let mut overlay = overlay();
        let request = find_coordinator(version);
        let cluster = FindCoordinatorResponse::default()
            .with_node_id(2.into())
            .with_host(text(&real_host(2)))
            .with_port(REAL_PORT);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FindCoordinatorResponse = read_by_a_client(truth.response());

        assert_eq!(
            (told.error_code, told.node_id.0, told.host.as_str(), told.port),
            (0, 2, ADVERTISED_HOST, 19093),
            "v{version}"
        );
        let upstream: FindCoordinatorResponse = truth.upstream().unwrap().body().unwrap();
        assert_eq!(upstream.host.as_str(), "kafka-2.internal", "v{version}");
    }
}

#[test]
fn find_coordinator_tells_the_listener_of_every_key_at_the_versions_with_several() {
    for version in COORDINATORS_VERSIONS {
        let mut overlay = overlay();
        let request = find_coordinator(version);
        let cluster = FindCoordinatorResponse::default()
            .with_coordinators(vec![coordinator_on("payments", 2), coordinator_on("analytics-etl", 3)]);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FindCoordinatorResponse = read_by_a_client(truth.response());

        let coordinators: Vec<(&str, i16, i32, &str, i32)> = told
            .coordinators
            .iter()
            .map(|c| (c.key.as_str(), c.error_code, c.node_id.0, c.host.as_str(), c.port))
            .collect();
        assert_eq!(
            coordinators,
            [
                ("payments", 0, 2, ADVERTISED_HOST, 19093),
                ("analytics-etl", 0, 3, ADVERTISED_HOST, 19094)
            ],
            "v{version}"
        );
    }
}

#[test]
fn a_coordinator_that_the_cluster_does_not_find_stays_as_the_cluster_said() {
    let not_available = ErrorCode::COORDINATOR_NOT_AVAILABLE.value();

    for version in COORDINATOR_VERSIONS {
        let mut overlay = overlay();
        let request = find_coordinator(version);
        let cluster = FindCoordinatorResponse::default()
            .with_error_code(not_available)
            .with_node_id((-1).into())
            .with_port(-1);
        let upstream = from_the_cluster(&request, &cluster);

        let truth = overlay.answer(&request, upstream.clone()).unwrap();

        assert_eq!(truth.response().frame(), upstream.frame(), "v{version}");
        assert!(truth.upstream().is_none(), "v{version}");
    }

    for version in COORDINATORS_VERSIONS {
        let mut overlay = overlay();
        let request = find_coordinator(version);
        let missing = Coordinator::default()
            .with_key(text("payments"))
            .with_error_code(not_available)
            .with_node_id((-1).into())
            .with_port(-1);
        let cluster =
            FindCoordinatorResponse::default().with_coordinators(vec![missing, coordinator_on("analytics-etl", 3)]);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FindCoordinatorResponse = read_by_a_client(truth.response());

        // The key that has a coordinator is translated, and the other one is untouched.
        assert_eq!(told.coordinators[0], cluster.coordinators[0], "v{version}");
        assert_eq!(told.coordinators[1].host.as_str(), ADVERTISED_HOST, "v{version}");
    }
}

#[test]
fn a_coordinator_on_a_broker_without_a_listener_is_not_available() {
    let not_available = ErrorCode::COORDINATOR_NOT_AVAILABLE.value();
    let only_broker_3 = || Overlay::new(AdvertisedAddresses::of([(broker(3), advertised_address(3))]).unwrap());

    for version in COORDINATOR_VERSIONS {
        let mut overlay = only_broker_3();
        let request = find_coordinator(version);
        let cluster = FindCoordinatorResponse::default()
            .with_node_id(2.into())
            .with_host(text(&real_host(2)))
            .with_port(REAL_PORT);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FindCoordinatorResponse = read_by_a_client(truth.response());

        assert_eq!(
            (told.error_code, told.node_id.0, told.host.as_str(), told.port),
            (not_available, -1, "", -1),
            "v{version}"
        );
        // Version 0 has no place for a message.
        if version >= 1 {
            assert_eq!(
                told.error_message,
                Some(text("The coordinator is not available.")),
                "v{version}"
            );
        }
        assert_eq!(overlay.unadvertised().collect::<Vec<_>>(), [broker(2)], "v{version}");
    }

    for version in COORDINATORS_VERSIONS {
        let mut overlay = only_broker_3();
        let request = find_coordinator(version);
        let cluster = FindCoordinatorResponse::default()
            .with_coordinators(vec![coordinator_on("payments", 2), coordinator_on("analytics-etl", 3)]);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FindCoordinatorResponse = read_by_a_client(truth.response());

        let payments = &told.coordinators[0];
        assert_eq!(
            (
                payments.key.as_str(),
                payments.error_code,
                payments.node_id.0,
                payments.host.as_str(),
                payments.port
            ),
            ("payments", not_available, -1, "", -1),
            "v{version}"
        );
        assert_eq!(
            payments.error_message,
            Some(text("The coordinator is not available.")),
            "v{version}"
        );
        // The other key has a coordinator with a listener.
        let analytics = &told.coordinators[1];
        assert_eq!(
            (
                analytics.error_code,
                analytics.node_id.0,
                analytics.host.as_str(),
                analytics.port
            ),
            (0, 3, ADVERTISED_HOST, 19094),
            "v{version}"
        );
        assert!(!format!("{told:?}").contains("kafka-"), "v{version}");
    }
}

#[test]
fn describe_cluster_tells_the_listeners_at_every_version() {
    for version in DESCRIBE_CLUSTER_VERSIONS {
        let mut overlay = Overlay::new(
            AdvertisedAddresses::of([(broker(1), advertised_address(1)), (broker(2), advertised_address(2))]).unwrap(),
        );
        let request = from_a_client(version, 5, &DescribeClusterRequest::default());
        let brokers = BROKERS.map(|id| {
            DescribeClusterBroker::default()
                .with_broker_id(i32::from(id).into())
                .with_host(text(&real_host(id)))
                .with_port(REAL_PORT)
                .with_rack(Some(text(&rack(id))))
        });
        let cluster = DescribeClusterResponse::default()
            .with_cluster_id(text("staging-eu"))
            .with_controller_id(3.into())
            .with_brokers(brokers.to_vec());

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: DescribeClusterResponse = read_by_a_client(truth.response());

        // Broker 3 has no listener, so the client is not told about it.
        let addresses: Vec<(i32, &str, i32, Option<&str>)> = told
            .brokers
            .iter()
            .map(|b| (b.broker_id.0, b.host.as_str(), b.port, b.rack.as_deref()))
            .collect();
        assert_eq!(
            addresses,
            [
                (1, ADVERTISED_HOST, 19092, Some("eu-west-1a")),
                (2, ADVERTISED_HOST, 19093, Some("eu-west-1b"))
            ],
            "v{version}"
        );
        assert_eq!(told.cluster_id.as_str(), "staging-eu", "v{version}");
        assert_eq!(told.controller_id.0, 3, "v{version}");
        assert_eq!(overlay.unadvertised().collect::<Vec<_>>(), [broker(3)], "v{version}");
    }
}

#[test]
fn describe_cluster_passes_a_list_of_controllers() {
    for version in [1, 2] {
        let mut overlay = Overlay::new(AdvertisedAddresses::of([(broker(1), advertised_address(1))]).unwrap());
        let request = from_a_client(version, 5, &DescribeClusterRequest::default().with_endpoint_type(2));
        // Controller 1 is not broker 1, and no listener stands for it.
        let controller = DescribeClusterBroker::default()
            .with_broker_id(1.into())
            .with_host(text("controller-1.internal"))
            .with_port(9093);
        let cluster = DescribeClusterResponse::default()
            .with_endpoint_type(2)
            .with_brokers(vec![controller]);
        let upstream = from_the_cluster(&request, &cluster);

        let truth = overlay.answer(&request, upstream.clone()).unwrap();

        assert_eq!(truth.response().frame(), upstream.frame(), "v{version}");
        assert!(truth.upstream().is_none(), "v{version}");
        assert_eq!(overlay.real_address(broker(1)), None, "v{version}");
    }
}

/// A Produce response in which `orders.eu/0` moved to broker 2, with the address of that broker.
fn produce_to_a_partition_that_moved() -> ProduceResponse {
    let moved = PartitionProduceResponse::default()
        .with_index(0)
        .with_error_code(ErrorCode::NOT_LEADER_OR_FOLLOWER.value())
        .with_current_leader(
            LeaderIdAndEpoch::default()
                .with_leader_id(2.into())
                .with_leader_epoch(5),
        );
    let orders = TopicProduceResponse::default()
        .with_name(text("orders.eu").into())
        .with_partition_responses(vec![moved]);
    let leader = NodeEndpoint::default()
        .with_node_id(2.into())
        .with_host(text(&real_host(2)))
        .with_port(REAL_PORT)
        .with_rack(Some(text(&rack(2))));
    ProduceResponse::default()
        .with_responses(vec![orders])
        .with_node_endpoints(vec![leader])
}

#[test]
fn produce_tells_the_listener_of_the_new_leader() {
    for version in [10, 11, 12] {
        let mut overlay = overlay();
        let request = from_a_client(version, 21, &ProduceRequest::default());
        let cluster = produce_to_a_partition_that_moved();

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: ProduceResponse = read_by_a_client(truth.response());

        let leader = &told.node_endpoints[0];
        assert_eq!(
            (
                leader.node_id.0,
                leader.host.as_str(),
                leader.port,
                leader.rack.as_deref()
            ),
            (2, ADVERTISED_HOST, 19093, Some("eu-west-1b")),
            "v{version}"
        );
        // The partition still fails as the cluster said, and still names its new leader.
        assert_eq!(told.responses, cluster.responses, "v{version}");
    }
}

#[test]
fn produce_does_not_name_a_new_leader_without_a_listener() {
    let mut overlay = Overlay::new(AdvertisedAddresses::of([(broker(1), advertised_address(1))]).unwrap());
    let request = from_a_client(11, 21, &ProduceRequest::default());
    let cluster = produce_to_a_partition_that_moved();

    let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
    let told: ProduceResponse = read_by_a_client(truth.response());

    assert_eq!(told.node_endpoints, []);
    assert_eq!(told.responses, cluster.responses);
    assert_eq!(overlay.unadvertised().collect::<Vec<_>>(), [broker(2)]);
}

#[test]
fn a_produce_response_that_tells_no_address_is_not_read() {
    let mut overlay = overlay();

    // Before version 10 a Produce response has no place for an address.
    // The overlay does not decode it: a body that is no Produce response passes.
    let request = from_a_client(9, 21, &ProduceRequest::default());
    let mut payload = BytesMut::new();
    payload.put_i32(21);
    // The header of a flexible version ends in its number of tagged fields.
    payload.put_u8(0);
    payload.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let upstream = Response::decode(request.api(), Frame::new(payload.freeze())).unwrap();
    let truth = overlay.answer(&request, upstream.clone()).unwrap();
    assert_eq!(truth.response().frame(), upstream.frame());
    assert!(truth.upstream().is_none());

    // Since version 10 it is read, and one that names no leader passes byte for byte.
    let request = from_a_client(11, 22, &ProduceRequest::default());
    let appended = PartitionProduceResponse::default().with_index(0).with_base_offset(120);
    let orders = TopicProduceResponse::default()
        .with_name(text("orders.eu").into())
        .with_partition_responses(vec![appended]);
    let upstream = from_the_cluster(&request, &ProduceResponse::default().with_responses(vec![orders]));
    let truth = overlay.answer(&request, upstream.clone()).unwrap();
    assert_eq!(truth.response().frame(), upstream.frame());
    assert!(truth.upstream().is_none());
}

#[test]
fn share_fetch_and_share_acknowledge_tell_the_listener_of_the_new_leader() {
    let mut overlay = overlay();

    let request = from_a_client(1, 31, &ShareFetchRequest::default());
    let leader = share_fetch_response::NodeEndpoint::default()
        .with_node_id(3.into())
        .with_host(text(&real_host(3)))
        .with_port(REAL_PORT);
    let cluster = ShareFetchResponse::default().with_node_endpoints(vec![leader]);
    let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
    let told: ShareFetchResponse = read_by_a_client(truth.response());
    let leader = &told.node_endpoints[0];
    assert_eq!(
        (leader.node_id.0, leader.host.as_str(), leader.port),
        (3, ADVERTISED_HOST, 19094)
    );

    let request = from_a_client(1, 32, &ShareAcknowledgeRequest::default());
    let leader = share_acknowledge_response::NodeEndpoint::default()
        .with_node_id(1.into())
        .with_host(text(&real_host(1)))
        .with_port(REAL_PORT);
    let cluster = ShareAcknowledgeResponse::default().with_node_endpoints(vec![leader]);
    let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
    let told: ShareAcknowledgeResponse = read_by_a_client(truth.response());
    let leader = &told.node_endpoints[0];
    assert_eq!(
        (leader.node_id.0, leader.host.as_str(), leader.port),
        (1, ADVERTISED_HOST, 19092)
    );
}

#[test]
fn a_response_that_tells_no_address_passes_byte_for_byte() {
    let mut overlay = overlay();

    let request = from_a_client(4, 41, &HeartbeatRequest::default());
    let upstream = from_the_cluster(&request, &HeartbeatResponse::default().with_error_code(27));
    let truth = overlay.answer(&request, upstream.clone()).unwrap();
    assert_eq!(truth.response().frame(), upstream.frame());
    assert!(truth.upstream().is_none());

    // An API without messages has a response nobody can read, and it passes too.
    let mut sent = BytesMut::new();
    sent.put_i16(9999);
    sent.put_i16(3);
    sent.put_i32(42);
    sent.put_i16(-1);
    sent.put_slice(&[1, 2, 3]);
    let request = Request::decode(Frame::new(sent.freeze())).unwrap();
    let Forwarding::Forward { to, request: forwarded } = overlay.forward(Endpoint::Bootstrap, &request).unwrap() else {
        panic!("a request of an unknown API is forwarded");
    };
    assert_eq!(to, Destination::AnyBroker);
    assert_eq!(forwarded.frame(), request.frame());

    let mut answered = BytesMut::new();
    answered.put_i32(42);
    answered.put_slice(&[4, 5, 6]);
    let upstream = Response::decode(request.api(), Frame::new(answered.freeze())).unwrap();
    let truth = overlay.answer(&request, upstream.clone()).unwrap();
    assert_eq!(truth.into_response().frame(), upstream.frame());
}

#[test]
fn a_response_that_cannot_be_read_gives_no_truth() {
    let mut overlay = overlay();
    let request = from_a_client(12, 7, &MetadataRequest::default());
    let whole = from_the_cluster(&request, &cluster_metadata(12));
    let cut = whole.frame().payload().slice(..whole.frame().payload().len() - 9);
    let upstream = Response::decode(request.api(), Frame::new(cut)).unwrap();

    let error = overlay.answer(&request, upstream).unwrap_err();

    assert!(matches!(&error, AnswerError::NotAnAnswer { .. }), "{error:?}");
    assert!(
        error
            .to_string()
            .starts_with("the Metadata v12 response of the cluster cannot be read: cannot decode response body: ")
    );
}

#[test]
fn a_response_to_another_request_gives_no_truth() {
    let mut overlay = overlay();
    let request = from_a_client(12, 7, &MetadataRequest::default());
    let other = from_a_client(12, 8, &MetadataRequest::default());
    let heartbeat = from_a_client(4, 7, &HeartbeatRequest::default());

    let of_another_correlation = from_the_cluster(&other, &cluster_metadata(12));
    assert_eq!(
        overlay.answer(&request, of_another_correlation).unwrap_err(),
        AnswerError::NotAnAnswer {
            reason: "the Metadata v12 response 8 does not answer the Metadata v12 request 7".to_owned(),
        }
    );

    let of_another_api = from_the_cluster(&heartbeat, &HeartbeatResponse::default());
    let error = overlay.answer(&request, of_another_api).unwrap_err();
    assert_eq!(
        error.to_string(),
        "the Heartbeat v4 response 7 does not answer the Metadata v12 request 7"
    );
    assert!(matches!(error, AnswerError::NotAnAnswer { .. }), "{error:?}");
}

#[test]
fn a_request_goes_to_the_real_broker_behind_the_endpoint_of_its_client() {
    let overlay = overlay();
    let request = from_a_client(12, 7, &MetadataRequest::default());

    for id in BROKERS {
        let Forwarding::Forward { to, request: forwarded } =
            overlay.forward(Endpoint::Broker(broker(id)), &request).unwrap()
        else {
            panic!("a Metadata request is forwarded");
        };
        assert_eq!(to.broker(), Some(broker(id)));
        assert_eq!(
            to.address().unwrap().to_string(),
            format!("{}:{REAL_PORT}", real_host(id))
        );
        assert_eq!(forwarded.frame(), request.frame());
    }

    // The bootstrap endpoint stands for no broker.
    let Forwarding::Forward { to, .. } = overlay.forward(Endpoint::Bootstrap, &request).unwrap() else {
        panic!("a Metadata request is forwarded");
    };
    assert_eq!(to, Destination::AnyBroker);

    // An endpoint for a broker that the cluster never named leads nowhere.
    let error = overlay.forward(Endpoint::Broker(broker(9)), &request).unwrap_err();
    assert_eq!(error, ForwardError::UnknownBroker { broker: broker(9) });
    assert_eq!(
        error.to_string(),
        "a request for broker 9 has nowhere to go: the cluster named no such broker"
    );
}
