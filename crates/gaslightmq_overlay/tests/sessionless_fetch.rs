//! Sessionless Fetch, on requests as a client encodes them and on responses as the real cluster does.
//!
//! Every test reads what the cluster gets and what the client gets from bytes, at the version of the request.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use bytes::{BufMut, Bytes, BytesMut};
use common::{
    ADVERTISED_HOST, REAL_PORT, advertised_port, from_a_client, from_the_cluster, overlay, read_by_a_client, real_host,
    text,
};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_overlay::prelude::*;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::fetch_response::{
    FetchableTopicResponse, LeaderIdAndEpoch, NodeEndpoint, PartitionData,
};
use gaslightmq_wire::messages::{FetchRequest, FetchResponse};
use gaslightmq_wire::{Frame, Request, Response, WireError};

/// The versions of Fetch without fetch sessions.
const BEFORE_SESSIONS: [i16; 3] = [4, 5, 6];

/// Versions of Fetch with fetch sessions: the first one, the last one before flexible versions, the first flexible one,
/// the first one that names topics by ID, the first one without a replica ID, the first one that names new leaders,
/// and the last two.
const WITH_SESSIONS: [i16; 8] = [7, 11, 12, 13, 15, 16, 17, 18];

/// The first version of Fetch that names a topic by its ID alone.
const FIRST_VERSION_WITH_TOPIC_IDS: i16 = 13;

/// The records of `orders.eu/3`, as opaque bytes: the overlay does not look into them.
const RECORDS: &[u8] = b"three records of orders.eu/3, as the cluster stored them";

/// The listener of broker 2, where the consumer of these tests fetches.
fn broker_2() -> Endpoint {
    Endpoint::Broker(BrokerId::from(2))
}

/// A full Fetch of `orders.eu/3` and `orders.eu/4`, with the session fields where the version has them.
fn fetch(version: i16, session_id: i32, session_epoch: i32) -> FetchRequest {
    let partition = |index: i32, offset: i64| {
        FetchPartition::default()
            .with_partition(index)
            .with_fetch_offset(offset)
            .with_partition_max_bytes(1_048_576)
    };
    let orders = FetchTopic::default().with_partitions(vec![partition(3, 40), partition(4, 0)]);
    let orders = if version >= FIRST_VERSION_WITH_TOPIC_IDS {
        orders.with_topic_id([7; 16].to_vec().try_into().unwrap())
    } else {
        orders.with_topic(text("orders.eu").into())
    };
    let body = FetchRequest::default()
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_topics(vec![orders]);
    if version >= 7 {
        body.with_session_id(session_id).with_session_epoch(session_epoch)
    } else {
        body
    }
}

/// What a real broker answers that Fetch: records for `orders.eu/3`, nothing new for `orders.eu/4`.
fn fetched(version: i16, session_id: i32) -> FetchResponse {
    let three = PartitionData::default()
        .with_partition_index(3)
        .with_high_watermark(43)
        .with_last_stable_offset(43)
        .with_records(Some(Bytes::from_static(RECORDS)));
    let four = PartitionData::default()
        .with_partition_index(4)
        .with_high_watermark(0)
        .with_last_stable_offset(0)
        .with_records(Some(Bytes::new()));
    let orders = FetchableTopicResponse::default().with_partitions(vec![three, four]);
    let orders = if version >= FIRST_VERSION_WITH_TOPIC_IDS {
        orders.with_topic_id([7; 16].to_vec().try_into().unwrap())
    } else {
        orders.with_topic(text("orders.eu").into())
    };
    let body = FetchResponse::default().with_responses(vec![orders]);
    if version >= 7 {
        body.with_session_id(session_id)
    } else {
        body
    }
}

/// The request that the overlay forwards, which must go to the real broker 2.
fn forwarded(overlay: &Overlay, request: &Request) -> Request {
    let Forwarding::Forward { to, request } = overlay.forward(broker_2(), request).unwrap() else {
        panic!("the {} request is forwarded", request.api());
    };
    assert_eq!(to.to_string(), "broker 2 at kafka-2.internal:9092");
    // The cluster reads it from bytes.
    Request::decode(Frame::new(request.frame().payload().clone())).unwrap()
}

#[test]
fn a_fetch_before_fetch_sessions_passes_byte_for_byte() {
    let mut overlay = overlay();

    for version in BEFORE_SESSIONS {
        let request = from_a_client(version, 42, &fetch(version, 0, -1));
        assert_eq!(forwarded(&overlay, &request).frame(), request.frame(), "v{version}");

        let upstream = from_the_cluster(&request, &fetched(version, 0));
        let truth = overlay.answer(&request, upstream.clone()).unwrap();
        assert_eq!(truth.response().frame(), upstream.frame(), "v{version}");
        assert!(truth.upstream().is_none(), "v{version}");
    }
}

#[test]
fn a_fetch_response_before_fetch_sessions_is_not_read() {
    let mut overlay = overlay();
    let request = from_a_client(6, 42, &fetch(6, 0, -1));
    let mut payload = BytesMut::new();
    payload.put_i32(42);
    payload.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let upstream = Response::decode(request.api(), Frame::new(payload.freeze())).unwrap();

    let truth = overlay.answer(&request, upstream.clone()).unwrap();

    assert_eq!(truth.response().frame(), upstream.frame());
}

#[test]
fn a_fetch_that_asks_for_no_session_passes_byte_for_byte() {
    let overlay = overlay();

    for version in WITH_SESSIONS {
        let request = from_a_client(version, 42, &fetch(version, 0, -1));
        assert_eq!(forwarded(&overlay, &request).frame(), request.frame(), "v{version}");
    }
}

#[test]
fn a_full_fetch_reaches_the_cluster_without_its_wish_for_a_session() {
    let overlay = overlay();

    // A new session, a new session in place of session 77, and no session in place of session 77.
    for (session_id, session_epoch) in [(0, 0), (77, 0), (77, -1)] {
        for version in WITH_SESSIONS {
            let asked = fetch(version, session_id, session_epoch);
            let request = from_a_client(version, 42, &asked);

            let sessionless = forwarded(&overlay, &request);

            assert_eq!(sessionless.api(), request.api(), "v{version}");
            assert_eq!(sessionless.correlation_id(), 42, "v{version}");
            assert_eq!(sessionless.client_id(), Some("order-ingest"), "v{version}");
            let seen: FetchRequest = sessionless.body().unwrap();
            assert_eq!((seen.session_id, seen.session_epoch), (0, -1), "v{version}");
            // The cluster is asked for the same partitions at the same offsets, with the same limits.
            assert_eq!(seen.topics, asked.topics, "v{version}");
            assert_eq!(
                seen.with_session_id(session_id).with_session_epoch(session_epoch),
                request.body::<FetchRequest>().unwrap(),
                "v{version}"
            );
        }
    }
}

#[test]
fn a_fetch_that_continues_a_session_is_answered_without_the_cluster() {
    let overlay = overlay();
    let no_session = ErrorCode::FETCH_SESSION_ID_NOT_FOUND.value();

    // Session 77 at its first and at a later epoch, and an epoch that belongs to no session.
    for (session_id, session_epoch) in [(77, 1), (77, 2048), (0, 5)] {
        for version in WITH_SESSIONS {
            let request = from_a_client(version, 42, &fetch(version, session_id, session_epoch));

            let Forwarding::Answer(response) = overlay.forward(broker_2(), &request).unwrap() else {
                panic!("a Fetch v{version} of session {session_id} at epoch {session_epoch} is answered");
            };

            assert_eq!(response.api(), request.api(), "v{version}");
            assert_eq!(response.correlation_id(), 42, "v{version}");
            let told: FetchResponse = read_by_a_client(&response);
            assert_eq!(told.error_code, no_session, "v{version}");
            assert_eq!(told.session_id, 0, "v{version}");
            // No partition has records, and none says anything but the error.
            let partitions: Vec<&PartitionData> = told.responses.iter().flat_map(|topic| &topic.partitions).collect();
            assert!(
                partitions.iter().all(
                    |partition| partition.error_code == no_session && partition.records.iter().all(Bytes::is_empty)
                ),
                "v{version}: {partitions:?}"
            );
        }
    }
}

#[test]
fn a_fetch_response_says_that_it_is_part_of_no_session() {
    for version in WITH_SESSIONS {
        let mut overlay = overlay();
        let request = from_a_client(version, 42, &fetch(version, 0, 0));
        // A broker that was asked for a session anyway grants session 77.
        let cluster = fetched(version, 77);

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();

        assert_eq!(truth.response().correlation_id(), 42, "v{version}");
        let told: FetchResponse = read_by_a_client(truth.response());
        assert_eq!(told.session_id, 0, "v{version}");
        // The records and the offsets are the cluster's.
        assert_eq!(
            told.responses[0].partitions[0].records.as_deref(),
            Some(RECORDS),
            "v{version}"
        );
        assert_eq!(told, cluster.clone().with_session_id(0), "v{version}");
        // The answer of the cluster stands next to the truth, with the session it granted.
        let upstream: FetchResponse = truth.upstream().unwrap().body().unwrap();
        assert_eq!(upstream.session_id, 77, "v{version}");
    }
}

#[test]
fn a_fetch_response_outside_a_session_passes_byte_for_byte() {
    for version in WITH_SESSIONS {
        let mut overlay = overlay();
        let request = from_a_client(version, 42, &fetch(version, 0, -1));
        let upstream = from_the_cluster(&request, &fetched(version, 0));

        let truth = overlay.answer(&request, upstream.clone()).unwrap();

        assert_eq!(truth.response().frame(), upstream.frame(), "v{version}");
        assert!(truth.upstream().is_none(), "v{version}");
    }
}

#[test]
fn a_fetch_response_tells_the_listener_of_the_new_leader() {
    for version in [16, 17, 18] {
        let mut overlay = overlay();
        let request = from_a_client(version, 42, &fetch(version, 0, -1));
        // `orders.eu/3` moved to broker 3, and the broker says where that is.
        let mut cluster = fetched(version, 0);
        let moved = &mut cluster.responses[0].partitions[0];
        moved.error_code = ErrorCode::NOT_LEADER_OR_FOLLOWER.value();
        moved.records = None;
        moved.current_leader = LeaderIdAndEpoch::default()
            .with_leader_id(3.into())
            .with_leader_epoch(5);
        cluster.node_endpoints = vec![
            NodeEndpoint::default()
                .with_node_id(3.into())
                .with_host(text(&real_host(3)))
                .with_port(REAL_PORT),
        ];

        let truth = overlay.answer(&request, from_the_cluster(&request, &cluster)).unwrap();
        let told: FetchResponse = read_by_a_client(truth.response());

        let leader = &told.node_endpoints[0];
        assert_eq!(
            (leader.node_id.0, leader.host.as_str(), leader.port),
            (3, ADVERTISED_HOST, i32::from(advertised_port(3))),
            "v{version}"
        );
        assert_eq!(told.responses, cluster.responses, "v{version}");
    }
}

#[test]
fn a_fetch_that_cannot_be_read_is_not_forwarded() {
    let overlay = overlay();
    let whole = from_a_client(12, 42, &fetch(12, 0, 0));
    let cut = whole.frame().payload().slice(..whole.frame().payload().len() - 4);
    let request = Request::decode(Frame::new(cut)).unwrap();

    let error = overlay.forward(broker_2(), &request).unwrap_err();

    assert!(matches!(
        &error,
        OverlayError::UnreadableRequest { api, error: WireError::Decode { .. } } if *api == request.api()
    ));
    assert!(
        error
            .to_string()
            .starts_with("the Fetch v12 request cannot be read: cannot decode request body: ")
    );
}
