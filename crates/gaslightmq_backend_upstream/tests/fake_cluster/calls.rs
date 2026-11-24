//! A call goes to the broker of its endpoint, on the connection of its client, and its answer comes back.

use std::sync::Arc;

use gaslightmq_backend::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::produce_request::ProduceRequest;
use gaslightmq_wire::messages::{FetchRequest, FetchResponse, HeartbeatResponse};
use gaslightmq_wire::{ApiKey, Request};
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::support::{
    A_WHILE, FakeCluster, Proxy, Reaction, backend, broker, calm, heartbeat, listening, metadata, named, soon, started,
    truth,
};

#[tokio::test]
async fn a_request_reaches_the_broker_of_its_endpoint_as_the_client_sent_it() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;
    let proxy = Proxy::new();

    let answer = soon(backend.call(proxy.forwards(7, broker(2), heartbeat(41)))).await;

    let truth = truth(answer);
    assert_eq!(truth.response().correlation_id(), 41);
    assert!(truth.response().body::<HeartbeatResponse>().is_ok());
    assert!(truth.upstream().is_none(), "the overlay changes no Heartbeat");
    let arrived = second.of_clients();
    assert_eq!(arrived.len(), 1);
    assert_eq!(arrived[0].request.frame(), heartbeat(41).frame());
    assert!(first.of_clients().is_empty());
}

#[tokio::test]
async fn a_metadata_answer_names_the_listeners_and_not_the_brokers() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;
    let proxy = Proxy::new();
    listening(&backend, 1, 50_001).await;
    listening(&backend, 2, 50_002).await;

    let answer = soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(5)))).await;

    let truth = truth(answer);
    assert_eq!(
        named(truth.response()),
        [
            (1, "gaslight.test:50001".to_owned()),
            (2, "gaslight.test:50002".to_owned())
        ]
    );
    assert_eq!(
        named(truth.upstream().unwrap()),
        [(1, first.address().to_string()), (2, second.address().to_string())]
    );
}

#[tokio::test]
async fn the_requests_of_one_connection_arrive_in_the_order_in_which_their_calls_start() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();

    // No call waits for the answer of the one before it.
    let calls = (1..=50).map(|correlation_id| backend.call(proxy.forwards(7, broker(1), heartbeat(correlation_id))));
    let calls = started(calls.collect()).await;
    let mut answered = Vec::new();
    for call in calls {
        answered.push(truth(soon(call).await).response().correlation_id());
    }

    let in_order: Vec<i32> = (1..=50).collect();
    assert_eq!(answered, in_order);
    let arrived: Vec<i32> = only.of_clients().iter().map(|a| a.request.correlation_id()).collect();
    assert_eq!(arrived, in_order);
    assert!(only.of_clients().iter().all(|arrival| arrival.connection == 1));
}

#[tokio::test]
async fn every_connection_of_a_client_has_its_own_connection_to_the_broker() {
    let cluster = FakeCluster::new();
    let held = Arc::new(Notify::new());
    let only = cluster
        .broker(1, {
            let held = Arc::clone(&held);
            move |request| {
                let held = Arc::clone(&held);
                async move {
                    // The broker holds the request with ID 1, as it holds a Fetch that waits for records.
                    if request.correlation_id() == 1 {
                        held.notified().await;
                    }
                    Reaction::Respond(calm(&request))
                }
            }
        })
        .await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();

    let waiting = started(vec![backend.call(proxy.forwards(7, broker(1), heartbeat(1)))]).await;
    let other = soon(backend.call(proxy.forwards(8, broker(1), heartbeat(2)))).await;

    assert_eq!(truth(other).response().correlation_id(), 2);
    // The connection of the backend's own question, and one for each of the two clients.
    assert_eq!(only.connections(), 3);
    held.notify_one();
    for call in waiting {
        assert_eq!(truth(soon(call).await).response().correlation_id(), 1);
    }
}

#[tokio::test]
async fn a_produce_without_acks_ends_once_it_is_on_its_way() {
    let cluster = FakeCluster::new();
    let only = cluster
        .broker(1, |request| async move {
            // The Produce says that it expects no response, and every other request gets one.
            if request.api().known() == Some(ApiKey::Produce) {
                Reaction::Nothing
            } else {
                Reaction::Respond(calm(&request))
            }
        })
        .await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();
    let produce = Request::new(9, 1, Some("order-ingest"), &ProduceRequest::default().with_acks(0)).unwrap();

    let answer = soon(backend.call(proxy.forwards(7, broker(1), produce))).await;
    assert!(matches!(answer, BackendAnswer::NoResponse), "{answer:?}");

    // The response that follows answers the request that follows.
    let next = soon(backend.call(proxy.forwards(7, broker(1), heartbeat(2)))).await;
    assert_eq!(truth(next).response().correlation_id(), 2);
    assert_eq!(only.of_clients().len(), 2);
}

#[tokio::test]
async fn a_connection_that_ended_closes_its_connection_to_the_broker() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();
    soon(backend.call(proxy.forwards(7, broker(1), heartbeat(1)))).await;
    soon(backend.call(proxy.forwards(8, broker(1), heartbeat(1)))).await;

    soon(backend.close(ConnectionNumber::new(7))).await;

    only.closed(1).await;
    // The other client goes on.
    let answer = soon(backend.call(proxy.forwards(8, broker(1), heartbeat(2)))).await;
    assert_eq!(truth(answer).response().correlation_id(), 2);
}

#[tokio::test]
async fn a_backend_that_is_let_go_closes_every_connection() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();
    soon(backend.call(proxy.forwards(7, broker(1), heartbeat(1)))).await;

    drop(backend);

    // The connection of the client, and the one the backend asks its own questions on.
    only.closed(2).await;
}

#[tokio::test]
async fn a_call_that_the_engine_dropped_leaves_the_next_answer_with_the_next_call() {
    let cluster = FakeCluster::new();
    let held = Arc::new(Notify::new());
    let only = cluster
        .broker(1, {
            let held = Arc::clone(&held);
            move |request| {
                let held = Arc::clone(&held);
                async move {
                    if request.correlation_id() == 1 {
                        held.notified().await;
                    }
                    Reaction::Respond(calm(&request))
                }
            }
        })
        .await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();

    let dropped = started(vec![backend.call(proxy.forwards(7, broker(1), heartbeat(1)))]).await;
    only.arrived(3).await;
    drop(dropped);
    let next = started(vec![backend.call(proxy.forwards(7, broker(1), heartbeat(2)))]).await;
    held.notify_one();

    // The broker answers both requests, and the first answer finds nobody.
    for call in next {
        assert_eq!(truth(soon(call).await).response().correlation_id(), 2);
    }
}

#[tokio::test]
async fn the_cluster_never_sees_a_fetch_session() {
    let cluster = FakeCluster::new();
    let only = cluster
        .broker(1, |request| async move {
            let granted = FetchResponse::default().with_session_id(77);
            Reaction::Respond(request.response(&granted).unwrap())
        })
        .await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();
    let fetch = |correlation_id, session_id, epoch| {
        let body = FetchRequest::default()
            .with_session_id(session_id)
            .with_session_epoch(epoch);
        Request::new(12, correlation_id, Some("analytics-etl"), &body).unwrap()
    };

    // A full Fetch that asks for a session goes on without that wish, and its answer grants none.
    let full = soon(backend.call(proxy.forwards(7, broker(1), fetch(1, 0, 0)))).await;
    let full = truth(full);
    assert_eq!(full.response().body::<FetchResponse>().unwrap().session_id, 0);
    assert_eq!(full.upstream().unwrap().body::<FetchResponse>().unwrap().session_id, 77);
    let arrived = only.of_clients();
    assert_eq!(arrived[0].request.body::<FetchRequest>().unwrap().session_epoch, -1);

    // A Fetch that continues a session is turned away, and the broker gets nothing.
    let continued = soon(backend.call(proxy.forwards(7, broker(1), fetch(2, 77, 1)))).await;
    let continued = truth(continued);
    let turned_away: FetchResponse = continued.response().body().unwrap();
    assert_eq!(turned_away.error_code, ErrorCode::FETCH_SESSION_ID_NOT_FOUND.value());
    assert!(continued.upstream().is_none());
    assert_eq!(only.of_clients().len(), 1);
}

#[tokio::test]
async fn the_backend_rejects_every_incident() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = backend(&only).await;

    assert_eq!(backend.incident_support(), IncidentSupport::none());
    let rebalance = Incident::Rebalance {
        group: GroupId::new("payments").unwrap(),
    };
    let rejection = backend.apply(rebalance).await.unwrap_err();
    assert_eq!(rejection.to_string(), "the backend cannot apply or emulate: rebalance");
}

#[tokio::test]
async fn a_broker_that_never_answers_keeps_its_call_in_flight() {
    let cluster = FakeCluster::new();
    let only = cluster.broker(1, |_| async { Reaction::Nothing }).await;
    let backend = backend(&only).await;
    let proxy = Proxy::new();

    let waited = timeout(A_WHILE, backend.call(proxy.forwards(7, broker(1), heartbeat(1)))).await;

    assert!(waited.is_err(), "the backend gave up on the broker: {waited:?}");
    assert_eq!(only.of_clients().len(), 1);
}
