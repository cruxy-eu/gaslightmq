//! The endpoints follow the brokers that the cluster names.

use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use tokio::time::timeout;

use crate::support::{
    A_WHILE, FakeCluster, Proxy, backend, broker, configuration, failure, heartbeat, listening, metadata, named, soon,
    truth,
};

/// A backend that asks the cluster for its brokers again and again, without anyone asking it to.
async fn curious_backend(bootstrap: &crate::support::FakeBroker) -> UpstreamBackend {
    let mut configuration = configuration(bootstrap);
    configuration.metadata_max_age = TimeSpan::from_millis(20);
    UpstreamBackend::start(configuration).await.unwrap()
}

fn open(endpoints: &Endpoints) -> Vec<Endpoint> {
    endpoints.iter().collect()
}

#[tokio::test]
async fn the_endpoints_are_the_bootstrap_endpoint_and_one_per_broker() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let _second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;

    let endpoints = soon(backend.endpoints(&Endpoints::none())).await;

    assert_eq!(open(&endpoints), [Endpoint::Bootstrap, broker(1), broker(2)]);
}

#[tokio::test]
async fn the_endpoints_wait_until_something_changes() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = curious_backend(&only).await;
    let known = soon(backend.endpoints(&Endpoints::none())).await;

    let waited = timeout(A_WHILE, backend.endpoints(&known)).await;

    assert!(waited.is_err(), "nothing changed, and the backend reported: {waited:?}");
}

#[tokio::test]
async fn a_broker_that_joins_opens_an_endpoint_and_one_that_leaves_closes_it() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let backend = curious_backend(&first).await;
    let known = soon(backend.endpoints(&Endpoints::none())).await;
    assert_eq!(open(&known), [Endpoint::Bootstrap, broker(1)]);

    let second = cluster.calm_broker(2).await;
    let joined = soon(backend.endpoints(&known)).await;
    assert_eq!(joined.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);
    assert_eq!(joined.closed_since(&known).count(), 0);

    cluster.forget(2);
    drop(second);
    let left = soon(backend.endpoints(&joined)).await;
    assert_eq!(left.closed_since(&joined).collect::<Vec<_>>(), [broker(2)]);
    assert_eq!(open(&left), [Endpoint::Bootstrap, broker(1)]);

    // An engine that only asks now still learns that the endpoint closed in between.
    let _again = cluster.calm_broker(2).await;
    let back = soon(backend.endpoints(&left)).await;
    assert_eq!(back.closed_since(&joined).collect::<Vec<_>>(), [broker(2)]);
    assert_eq!(back.opened_since(&joined).collect::<Vec<_>>(), [broker(2)]);
}

#[tokio::test]
async fn a_call_that_loses_its_broker_asks_the_cluster_again_at_once() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let second = cluster.calm_broker(2).await;
    // This backend asks on its own every five minutes.
    let backend = backend(&first).await;
    let proxy = Proxy::new();
    let known = soon(backend.endpoints(&Endpoints::none())).await;
    soon(backend.call(proxy.forwards(7, broker(2), heartbeat(1)))).await;

    cluster.forget(2);
    drop(second);
    let answer = soon(backend.call(proxy.forwards(7, broker(2), heartbeat(2)))).await;

    assert!(matches!(failure(answer), BackendFailure::ConnectionLost { .. }));
    let now = soon(backend.endpoints(&known)).await;
    assert_eq!(now.closed_since(&known).collect::<Vec<_>>(), [broker(2)]);
    // A session that is still on that endpoint has nowhere to go.
    let late = soon(backend.call(proxy.forwards(8, broker(2), heartbeat(1)))).await;
    assert_eq!(failure(late), BackendFailure::NoSuchEndpoint { endpoint: broker(2) });
}

#[tokio::test]
async fn an_answer_that_names_a_new_broker_asks_the_cluster_again_at_once() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    // This backend asks on its own every five minutes.
    let backend = backend(&first).await;
    let proxy = Proxy::new();
    let known = soon(backend.endpoints(&Endpoints::none())).await;
    listening(&backend, 1, 50_001).await;

    let _second = cluster.calm_broker(2).await;
    let before = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(1)))).await);

    // The client is not told about a broker that has no endpoint.
    assert_eq!(named(before.response()), [(1, "gaslight.test:50001".to_owned())]);
    // The broker gets its endpoint, the engine opens a listener for it, and the next answer names it.
    let now = soon(backend.endpoints(&known)).await;
    assert_eq!(now.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);
    listening(&backend, 2, 50_002).await;
    let after = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(2)))).await);
    assert_eq!(
        named(after.response()),
        [
            (1, "gaslight.test:50001".to_owned()),
            (2, "gaslight.test:50002".to_owned())
        ]
    );
}

#[tokio::test]
async fn a_client_is_told_about_a_broker_once_the_engine_says_where_its_endpoint_listens() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let _second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;
    let proxy = Proxy::new();

    // No endpoint has a listener yet: a client that asks now is told about no broker, and asks again.
    let before = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(1)))).await);
    assert_eq!(named(before.response()), []);

    // The engine opens a listener for every endpoint, and says which address each one got.
    soon(backend.listens(Endpoint::Bootstrap, "127.0.0.1:50000".parse().unwrap()))
        .await
        .unwrap();
    listening(&backend, 2, 50_002).await;
    let between = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(2)))).await);
    assert_eq!(named(between.response()), [(2, "gaslight.test:50002".to_owned())]);

    listening(&backend, 1, 50_001).await;
    let after = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(3)))).await);
    assert_eq!(
        named(after.response()),
        [
            (1, "gaslight.test:50001".to_owned()),
            (2, "gaslight.test:50002".to_owned())
        ]
    );
}

#[tokio::test]
async fn a_listener_on_the_port_of_another_broker_is_rejected() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let _second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;
    let proxy = Proxy::new();
    soon(backend.listens(broker(1), "10.0.4.1:19092".parse().unwrap()))
        .await
        .unwrap();

    // Clients reach every listener under one host, so they could not tell the two apart.
    let rejection = soon(backend.listens(broker(2), "10.0.4.2:19092".parse().unwrap()))
        .await
        .unwrap_err();

    assert!(matches!(rejection, ListenerRejection::Unadvertisable { .. }));
    assert_eq!(
        rejection.to_string(),
        "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
         broker 2 cannot be advertised at gaslight.test:19092: broker 1 is advertised there"
    );
    let answer = truth(soon(backend.call(proxy.forwards(7, Endpoint::Bootstrap, metadata(1)))).await);
    assert_eq!(named(answer.response()), [(1, "gaslight.test:19092".to_owned())]);
}
