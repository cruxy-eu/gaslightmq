//! How a call ends when the cluster does not answer it, and how a backend fails to start.

use bytes::Bytes;
use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_wire::messages::{FetchResponse, HeartbeatResponse};
use gaslightmq_wire::{Frame, WireError};
use tokio::net::TcpListener;

use crate::support::{
    A_WHILE, FakeCluster, Reaction, backend, broker, calm, configuration, failure, heartbeat, metadata, on, soon,
    started, truth,
};

/// An address of this machine that takes no connection: the port of a listener that is gone.
async fn nobody_home() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn a_request_for_a_broker_the_cluster_does_not_have_has_no_endpoint() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let backend = backend(&only).await;

    let answer = soon(backend.call(on(7, 1, broker(9), heartbeat(1)))).await;

    assert_eq!(failure(answer), BackendFailure::NoSuchEndpoint { endpoint: broker(9) });
    assert!(only.of_clients().is_empty());
}

#[tokio::test]
async fn a_broker_that_takes_no_connection_is_unreachable() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let gone = nobody_home().await;
    cluster.name(2, gone);
    let backend = backend(&first).await;

    let answer = soon(backend.call(on(7, 1, broker(2), heartbeat(1)))).await;

    let BackendFailure::Unreachable { reason } = failure(answer) else {
        panic!("nothing listens on the address of broker 2");
    };
    assert!(reason.starts_with(&format!("{gone}: ")), "{reason}");
}

#[tokio::test]
async fn a_broker_that_hangs_up_loses_the_connection_for_good() {
    let cluster = FakeCluster::new();
    let only = cluster.broker(1, |_| async { Reaction::HangUp }).await;
    let backend = backend(&only).await;

    // Both requests are in flight when the broker hangs up.
    let calls = vec![
        backend.call(on(7, 1, broker(1), heartbeat(1))),
        backend.call(on(7, 2, broker(1), heartbeat(2))),
    ];
    let mut failures = Vec::new();
    for call in started(calls).await {
        failures.push(failure(soon(call).await));
    }
    let lost = failures[0].clone();
    assert!(matches!(lost, BackendFailure::ConnectionLost { .. }), "{lost}");
    assert_eq!(failures[1], lost);

    // A later request of that client finds the connection gone, and the backend opens no new one.
    let later = soon(backend.call(on(7, 3, broker(1), heartbeat(3)))).await;
    assert_eq!(failure(later), lost);
    assert!(only.of_clients().iter().all(|arrival| arrival.connection == 1));

    // Another client gets a connection of its own.
    let other = soon(backend.call(on(8, 1, broker(1), heartbeat(1)))).await;
    assert!(matches!(failure(other), BackendFailure::ConnectionLost { .. }));
    assert!(only.of_clients().iter().any(|arrival| arrival.connection == 2));
}

#[tokio::test]
async fn an_answer_with_the_id_of_another_request_ends_the_connection_as_no_response() {
    let cluster = FakeCluster::new();
    let only = cluster
        .broker(1, |_| async {
            let of_another = heartbeat(999).response(&HeartbeatResponse::default()).unwrap();
            Reaction::Respond(of_another)
        })
        .await;
    let backend = backend(&only).await;

    let answer = soon(backend.call(on(7, 1, broker(1), heartbeat(1)))).await;

    let BackendAnswer::NotAResponse { reason } = answer else {
        panic!("the broker answered another request: {answer:?}");
    };
    assert_eq!(reason, "correlation ID 999 does not answer the Heartbeat v4 request 1");
    // Nothing says any more which response answers which request.
    let later = soon(backend.call(on(7, 2, broker(1), heartbeat(2)))).await;
    assert!(matches!(later, BackendAnswer::NotAResponse { .. }), "{later:?}");
    assert_eq!(only.of_clients().len(), 1);
}

#[tokio::test]
async fn a_body_that_does_not_read_is_no_response_and_the_connection_goes_on() {
    let cluster = FakeCluster::new();
    let only = cluster
        .broker(1, |request| async move {
            if request.correlation_id() == 1 {
                // The header of a Fetch response, and a body that ends at once.
                let whole = request.response(&FetchResponse::default()).unwrap();
                let cut = Frame::new(whole.frame().payload().slice(..6));
                Reaction::Send(cut.to_bytes())
            } else {
                Reaction::Respond(calm(&request))
            }
        })
        .await;
    let backend = backend(&only).await;
    let fetch = gaslightmq_wire::Request::new(12, 1, None, &gaslightmq_wire::messages::FetchRequest::default());

    let answer = soon(backend.call(on(7, 1, broker(1), fetch.unwrap()))).await;
    assert!(matches!(answer, BackendAnswer::NotAResponse { .. }), "{answer:?}");

    let next = soon(backend.call(on(7, 2, broker(1), heartbeat(2)))).await;
    assert_eq!(truth(next).response().correlation_id(), 2);
}

#[tokio::test]
async fn a_response_over_the_limit_is_no_response() {
    let cluster = FakeCluster::new();
    // The broker announces a response of 256 bytes.
    let only = cluster
        .broker(1, |_| async { Reaction::Send(Bytes::from_static(&[0, 0, 1, 0])) })
        .await;
    let mut configuration = configuration(&only);
    // The answers to the backend's own questions fit.
    configuration.response_max_bytes = ResponseSizeLimit::new(64).unwrap();
    let backend = UpstreamBackend::start(configuration).await.unwrap();

    let answer = soon(backend.call(on(7, 1, broker(1), heartbeat(1)))).await;

    let BackendAnswer::NotAResponse { reason } = answer else {
        panic!("the broker announced more than the backend takes: {answer:?}");
    };
    assert_eq!(reason, WireError::FrameTooLarge { len: 256, max: 64 }.to_string());
}

#[tokio::test]
async fn a_cluster_that_takes_no_connection_does_not_start() {
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let mut configuration = configuration(&only);
    let gone = nobody_home().await;
    configuration.upstream = Upstream::new(gone.to_string().parse().unwrap());

    let error = UpstreamBackend::start(configuration).await.unwrap_err();

    let UpstreamBackendError::ClusterUndiscovered { reason } = &error else {
        panic!("nothing listens behind the bootstrap address: {error}");
    };
    assert!(
        reason.starts_with(&format!("the backend cannot reach the cluster: {gone}: ")),
        "{reason}"
    );
}

#[tokio::test]
async fn a_server_that_does_not_answer_the_greeting_does_not_start() {
    // A listener that accepts, and never reads.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cluster = FakeCluster::new();
    let only = cluster.calm_broker(1).await;
    let mut configuration = configuration(&only);
    configuration.upstream = Upstream::new(listener.local_addr().unwrap().to_string().parse().unwrap());
    configuration.request_timeout = TimeSpan::from_duration(A_WHILE);

    let error = soon(UpstreamBackend::start(configuration)).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "the cluster did not name its brokers: {} did not answer within 200ms",
            listener.local_addr().unwrap()
        )
    );
}

#[tokio::test]
async fn a_bootstrap_request_goes_to_a_broker_when_the_bootstrap_server_is_gone() {
    let cluster = FakeCluster::new();
    let first = cluster.calm_broker(1).await;
    let second = cluster.calm_broker(2).await;
    let backend = backend(&first).await;

    drop(first);
    let answer = soon(backend.call(on(7, 1, Endpoint::Bootstrap, metadata(1)))).await;

    assert_eq!(truth(answer).response().correlation_id(), 1);
    assert_eq!(second.of_clients().len(), 1);
}
