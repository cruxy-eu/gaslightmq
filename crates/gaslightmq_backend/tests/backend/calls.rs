//! A backend answers a request with the truth, at once or later, or it says that it has no answer.

use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use futures::executor::{LocalPool, block_on};
use futures::task::LocalSpawnExt;
use futures::{FutureExt, poll};
use gaslightmq_backend::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::Api;
use gaslightmq_wire::messages::MetadataResponse;

use crate::cluster::{Cluster, broker, correlation_id, fetch, metadata, produce, truth_of};

#[test]
fn a_backend_answers_a_request_with_the_truth() {
    let cluster = Cluster::default();

    let answer = block_on(cluster.call(metadata("#7.1", Endpoint::Bootstrap)));

    let truth = truth_of(answer);
    assert_eq!(truth.response().api(), Api::new(gaslightmq_wire::ApiKey::Metadata, 12));
    assert_eq!(truth.response().correlation_id(), correlation_id("#7.1"));
    assert!(truth.response().body::<MetadataResponse>().is_ok());
    assert!(truth.upstream().is_none(), "no overlay changed the answer");
    assert_eq!(cluster.taken(), ["#7.1"]);
}

#[test]
fn an_answer_comes_when_the_cluster_has_one() {
    let cluster = Arc::new(Cluster::default());
    let mut tasks = LocalPool::new();
    let spawner = tasks.spawner();

    // The session of a consumer forwards a Fetch, and no record is there yet.
    let mut fetched = spawner
        .spawn_local_with_handle({
            let cluster = Arc::clone(&cluster);
            async move { cluster.call(fetch("#7.1", broker(2))).await }
        })
        .unwrap();
    tasks.run_until_stalled();
    assert!((&mut fetched).now_or_never().is_none(), "the Fetch waits for a record");

    // The session of a producer forwards a Produce, on another connection and in another task.
    let produced = spawner
        .spawn_local_with_handle({
            let cluster = Arc::clone(&cluster);
            async move { cluster.call(produce("#8.1", broker(2), -1)).await }
        })
        .unwrap();
    tasks.run_until_stalled();

    // The record woke the Fetch, and both calls have their truth.
    let produced = truth_of(produced.now_or_never().unwrap());
    let fetched = truth_of(fetched.now_or_never().unwrap());
    assert_eq!(produced.response().correlation_id(), correlation_id("#8.1"));
    assert_eq!(fetched.response().correlation_id(), correlation_id("#7.1"));
}

#[test]
fn the_requests_of_one_connection_keep_the_order_of_their_calls() {
    let cluster = Cluster::default();

    block_on(async {
        // The engine starts each call when the pipeline forwards its request: it polls the future once.
        let mut first = pin!(cluster.call(fetch("#7.1", broker(2))));
        assert!(poll!(&mut first).is_pending());
        let mut second = pin!(cluster.call(metadata("#7.2", broker(2))));
        let second_answer = poll!(&mut second);
        let mut third = pin!(cluster.call(fetch("#7.3", broker(2))));
        assert!(poll!(&mut third).is_pending());

        // The cluster took the requests in that order, and it answers the second one first.
        assert_eq!(cluster.taken(), ["#7.1", "#7.2", "#7.3"]);
        let Poll::Ready(second_answer) = second_answer else {
            unreachable!("Metadata waits for nothing");
        };
        assert_eq!(
            truth_of(second_answer).response().correlation_id(),
            correlation_id("#7.2")
        );

        // A record arrives, and the calls that waited get their answers.
        truth_of(cluster.call(produce("#8.1", broker(2), 1)).await);
        assert_eq!(
            truth_of(first.await).response().correlation_id(),
            correlation_id("#7.1")
        );
        assert_eq!(
            truth_of(third.await).response().correlation_id(),
            correlation_id("#7.3")
        );
    });
}

#[test]
fn a_produce_that_asks_for_no_acks_has_no_response() {
    let cluster = Cluster::default();

    let answer = block_on(cluster.call(produce("#7.1", broker(2), 0)));

    assert!(matches!(answer, BackendAnswer::NoResponse), "{answer:?}");
    // The cluster has the request all the same: its record lets a Fetch through.
    truth_of(block_on(cluster.call(fetch("#8.1", broker(2)))));
}

#[test]
fn a_backend_that_has_no_answer_fails_the_call() {
    let cluster = Cluster::default();
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: false,
    };
    block_on(cluster.apply(down)).unwrap();

    // A session on the endpoint of broker 2 forwarded this request just before the broker went down.
    let answer = block_on(cluster.call(metadata("#7.1", broker(2))));

    let BackendAnswer::Failed(failure) = answer else {
        unreachable!("broker 2 is down, and the cluster answered {answer:?}");
    };
    assert_eq!(failure, BackendFailure::NoSuchEndpoint { endpoint: broker(2) });
    // The engine puts this line into the record of the call, and ends the session as closed by the backend.
    assert_eq!(
        failure.to_string(),
        "the cluster takes no requests on endpoint `broker 2`"
    );
    // The other brokers answer.
    truth_of(block_on(cluster.call(metadata("#8.1", broker(1)))));
}

#[test]
fn a_call_that_never_gets_an_answer_ends_when_the_engine_drops_it() {
    let cluster = Cluster::default();
    let unresponsive = Incident::BrokerGoesUnresponsive {
        broker: BrokerId::from(2),
    };

    block_on(async {
        cluster.apply(unresponsive).await.unwrap();
        // The broker still accepts connections.
        assert!(cluster.endpoints_now().contains(broker(2)));

        {
            let mut call = pin!(cluster.call(metadata("#7.1", broker(2))));
            assert!(poll!(&mut call).is_pending());
            assert!(poll!(&mut call).is_pending());
            // The client gives up, and the session ends. The engine drops the call here.
        }
        cluster.close(ConnectionNumber::new(7)).await;
    });

    assert_eq!(cluster.taken(), ["#7.1"]);
    assert_eq!(cluster.closed(), [ConnectionNumber::new(7)]);
}

#[test]
fn a_backend_its_messages_and_its_futures_move_between_tasks() {
    fn moves_between_threads<T: Send>(_: &T) {}
    fn is_shared_by_sessions<T: Backend + Clone + 'static>(_: &T) {}

    let cluster = Arc::new(Cluster::default());
    let request = metadata("#7.1", Endpoint::Bootstrap);
    let known = Endpoints::none();
    let rebalance = Incident::Rebalance {
        group: GroupId::new("payments").unwrap(),
    };

    // A session task holds the backend of its engine, on whichever thread the runtime runs it.
    is_shared_by_sessions(&cluster);
    moves_between_threads(&request);
    moves_between_threads(&cluster.call(request));
    moves_between_threads(&cluster.close(ConnectionNumber::new(7)));
    moves_between_threads(&cluster.endpoints(&known));
    moves_between_threads(&cluster.apply(rebalance));
    moves_between_threads(&BackendAnswer::NoResponse);

    // A reference to a backend is a backend too.
    let borrowed: &Cluster = &cluster;
    truth_of(block_on(Backend::call(
        &borrowed,
        metadata("#7.2", Endpoint::Bootstrap),
    )));
}
