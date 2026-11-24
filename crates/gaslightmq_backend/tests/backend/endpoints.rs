//! A backend reports the endpoints clients connect to, and every change of them.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use futures::FutureExt;
use futures::executor::{LocalPool, block_on};
use futures::task::LocalSpawnExt;
use gaslightmq_backend::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::MetadataResponse;

use crate::cluster::{CONSUMER, Cluster, broker, metadata, truth_of};

fn names(endpoints: &Endpoints) -> Vec<String> {
    endpoints.iter().map(|endpoint| endpoint.to_string()).collect()
}

/// The brokers that the Metadata answer of `cluster` to `call` names to a client, each with its address.
fn told(cluster: &Cluster, call: CallId) -> Vec<(i32, String)> {
    let answer = block_on(cluster.call(metadata(call, CONSUMER, Endpoint::Bootstrap)));
    let named: MetadataResponse = truth_of(answer).response().body().unwrap();
    let address = |broker: &gaslightmq_wire::messages::metadata_response::MetadataResponseBroker| {
        (broker.node_id.0, format!("{}:{}", broker.host, broker.port))
    };
    named.brokers.iter().map(address).collect()
}

#[test]
fn an_engine_that_knows_no_endpoint_learns_those_of_the_cluster_at_once() {
    let cluster = Cluster::default();

    let endpoints = block_on(cluster.endpoints(&Endpoints::none()));

    assert_eq!(names(&endpoints), ["bootstrap", "broker 1", "broker 2", "broker 3"]);
}

#[test]
fn an_engine_learns_of_every_change_of_the_endpoints() {
    let cluster = Arc::new(Cluster::default());
    let mut tasks = LocalPool::new();

    // The engine keeps one task that asks for the endpoints, again and again, with what it knows.
    let seen = Rc::new(RefCell::new(Vec::new()));
    let (watched, reported) = (Arc::clone(&cluster), Rc::clone(&seen));
    tasks
        .spawner()
        .spawn_local(async move {
            let mut known = Endpoints::none();
            loop {
                known = watched.endpoints(&known).await;
                reported.borrow_mut().push(names(&known));
            }
        })
        .unwrap();
    tasks.run_until_stalled();
    assert_eq!(seen.borrow().len(), 1, "nothing changed since the first answer");

    // Broker 2 goes down: the engine closes its listener and the sessions on it.
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: false,
    };
    block_on(cluster.apply(down)).unwrap();
    tasks.run_until_stalled();

    // An unresponsive broker keeps its endpoint, so nothing is reported.
    let unresponsive = Incident::BrokerGoesUnresponsive {
        broker: BrokerId::from(3),
    };
    block_on(cluster.apply(unresponsive)).unwrap();
    tasks.run_until_stalled();

    // Broker 2 comes back: the engine opens its listener again.
    let back = Incident::BrokerComesBack {
        broker: BrokerId::from(2),
    };
    block_on(cluster.apply(back)).unwrap();
    tasks.run_until_stalled();

    assert_eq!(
        *seen.borrow(),
        [
            vec!["bootstrap", "broker 1", "broker 2", "broker 3"],
            vec!["bootstrap", "broker 1", "broker 3"],
            vec!["bootstrap", "broker 1", "broker 2", "broker 3"],
        ]
    );
    assert!(cluster.endpoints_now().contains(broker(3)));
}

#[test]
fn a_broker_that_went_down_and_came_back_between_two_answers_is_not_missed() {
    let cluster = Cluster::default();
    let known = block_on(cluster.endpoints(&Endpoints::none()));

    // Both incidents happen before the engine asks again.
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2u16),
        failover: false,
    };
    let back = Incident::BrokerComesBack {
        broker: BrokerId::from(2u16),
    };
    block_on(cluster.apply(down)).unwrap();
    block_on(cluster.apply(back)).unwrap();

    // The same endpoints accept connections as before, and the backend still has something to report.
    let reported = cluster.endpoints(&known).now_or_never();
    let endpoints = reported.expect("an endpoint that closed in between is a change");
    assert_eq!(names(&endpoints), names(&known));
    assert_ne!(endpoints, known);

    // The engine closes the sessions that were on broker 2, and opens its listener anew.
    assert_eq!(endpoints.closed_since(&known).collect::<Vec<_>>(), [broker(2)]);
    assert_eq!(endpoints.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);

    // Once the engine knows, the backend has nothing more to report.
    assert!(cluster.endpoints(&endpoints).now_or_never().is_none());
}

#[test]
fn a_client_is_sent_to_the_listener_of_a_broker_once_the_engine_says_where_it_listens() {
    let cluster = Cluster::default();
    let endpoints = block_on(cluster.endpoints(&Endpoints::none()));
    let before = CallId::FIRST;
    let after = before.next();
    assert_eq!(told(&cluster, before), [], "no endpoint has a listener yet");

    // The engine opens a listener for every endpoint, each on a free port, and says which port it got.
    for (endpoint, port) in endpoints.iter().zip(50_001..) {
        let address = format!("127.0.0.1:{port}").parse().unwrap();
        block_on(cluster.listens(endpoint, address)).unwrap();
    }

    // The bootstrap endpoint took the first port, and no answer names it.
    assert_eq!(
        told(&cluster, after),
        [
            (1, "gaslight.test:50002".to_owned()),
            (2, "gaslight.test:50003".to_owned()),
            (3, "gaslight.test:50004".to_owned()),
        ]
    );
}

#[test]
fn a_broker_that_came_back_is_left_out_until_its_new_listener_is_open() {
    let cluster = Cluster::default();
    let known = block_on(cluster.endpoints(&Endpoints::none()));
    block_on(cluster.listens(broker(1), "127.0.0.1:50001".parse().unwrap())).unwrap();
    block_on(cluster.listens(broker(2), "127.0.0.1:50002".parse().unwrap())).unwrap();

    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: false,
    };
    let back = Incident::BrokerComesBack {
        broker: BrokerId::from(2),
    };
    block_on(cluster.apply(down)).unwrap();
    // The engine has not closed the old listener yet, and says where it listens. Its endpoint is closed.
    block_on(cluster.listens(broker(2), "127.0.0.1:50002".parse().unwrap())).unwrap();
    block_on(cluster.apply(back)).unwrap();
    let before = CallId::FIRST;
    let after = before.next();
    assert_eq!(told(&cluster, before), [(1, "gaslight.test:50001".to_owned())]);

    // The engine learns that the endpoint opened anew, and opens a listener for it, on another port.
    let endpoints = block_on(cluster.endpoints(&known));
    assert_eq!(endpoints.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);
    block_on(cluster.listens(broker(2), "127.0.0.1:50007".parse().unwrap())).unwrap();

    assert_eq!(
        told(&cluster, after),
        [
            (1, "gaslight.test:50001".to_owned()),
            (2, "gaslight.test:50007".to_owned())
        ]
    );
}

#[test]
fn a_backend_rejects_a_listener_that_clients_cannot_tell_from_another() {
    let cluster = Cluster::default();
    block_on(cluster.listens(broker(1), "10.0.4.1:19092".parse().unwrap())).unwrap();

    // Clients reach every listener under one host, so the same port of another address is the same address to them.
    let rejection = block_on(cluster.listens(broker(2), "10.0.4.2:19092".parse().unwrap())).unwrap_err();

    assert_eq!(
        rejection.to_string(),
        "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
         broker 1 is advertised at gaslight.test:19092"
    );
    assert_eq!(told(&cluster, CallId::FIRST), [(1, "gaslight.test:19092".to_owned())]);
}
