//! A backend reports the endpoints clients connect to, and every change of them.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use futures::FutureExt;
use futures::executor::{LocalPool, block_on};
use futures::task::LocalSpawnExt;
use gaslightmq_backend::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::cluster::{Cluster, broker};

fn names(endpoints: &Endpoints) -> Vec<String> {
    endpoints.iter().map(|endpoint| endpoint.to_string()).collect()
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
