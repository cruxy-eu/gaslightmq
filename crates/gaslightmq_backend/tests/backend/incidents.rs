//! A backend declares which incidents it can do, applies them, and rejects the rest.

use std::pin::pin;
use std::task::Poll;

use futures::executor::block_on;
use futures::poll;
use gaslightmq_backend::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::cluster::Cluster;

#[test]
fn a_backend_declares_the_incidents_it_can_do() {
    let support = Cluster::default().incident_support();

    assert_eq!(support.support(IncidentKind::BrokerGoesDown), Support::Emulated);
    assert_eq!(support.support(IncidentKind::LeaderMoves), Support::Unsupported);

    // The engine checks a scenario when it loads.
    let failover = IncidentDefinition::BrokerGoesDown {
        broker: None,
        failover: true,
    };
    assert!(support.check(&Requirements::from(&failover)).is_err());
}

#[test]
fn a_backend_returns_an_incident_with_the_consequences_it_had() {
    let cluster = Cluster::default();
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: false,
    };

    let expanded = block_on(cluster.apply(down.clone())).unwrap();

    // The consequences come from the topology of the cluster: broker 2 led a partition and hosted a coordinator.
    assert_eq!(expanded.incident(), &down);
    assert_eq!(
        expanded.consequences(),
        [
            Consequence::Leaderless {
                partition: "orders.eu/3".parse().unwrap()
            },
            Consequence::CoordinatorUnavailable {
                coordinator: GroupId::new("payments").unwrap().into()
            },
        ]
    );

    // Broker 1 leads nothing, so its incident has no consequence.
    let idle = Incident::BrokerGoesDown {
        broker: BrokerId::from(1),
        failover: false,
    };
    assert_eq!(block_on(cluster.apply(idle)).unwrap().consequences(), []);
}

#[test]
fn a_backend_rejects_an_incident_it_cannot_do_and_changes_nothing() {
    let cluster = Cluster::default();
    let before = cluster.endpoints_now();

    let moves = Incident::LeaderMoves {
        partition: "orders.eu/3".parse().unwrap(),
        to: None,
    };
    let rejection = block_on(cluster.apply(moves)).unwrap_err();
    assert!(
        matches!(rejection, IncidentRejection::Unsupported { .. }),
        "{rejection:?}"
    );
    assert_eq!(
        rejection.to_string(),
        "the backend cannot apply or emulate: leader-moves"
    );

    // The backend can take the broker down, and it cannot do what a failover brings about.
    let failover = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: true,
    };
    assert_eq!(
        block_on(cluster.apply(failover)).unwrap_err().to_string(),
        "the backend cannot apply or emulate: leader-moves (a consequence of broker-goes-down), \
         coordinator-moves (a consequence of broker-goes-down)"
    );

    let stranger = Incident::BrokerGoesDown {
        broker: BrokerId::from(9),
        failover: false,
    };
    assert_eq!(
        block_on(cluster.apply(stranger)).unwrap_err(),
        IncidentRejection::Impossible {
            incident: IncidentKind::BrokerGoesDown,
            reason: "the cluster has no broker 9".to_owned(),
        }
    );

    assert_eq!(cluster.endpoints_now(), before);
}

#[test]
fn an_incident_starts_at_the_first_poll_and_is_not_taken_back() {
    let cluster = Cluster::default();
    let before = cluster.endpoints_now();
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2u16),
        failover: false,
    };

    // The engine drops the future before it polled it: nothing happened.
    drop(cluster.apply(down.clone()));
    assert_eq!(cluster.endpoints_now(), before);

    // The engine polls the future once, and drops it. The incident happened as a whole.
    block_on(async {
        let mut applying = pin!(cluster.apply(down));
        let Poll::Ready(applied) = poll!(&mut applying) else {
            unreachable!("this cluster applies an incident in one step");
        };
        assert_eq!(applied.unwrap().consequences().len(), 2);
    });
    let after = cluster.endpoints_now();
    assert_eq!(after.closed_since(&before).count(), 1);
}
