//! The engine keeps a listener for every endpoint that its backend reports as open.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_clock_waitable::SteppedClock;
use gaslightmq_engine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::{ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_recording_conformance::MemoryStorage;
use tokio::net::TcpStream;

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn every_endpoint_of_the_backend_listens_on_an_address_of_its_own() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;

    let brokers = ScriptedBackend::brokers().map(Endpoint::Broker);
    let endpoints = proxy.engine().endpoints();
    assert_eq!(endpoints, [Endpoint::Bootstrap, brokers[0], brokers[1], brokers[2]]);
    let mut ports = Vec::new();
    for endpoint in endpoints {
        ports.push(proxy.address_of(endpoint)?.port());
    }
    ports.sort_unstable();
    ports.dedup();
    // Every endpoint asked for any free port, and the system picked one for each.
    assert_eq!(ports.len(), 4);
    assert!(!ports.contains(&0));
    assert_eq!(proxy.engine().address_of("broker 4".parse()?), None);
    Ok(())
}

#[tokio::test]
async fn a_request_names_the_endpoint_it_arrived_on() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let broker_2: Endpoint = "broker 2".parse()?;
    let member = RawKafkaClient::new(proxy.address_of(broker_2)?);
    let group: GroupId = "analytics-etl".parse()?;

    member.heartbeat(group.as_str()).await?;

    let record = proxy.next_record().await?;
    let arrived_on = record.fields().read(Field::Endpoint).present();
    assert_eq!(
        arrived_on.map(|endpoint| endpoint.to_string()),
        Some(broker_2.to_string())
    );
    Ok(())
}

#[tokio::test]
async fn an_endpoint_that_closes_ends_its_sessions_and_refuses_connections() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let [broker_1, broker_2, broker_3] = ScriptedBackend::brokers();
    let group: GroupId = "analytics-etl".parse()?;
    let on_broker_3 = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_3))?);
    assert_eq!(on_broker_3.heartbeat(group.as_str()).await, Ok(()));
    proxy.next_record().await?;
    let on_broker_2 = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_2))?);
    let address = proxy.address_of(Endpoint::Broker(broker_2))?;
    proxy.backend().hold_answers();
    on_broker_2.send_metadata().await?;
    proxy.backend().calls_taken(2).await?;

    proxy.backend().take_down(broker_2);

    // The session on the endpoint ends, and the record of its call says why.
    assert_eq!(EngineUnderTest::soon(on_broker_2.closed()).await?, Ok(()));
    let record = proxy.next_record().await?;
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some("the backend closed the session: endpoint `broker 2` accepts no connections any more")
    );
    let connection_to_broker_2: ConnectionNumber = "#2".parse()?;
    assert_eq!(proxy.backend().connections_closed(1).await?, [connection_to_broker_2]);
    // The endpoint is gone, and its address refuses connections.
    assert_eq!(
        proxy.engine().endpoints(),
        [
            Endpoint::Bootstrap,
            Endpoint::Broker(broker_1),
            Endpoint::Broker(broker_3)
        ]
    );
    assert!(TcpStream::connect(address).await.is_err());
    // The sessions on the other endpoints go on.
    proxy.backend().release_answers();
    assert_eq!(on_broker_3.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn an_endpoint_that_opens_gets_a_listener() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let [broker_1, broker_2, broker_3] = ScriptedBackend::brokers();
    let watching = RawKafkaClient::connect(proxy.address_of(Endpoint::Broker(broker_2))?).await?;
    proxy.backend().take_down(broker_2);
    assert_eq!(EngineUnderTest::soon(watching.closed()).await?, Ok(()));
    drop(watching);
    assert_eq!(
        proxy.engine().endpoints(),
        [
            Endpoint::Bootstrap,
            Endpoint::Broker(broker_1),
            Endpoint::Broker(broker_3)
        ]
    );

    proxy.backend().bring_back(broker_2);

    // The engine opens a listener, and tells the backend where it listens: for the second time.
    let listeners = proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;
    assert_eq!(
        proxy.engine().address_of(Endpoint::Broker(broker_2)),
        Some(listeners[1])
    );
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_2))?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_broker_that_joins_the_cluster_gets_a_listener_on_a_free_port() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let brokers = ScriptedBackend::brokers().map(Endpoint::Broker);
    let mut listening = Vec::new();
    for broker in brokers {
        listening.push(proxy.address_of(broker)?);
    }
    let broker_4 = BrokerId::from(4);

    // A broker that nobody named joins the cluster.
    proxy.backend().join(broker_4);

    // Every broker takes a free port, so the new one gets a listener of its own, and the backend hears its address.
    let got = proxy.backend().listeners_of(Endpoint::Broker(broker_4), 1).await?[0];
    assert_ne!(got.port(), 0);
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_4)), Some(got));
    assert!(!listening.contains(&got));
    // The brokers that were there keep their listeners.
    for (broker, address) in brokers.into_iter().zip(listening) {
        assert_eq!(proxy.address_of(broker)?, address, "{broker}");
    }
    let member = RawKafkaClient::new(got);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_broker_on_a_free_port_keeps_its_listener_when_another_leaves() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let [broker_1, broker_2, broker_3] = ScriptedBackend::brokers().map(Endpoint::Broker);
    let on_broker_1 = RawKafkaClient::connect(proxy.address_of(broker_1)?).await?;
    let on_broker_2 = RawKafkaClient::connect(proxy.address_of(broker_2)?).await?;
    let listening = [proxy.address_of(broker_2)?, proxy.address_of(broker_3)?];

    // Broker 1 leaves the cluster.
    proxy.backend().take_down(ScriptedBackend::brokers()[0]);

    // No port is given in the order of the broker IDs, so no other broker gets another one: no listener moves.
    assert_eq!(EngineUnderTest::soon(on_broker_1.closed()).await?, Ok(()));
    assert_eq!(proxy.engine().endpoints(), [Endpoint::Bootstrap, broker_2, broker_3]);
    assert_eq!([proxy.address_of(broker_2)?, proxy.address_of(broker_3)?], listening);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(on_broker_2.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn an_endpoint_that_closed_and_opened_again_ended_its_sessions_in_between() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let broker_2 = ScriptedBackend::brokers()[1];
    let client = RawKafkaClient::connect(proxy.address_of(Endpoint::Broker(broker_2))?).await?;

    // The broker goes down and comes back before the engine looks.
    proxy.backend().bounce(broker_2);

    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));
    drop(client);
    proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_2))?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn the_backend_hears_the_address_that_every_listener_got() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;

    // Every listener asked for any free port, so only the engine knows which one it got.
    let endpoints = proxy.engine().endpoints();
    assert_eq!(endpoints.len(), 4);
    for endpoint in endpoints {
        assert_eq!(
            proxy.backend().listener_of(endpoint),
            proxy.engine().address_of(endpoint),
            "{endpoint}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_listener_that_the_backend_rejects_closes_again() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let [broker_1, broker_2, broker_3] = ScriptedBackend::brokers();
    let on_broker_3 = RawKafkaClient::connect(proxy.address_of(Endpoint::Broker(broker_3))?).await?;
    proxy.backend().reject_listeners_of(broker_2);

    // The endpoint closes and opens again, and the backend does not take its new listener.
    proxy.backend().bounce(broker_2);
    let rejected = proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?[1];
    // The engine is done with that listener once it follows the next change of the backend.
    proxy.backend().take_down(broker_3);
    assert_eq!(EngineUnderTest::soon(on_broker_3.closed()).await?, Ok(()));

    // No client is sent to the broker: its endpoint has no address.
    assert_eq!(
        proxy.engine().endpoints(),
        [Endpoint::Bootstrap, Endpoint::Broker(broker_1)]
    );
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_2)), None);
    // And nothing is left behind on the address that the backend rejected.
    proxy.stop().await;
    assert!(
        TcpStream::connect(rejected).await.is_err(),
        "{rejected} accepts connections"
    );
    Ok(())
}

#[tokio::test]
async fn an_endpoint_that_did_not_open_opens_when_the_backend_reports_again() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let [_, broker_2, broker_3] = ScriptedBackend::brokers();
    proxy.backend().reject_listeners_of(broker_2);
    proxy.backend().bounce(broker_2);
    proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_2)), None);

    // The backend takes listeners for the broker again, and its cluster changes somewhere else.
    proxy.backend().accept_listeners_of(broker_2);
    proxy.backend().bounce(broker_3);

    // Every report of the backend is another try for an endpoint that has no listener.
    proxy.backend().listeners_of(Endpoint::Broker(broker_3), 2).await?;
    let third_try = proxy.backend().listeners_of(Endpoint::Broker(broker_2), 3).await?[2];
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_2)), Some(third_try));
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_2))?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn an_endpoint_that_did_not_open_is_tried_again_after_the_retry_backoff() -> anyhow::Result<()> {
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.endpoint_retry_backoff = TimeSpan::from_secs(30);
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let broker_2 = ScriptedBackend::brokers()[1];
    let group: GroupId = "analytics-etl".parse()?;
    proxy.backend().reject_listeners_of(broker_2);
    proxy.backend().bounce(broker_2);
    proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;

    // The backend takes listeners for the broker again, and has nothing new to report about its cluster.
    proxy.backend().accept_listeners_of(broker_2);

    // One second before the backoff is over, the engine did not try again: the proxy serves its other endpoints.
    proxy.advance(TimeSpan::from_secs(29));
    let elsewhere = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    assert_eq!(elsewhere.heartbeat(group.as_str()).await, Ok(()));
    let tries = proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;
    assert_eq!(tries.len(), 2);
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_2)), None);

    // The backoff is over, and the engine tries again by itself: the endpoint opens.
    proxy.advance(TimeSpan::from_secs(1));
    let third_try = proxy.backend().listeners_of(Endpoint::Broker(broker_2), 3).await?[2];
    assert_eq!(proxy.engine().address_of(Endpoint::Broker(broker_2)), Some(third_try));
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Broker(broker_2))?);
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn a_broker_that_gets_another_port_moves_its_listener() -> anyhow::Result<()> {
    let (mut proxy, first_port) = EngineUnderTest::start_on_a_range_of_ports().await?;
    let [broker_1, broker_2, broker_3] = ScriptedBackend::brokers().map(Endpoint::Broker);
    // A range gives its ports in the order of the broker IDs.
    assert_eq!(proxy.address_of(broker_1)?.port(), first_port);
    assert_eq!(proxy.address_of(broker_2)?.port(), first_port + 1);
    assert_eq!(proxy.address_of(broker_3)?.port(), first_port + 2);
    let on_broker_2 = RawKafkaClient::new(proxy.address_of(broker_2)?);
    proxy.backend().hold_answers();
    on_broker_2.send_metadata().await?;
    proxy.backend().calls_taken(1).await?;

    // Broker 1 leaves the cluster, so the brokers behind it get the port before theirs.
    proxy.backend().take_down(ScriptedBackend::brokers()[0]);

    // The session on the listener that moves ends, and the record of its call says why.
    assert_eq!(EngineUnderTest::soon(on_broker_2.closed()).await?, Ok(()));
    let record = proxy.next_record().await?;
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some(
            "the backend closed the session: \
             endpoint `broker 2` moves to another address, because the brokers of the cluster changed"
        )
    );
    drop(on_broker_2);
    // The backend hears the new addresses, and a client reaches the broker there.
    proxy.backend().listeners_of(broker_3, 2).await?;
    assert_eq!(proxy.address_of(broker_2)?.port(), first_port);
    assert_eq!(proxy.address_of(broker_3)?.port(), first_port + 1);
    assert_eq!(
        proxy.backend().listener_of(broker_2),
        proxy.engine().address_of(broker_2)
    );
    assert_eq!(
        proxy.backend().listener_of(broker_3),
        proxy.engine().address_of(broker_3)
    );
    proxy.backend().release_answers();
    let member = RawKafkaClient::new(proxy.address_of(broker_2)?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn an_engine_does_not_start_with_a_listener_that_its_backend_rejects() -> anyhow::Result<()> {
    let broker_2 = ScriptedBackend::brokers()[1];
    let backend = ScriptedBackend::with_three_brokers();
    backend.reject_listeners_of(broker_2);
    let configuration = EngineUnderTest::configuration()?;

    let started = Engine::start(
        configuration,
        backend,
        SteppedClock::new(),
        Lies::of_every_build(),
        Recordings::new(MemoryStorage::new()),
    )
    .await;

    let Err(EngineStartError::EndpointNotOpened {
        error: EndpointOpenError::ListenerRejected { endpoint, reason },
    }) = started
    else {
        anyhow::bail!("the engine started, or failed for another reason: {started:?}");
    };
    assert_eq!(endpoint, Endpoint::Broker(broker_2));
    assert!(
        reason.starts_with("clients cannot be told that endpoint `broker 2` listens on 127.0.0.1:"),
        "{reason}"
    );
    assert!(
        reason.ends_with(": the script of the test rejects the listeners of this broker"),
        "{reason}"
    );
    Ok(())
}

#[tokio::test]
async fn an_engine_that_stops_while_a_listener_moves_still_frees_every_address() -> anyhow::Result<()> {
    let (proxy, first_port) = EngineUnderTest::start_on_a_range_of_ports().await?;
    let [broker_1, broker_2, _] = ScriptedBackend::brokers();
    // A client that keeps its side of the connection open, so the session on the listener takes a moment to end.
    let on_broker_2 = RawKafkaClient::connect(proxy.address_of(Endpoint::Broker(broker_2))?).await?;
    // Broker 1 leaves the cluster, so the listeners of the brokers behind it move to the port before theirs.
    proxy.backend().take_down(broker_1);
    assert_eq!(EngineUnderTest::soon(on_broker_2.closed()).await?, Ok(()));

    // The listeners are on their way when the engine stops.
    proxy.stop().await;

    // Every port of the range takes a listener at once: no listener that nobody closed holds one.
    for port in first_port..=first_port + 2 {
        let taken_again = std::net::TcpListener::bind(("127.0.0.1", port));
        assert!(taken_again.is_ok(), "port {port} is not free: {taken_again:?}");
    }
    drop(on_broker_2);
    Ok(())
}
