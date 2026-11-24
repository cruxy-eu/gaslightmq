//! An engine starts on the endpoints of its backend, and stops with every session it runs.

use std::net::TcpListener;

use anyhow::Context;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_engine::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::{ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use tokio::net::TcpStream;

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn an_engine_does_not_start_on_an_address_that_is_taken() -> anyhow::Result<()> {
    let taken = TcpListener::bind("127.0.0.1:0")?;
    let address = taken.local_addr()?;
    let mut configuration = EngineUnderTest::configuration()?;
    let brokers = configuration.listeners.brokers().clone();
    configuration.listeners = Listeners::new(address.into(), brokers, "localhost".parse()?)?;
    let backend = ScriptedBackend::with_three_brokers();

    let started = Engine::start(configuration, backend, SteppedClock::new(), Lies::of_every_build()).await;

    let Err(error) = started else {
        anyhow::bail!("the engine started on an address that another socket holds");
    };
    assert!(
        matches!(
            &error,
            EngineStartError::EndpointNotOpened {
                error: EndpointOpenError::AddressUnavailable {
                    endpoint: Endpoint::Bootstrap,
                    ..
                }
            }
        ),
        "{error:?}"
    );
    let says = format!("endpoint `bootstrap` did not open: cannot listen on {address}: ");
    assert!(error.to_string().starts_with(&says), "{error}");
    Ok(())
}

#[tokio::test]
async fn an_engine_does_not_start_when_the_listeners_do_not_hold_the_brokers() -> anyhow::Result<()> {
    // A range of two ports for the three brokers of the cluster. Nothing ever listens on them.
    let two_ports: BrokerListeners = "127.0.0.1:19092-19093".parse()?;
    let listeners = Listeners::new("127.0.0.1:0".parse()?, two_ports, "localhost".parse()?)?;
    let configuration = EngineConfiguration::new(listeners);
    let backend = ScriptedBackend::with_three_brokers();

    let started = Engine::start(configuration, backend, SteppedClock::new(), Lies::of_every_build()).await;

    let Err(error) = started else {
        anyhow::bail!("the engine started with a broker that has no listener");
    };
    assert!(
        matches!(error, EngineStartError::BrokersWithoutListener { .. }),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "the listeners do not hold the brokers of the cluster: \
         3 brokers need a port each, and the range 19092-19093 has 2"
    );
    Ok(())
}

#[tokio::test]
async fn stopping_an_engine_ends_its_sessions_and_frees_its_addresses() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut events = proxy.engine().live_events();
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let metadata = client.send_metadata().await?;
    EngineUnderTest::soon(client.receive_response(&metadata)).await??;
    let address = proxy.address_of(Endpoint::Bootstrap)?;

    proxy.stop().await;

    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));
    assert!(TcpStream::connect(address).await.is_err());
    // A listener of the events gets what the engine kept, and then hears that it stopped.
    assert!(matches!(events.next().await, Ok(EngineEvent::CallRecorded(_))));
    assert_eq!(events.next().await.err(), Some(LiveEventsError::EngineStopped));
    // A new engine takes the address.
    let mut configuration = EngineUnderTest::configuration()?;
    let brokers = configuration.listeners.brokers().clone();
    configuration.listeners = Listeners::new(address.into(), brokers, "localhost".parse()?)?;
    let backend = ScriptedBackend::with_three_brokers();
    let again = Engine::start(configuration, backend, SteppedClock::new(), Lies::of_every_build()).await?;
    assert_eq!(again.address_of(Endpoint::Bootstrap), Some(address));
    Ok(())
}

#[tokio::test]
async fn an_engine_stops_also_when_its_backend_never_says_whether_it_takes_a_listener() -> anyhow::Result<()> {
    let stop_timeout = TimeSpan::from_millis(200);
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.stop_timeout = stop_timeout;
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let bootstrap = proxy.address_of(Endpoint::Bootstrap)?;
    let broker_2 = ScriptedBackend::brokers()[1];
    // The endpoint of a broker opens again, and the backend never says whether it takes the new listener.
    proxy.backend().hold_listeners();
    proxy.backend().bounce(broker_2);
    proxy.backend().listeners_of(Endpoint::Broker(broker_2), 2).await?;

    let began_at = std::time::Instant::now();
    let far_longer_than_the_stop_timeout = Duration::from_secs(10);
    let stopped = tokio::time::timeout(far_longer_than_the_stop_timeout, proxy.stop()).await;

    // The engine waited for its listeners for the time a stop may take, and then stopped without them.
    assert!(stopped.is_ok(), "the engine did not stop");
    assert!(
        began_at.elapsed() >= stop_timeout.duration(),
        "{:?}",
        began_at.elapsed()
    );
    // It let go of what was open, which closes a moment later.
    let refusing = async {
        while TcpStream::connect(bootstrap).await.is_ok() {
            tokio::task::yield_now().await;
        }
    };
    let refused = tokio::time::timeout(far_longer_than_the_stop_timeout, refusing).await;
    assert!(refused.is_ok(), "{bootstrap} still accepts connections");
    Ok(())
}

#[tokio::test]
async fn an_engine_stops_also_when_its_backend_never_lets_go_of_a_connection() -> anyhow::Result<()> {
    let stop_timeout = TimeSpan::from_millis(200);
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.stop_timeout = stop_timeout;
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    // The client leaves, and the backend hears that its connection ended, and never lets go of it:
    // the session of that connection does not end.
    proxy.backend().hold_connections();
    drop(member);
    proxy.backend().connections_closed(1).await?;

    let began_at = std::time::Instant::now();
    let far_longer_than_the_stop_timeout = Duration::from_secs(10);
    let stopped = tokio::time::timeout(far_longer_than_the_stop_timeout, proxy.stop()).await;

    // The engine waited for the session for the time a stop may take, and then stopped without it.
    assert!(stopped.is_ok(), "the engine did not stop");
    assert!(
        began_at.elapsed() >= stop_timeout.duration(),
        "{:?}",
        began_at.elapsed()
    );
    Ok(())
}

#[tokio::test]
async fn an_engine_that_someone_lets_go_of_stops_too() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::connect(proxy.address_of(Endpoint::Bootstrap)?).await?;

    drop(proxy);

    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));
    Ok(())
}

#[tokio::test]
async fn the_sessions_of_one_client_share_what_they_learned_until_the_context_lingered() -> anyhow::Result<()> {
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.client_context_linger = TimeSpan::from_secs(30);
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let contexts = proxy.engine().shared_by_sessions().contexts().clone();
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;
    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));
    assert_eq!(contexts.len(), 1);

    // The session closes, and its context stays for a client that reconnects.
    drop(member);
    proxy.backend().connections_closed(1).await?;
    proxy.advance(TimeSpan::from_secs(29));
    tokio::task::yield_now().await;
    assert_eq!(contexts.len(), 1);

    // A second later it lingered long enough, and the engine ends it as soon as its task has its turn.
    proxy.advance(TimeSpan::from_secs(1));
    tokio::task::yield_now().await;
    assert!(contexts.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_engine_runs_on_the_wall_clock_too() -> anyhow::Result<()> {
    let configuration = EngineUnderTest::configuration()?;
    let backend = ScriptedBackend::with_three_brokers();
    let engine = Engine::start(configuration, backend, WallClock::new(), Lies::of_every_build()).await?;
    let late = Rule::new(
        "Late heartbeat".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    engine
        .rules()
        .arm(late.resolve(&SelectorSet::default(), &PresetSet::default())?)?;
    let mut events = engine.live_events();
    let bootstrap = engine.address_of(Endpoint::Bootstrap);
    let member = RawKafkaClient::new(bootstrap.context("the bootstrap endpoint is open")?);
    let group: GroupId = "analytics-etl".parse()?;

    assert_eq!(member.heartbeat(group.as_str()).await, Ok(()));

    let EngineEvent::CallRecorded(record) = events.next().await? else {
        anyhow::bail!("the call has a record");
    };
    assert!(record.timing().client_waited() >= Some(Duration::from_millis(20)));
    Ok(())
}
