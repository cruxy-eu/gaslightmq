//! The control plane without a real cluster, driven through the client as the UI, the CLI and tests drive it:
//! the definitions of the workspace, what an operation answers about an environment that does not run,
//! an operation that the server does not serve yet, and the live stream.
//!
//! Each test starts a host whose workspace lives in memory, puts a server in front of it on a free port,
//! and speaks to that server with a client. The tests in `tests/real_cluster` run environments in front of real
//! Kafka brokers.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use gaslightmq_control_api::clock::Duration;
use gaslightmq_control_api::environment::{
    BrokerDefinition, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition,
};
use gaslightmq_control_api::field::ConnectionNumber;
use gaslightmq_control_api::kernel::ApiKey;
use gaslightmq_control_api::lie::TimingLie;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, PresetName, Rule};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorRef};
use gaslightmq_control_api::surface::{Column, Intent, Layer, Pad, Row};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, StreamError};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::{Host, HostConfiguration};
use gaslightmq_lie_api::prelude::*;
use gaslightmq_workspace::prelude::*;

/// How long a test waits for a message of the live stream that the server has a reason to send.
const PATIENCE: Duration = Duration::from_secs(10);

/// The address that the server of a test listens on, and that its client connects to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port, so the tests run side by side.
const ANY_PORT: u16 = 0;

#[tokio::test]
async fn a_definition_changes_in_the_workspace_and_every_client_of_the_stream_hears_of_it() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    );
    let delay: PresetName = "delay-1s".parse()?;
    let slow = Rule::new(
        "slow-order-producers".parse()?,
        order_producers.name().clone(),
        delay.clone(),
    );

    // The workspace is empty, and the stream says so first.
    let first = tokio::time::timeout(PATIENCE, stream.next()).await??;
    assert_eq!(first, StreamMessage::Snapshot(ProcessState::default()));

    let created = client
        .send(CreateDefinition {
            definition: order_producers.clone(),
        })
        .await?;
    let changed = tokio::time::timeout(PATIENCE, stream.next()).await??;
    let got = client
        .send(GetDefinition::<Selector> {
            name: order_producers.name().clone(),
        })
        .await?;
    let listed = client.send(ListDefinitions::<Selector>::new()).await?;

    assert_eq!(created, Ok(()));
    assert_eq!(
        changed,
        StreamMessage::DefinitionChanged {
            definition: AnyDefinition::Selector(order_producers.clone())
        }
    );
    // A definition travels as its file holds it: the UUID that the workspace gave it stays in the process.
    assert_eq!(got, Ok(order_producers.clone()));
    assert_eq!(listed, Ok(vec![order_producers.clone()]));

    // The rule names a preset that the workspace does not have yet.
    let early = client
        .send(CreateDefinition {
            definition: slow.clone(),
        })
        .await?;
    let Err(CreateDefinitionError::DoesNotFit { name, reason }) = early else {
        anyhow::bail!("a rule was created with a preset that does not exist: {early:?}");
    };
    assert_eq!(name, slow.name().clone().into());
    assert_eq!(
        reason,
        "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist"
    );

    // Once the preset is there, the rule is too, and the selector stays as long as the rule uses it.
    let preset = Preset::new(delay, TimingLie::new().with_delay("1s".parse()?));
    assert_eq!(client.send(CreateDefinition { definition: preset }).await?, Ok(()));
    assert_eq!(
        client
            .send(CreateDefinition {
                definition: slow.clone()
            })
            .await?,
        Ok(())
    );
    let in_use = client
        .send(DeleteDefinition::<Selector> {
            name: order_producers.name().clone(),
        })
        .await?;
    assert_eq!(
        in_use,
        Err(DeleteDefinitionError::InUse {
            name: order_producers.name().clone().into(),
            users: vec![slow.name().clone().into()],
        })
    );

    let deleted = client
        .send(DeleteDefinition::<Rule> {
            name: slow.name().clone(),
        })
        .await?;
    assert_eq!(deleted, Ok(()));
    // The stream heard of the preset and the rule, and then of the rule that went.
    // A change tells the state of what it names when it travels, so the rule that went may have no change of its
    // own by then. The deletion comes last, whatever came before it.
    let deleted = StreamMessage::DefinitionDeleted {
        name: slow.name().clone().into(),
    };
    let last = loop {
        let message = tokio::time::timeout(PATIENCE, stream.next()).await??;
        if message == deleted || !matches!(message, StreamMessage::DefinitionChanged { .. }) {
            break message;
        }
    };
    assert_eq!(last, deleted);
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_command_on_an_environment_that_does_not_run_fails_as_its_operation_says() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let staging: EnvironmentName = "staging-proxy".parse()?;

    let stopped = client
        .send(StopEnvironment {
            environment: staging.clone(),
        })
        .await?;
    let armed = client
        .send(ArmRule {
            environment: staging.clone(),
            rule: "slow-order-producers".parse()?,
        })
        .await?;
    let rules = client
        .send(ListEnvironmentRules {
            environment: staging.clone(),
        })
        .await?;
    let parsed = client
        .send(ParseSentence {
            environment: staging.clone(),
            text: "delay produce".to_owned(),
        })
        .await?;

    assert_eq!(
        stopped,
        Err(EnvironmentNotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        armed,
        Err(RuleCommandError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        rules,
        Err(EnvironmentReadError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(parsed, Err(EnvironmentReadError::NotRunning { environment: staging }));
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_environment_that_the_workspace_does_not_have_or_that_runs_a_broker_does_not_start() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A proxy in front of a broker, which the host cannot run yet.
    let listen = Listeners::new(
        SocketAddr::new(LOCALHOST, ANY_PORT).into(),
        "127.0.0.1:29093-29192".parse()?,
        LOCALHOST.into(),
    )?;
    let orders = Environment::new(
        "orders-sim".parse()?,
        Instances::ProxyInFrontOfBroker {
            proxy: ProxyDefinition::new(listen),
            broker: BrokerDefinition::new("orders-sim".parse()?),
        },
    );
    host.create(orders.clone()).await?;
    let staging: EnvironmentName = "staging-proxy".parse()?;

    let unknown = client
        .send(StartEnvironment {
            environment: staging.clone(),
        })
        .await?;
    let with_broker = client
        .send(StartEnvironment {
            environment: orders.name().clone(),
        })
        .await?;
    let statuses = client.send(ListEnvironments {}).await?;

    assert_eq!(unknown, Err(StartEnvironmentError::NotFound { environment: staging }));
    assert_eq!(
        with_broker,
        Err(StartEnvironmentError::Failed {
            environment: orders.name().clone(),
            reason: "it runs a broker, and the host cannot run a broker yet".to_owned(),
        })
    );
    assert_eq!(
        statuses,
        Ok(vec![EnvironmentStatus {
            environment: orders,
            running: false,
        }])
    );
    assert_eq!(host.running(), Vec::<EnvironmentName>::new());
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn live_rules_pads_and_sessions_need_an_environment_that_runs() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay("20ms".parse()?),
    );
    let pad = Pad::new(
        "order-desk".parse()?,
        Layer::all_clients(),
        Row::from_index(0),
        Column::from_index(0),
    );

    let created = client
        .send(CreateLiveRule {
            environment: staging.clone(),
            rule: hold.clone(),
        })
        .await?;
    let kept = client
        .send(KeepRule {
            environment: staging.clone(),
            rule: hold.name().clone(),
        })
        .await?;
    let pressed = client
        .send(SendIntent {
            environment: staging.clone(),
            intent: Intent::Arm { pad },
        })
        .await?;
    let sessions = client
        .send(ListSessions {
            environment: staging.clone(),
        })
        .await?;
    let closed = client
        .send(CloseSession {
            environment: staging.clone(),
            connection: ConnectionNumber::new(7),
        })
        .await?;

    assert_eq!(
        created,
        Err(CreateLiveRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        kept,
        Err(KeepRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        pressed,
        Err(IntentError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        sessions,
        Err(EnvironmentNotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(closed, Err(CloseSessionError::NotRunning { environment: staging }));
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_subscription_is_answered_and_a_stop_of_the_server_ends_the_stream() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;
    let first = tokio::time::timeout(PATIENCE, stream.next()).await??;

    stream
        .follow(Subscription::Calls {
            environment: "staging-proxy".parse()?,
            selector: SelectorRef::Inline(Conditions::new()),
        })
        .await;
    let refused = tokio::time::timeout(PATIENCE, stream.next()).await??;
    stream.follow(Subscription::Nothing).await;
    let nothing = tokio::time::timeout(PATIENCE, stream.next()).await??;

    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    assert_eq!(
        refused,
        StreamMessage::SubscriptionRefused {
            reason: "environment \"staging-proxy\" does not run".to_owned()
        }
    );

    // A subscription to nothing is in force at once.
    assert_eq!(
        nothing,
        StreamMessage::Following {
            subscription: Subscription::Nothing
        }
    );

    // The server stops, and closes the stream on its way.
    server.stop().await;
    let ended = tokio::time::timeout(PATIENCE, stream.next()).await?;
    assert!(matches!(ended, Err(StreamError::Unreachable { .. })), "{ended:?}");
    Ok(())
}

/// The first line and the headers of what the server answers to `request`, sent as it is on a new connection.
async fn exchange(server: SocketAddr, request: &str) -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut connection = tokio::net::TcpStream::connect(server).await?;
    connection.write_all(request.as_bytes()).await?;
    let mut answer = Vec::new();
    tokio::time::timeout(PATIENCE, connection.read_to_end(&mut answer)).await??;
    let answer = String::from_utf8_lossy(&answer).into_owned();
    Ok(answer.split("\r\n\r\n").next().unwrap_or_default().to_lowercase())
}

#[tokio::test]
async fn every_path_that_the_control_plane_leaves_free_serves_the_page_of_the_ui() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;

    // A view of the UI opens at its own address. A build without the files of the UI says how to build them, on a
    // page of its own.
    let view = exchange(
        server.address(),
        "GET /rules HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
    )
    .await?;
    let elsewhere = exchange(
        server.address(),
        "GET /api/recordings HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n",
    )
    .await?;
    let posted = exchange(
        server.address(),
        "POST /rules HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
    )
    .await?;

    assert!(view.contains("content-type: text/html"), "{view}");
    assert!(elsewhere.starts_with("http/1.1 404"), "{elsewhere}");
    assert!(posted.starts_with("http/1.1 405"), "{posted}");
    server.stop().await;
    Ok(())
}
