//! The control plane without a real cluster, driven through the client as the UI, the CLI and tests drive it:
//! the definitions of the workspace with their revisions, a change that expects another revision or another run,
//! what an operation answers about an environment that does not run, a request that does not say where it comes
//! from, the live stream, and the presence of its client.
//!
//! Each test starts a host whose workspace lives in memory, puts a server in front of it on a free port,
//! and speaks to that server with a client. The tests in `tests/real_cluster` run environments in front of real
//! Kafka brokers.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use gaslightmq_clock_waitable::SteppedClock;
use gaslightmq_control_api::clock::{Duration, TimeSpan};
use gaslightmq_control_api::environment::{
    BrokerDefinition, Environment, EnvironmentName, Instances, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::ConnectionNumber;
use gaslightmq_control_api::journal::ChangeSurface;
use gaslightmq_control_api::kernel::ApiKey;
use gaslightmq_control_api::lie::TimingLie;
use gaslightmq_control_api::prelude::*;
// The revision of the control plane, and not the one of the workspace behind it, whose prelude names its own.
use gaslightmq_control_api::Revision;
use gaslightmq_control_api::rule::{Preset, PresetName, Rule};
use gaslightmq_control_api::scenario::{Mode, Scenario, Step};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorRef};
use gaslightmq_control_api::surface::{Column, Intent, Layer, Pad, Row};
use gaslightmq_control_api::trigger::Seed;
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, OperationError, StreamError};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::{EnvironmentConfiguration, Host, HostConfiguration};
use gaslightmq_lie_api::prelude::*;
use gaslightmq_workspace::prelude::*;

/// How long a test waits for a message of the live stream that the server has a reason to send.
const PATIENCE: Duration = Duration::from_secs(10);

/// The interval of the pings in a test that steps the clock of the server and is not about them:
/// longer than the test steps, so the server takes no client of the test for silent.
const QUIET: TimeSpan = TimeSpan::from_secs(3600);

/// How long the backend of an environment waits for a cluster that never answers, before its start gives up.
const GIVE_UP: TimeSpan = TimeSpan::from_millis(200);

/// The address that the server of a test listens on, and that its client connects to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port, so the tests run side by side.
const ANY_PORT: u16 = 0;

/// The run of the host of a test, as the snapshot writes it. A test names its own run.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// The run of the process before it started again: the one that a client that read back then still names.
const EARLIER_RUN: &str = "1f0a52c79be34d168a703c5ed2946b08";

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the test follows one workspace from its first create to its last delete"
)]
async fn a_definition_changes_in_the_workspace_and_every_client_of_the_stream_hears_of_it() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
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
    let StreamMessage::Snapshot(first) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream did not start with a snapshot");
    };
    assert_eq!(first.definitions, Definitions::default());

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
        .await;
    let listed = client.send(ListDefinitions::<Selector>::new()).await;

    // The create answered with the revision of the selector, and whoever reads the selector gets it beside it.
    assert_eq!(
        changed,
        StreamMessage::DefinitionChanged {
            definition: AnyDefinition::Selector(order_producers.clone()),
            revision: created.revision,
            loses_comment: false,
        }
    );
    // A definition travels as its file holds it, and its revision is no part of it.
    let held = DefinitionStatus {
        definition: order_producers.clone(),
        revision: created.revision,
        loses_comment: false,
    };
    assert_eq!(got, Ok(held.clone()));
    assert_eq!(listed, Ok(vec![held]));

    // The rule names a preset that the workspace does not have yet.
    let early = client
        .send(CreateDefinition {
            definition: slow.clone(),
        })
        .await;
    let Err(OperationError::Refused {
        error: CreateDefinitionError::DoesNotFit { name, reason },
    }) = early
    else {
        anyhow::bail!("a rule was created with a preset that does not exist: {early:?}");
    };
    assert_eq!(name, slow.name().clone().into());
    assert_eq!(
        reason,
        "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist"
    );

    // Once the preset is there, the rule is too, and the selector stays as long as the rule uses it.
    let preset = Preset::new(delay, TimingLie::new().with_delay("1s".parse()?));
    client.send(CreateDefinition { definition: preset }).await?;
    let definition = slow.clone();
    client.send(CreateDefinition { definition }).await?;
    let in_use = client
        .send(DeleteDefinition::<Selector> {
            name: order_producers.name().clone(),
            expects: None,
        })
        .await;
    assert_eq!(
        in_use,
        Err(OperationError::Refused {
            error: DeleteDefinitionError::InUse {
                name: order_producers.name().clone().into(),
                users: vec![slow.name().clone().into()],
            }
        })
    );

    let deleted = client
        .send(DeleteDefinition::<Rule> {
            name: slow.name().clone(),
            expects: None,
        })
        .await?;
    // Every change takes the next number of one counter, whatever it changes.
    assert!(created.revision < deleted.revision, "{created:?} and then {deleted:?}");
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
        RUN.parse()?,
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
        .await;
    let armed = client
        .send(ArmRule {
            environment: staging.clone(),
            rule: "slow-order-producers".parse()?,
            expects: None,
        })
        .await;
    let rules = client
        .send(ListEnvironmentRules {
            environment: staging.clone(),
        })
        .await;
    let parsed = client
        .send(ParseSentence {
            environment: staging.clone(),
            text: "delay produce".to_owned(),
        })
        .await;
    // An environment that does not run has no journal either: a journal begins and ends with its environment.
    let journal = client
        .send(QueryJournal {
            environment: staging.clone(),
            after: None,
        })
        .await;

    assert_eq!(
        stopped,
        Err(OperationError::Refused {
            error: EnvironmentNotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        armed,
        Err(OperationError::Refused {
            error: RuleCommandError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        rules,
        Err(OperationError::Refused {
            error: EnvironmentReadError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        parsed,
        Err(OperationError::Refused {
            error: EnvironmentReadError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        journal,
        Err(OperationError::Refused {
            error: EnvironmentNotRunning { environment: staging }
        })
    );
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_scenario_is_kept_in_the_workspace_and_runs_only_in_an_environment_that_runs() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let steps = vec![Step::new("warm-up".parse()?), Step::new("back-to-normal".parse()?)];
    let scenario = Scenario::new("by-hand".parse()?, Mode::Sequence(steps), Seed::new(7), [])?;

    let created = client
        .send(CreateDefinition {
            definition: scenario.clone(),
        })
        .await?;
    let got = client
        .send(GetDefinition::<Scenario> {
            name: scenario.name().clone(),
        })
        .await?;
    let activated = client
        .send(ActivateScenario {
            environment: staging.clone(),
            scenario: scenario.name().clone(),
            expects: None,
        })
        .await;
    let stepped = client
        .send(StepScenario {
            environment: staging.clone(),
            expects: None,
        })
        .await;
    let deactivated = client
        .send(DeactivateScenario {
            environment: staging.clone(),
            expects: None,
        })
        .await;

    assert_eq!(got.definition, scenario);
    assert_eq!(got.revision, created.revision);
    assert_eq!(
        activated,
        Err(OperationError::Refused {
            error: ActivateScenarioError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        stepped,
        Err(OperationError::Refused {
            error: StepScenarioError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        deactivated,
        Err(OperationError::Refused {
            error: DeactivateScenarioError::NotRunning { environment: staging }
        })
    );
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_capture_starts_stops_and_saves_only_in_an_environment_that_runs() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let staging: EnvironmentName = "staging-proxy".parse()?;

    let started = client
        .send(StartCapture {
            environment: staging.clone(),
        })
        .await;
    let stopped = client
        .send(StopCapture {
            environment: staging.clone(),
        })
        .await;
    let saved = client
        .send(SaveCapture {
            environment: staging.clone(),
            scenario: "slow-then-refused".parse()?,
            title: None,
        })
        .await;

    assert_eq!(
        started,
        Err(OperationError::Refused {
            error: StartCaptureError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        stopped,
        Err(OperationError::Refused {
            error: StopCaptureError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        saved,
        Err(OperationError::Refused {
            error: SaveCaptureError::NotRunning { environment: staging }
        })
    );
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn an_environment_that_the_workspace_does_not_have_or_that_runs_a_broker_does_not_start() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
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
    // The host answers the create with the revision of the environment, which the control plane hands out by its
    // number.
    let created = Revision::new(host.create(orders.clone(), ChangeSurface::Test).await?.number());
    let staging: EnvironmentName = "staging-proxy".parse()?;

    let unknown = client
        .send(StartEnvironment {
            environment: staging.clone(),
        })
        .await;
    let with_broker = client
        .send(StartEnvironment {
            environment: orders.name().clone(),
        })
        .await;
    let statuses = client.send(ListEnvironments {}).await;

    assert_eq!(
        unknown,
        Err(OperationError::Refused {
            error: StartEnvironmentError::NotFound { environment: staging }
        })
    );
    assert_eq!(
        with_broker,
        Err(OperationError::Refused {
            error: StartEnvironmentError::Failed {
                environment: orders.name().clone(),
                reason: "it runs a broker, and the host cannot run a broker yet".to_owned(),
            }
        })
    );
    assert_eq!(
        statuses,
        Ok(vec![EnvironmentStatus {
            environment: orders,
            revision: created,
            running: false,
            loses_comment: false,
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
        RUN.parse()?,
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
            armed: true,
        })
        .await;
    let kept = client
        .send(KeepRule {
            environment: staging.clone(),
            rule: hold.name().clone(),
            expects: None,
        })
        .await;
    let pressed = client
        .send(SendIntent {
            environment: staging.clone(),
            intent: Intent::Arm { pad },
        })
        .await;
    let sessions = client
        .send(ListSessions {
            environment: staging.clone(),
        })
        .await;
    let closed = client
        .send(CloseSession {
            environment: staging.clone(),
            connection: ConnectionNumber::new(7),
        })
        .await;

    assert_eq!(
        created,
        Err(OperationError::Refused {
            error: CreateLiveRuleError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        kept,
        Err(OperationError::Refused {
            error: KeepRuleError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        pressed,
        Err(OperationError::Refused {
            error: IntentError::NotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        sessions,
        Err(OperationError::Refused {
            error: EnvironmentNotRunning {
                environment: staging.clone()
            }
        })
    );
    assert_eq!(
        closed,
        Err(OperationError::Refused {
            error: CloseSessionError::NotRunning { environment: staging }
        })
    );
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_subscription_is_answered_and_a_stop_of_the_server_ends_the_stream() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
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

#[tokio::test]
async fn a_client_that_comes_back_within_the_grace_period_is_the_presence_it_was_and_a_new_one_after_it()
-> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.ping_interval = PingInterval::new(QUIET)?;
    let grace = configuration.presence_grace;
    // The grace period of a presence runs on the clock of the server, which the test steps.
    let clock = SteppedClock::new();
    let server = ControlServer::start_on(configuration, Arc::clone(&host), clock.clone()).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));

    // Two tabs of the UI open the live stream, and each is a presence of its own.
    let tab = client.live_stream().await?;
    let other_tab = client.live_stream().await?;
    let presence = tab.presence();
    assert_ne!(presence, other_tab.presence());

    // The stream of the first tab ends, and the tab comes back before the grace period is over, as the same presence.
    tab.close().await;
    clock.advance(grace.duration() / 2);
    let back = client.live_stream_as(presence).await?;
    assert_eq!(back.presence(), presence);

    // Its stream ends again, and this time the tab stays away until the grace period is over.
    // Its presence has ended, so it is a new one when it comes back, and its stream starts as any other.
    back.close().await;
    clock.advance(grace.duration());
    let mut late = client.live_stream_as(presence).await?;
    assert_ne!(late.presence(), presence);
    assert_ne!(late.presence(), other_tab.presence());
    let first = tokio::time::timeout(PATIENCE, late.next()).await??;
    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_request_says_where_it_comes_from_or_the_server_does_not_read_it() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let mut of_the_ui = ControlClientConfiguration::new(server.address().into());
    of_the_ui.surface = RequestSurface::Ui;
    let client = ControlClient::new(of_the_ui);
    // What the server answers to `body`, posted to the operations as it is: the first line, and the body.
    let post = async |body: &str| -> anyhow::Result<(String, String)> {
        let request = format!(
            "POST /api/operations HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut connection = tokio::net::TcpStream::connect(server.address()).await?;
        connection.write_all(request.as_bytes()).await?;
        let mut answer = String::new();
        tokio::time::timeout(PATIENCE, connection.read_to_string(&mut answer)).await??;
        let (head, body) = answer
            .split_once("\r\n\r\n")
            .context("an answer has a head and a body")?;
        let status = head.lines().next().context("an answer has a first line")?;
        Ok((status.to_owned(), body.to_owned()))
    };

    // A view of the UI lists the rules, a pad asks the same, and ⌘Z asks it as an undo: each says so, and is answered.
    let from_a_view = client.send(ListDefinitions::<Rule>::new()).await;
    let from_a_pad = client
        .send_from(RequestSurface::Pad, ListDefinitions::<Rule>::new())
        .await;
    let as_an_undo = client
        .send_from(
            RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo),
            ListDefinitions::<Rule>::new(),
        )
        .await;
    // An operation alone says nothing about its sender, and no request is a lifetime that ended.
    let (unsigned_status, unsigned) = post(r#"{ "operation": "list-rules" }"#).await?;
    let (claimed_status, claimed) = post(r#"{ "from": { "surface": "lifetime" }, "operation": "list-rules" }"#).await?;

    assert_eq!(from_a_view, Ok(Vec::new()));
    assert_eq!(from_a_pad, Ok(Vec::new()));
    assert_eq!(as_an_undo, Ok(Vec::new()));
    // Why a request was not read is text: the status and the error say that it was not.
    assert_eq!(unsigned_status, "HTTP/1.1 400 Bad Request");
    let unsigned: RequestError = serde_json::from_str(&unsigned)?;
    assert!(matches!(unsigned, RequestError::Unreadable { .. }), "{unsigned}");
    assert_eq!(claimed_status, "HTTP/1.1 400 Bad Request");
    let claimed: RequestError = serde_json::from_str(&claimed)?;
    assert!(matches!(claimed, RequestError::Unreadable { .. }), "{claimed}");
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_change_that_expects_another_revision_changes_nothing_and_says_which_revision_holds() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let mut of_the_ui = ControlClientConfiguration::new(server.address().into());
    of_the_ui.surface = RequestSurface::Ui;
    // Two tabs of the UI.
    let one_tab = ControlClient::new(of_the_ui.clone());
    let other_tab = ControlClient::new(of_the_ui);
    let blackhole = Preset::new("blackhole".parse()?, TimingLie::new().with_delay("1s".parse()?));
    one_tab
        .send(CreateDefinition {
            definition: blackhole.clone(),
        })
        .await?;
    // Both tabs read the preset: the snapshot names the run, and holds the preset with its revision.
    let mut stream = other_tab.live_stream().await?;
    let StreamMessage::Snapshot(snapshot) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream did not start with a snapshot");
    };
    let read = snapshot
        .definitions
        .presets
        .first()
        .context("the snapshot holds the preset")?;
    let seen = Some(Expects {
        run: snapshot.run,
        revision: read.revision,
    });

    // One tab gives the preset a title, and the other one a different title, without having seen the first.
    let titled = blackhole.clone().with_title("Blackhole".parse()?);
    let first = one_tab
        .send(UpdateDefinition {
            expects: seen,
            definition: titled.clone(),
        })
        .await?;
    let late = other_tab
        .send(UpdateDefinition {
            expects: seen,
            definition: blackhole.clone().with_title("Silence".parse()?),
        })
        .await;
    let deleted_late = other_tab
        .send(DeleteDefinition::<Preset> {
            name: blackhole.name().clone(),
            expects: seen,
        })
        .await;

    assert_eq!(read.definition, blackhole);
    assert!(read.revision < first.revision, "{read:?} and then {first:?}");
    // The late changes change nothing, and say which revision the preset has now.
    assert_eq!(
        late,
        Err(OperationError::Refused {
            error: UpdateDefinitionError::Changed {
                name: blackhole.name().clone().into(),
                revision: first.revision,
            }
        })
    );
    assert_eq!(
        deleted_late,
        Err(OperationError::Refused {
            error: DeleteDefinitionError::Changed {
                name: blackhole.name().clone().into(),
                revision: first.revision,
            }
        })
    );
    let held = other_tab
        .send(GetDefinition::<Preset> {
            name: blackhole.name().clone(),
        })
        .await;
    assert_eq!(
        held,
        Ok(DefinitionStatus {
            definition: titled,
            revision: first.revision,
            loses_comment: false,
        })
    );
    // The other tab changes the preset as it is now: it expects the revision that the error named.
    let seen_now = Some(Expects {
        run: snapshot.run,
        revision: first.revision,
    });
    let second = other_tab
        .send(UpdateDefinition {
            expects: seen_now,
            definition: blackhole.clone().with_title("Silence".parse()?),
        })
        .await?;
    assert!(first.revision < second.revision, "{first:?} and then {second:?}");
    server.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_change_that_expects_a_revision_of_another_run_changes_nothing_and_names_the_run() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let blackhole = Preset::new("blackhole".parse()?, TimingLie::new().with_delay("1s".parse()?));
    let created = client
        .send(CreateDefinition {
            definition: blackhole.clone(),
        })
        .await?;
    // A tab read the preset before the process started again. The counter starts again with every run, so the
    // revision alone can fit, and the run does not.
    let before_the_restart = Some(Expects {
        run: EARLIER_RUN.parse()?,
        revision: created.revision,
    });
    let other_run = RUN.parse::<RunId>()?;

    let updated = client
        .send(UpdateDefinition {
            expects: before_the_restart,
            definition: blackhole.clone().with_title("Blackhole".parse()?),
        })
        .await;
    let deleted = client
        .send(DeleteDefinition::<Preset> {
            name: blackhole.name().clone(),
            expects: before_the_restart,
        })
        .await;
    // An undo of the earlier run arms nothing: it learns of the other run before anything else,
    // also where its environment does not run any more.
    let armed = client
        .send(ArmRule {
            environment: staging.clone(),
            rule: "slow-order-producers".parse()?,
            expects: before_the_restart,
        })
        .await;
    let disarmed = client
        .send(DisarmRule {
            environment: staging.clone(),
            rule: "slow-order-producers".parse()?,
            expects: before_the_restart,
        })
        .await;
    let kept = client
        .send(KeepRule {
            environment: staging.clone(),
            rule: "slow-order-producers".parse()?,
            expects: before_the_restart,
        })
        .await;
    let removed = client
        .send(DeleteLiveRule {
            environment: staging,
            rule: "slow-order-producers".parse()?,
            expects: before_the_restart,
        })
        .await;

    assert_eq!(
        updated.err().and_then(OperationError::refusal),
        Some(UpdateDefinitionError::OtherRun { run: other_run })
    );
    assert_eq!(
        deleted.err().and_then(OperationError::refusal),
        Some(DeleteDefinitionError::OtherRun { run: other_run })
    );
    assert_eq!(
        armed.err().and_then(OperationError::refusal),
        Some(RuleCommandError::OtherRun { run: other_run })
    );
    assert_eq!(
        disarmed.err().and_then(OperationError::refusal),
        Some(RuleCommandError::OtherRun { run: other_run })
    );
    assert_eq!(
        kept.err().and_then(OperationError::refusal),
        Some(KeepRuleError::OtherRun { run: other_run })
    );
    assert_eq!(
        removed.err().and_then(OperationError::refusal),
        Some(DeleteLiveRuleError::OtherRun { run: other_run })
    );
    // The preset is as it was created.
    let held = client
        .send(GetDefinition::<Preset> {
            name: blackhole.name().clone(),
        })
        .await?;
    assert_eq!(
        (held.definition, held.revision, held.loses_comment),
        (blackhole, created.revision, false)
    );
    server.stop().await;
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
async fn a_process_is_healthy_while_every_environment_of_its_start_runs() -> anyhow::Result<()> {
    // A server that takes connections and never answers a request: the cluster of an environment that never starts.
    let silent = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, ANY_PORT))?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let probe = "GET /health HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n";

    // A process that started with no environment has nothing that does not run.
    let healthy = client.send(CheckHealth {}).await;
    let probed_healthy = exchange_with_body(server.address(), probe).await?;
    // The process starts with an environment whose cluster never answers, so it does not run.
    let listen = Listeners::new(
        SocketAddr::new(LOCALHOST, ANY_PORT).into(),
        "127.0.0.1:19092-19191".parse()?,
        "localhost".parse()?,
    )?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen),
            upstream: Upstream::new(silent.local_addr()?.to_string().parse()?),
        },
    );
    let mut starting = EnvironmentConfiguration::new(&staging)?;
    starting.upstream.request_timeout = GIVE_UP;
    let started = host.start_with([starting]).await;
    let unhealthy = client.send(CheckHealth {}).await;
    let probed_unhealthy = exchange_with_body(server.address(), probe).await?;

    assert_eq!(healthy, Ok(Health::default()));
    assert!(probed_healthy.starts_with("HTTP/1.1 200"), "{probed_healthy}");
    assert!(probed_healthy.ends_with(r#"{"environments":[]}"#), "{probed_healthy}");
    assert!(started.is_err(), "{started:?}");
    let stopped = Health {
        environments: vec![EnvironmentHealth {
            environment: staging.name().clone(),
            running: false,
        }],
    };
    assert_eq!(
        unhealthy,
        Err(OperationError::Refused {
            error: Unhealthy { health: stopped }
        })
    );
    assert!(probed_unhealthy.starts_with("HTTP/1.1 503"), "{probed_unhealthy}");
    assert!(
        probed_unhealthy.ends_with(r#"{"environments":[{"environment":"staging-proxy","running":false}]}"#),
        "{probed_unhealthy}"
    );
    server.stop().await;
    Ok(())
}

/// The whole answer of the server to `request`, sent as it is on a new connection: its first line, its headers
/// and its body.
async fn exchange_with_body(server: SocketAddr, request: &str) -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut connection = tokio::net::TcpStream::connect(server).await?;
    connection.write_all(request.as_bytes()).await?;
    let mut answer = Vec::new();
    tokio::time::timeout(PATIENCE, connection.read_to_end(&mut answer)).await??;
    Ok(String::from_utf8(answer)?)
}

#[tokio::test]
async fn every_path_that_the_control_plane_leaves_free_serves_the_page_of_the_ui() -> anyhow::Result<()> {
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
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
