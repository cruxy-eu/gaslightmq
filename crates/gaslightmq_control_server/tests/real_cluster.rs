//! The control plane runs environments of the workspace in front of real Kafka brokers, streams what happens in
//! them: their calls, their rules, their sessions and their statistics, reads ⌘K sentences with their names,
//! and ends the holds of a client that stayed away.
//!
//! The tests need Docker.
//! Each test starts the cluster it needs, a host whose workspace lives in memory, and a server in front of the host,
//! and drives the server through the client as the UI does.
//! A Kafka client is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address that the
//! control plane names, and nothing else.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use gaslightmq_clock_waitable::SteppedClock;
use gaslightmq_control_api::clock::{Duration, TimeSpan};
use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, Instances, ListenAddress, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::{Endpoint, Field, FieldSource};
use gaslightmq_control_api::journal::{
    Change, ChangeCause, ChangeSource, ChangeSurface, JournalEntry, RecordedDefinition, RecordedName,
};
use gaslightmq_control_api::kernel::{ApiKey, ClientId};
use gaslightmq_control_api::lie::TimingLie;
use gaslightmq_control_api::prelude::*;
// The revision of the control plane, and not the one of the workspace behind it, whose prelude names its own.
use gaslightmq_control_api::Revision;
use gaslightmq_control_api::rule::{Preset, PresetSet, Rule, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector, SelectorRef, SelectorSet};
use gaslightmq_control_api::surface::{
    Column, Encoder, EncoderMapping, EncoderTurn, Intent, Layer, Pad, PadLayout, PresenceId, Row, Steps,
};
use gaslightmq_control_api::trigger::{Count, Lifetime, Trigger};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, LiveStream, OperationError};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::{Host, HostConfiguration};
use gaslightmq_kafka_client_native::{KafkaProducer, ProducerConfiguration};
use gaslightmq_lie_api::prelude::*;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::{Offset, Partition, Record};
use gaslightmq_workspace::prelude::*;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// How long a test waits for a message of the live stream that the server has a reason to send.
const PATIENCE: Duration = Duration::from_secs(30);

/// The interval of the statistics, and of the pings, in a test that is not about them: longer than any test runs
/// or steps its clock, so no statistics come between the messages that the test looks at, and the server takes no
/// client of the test for silent.
const QUIET: TimeSpan = TimeSpan::from_secs(3600);

/// The interval of the statistics in the test that is about them.
const STATS_INTERVAL: TimeSpan = TimeSpan::from_millis(100);

/// The address that the listeners and the server of the test listen on, and that its clients connect to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The run of the host of a test, as the snapshot writes it. A test names its own run.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

/// The live stream of a test, which reads the messages that the test looks at, and passes over the others.
struct Watched {
    stream: LiveStream,
}

impl Watched {
    /// The next message that `wanted` keeps, within the patience of the test. The messages before it pass.
    async fn next_where(&mut self, wanted: impl Fn(&StreamMessage) -> bool) -> anyhow::Result<StreamMessage> {
        let next = async {
            loop {
                let message = self.stream.next().await?;
                if wanted(&message) {
                    return Ok::<_, anyhow::Error>(message);
                }
            }
        };
        tokio::time::timeout(PATIENCE, next).await?
    }

    /// The next message that is no message of a session, no call and no entry of a journal: a change of a
    /// definition, a rule or an environment, or the answer to a subscription.
    async fn next_change(&mut self) -> anyhow::Result<StreamMessage> {
        self.next_where(|message| {
            !matches!(
                message,
                StreamMessage::Call { .. }
                    | StreamMessage::JournalEntry { .. }
                    | StreamMessage::SessionOpened { .. }
                    | StreamMessage::SessionIdentified { .. }
                    | StreamMessage::SessionSoftwareNamed { .. }
                    | StreamMessage::SessionClosed { .. }
            )
        })
        .await
    }
}

#[tokio::test]
async fn an_environment_started_by_its_name_streams_its_calls_until_it_stops() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    // The environment stands in front of the cluster, and every listener of it takes a free port.
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };

    // The snapshot knows the environment, and nothing runs yet.
    let StreamMessage::Snapshot(state) = stream.next_change().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    let environments: Vec<&Environment> = state
        .definitions
        .environments
        .iter()
        .map(|held| &held.definition)
        .collect();
    assert_eq!(environments, [&staging]);
    assert_eq!(state.environments, []);
    assert_eq!(state.run, RUN.parse()?);

    let started = client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await;
    let StreamMessage::EnvironmentStarted { environment } = stream.next_change().await? else {
        anyhow::bail!("the start of the environment sent no message");
    };
    let backend = client
        .send(GetBackend {
            environment: staging.name().clone(),
        })
        .await?;

    assert_eq!(started, Ok(()));
    assert_eq!(&environment.environment, staging.name());
    assert_eq!(environment.backend, backend);
    assert_eq!(backend.kind, BackendKind::Upstream);
    assert_eq!(environment.sessions, []);

    // The client follows the Produce calls of the environment, and writes through its bootstrap endpoint
    // once the server says that it follows them.
    let produces = Subscription::Calls {
        environment: staging.name().clone(),
        selector: SelectorRef::Inline(Conditions::from(Condition::is(ApiKey::PRODUCE))),
    };
    stream.stream.follow(produces.clone()).await;
    let following = stream.next_change().await?;
    assert_eq!(following, StreamMessage::Following { subscription: produces });
    let bootstrap = backend
        .endpoints
        .get(&Endpoint::Bootstrap)
        .copied()
        .flatten()
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let placed = Record::new("placed").with_key("o-1");
    let written = KafkaProducer::from(SocketAddr::from(bootstrap))
        .send(orders.name().as_str(), Partition::new(0), &[placed])
        .await;
    let call = stream
        .next_where(|message| matches!(message, StreamMessage::Call { .. }))
        .await?;
    let StreamMessage::Call { environment, call } = call else {
        anyhow::bail!("the stream carried another message than the call of the record");
    };
    assert_eq!(written, Ok(Offset::new(0)));
    assert_eq!(&environment, staging.name());
    let api = call.fields().read(Field::Api).present().context("a call has an API")?;
    assert!(api.contains(&ApiKey::PRODUCE.into()), "{api:?}");

    // The environment stops, and the stream says so once its sessions closed.
    let stopped = client
        .send(StopEnvironment {
            environment: staging.name().clone(),
        })
        .await;
    let gone = stream.next_change().await?;
    assert_eq!(stopped, Ok(()));
    assert_eq!(
        gone,
        StreamMessage::EnvironmentStopped {
            environment: staging.name().clone(),
        }
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_of_the_workspace_arms_and_fires_in_the_environment_that_runs() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A pad that holds the next Produce a moment, once someone presses it.
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    )
    .with_trigger(Trigger::Manual);
    client
        .send(CreateDefinition {
            definition: hold.clone(),
        })
        .await?;
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let on_the_rule = |expects| ArmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects,
    };

    let rules = client
        .send(ListEnvironmentRules {
            environment: staging.name().clone(),
        })
        .await?;
    let arming = client.send(on_the_rule(None)).await?;
    // A change from before the stream opened can still come after its snapshot, as the start of the environment.
    let changed = stream
        .next_where(|message| matches!(message, StreamMessage::RuleChanged { .. }))
        .await?;
    let firing = client
        .send(FireRule {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
        })
        .await;
    let again = client.send(on_the_rule(None)).await;

    let [listed] = rules.as_slice() else {
        anyhow::bail!("the environment holds another set of rules than the one of the workspace: {rules:?}");
    };
    assert_eq!(
        (&listed.rule, listed.live, listed.state),
        (&hold, false, RuleState::Disarmed)
    );
    // The arm answered with the revision that the rule has in the environment, and the stream tells the same.
    assert!(listed.revision < arming.revision, "{listed:?} and then {arming:?}");
    assert_eq!(
        changed,
        StreamMessage::RuleChanged {
            environment: staging.name().clone(),
            rule: RuleStatus {
                rule: hold.clone(),
                live: false,
                state: RuleState::Armed,
                revision: arming.revision,
            },
        }
    );
    assert_eq!(firing, Ok(()));
    assert_eq!(
        again,
        Err(OperationError::Refused {
            error: RuleCommandError::Refused {
                environment: staging.name().clone(),
                rule: hold.name().clone(),
                reason: "it is armed already".to_owned(),
            }
        })
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_arms_and_disarms_only_as_its_sender_saw_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let created = client
        .send(CreateDefinition {
            definition: hold.clone(),
        })
        .await?;
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    // Two tabs open, and both read the rule in the environment: the snapshot names the run, and the rule comes
    // with its revision there.
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let StreamMessage::Snapshot(snapshot) = stream.next_change().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    let read = snapshot.environments.first().and_then(|running| running.rules.first());
    let read = read.context("the snapshot holds the rule of the workspace in the environment that runs")?;
    let seen = |revision| {
        Some(Expects {
            run: snapshot.run,
            revision,
        })
    };
    let arm = |expects| ArmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects,
    };
    let disarm = |expects| DisarmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects,
    };

    // One tab arms the rule as it read it. The other one still shows it disarmed, and arms nothing over that.
    let armed = client.send(arm(seen(read.revision))).await?;
    let armed_late = client.send(arm(seen(read.revision))).await;
    // Someone gives the rule a title in the workspace, and the first tab disarms what it armed without seeing that.
    let retitled = client
        .send(UpdateDefinition {
            expects: None,
            definition: hold.clone().with_title("Hold order producers".parse()?),
        })
        .await?;
    let disarmed_late = client.send(disarm(seen(armed.revision))).await;
    // Whoever saw the title disarms the rule.
    let disarmed = client.send(disarm(seen(retitled.revision))).await?;

    // The environment started after the workspace got the rule, and its start is the last change of the rule there.
    assert!(created.revision < read.revision, "{created:?} and then {read:?}");
    let changed = |revision| RuleCommandError::Changed {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        revision,
    };
    assert_eq!(
        armed_late,
        Err(OperationError::Refused {
            error: changed(armed.revision)
        })
    );
    // A rule in an environment has one revision, for its definition and its state together.
    assert_eq!(
        disarmed_late,
        Err(OperationError::Refused {
            error: changed(retitled.revision)
        })
    );
    assert!(
        retitled.revision < disarmed.revision,
        "{retitled:?} and then {disarmed:?}"
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_press_of_an_empty_pad_makes_its_live_rule_and_arms_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A desk with one pad: a delay of 20 ms on the order producers, for every client.
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let delay = Preset::new(
        "delay-20ms".parse()?,
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let desk = PadLayout::new(
        "order-desk".parse()?,
        [order_producers.name().clone()],
        [delay.name().clone()],
        [Layer::all_clients()],
    )?;
    client
        .send(CreateDefinition {
            definition: order_producers.clone(),
        })
        .await?;
    client
        .send(CreateDefinition {
            definition: delay.clone(),
        })
        .await?;
    client
        .send(CreateDefinition {
            definition: desk.clone(),
        })
        .await?;
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let first = stream.next_change().await?;
    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    let selectors = SelectorSet::new([order_producers])?;
    let presets = PresetSet::new([delay])?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let made = desk
        .rule(&pad, &selectors, &presets)
        .context("the pad stands on a selector and a preset of the workspace")?;
    // The stream tells that `rule` is a live rule of the environment, and armed.
    let armed = |message: &StreamMessage, rule: &Rule| {
        matches!(message, StreamMessage::RuleChanged { environment, rule: status }
            if environment == staging.name() && &status.rule == rule && status.live && status.state == RuleState::Armed)
    };

    // Someone presses the empty pad: the environment makes its live rule, and arms it.
    let pressed = client
        .send(SendIntent {
            environment: staging.name().clone(),
            intent: Intent::Arm { pad },
        })
        .await?;
    let told = stream.next_where(|message| armed(message, &made)).await?;
    assert_eq!((pressed.made, pressed.changed, pressed.hold), (true, true, None));
    // The answer holds the rule as the stream tells it, with the revision that the press left.
    let answered = pressed.rule.map(|rule| StreamMessage::RuleChanged {
        environment: staging.name().clone(),
        rule,
    });
    assert_eq!(answered, Some(told));

    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_turn_changes_the_live_rule_of_a_pad_and_it_stays_armed() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A desk with one pad: a delay of 20 ms on the order producers, for every client.
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let delay = Preset::new(
        "delay-20ms".parse()?,
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let desk = PadLayout::new(
        "order-desk".parse()?,
        [order_producers.name().clone()],
        [delay.name().clone()],
        [Layer::all_clients()],
    )?;
    client
        .send(CreateDefinition {
            definition: order_producers.clone(),
        })
        .await?;
    client
        .send(CreateDefinition {
            definition: delay.clone(),
        })
        .await?;
    client
        .send(CreateDefinition {
            definition: desk.clone(),
        })
        .await?;
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let first = stream.next_change().await?;
    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    let selectors = SelectorSet::new([order_producers])?;
    let presets = PresetSet::new([delay])?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let made = desk
        .rule(&pad, &selectors, &presets)
        .context("the pad stands on a selector and a preset of the workspace")?;
    let turn = EncoderTurn::new(Encoder::Delay, Steps::new(2));
    let turned = EncoderMapping::default().apply(turn, &made, &presets)?;
    // The stream tells that `rule` is a live rule of the environment, and armed.
    let armed = |message: &StreamMessage, rule: &Rule| {
        matches!(message, StreamMessage::RuleChanged { environment, rule: status }
            if environment == staging.name() && &status.rule == rule && status.live && status.state == RuleState::Armed)
    };

    // Someone presses the empty pad, which makes its live rule and arms it. The pad pointed at no rule.
    let pressing = client
        .send(SendIntent {
            environment: staging.name().clone(),
            intent: Intent::Arm { pad: pad.clone() },
        })
        .await?;
    stream.next_where(|message| armed(message, &made)).await?;
    assert_eq!(pressing.found, None);

    // Someone turns the delay encoder two steps up: the live rule changes, and stays armed.
    let turning = client
        .send(SendIntent {
            environment: staging.name().clone(),
            intent: Intent::Turn { pad, turn },
        })
        .await?;
    let told = stream.next_where(|message| armed(message, &turned)).await?;
    assert_eq!((turning.made, turning.changed), (false, true));
    // The answer holds the rule as the stream tells it, with the revision that the turn left.
    let answered = turning.rule.map(|rule| StreamMessage::RuleChanged {
        environment: staging.name().clone(),
        rule,
    });
    assert_eq!(answered, Some(told));
    // The turn found the rule at the revision that the press left, which a surface compares with what it showed.
    assert_eq!(turning.found, pressing.rule.map(|rule| rule.revision));

    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_kept_live_rule_leaves_its_environment_for_the_workspace_and_stays_armed() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A live rule that ⌘K made: "delay produce 20ms".
    let made = Rule::new(
        "delay-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let armed = client
        .send(CreateLiveRule {
            environment: staging.name().clone(),
            rule: made.clone(),
            armed: true,
        })
        .await?;
    // The stream opens once the rule is armed, so its snapshot holds it.
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let StreamMessage::Snapshot(snapshot) = stream.next_change().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };

    // Someone keeps the rule as they armed it: it leaves the environment as a live rule, the workspace has it,
    // and it stays armed.
    let (run, revision) = (snapshot.run, armed.revision);
    let keep = KeepRule {
        environment: staging.name().clone(),
        rule: made.name().clone(),
        expects: Some(Expects { run, revision }),
    };
    let kept = client.send(keep).await?;
    // Changes from before the stream opened can still come after its snapshot: the start, the rule made and armed.
    let mut heard = vec![
        stream
            .next_where(|message| matches!(message, StreamMessage::RuleRemoved { .. }))
            .await?,
    ];
    while heard.len() < 3 {
        heard.push(stream.next_change().await?);
    }
    // The keep is one change: the rule has its revision in the workspace, and in the environment that held it.
    assert!(armed.revision < kept.revision, "{armed:?} and then {kept:?}");
    assert_eq!(
        heard,
        [
            StreamMessage::RuleRemoved {
                environment: staging.name().clone(),
                rule: made.name().clone(),
            },
            StreamMessage::DefinitionChanged {
                definition: AnyDefinition::Rule(made.clone()),
                revision: kept.revision,
                loses_comment: false,
            },
            StreamMessage::RuleChanged {
                environment: staging.name().clone(),
                rule: RuleStatus {
                    rule: made.clone(),
                    live: false,
                    state: RuleState::Armed,
                    revision: kept.revision,
                },
            },
        ]
    );
    let listed = client
        .send(ListEnvironmentRules {
            environment: staging.name().clone(),
        })
        .await?;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        (&listed[0].rule, listed[0].live, listed[0].revision),
        (&made, false, kept.revision)
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_session_is_listed_while_it_is_open_and_closes_when_someone_closes_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let first = stream.next_change().await?;
    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    let bootstrap = client
        .send(GetBackend {
            environment: staging.name().clone(),
        })
        .await?
        .endpoints
        .get(&Endpoint::Bootstrap)
        .copied()
        .flatten()
        .context("the bootstrap endpoint of the staging proxy is open")?;

    // A client connects, and sends nothing: its session is open, and anonymous.
    let mut connection = TcpStream::connect(SocketAddr::from(bootstrap)).await?;
    let opened = stream
        .next_where(|message| matches!(message, StreamMessage::SessionOpened { .. }))
        .await?;
    let StreamMessage::SessionOpened { environment, session } = opened else {
        anyhow::bail!("the stream told another message than the session that opened");
    };
    let listed = client
        .send(ListSessions {
            environment: staging.name().clone(),
        })
        .await;
    assert_eq!(&environment, staging.name());
    assert_eq!(session.endpoint, Endpoint::Bootstrap);
    assert_eq!(session.identity, None);
    assert_eq!(listed, Ok(vec![session.clone()]));

    // Someone closes the session: the client finds its connection closed, and the stream says the proxy did it.
    let close = CloseSession {
        environment: staging.name().clone(),
        connection: session.connection,
    };
    let closing = client.send(close.clone()).await;
    let closed = stream
        .next_where(|message| matches!(message, StreamMessage::SessionClosed { .. }))
        .await?;
    let mut nothing = Vec::new();
    let read = tokio::time::timeout(PATIENCE, connection.read_to_end(&mut nothing)).await??;
    let again = client.send(close).await;
    let after = client
        .send(ListSessions {
            environment: staging.name().clone(),
        })
        .await;

    assert_eq!(closing, Ok(()));
    assert_eq!(
        closed,
        StreamMessage::SessionClosed {
            environment: staging.name().clone(),
            connection: session.connection,
            by: SessionCloser::Proxy,
        }
    );
    assert_eq!(read, 0, "the proxy sent nothing before it closed the connection");
    assert_eq!(
        again,
        Err(OperationError::Refused {
            error: CloseSessionError::NotFound {
                environment: staging.name().clone(),
                connection: session.connection,
            }
        })
    );
    assert_eq!(after, Ok(vec![]));
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_that_spends_its_budget_disarms_and_the_statistics_count_its_firing() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(STATS_INTERVAL)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // "Hold the next Produce 20 ms, once."
    let once = Rule::new(
        "hold-the-next-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    )
    .with_lifetime(Lifetime::Firings(Count::ONE));
    let live = |state: RuleState, revision: Revision| StreamMessage::RuleChanged {
        environment: staging.name().clone(),
        rule: RuleStatus {
            rule: once.clone(),
            live: true,
            state,
            revision,
        },
    };
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let armed = client
        .send(CreateLiveRule {
            environment: staging.name().clone(),
            rule: once.clone(),
            armed: true,
        })
        .await?;
    stream
        .next_where(|message| *message == live(RuleState::Armed, armed.revision))
        .await?;
    let bootstrap = client
        .send(GetBackend {
            environment: staging.name().clone(),
        })
        .await?
        .endpoints
        .get(&Endpoint::Bootstrap)
        .copied()
        .flatten()
        .context("the bootstrap endpoint of the staging proxy is open")?;

    let placed = Record::new("placed").with_key("o-1");
    let written = KafkaProducer::from(SocketAddr::from(bootstrap))
        .send(orders.name().as_str(), Partition::new(0), &[placed])
        .await;
    // The Produce spent the only firing, so the rule disarmed itself.
    let disarmed = |message: &StreamMessage| matches!(message, StreamMessage::RuleChanged { rule, .. } if rule.state == RuleState::Disarmed);
    let spent = stream.next_where(disarmed).await?;
    let StreamMessage::RuleChanged { rule: spent, .. } = spent else {
        anyhow::bail!("the stream told another message than the rule that disarmed");
    };
    // Nobody asked for that change, and it took the next number all the same: the stream tells the rule with it.
    assert!(armed.revision < spent.revision, "{armed:?} and then {spent:?}");
    let counted = stream
        .next_where(|message| {
            matches!(message, StreamMessage::Stats { stats, .. }
                if stats.rules.get(once.name()).is_some_and(|rule| rule.firings == 1))
        })
        .await?;

    assert_eq!(written, Ok(Offset::new(0)));
    let StreamMessage::Stats { environment, stats } = counted else {
        anyhow::bail!("the stream told another message than the statistics");
    };
    assert_eq!(&environment, staging.name());
    assert_eq!(stats.apis.get(&ApiKey::PRODUCE), Some(&1));
    assert_eq!(
        stats.rules.get(once.name()),
        Some(&RuleStats {
            matches: 1,
            firings: 1,
            left: None,
        })
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_sentence_knows_the_client_that_wrote_through_the_environment_and_enter_creates_its_rule_armed()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders.eu").await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let order_ingest: ClientId = "order-ingest".parse()?;
    let of_order_ingest = Subscription::Calls {
        environment: staging.name().clone(),
        selector: SelectorRef::Inline(Conditions::from(Condition::is(order_ingest.clone()))),
    };
    stream.stream.follow(of_order_ingest.clone()).await;
    let following = StreamMessage::Following {
        subscription: of_order_ingest,
    };
    stream.next_where(|message| *message == following).await?;
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let mut producing = ProducerConfiguration::new(bootstrap);
    producing.client.client_id = order_ingest;

    // `order-ingest` writes through the environment, and the stream hands over its first call, once the traffic saw it.
    let written = KafkaProducer::new(producing)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1")],
        )
        .await;
    stream
        .next_where(|message| matches!(message, StreamMessage::Call { .. }))
        .await?;
    // Someone types the sentence of the spec in ⌘K: the client ID completes, and the whole sentence is a rule.
    let typing = client
        .send(ParseSentence {
            environment: staging.name().clone(),
            text: "delay produce orders.* 750ms 40% order-".to_owned(),
        })
        .await?;
    let whole = client
        .send(ParseSentence {
            environment: staging.name().clone(),
            text: "delay produce orders.* 750ms 40% order-ingest".to_owned(),
        })
        .await?;

    assert_eq!(written, Ok(Offset::new(0)));
    let completes =
        |completion: &WordCompletion| completion.kind == WordKind::Client && completion.word == "order-ingest";
    assert!(typing.completions.iter().any(completes), "{:?}", typing.completions);
    assert_eq!(whole.unread, []);
    let rule = whole.rule.context("the sentence names a lie")?;
    assert_eq!(rule.name(), "delay-750ms-on-produce-orders-for-order-ingest");
    assert_eq!(rule.scope().to_string(), "client.id is order-ingest");

    // Enter creates the rule as a live rule of the environment, armed, with one request of ⌘K.
    let enter = CreateLiveRule {
        environment: staging.name().clone(),
        rule: rule.clone(),
        armed: true,
    };
    let entered = client.send_from(RequestSurface::QuickInject, enter).await?;
    // Making the rule and arming it was one change, so the stream tells the rule with the revision of that answer.
    let armed_rule = StreamMessage::RuleChanged {
        environment: staging.name().clone(),
        rule: RuleStatus {
            rule,
            live: true,
            state: RuleState::Armed,
            revision: entered.revision,
        },
    };
    stream.next_where(|message| *message == armed_rule).await?;
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_client_that_stays_away_for_the_grace_period_loses_its_holds() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    // A desk with one pad: a delay of 1 s on the order producers, for every client.
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let delay = Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)));
    let desk = PadLayout::new(
        "order-desk".parse()?,
        [order_producers.name().clone()],
        [delay.name().clone()],
        [Layer::all_clients()],
    )?;
    host.create(staging.clone(), ChangeSurface::Test).await?;
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    configuration.ping_interval = PingInterval::new(QUIET)?;
    let grace = configuration.presence_grace;
    // The grace period of a presence runs on the clock of the server, which the test steps.
    let clock = SteppedClock::new();
    let server = ControlServer::start_on(configuration, Arc::clone(&host), clock.clone()).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let rule = desk.rule_name(&pad).context("the pad points at a rule by its name")?;
    let state_of_the_rule = async || -> anyhow::Result<Option<RuleState>> {
        let rules = client
            .send(ListEnvironmentRules {
                environment: staging.name().clone(),
            })
            .await?;
        let of_the_pad = rules.into_iter().find(|status| status.rule.name() == &rule);
        Ok(of_the_pad.map(|status| status.state))
    };

    // Ana plays at a tab of the UI, and Dan watches at another one. Each tab is a presence.
    let anas_tab = client.live_stream().await?;
    let mut dans_tab = Watched {
        stream: client.live_stream().await?,
    };
    // Ana holds the pad. Her request says which presence it comes from, so her hold belongs to that presence.
    let from_ana = RequestSource::new(RequestSurface::Pad).with_presence(anas_tab.presence());
    let held = client
        .send_from(
            from_ana,
            SendIntent {
                environment: staging.name().clone(),
                intent: Intent::Hold { pad },
            },
        )
        .await?;
    assert!(held.hold.is_some(), "{held:?}");
    assert_eq!(state_of_the_rule().await?, Some(RuleState::Armed));

    // Ana closes her tab while she holds the pad, so her release never comes.
    anas_tab.close().await;
    // The server keeps her presence for the grace period, in case her tab comes back, and the rule stays armed.
    clock.advance(grace.duration() / 2);
    assert_eq!(state_of_the_rule().await?, Some(RuleState::Armed));

    // Her tab stays away until the grace period is over. Her presence ends, her hold with it, and the rule disarms.
    clock.advance(grace.duration());
    dans_tab
        .next_where(|message| {
            matches!(
                message,
                StreamMessage::RuleChanged { rule: status, .. }
                    if status.rule.name() == &rule && status.state == RuleState::Disarmed
            )
        })
        .await?;
    assert_eq!(state_of_the_rule().await?, Some(RuleState::Disarmed));
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_client_that_comes_back_within_the_grace_period_keeps_its_holds_until_the_control_plane_stops()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    // A desk with two pads: a delay of 1 s on the order producers and one on the fetches, for every client.
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let fetches = Selector::new("fetches".parse()?, Conditions::from(Condition::is(ApiKey::FETCH)));
    let delay = Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)));
    let desk = PadLayout::new(
        "order-desk".parse()?,
        [order_producers.name().clone(), fetches.name().clone()],
        [delay.name().clone()],
        [Layer::all_clients()],
    )?;
    host.create(staging.clone(), ChangeSurface::Test).await?;
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(fetches, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    configuration.ping_interval = PingInterval::new(QUIET)?;
    let grace = configuration.presence_grace;
    // The grace period of a presence runs on the clock of the server, which the test steps.
    let clock = SteppedClock::new();
    let server = ControlServer::start_on(configuration, Arc::clone(&host), clock.clone()).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let slow_producers = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row")?;
    let slow_fetches = desk
        .pad(&Layer::all_clients(), Row::from_index(1), Column::from_index(0))
        .context("the desk has a pad at its second row")?;
    let rule_of = |pad: &Pad| desk.rule_name(pad).context("the pad points at a rule by its name");
    let (anas_rule, dans_rule) = (rule_of(&slow_producers)?, rule_of(&slow_fetches)?);
    // A press in hold of a tab says which presence it comes from, so its hold belongs to that presence.
    let hold = async |presence: PresenceId, pad: Pad| {
        let from = RequestSource::new(RequestSurface::Pad).with_presence(presence);
        let environment = staging.name().clone();
        let intent = Intent::Hold { pad };
        client.send_from(from, SendIntent { environment, intent }).await
    };

    // Ana and Dan each play at a tab of their own, and Cleo watches at a third. Each tab is a presence.
    let anas_tab = client.live_stream().await?;
    let dans_tab = client.live_stream().await?;
    let mut cleos_tab = Watched {
        stream: client.live_stream().await?,
    };
    let ana = anas_tab.presence();
    // Ana holds the pad of the order producers, and Dan the pad of the fetches.
    hold(ana, slow_producers).await?;
    hold(dans_tab.presence(), slow_fetches).await?;

    // The streams of both end while they hold their pads.
    anas_tab.close().await;
    dans_tab.close().await;
    // Ana's tab comes back before the grace period is over, as the presence it was. Dan's tab stays away.
    clock.advance(grace.duration() / 2);
    let anas_tab = client.live_stream_as(ana).await?;
    assert_eq!(anas_tab.presence(), ana);

    // The grace period of both streams is over. Dan's presence ends, and the pad he held disarms.
    clock.advance(grace.duration());
    cleos_tab
        .next_where(|message| {
            matches!(
                message,
                StreamMessage::RuleChanged { rule: status, .. }
                    if status.rule.name() == &dans_rule && status.state == RuleState::Disarmed
            )
        })
        .await?;
    // Ana's presence went on, and her hold with it: her pad stays armed.
    let still_held = host.rule_of(staging.name(), &anas_rule).await?;
    assert_eq!(still_held.map(|rule| rule.state), Some(RuleState::Armed));

    // The control plane stops while Ana holds her pad. No client comes back to a control plane that stopped,
    // so her presence ends at once, and the host that lives on holds the pad no more.
    server.stop().await;
    let after_the_stop = host.rule_of(staging.name(), &anas_rule).await?;
    assert_eq!(after_the_stop.map(|rule| rule.state), Some(RuleState::Disarmed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_client_that_goes_silent_loses_its_holds_after_the_grace_period() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    // A desk with one pad: a delay of 1 s on the order producers, for every client.
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let delay = Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)));
    let desk = PadLayout::new(
        "order-desk".parse()?,
        [order_producers.name().clone()],
        [delay.name().clone()],
        [Layer::all_clients()],
    )?;
    host.create(staging.clone(), ChangeSurface::Test).await?;
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let (ping_interval, grace) = (configuration.ping_interval.span(), configuration.presence_grace);
    // The pings and the grace period of a presence run on the clock of the server, which the test steps.
    let clock = SteppedClock::new();
    let server = ControlServer::start_on(configuration, Arc::clone(&host), clock.clone()).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let rule = desk.rule_name(&pad).context("the pad points at a rule by its name")?;
    let mut changes = host.changes();

    // Ana's tab is a WebSocket that the test reads frame by frame, so the test decides what the tab answers.
    // Its first frame names its presence.
    let (mut anas_tab, _) = tokio_tungstenite::connect_async(format!("ws://{}/api/stream", server.address())).await?;
    let first = tokio::time::timeout(PATIENCE, anas_tab.next())
        .await?
        .context("the stream names the presence of its client")??;
    let StreamMessage::Present { presence } = serde_json::from_str(first.to_text()?)? else {
        anyhow::bail!("the stream started with another message than the presence: {first}");
    };
    // Ana holds the pad, as that presence.
    let held = client
        .send_from(
            RequestSource::new(RequestSurface::Pad).with_presence(presence),
            SendIntent {
                environment: staging.name().clone(),
                intent: Intent::Hold { pad },
            },
        )
        .await?;
    assert!(held.hold.is_some(), "{held:?}");

    // The server asks every client of the stream whether it is still there, and its ping reaches the tab.
    clock.advance(ping_interval.duration());
    let pinged = async {
        loop {
            let frame = anas_tab.next().await.context("the stream goes on")??;
            if frame.is_ping() {
                return Ok::<_, anyhow::Error>(());
            }
        }
    };
    tokio::time::timeout(PATIENCE, pinged).await??;
    // Ana's network drops with the pad down. Her tab answers nothing from here on, and closes nothing:
    // the test reads the tab no more, so the answer to the ping never leaves it.
    // The next ping is due, and the tab is silent. A grace period later its presence ends, and its hold with it.
    clock.advance(ping_interval.duration() + grace.duration());
    let disarmed = async {
        loop {
            changes.next().await?;
            let of_the_pad = host.rule_of(staging.name(), &rule).await?;
            if of_the_pad.map(|rule| rule.state) == Some(RuleState::Disarmed) {
                return Ok::<_, anyhow::Error>(());
            }
        }
    };
    tokio::time::timeout(PATIENCE, disarmed).await??;
    drop(anas_tab);
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn the_journal_of_an_environment_says_what_each_request_changed_and_where_it_came_from() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    // A client that says nothing about itself: its requests come from the surface `api`.
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let query = |after| QueryJournal {
        environment: staging.name().clone(),
        after,
    };
    // The journal begins with its environment, and a start is no entry of it.
    assert_eq!(client.send(query(None)).await?, Vec::<JournalEntry>::new());

    // The client creates a rule in the workspace. Someone arms it at a pad, and takes that back with ⌘Z in the UI.
    let created = client
        .send(CreateDefinition {
            definition: hold.clone(),
        })
        .await?;
    let pad = RequestSource::new(RequestSurface::Pad);
    let arm = ArmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects: None,
    };
    let armed = client.send_from(pad, arm).await?;
    let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
    let disarm = DisarmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects: None,
    };
    let disarmed = client.send_from(undo, disarm).await?;

    // The journal holds an entry for each, with the revision that the request answered with and the surface and
    // the cause that the request named. The undo took nothing out: it is one more entry.
    let journal = client.send(query(None)).await?;
    let rule: RecordedName = hold.name().as_str().parse()?;
    let definition = RecordedDefinition::Rule(rule.clone());
    let api = ChangeSource::new(ChangeSurface::Api);
    let said: Vec<(u64, Option<Revision>, ChangeSource, Change)> = journal
        .iter()
        .map(|entry| (entry.id.value(), entry.revision, entry.from, entry.change.clone()))
        .collect();
    assert_eq!(
        said,
        [
            (1, Some(created.revision), api, Change::DefinitionCreated { definition }),
            (
                2,
                Some(armed.revision),
                pad.into(),
                Change::RuleArmed { rule: rule.clone() }
            ),
            (3, Some(disarmed.revision), undo.into(), Change::RuleDisarmed { rule }),
        ]
    );
    // A client that read up to the arming reads on from there.
    let the_arming = journal[1].id;
    assert_eq!(client.send(query(Some(the_arming))).await?, &journal[2..]);

    // Every entry also came as a message of the live stream, in the order of the entries.
    for entry in &journal {
        let message = stream
            .next_where(|message| matches!(message, StreamMessage::JournalEntry { .. }))
            .await?;
        assert_eq!(
            message,
            StreamMessage::JournalEntry {
                environment: staging.name().clone(),
                entry: entry.clone(),
            }
        );
    }

    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_undo_that_arms_a_rule_again_is_an_entry_of_the_journal_with_its_cause() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    host.create(hold.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    // A tab of the UI: its requests come from the surface `ui`.
    let mut tab = ControlClientConfiguration::new(server.address().into());
    tab.surface = RequestSurface::Ui;
    let client = ControlClient::new(tab);
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let arm = || ArmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects: None,
    };
    let disarm = DisarmRule {
        environment: staging.name().clone(),
        rule: hold.name().clone(),
        expects: None,
    };

    // Someone arms the rule and disarms it. ⌘Z takes the disarm back: its inverse is an arm, sent with its cause.
    client.send(arm()).await?;
    client.send(disarm).await?;
    let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
    let armed_again = client.send_from(undo, arm()).await?;

    // The undo is the third entry of the journal, next to the two changes before it. It is an arming like the
    // first one, and its entry says that it took an earlier change back.
    let journal = client
        .send(QueryJournal {
            environment: staging.name().clone(),
            after: None,
        })
        .await?;
    let rule: RecordedName = hold.name().as_str().parse()?;
    let ui = ChangeSource::new(ChangeSurface::Ui);
    let said: Vec<(u64, ChangeSource, Change)> = journal
        .iter()
        .map(|entry| (entry.id.value(), entry.from, entry.change.clone()))
        .collect();
    assert_eq!(
        said,
        [
            (1, ui, Change::RuleArmed { rule: rule.clone() }),
            (2, ui, Change::RuleDisarmed { rule: rule.clone() }),
            (3, ui.with_cause(ChangeCause::Undo), Change::RuleArmed { rule }),
        ]
    );
    assert_eq!(journal[2].revision, Some(armed_again.revision));
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the test follows one recording from its start to the query of what it holds"
)]
async fn a_recording_started_through_the_control_plane_keeps_the_calls_until_it_stops_and_answers_a_query()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let StreamMessage::Snapshot(_) = stream.next_change().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let StreamMessage::EnvironmentStarted { .. } = stream.next_change().await? else {
        anyhow::bail!("the start of the environment sent no message");
    };
    let produces = Subscription::Calls {
        environment: staging.name().clone(),
        selector: SelectorRef::Inline(Conditions::from(Condition::is(ApiKey::PRODUCE))),
    };
    stream.stream.follow(produces.clone()).await;
    assert_eq!(
        stream.next_change().await?,
        StreamMessage::Following { subscription: produces }
    );

    let recording = client
        .send(StartRecording {
            environment: staging.name().clone(),
            mode: None,
        })
        .await?;
    let started = stream.next_change().await?;
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    KafkaProducer::from(bootstrap)
        .send(orders.name().as_str(), Partition::new(0), &[Record::new("placed")])
        .await?;
    // The call reaches the stream once the proxy handed its record over, to the recording too.
    stream
        .next_where(|message| matches!(message, StreamMessage::Call { .. }))
        .await?;
    let end = client
        .send(StopRecording {
            environment: staging.name().clone(),
        })
        .await?;
    let stopped = stream.next_change().await?;

    assert_eq!(
        started,
        StreamMessage::RecordingStarted {
            environment: staging.name().clone(),
            recording: recording.clone(),
        }
    );
    assert_eq!(
        stopped,
        StreamMessage::RecordingStopped {
            environment: staging.name().clone(),
            recording: recording.clone(),
            why: None,
        }
    );
    assert_eq!(end.ending, RecordingEnding::Stopped);
    assert!(end.calls >= 1, "the recording holds the Produce: {end:?}");
    assert_eq!(client.send(ListRecordings {}).await?, std::slice::from_ref(&recording));
    let opened = client
        .send(OpenRecording {
            recording: recording.clone(),
        })
        .await?;
    assert_eq!(opened.description.recording, recording);
    assert_eq!(opened.end, Some(end));
    // The recording answers a query with the Produce, and its index groups it under its topic.
    let every_produce = SelectorRef::Inline(Conditions::from(Condition::is(ApiKey::PRODUCE)));
    let queried = client
        .send(QueryRecording {
            recording: recording.clone(),
            selector: every_produce.clone(),
            draft: None,
            skip: 0,
        })
        .await?;
    assert_eq!(queried.matched, 1);
    let indexed = client
        .send(GetRecordingIndex {
            recording,
            filter: every_produce,
        })
        .await?;
    let topics = indexed.facets.iter().find(|facet| facet.field == Field::Topic);
    let counted: Option<Vec<(String, u64)>> = topics.map(|topics| {
        topics
            .values
            .iter()
            .map(|counted| (counted.value.to_string(), counted.calls))
            .collect()
    });
    assert_eq!(counted, Some(vec![(orders.name().to_string(), 1)]));
    server.stop().await;
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_capture_through_the_control_plane_answers_with_its_steps_and_saves_them_as_a_scenario() -> anyhow::Result<()>
{
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN.parse()?,
    ));
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::any_port(LOCALHOST);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.create(staging.clone(), ChangeSurface::Test).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    configuration.stats_interval = StatsInterval::new(QUIET)?;
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let mut stream = Watched {
        stream: client.live_stream().await?,
    };
    let slow = Rule::new(
        "slow-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );

    // Someone captures while they arm a live rule.
    client
        .send(StartCapture {
            environment: staging.name().clone(),
        })
        .await?;
    client
        .send(CreateLiveRule {
            environment: staging.name().clone(),
            rule: slow.clone(),
            armed: true,
        })
        .await?;
    let captured = client
        .send(StopCapture {
            environment: staging.name().clone(),
        })
        .await?;
    let saved = client
        .send(SaveCapture {
            environment: staging.name().clone(),
            scenario: "slow-produce-captured".parse()?,
            title: None,
        })
        .await?;

    let arms: Vec<Vec<String>> = captured
        .steps
        .iter()
        .map(|step| step.arm().iter().map(|rule| rule.name().to_string()).collect())
        .collect();
    assert_eq!(arms, [vec![], vec![slow.name().to_string()]]);
    assert_eq!(captured.rules, std::slice::from_ref(&slow));
    // Every client learns of the start and the stop from the journal of the environment.
    let is_capture_entry = |message: &StreamMessage| {
        matches!(
            message,
            StreamMessage::JournalEntry { entry, .. }
                if matches!(entry.change, Change::CaptureStarted | Change::CaptureStopped)
        )
    };
    for change in [Change::CaptureStarted, Change::CaptureStopped] {
        let StreamMessage::JournalEntry { entry, .. } = stream.next_where(is_capture_entry).await? else {
            anyhow::bail!("the stream passed over the entry of the capture");
        };
        assert_eq!(entry.change, change);
        assert_eq!(entry.from, ChangeSource::new(ChangeSurface::Api));
    }
    // The scenario and the live rule that it uses are in the workspace.
    let scenario = client
        .send(GetDefinition::<gaslightmq_control_api::scenario::Scenario> {
            name: "slow-produce-captured".parse()?,
        })
        .await?;
    assert_eq!(scenario.revision, saved.revision);
    assert_eq!(scenario.definition.steps(), &captured.steps[..]);
    let kept = client
        .send(GetDefinition::<Rule> {
            name: slow.name().clone(),
        })
        .await?;
    assert_eq!(kept.definition, slow);
    server.stop().await;
    host.stop_all().await;
    Ok(())
}
