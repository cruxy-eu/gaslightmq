//! The control plane starts an environment of the workspace in front of real Kafka brokers, and streams its calls.
//!
//! The test needs Docker.
//! It starts the cluster it needs, a host whose workspace lives in memory, and a server in front of the host,
//! and drives the server through the client as the UI does.
//! The Kafka client is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address that the
//! control plane names, and nothing else.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use gaslightmq_control_api::clock::Duration;
use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, Instances, ListenAddress, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::{Endpoint, Field, FieldSource};
use gaslightmq_control_api::kernel::ApiKey;
use gaslightmq_control_api::lie::TimingLie;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleState};
use gaslightmq_control_api::selector::{Condition, Conditions, SelectorRef};
use gaslightmq_control_api::trigger::Trigger;
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_kafka_client_native::KafkaProducer;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::{Offset, Partition, Record};
use gaslightmq_workspace::prelude::*;

/// How long a test waits for a message of the live stream that the server has a reason to send.
const PATIENCE: Duration = Duration::from_secs(30);

/// The address that the listeners and the server of the test listen on, and that its clients connect to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

#[tokio::test]
async fn an_environment_started_by_its_name_streams_its_calls_until_it_stops() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    ));
    // The environment stands in front of the cluster, and every listener of it takes a free port.
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.workspace().await.create(staging.clone()).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    let mut stream = client.live_stream().await?;
    // The snapshot knows the environment, and nothing runs yet.
    let StreamMessage::Snapshot(state) = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    assert_eq!(state.definitions.environments, std::slice::from_ref(&staging));
    assert_eq!(state.environments, []);

    let started = client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let StreamMessage::EnvironmentStarted { environment } = tokio::time::timeout(PATIENCE, stream.next()).await??
    else {
        anyhow::bail!("the start of the environment sent no message");
    };
    let backend = client
        .send(GetBackend {
            environment: staging.name().clone(),
        })
        .await??;

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
    stream.follow(produces.clone()).await;
    let following = tokio::time::timeout(PATIENCE, stream.next()).await??;
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
    let StreamMessage::Call { environment, call } = tokio::time::timeout(PATIENCE, stream.next()).await?? else {
        anyhow::bail!("the stream carried another message than the call of the record");
    };
    assert_eq!(written, Ok(Offset::new(0)));
    assert_eq!(&environment, staging.name());
    let api = call.fields().read(Field::Api).present().context("a call has an API")?;
    assert!(api.contains(&ApiKey::PRODUCE.into()), "{api:?}");

    // The environment stops, and the stream says so.
    let stopped = client
        .send(StopEnvironment {
            environment: staging.name().clone(),
        })
        .await?;
    let gone = tokio::time::timeout(PATIENCE, stream.next()).await??;
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
    ));
    // The environment stands in front of the cluster, and every listener of it takes a free port.
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.workspace().await.create(staging.clone()).await?;
    let mut configuration = ControlServerConfiguration::default();
    configuration.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    // A pad that holds the next Produce a moment, once someone presses it.
    let hold = Rule::new(
        "hold-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay("20ms".parse()?),
    )
    .with_trigger(Trigger::Manual);
    assert_eq!(
        client
            .send(CreateDefinition {
                definition: hold.clone()
            })
            .await?,
        Ok(())
    );
    let disarmed = RuleStatus {
        rule: hold.clone(),
        live: false,
        state: RuleState::Disarmed,
    };
    let armed = RuleStatus {
        state: RuleState::Armed,
        ..disarmed.clone()
    };

    // The stream opens once the workspace has the rule, so its snapshot holds it.
    let mut stream = client.live_stream().await?;
    let first = tokio::time::timeout(PATIENCE, stream.next()).await??;
    assert!(matches!(first, StreamMessage::Snapshot(_)), "{first:?}");
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await??;
    let rules = client
        .send(ListEnvironmentRules {
            environment: staging.name().clone(),
        })
        .await?;

    let arming = client
        .send(ArmRule {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
        })
        .await?;
    let firing = client
        .send(FireRule {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
        })
        .await?;
    let again = client
        .send(ArmRule {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
        })
        .await?;

    assert_eq!(rules, Ok(vec![disarmed]));
    assert_eq!(arming, Ok(()));
    assert_eq!(firing, Ok(()));
    assert_eq!(
        again,
        Err(RuleCommandError::Refused {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
            reason: "it is armed already".to_owned(),
        })
    );
    // The stream heard of the start, and of the arming.
    let mut heard = Vec::new();
    while heard.len() < 2 {
        heard.push(tokio::time::timeout(PATIENCE, stream.next()).await??);
    }
    assert_eq!(
        heard[1],
        StreamMessage::RuleChanged {
            environment: staging.name().clone(),
            rule: armed,
        }
    );
    server.stop().await;
    host.stop_all().await;
    Ok(())
}
