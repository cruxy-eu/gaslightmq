//! The UI in front of live traffic: a proxy in front of a real Kafka cluster, clients that keep it busy,
//! and the control plane with the UI on `http://127.0.0.1:7092`. It prints the address of the proxy for a Kafka
//! client, and the bootstrap address of the cluster itself, which bypasses the proxy.
//!
//! `just demo` runs it. It needs Docker for the cluster, and `just ui` for the files of the UI.
//! An address as the first argument puts the control plane there instead: `just demo 127.0.0.1:8081`.
//! The workspace lives in memory and holds the order desk of the specs: selectors, presets, a rule and a pad layout.
//! Ctrl-C stops everything, the cluster with it.

#![expect(
    clippy::print_stdout,
    reason = "the demo tells whoever runs it where the UI and the proxy are"
)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, Instances, ListenAddress, Listeners, ProxyDefinition, Upstream,
};
use gaslightmq_control_api::field::{Endpoint, Field};
use gaslightmq_control_api::kernel::{ApiKey, ClientId, ErrorCode};
use gaslightmq_control_api::lie::{HangUpLie, LieDefinition, RefusalLie, TimingLie};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule};
use gaslightmq_control_api::selector::{Condition, Conditions, Selector};
use gaslightmq_control_api::surface::{Layer, PadLayout};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration};
use gaslightmq_control_server::prelude::*;
use gaslightmq_host::{Host, HostConfiguration};
use gaslightmq_kafka_client_native::{ConsumerConfiguration, KafkaConsumer, KafkaProducer, ProducerConfiguration};
use gaslightmq_lie_api::prelude::*;
use gaslightmq_test_kafka::{KafkaCluster, NewTopic};
use gaslightmq_wire::{Offset, Partition, Record};
use gaslightmq_workspace::prelude::*;

/// Where the proxy listens, and where its clients connect.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The topics of the order desk, each with its partitions.
const TOPICS: [(&str, u32); 3] = [("orders.eu", 6), ("orders.us", 3), ("payments.settled", 3)];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting a Kafka cluster of three brokers in Docker…");
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    for (topic, partitions) in TOPICS {
        cluster
            .topics()
            .create(NewTopic::named(topic).with_partitions(partitions))
            .await?;
    }
    // A process reads the randomness of the system once, for the ID of its run. A tab of the UI that still holds
    // what an earlier demo told it sees another run in the snapshot, and drops it.
    let mut random = [0; 16];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("the system gave no randomness for the ID of the run: {error}"))?;
    let run = gaslightmq_host::RunId::from_random_bits(u128::from_be_bytes(random));
    let host = Arc::new(Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        run,
    ));
    // Every listener of the proxy takes a free port, which the UI shows under Endpoints.
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    )
    .with_title("Staging proxy".parse()?);
    host.create(staging.clone()).await?;
    let mut configuration = ControlServerConfiguration::default();
    if let Some(listen) = std::env::args().nth(1) {
        configuration.listen = listen.parse()?;
    }
    let server = ControlServer::start(configuration, Arc::clone(&host)).await?;
    let client = ControlClient::new(ControlClientConfiguration::new(server.address().into()));
    order_desk(&client).await?;
    client
        .send(StartEnvironment {
            environment: staging.name().clone(),
        })
        .await??;
    let backend = client
        .send(GetBackend {
            environment: staging.name().clone(),
        })
        .await??;
    let bootstrap = backend
        .endpoints
        .get(&Endpoint::Bootstrap)
        .copied()
        .flatten()
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let bootstrap = SocketAddr::from(bootstrap);
    produce(
        bootstrap,
        "order-ingest",
        &["orders.eu", "orders.us"],
        Duration::from_millis(120),
    );
    produce(
        bootstrap,
        "payments-svc",
        &["payments.settled"],
        Duration::from_millis(400),
    );
    consume(bootstrap, "analytics-etl", "orders.eu", Duration::from_millis(300));

    println!("The UI:                       http://{}", server.address());
    println!("The proxy, for a Kafka client: {bootstrap}");
    println!(
        "The cluster, past the proxy:   {}  (kcat -b {} -L)",
        cluster.brokers()[0].address(),
        cluster.brokers()[0].address()
    );
    println!("Ctrl-C stops the demo.");
    tokio::signal::ctrl_c().await?;
    server.stop().await;
    host.stop_all().await;
    drop(cluster);
    Ok(())
}

/// The definitions of the order desk: selectors for the rows, presets for the columns, one rule and the layout.
async fn order_desk(client: &ControlClient) -> anyhow::Result<()> {
    let selectors = [
        Selector::new(
            "order-producers".parse()?,
            Conditions::new()
                .with(Condition::is(ApiKey::PRODUCE))
                .with(Condition::parse(Field::Topic, "orders.*")?),
        )
        .with_title("Order producers".parse()?),
        Selector::new("fetches".parse()?, Conditions::from(Condition::is(ApiKey::FETCH))).with_title("Fetch".parse()?),
        Selector::new("metadata".parse()?, Conditions::from(Condition::is(ApiKey::METADATA)))
            .with_title("Metadata".parse()?),
        Selector::new(
            "payment-producers".parse()?,
            Conditions::new()
                .with(Condition::is(ApiKey::PRODUCE))
                .with(Condition::parse(Field::Topic, "payments.*")?),
        )
        .with_title("Payment producers".parse()?),
    ];
    let presets = [
        Preset::new(
            "error-retriable".parse()?,
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        )
        .with_title("Error retriable".parse()?),
        Preset::new(
            "error-fatal".parse()?,
            RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
        )
        .with_title("Error fatal".parse()?),
        Preset::new(
            "delay-100ms".parse()?,
            TimingLie::new().with_delay(TimeSpan::from_millis(100)),
        )
        .with_title("Delay 100 ms".parse()?),
        Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)))
            .with_title("Delay 1 s".parse()?),
        Preset::new("blackhole".parse()?, LieDefinition::Silence).with_title("Blackhole".parse()?),
        Preset::new("disconnect".parse()?, HangUpLie::new()).with_title("Disconnect".parse()?),
        Preset::new(
            "throttle".parse()?,
            TimingLie::new().with_throttle(TimeSpan::from_secs(2)),
        )
        .with_title("Throttle".parse()?),
    ];
    let rule = Rule::new(
        "slow-order-producers".parse()?,
        selectors[0].name().clone(),
        presets[3].name().clone(),
    )
    .with_title("Slow down order producers".parse()?);
    let layout = PadLayout::new(
        "order-desk".parse()?,
        selectors.iter().map(|selector| selector.name().clone()),
        presets.iter().map(|preset| preset.name().clone()),
        [
            Layer::all_clients(),
            "order-ingest".parse()?,
            "payments-svc".parse()?,
            "analytics-etl".parse()?,
        ],
    )?
    .with_title("Order desk".parse()?);
    for definition in selectors {
        client.send(CreateDefinition { definition }).await??;
    }
    for definition in presets {
        client.send(CreateDefinition { definition }).await??;
    }
    client.send(CreateDefinition { definition: rule }).await??;
    client.send(CreateDefinition { definition: layout }).await??;
    Ok(())
}

/// A producer with the client ID `client` that writes a record to the topics in turn, every `interval`.
/// A call that a lie refuses or delays only slows it down.
fn produce(bootstrap: SocketAddr, client: &str, topics: &'static [&'static str], interval: Duration) {
    let mut configuration = ProducerConfiguration::new(bootstrap);
    configuration.client.client_id = ClientId::new(client).expect("a client ID");
    let producer = KafkaProducer::new(configuration);
    tokio::spawn(async move {
        let mut count = 0_u64;
        loop {
            let topic = topics[usize::try_from(count).unwrap_or(0) % topics.len()];
            let partition = Partition::new(u32::try_from(count % 3).unwrap_or(0));
            let record = Record::new(format!("order {count}")).with_key(format!("o-{count}"));
            let _ = producer.send(topic, partition, &[record]).await;
            tokio::time::sleep(interval).await;
            count = count.wrapping_add(1);
        }
    });
}

/// A consumer with the client ID `client` that reads the first partition of `topic` from where it got, every
/// `interval`.
fn consume(bootstrap: SocketAddr, client: &str, topic: &'static str, interval: Duration) {
    let mut configuration = ConsumerConfiguration::new(bootstrap);
    configuration.client.client_id = ClientId::new(client).expect("a client ID");
    let consumer = KafkaConsumer::new(configuration);
    tokio::spawn(async move {
        let mut offset = Offset::new(0);
        loop {
            if let Ok(records) = consumer.fetch(topic, Partition::new(0), offset).await {
                let read = u64::try_from(records.len()).unwrap_or(0);
                offset = Offset::new(offset.value() + read);
            }
            tokio::time::sleep(interval).await;
        }
    });
}
