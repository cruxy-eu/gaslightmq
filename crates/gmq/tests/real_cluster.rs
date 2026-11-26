//! `gmq` as whoever runs it: the binary in a process of its own, in front of a real cluster, with a real client in
//! front of it.
//!
//! The cluster is the one of `gaslightmq_test_kafka`, so the tests need Docker.
//! The client is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address of the proxy
//! and nothing else.
//! Each test reads what `gmq proxy` prints: where it listens, and `ready` once it accepts connections.

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;

use anyhow::Context;
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::RuleName;
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, ControlPlaneAddress};
use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kafka_client_native::{KafkaProducer, ProducerConfiguration};
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::{Offset, Partition, Record, ResponseError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

/// The binary under test.
const GMQ: &str = env!("CARGO_BIN_EXE_gmq");

/// Example 09: a rule that loses one in ten acknowledgements of the order topics.
const EXAMPLE_09: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/09-proxy-in-front-of-real-kafka"
);

/// The rule of example 09: the real cluster writes the records, and the client is told that the Produce timed out.
const LOST_ACKNOWLEDGEMENTS: &str = "order-acknowledgements-get-lost";

/// When the orders of these tests were placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

#[tokio::test]
async fn a_proxy_of_the_command_line_tells_a_real_client_that_a_record_it_wrote_was_lost() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders.eu").await?;
    let broker = cluster.brokers()[0].id();
    // The proxy reads the files of example 09, and never writes them. Every listener takes a free port.
    let mut gmq = Command::new(GMQ)
        .args(["proxy", "--upstream", &cluster.bootstrap_servers()])
        .args(["--listen", "127.0.0.1:0", "--brokers", &format!("{broker}=127.0.0.1:0")])
        .args(["--advertise", "127.0.0.1", "--control-listen", "127.0.0.1:0"])
        .args(["--workspace", EXAMPLE_09, "--read-only"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    // `gmq` prints where the proxy and the control plane listen, and `ready` last.
    let mut lines = BufReader::new(gmq.stdout.take().context("the output of gmq")?).lines();
    let (mut bootstrap, mut control) = (None, None);
    while let Some(line) = lines.next_line().await? {
        if let Some(address) = line.strip_prefix("bootstrap on ") {
            bootstrap = Some(address.parse::<SocketAddr>()?);
        }
        if let Some(address) = line.strip_prefix("control plane and UI on ") {
            control = Some(address.parse::<ControlPlaneAddress>()?);
        }
        if line.starts_with("ready") {
            break;
        }
    }
    let bootstrap = bootstrap.context("gmq printed where the proxy listens")?;
    let mut configuration = ControlClientConfiguration::new(control.context("gmq printed its control plane")?);
    configuration.surface = RequestSurface::Test;
    let control = ControlClient::new(configuration);

    // The rule of the workspace loses one in ten acknowledgements. A firing skips the chance for the next Produce.
    let proxy: EnvironmentName = "proxy".parse()?;
    let lost: RuleName = LOST_ACKNOWLEDGEMENTS.parse()?;
    control
        .send(ArmRule {
            environment: proxy.clone(),
            rule: lost.clone(),
            expects: None,
        })
        .await??;
    control
        .send(FireRule {
            environment: proxy,
            rule: lost,
        })
        .await??;
    // The producer does not try again, so it reports the first answer it gets.
    let mut producer = ProducerConfiguration::new(bootstrap);
    producer.retries = 0;
    let producer = KafkaProducer::new(producer);
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);
    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    assert_eq!(
        written,
        Err(ClientError::Refused {
            error: ResponseError::RequestTimedOut
        })
    );
    // The real cluster holds the record that the client was told it lost.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    gmq.kill().await?;
    Ok(())
}

#[tokio::test]
async fn a_reload_of_the_command_line_takes_in_a_rule_that_someone_edited() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let broker = cluster.brokers()[0].id();
    // A workspace of its own, with the rules of example 09, which the test edits.
    let workspace = tempfile::tempdir()?;
    let rules = workspace.path().join("rules");
    std::fs::create_dir(&rules)?;
    let orders = Path::new(EXAMPLE_09).join("rules/orders.yaml");
    std::fs::copy(&orders, rules.join("orders.yaml"))?;
    let mut gmq = Command::new(GMQ)
        .args(["proxy", "--upstream", &cluster.bootstrap_servers()])
        .args(["--listen", "127.0.0.1:0", "--brokers", &format!("{broker}=127.0.0.1:0")])
        .args(["--advertise", "127.0.0.1", "--control-listen", "127.0.0.1:0"])
        .arg("--workspace")
        .arg(workspace.path())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(gmq.stdout.take().context("the output of gmq")?).lines();
    let mut control = None;
    while let Some(line) = lines.next_line().await? {
        if let Some(address) = line.strip_prefix("control plane and UI on ") {
            control = Some(address.to_owned());
        }
        if line.starts_with("ready") {
            break;
        }
    }
    let control = control.context("gmq printed its control plane")?;

    // Someone loses two in ten acknowledgements from now on, while the process runs.
    let edited = std::fs::read_to_string(&orders)?.replace("chance: 10%", "chance: 20%");
    std::fs::write(rules.join("orders.yaml"), edited)?;
    let reload = Command::new(GMQ)
        .args(["reload", "--control", &control])
        .output()
        .await?;

    let printed = String::from_utf8(reload.stdout)?;
    let printed: Vec<&str> = printed.lines().collect();
    assert!(reload.status.success(), "{}", String::from_utf8_lossy(&reload.stderr));
    assert_eq!(printed[0], format!("rule {LOST_ACKNOWLEDGEMENTS:?} changed"));
    assert!(printed[1].starts_with("reloaded at revision "), "{printed:?}");
    assert_eq!(printed.len(), 2);
    gmq.kill().await?;
    Ok(())
}
