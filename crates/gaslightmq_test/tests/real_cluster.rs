//! A test of a Kafka client starts GaslightMQ in its own process, in front of a real cluster, and arms a rule through
//! the control plane that makes the client's produce fail, or activates a scenario that plays a failure out over time.
//!
//! This is how a user of GaslightMQ writes such a test.
//! The cluster is the one of `gaslightmq_test_kafka`, so the tests need Docker.
//! The client under test is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address of
//! the proxy and nothing else.

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
use gaslightmq_control_api::lie::RefusalLie;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::Rule;
use gaslightmq_control_api::scenario::{Mode, Scenario, ScenarioPosition, Step, Until};
use gaslightmq_control_api::selector::{Condition, Conditions};
use gaslightmq_control_api::trigger::{Count, Seed};
use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kafka_client_native::KafkaProducer;
use gaslightmq_test::GaslightMq;
use gaslightmq_test_kafka::{KafkaCluster, NewTopic};
use gaslightmq_wire::{Offset, Partition, Record, ResponseError};

/// When the orders of these tests were placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

/// How many Produce calls the first step of a scenario refuses before the scenario moves on to its next step.
const REFUSED_PRODUCES: Count = Count::ONE;

#[tokio::test]
async fn a_rule_armed_through_the_control_plane_refuses_the_produce_of_a_real_client() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let producer = KafkaProducer::from(gaslight.bootstrap());
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);

    // The proxy refuses every Produce: the client may not write to the topic.
    let refuse = Rule::new(
        "refuse-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
    );
    gaslight
        .control()
        .send(CreateLiveRule {
            environment: gaslight.environment().clone(),
            rule: refuse.clone(),
            armed: true,
        })
        .await??;
    let refused = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    // Once the rule is disarmed, the same client writes the record.
    gaslight
        .control()
        .send(DisarmRule {
            environment: gaslight.environment().clone(),
            rule: refuse.name().clone(),
            expects: None,
        })
        .await??;
    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::TopicAuthorizationFailed
        })
    );
    assert_eq!(written, Ok(Offset::new(0)));
    // The broker holds the record once: the refused Produce never reached it.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_proxy_in_front_of_several_brokers_runs_in_the_run_that_the_test_names() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster
        .topics()
        .create(NewTopic::named("orders").with_partitions(3))
        .await?;
    // The proxy gives each broker that it finds in the cluster a listener of its own: the test names none.
    let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
    let gaslight = GaslightMq::proxy_in_front_of(cluster.brokers()[0].address().to_string().parse()?)
        .with_run(run)
        .start()
        .await?;
    let producer = KafkaProducer::from(gaslight.bootstrap());

    // The control plane knows the environment of the proxy, which runs, in the run of the test.
    let mut stream = gaslight.control().live_stream().await?;
    let StreamMessage::Snapshot(state) = stream.next().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    // Each partition has its leader on another broker, and the client writes to each through the proxy.
    let mut written = Vec::new();
    for partition in 0..orders.partitions() {
        let placed = Record::new("placed").with_key(format!("o-{partition}"));
        written.push(
            producer
                .send(orders.name().as_str(), Partition::from(partition), &[placed])
                .await,
        );
    }

    assert_eq!(state.run, run);
    let running: Vec<&EnvironmentName> = state.environments.iter().map(|running| &running.environment).collect();
    assert_eq!(running, [gaslight.environment()]);
    assert_eq!(written, [Ok(Offset::new(0)), Ok(Offset::new(0)), Ok(Offset::new(0))]);
    stream.close().await;
    gaslight.stop().await;
    Ok(())
}

#[tokio::test]
async fn a_scenario_refuses_the_first_produce_of_a_real_client_and_then_lets_it_write() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let gaslight = GaslightMq::proxy_in_front_of(cluster.bootstrap_servers().parse()?)
        .start()
        .await?;
    let producer = KafkaProducer::from(gaslight.bootstrap());
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);
    // The first step refuses one Produce, and ends with it. The step after it lets every call through.
    let refuse = Rule::new(
        "refuse-order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
    );
    let refuse_once = Step::new("refuse-once".parse()?)
        .arming([refuse.name().clone()])
        .ending_on(Until::Firings {
            count: REFUSED_PRODUCES,
            rule: refuse.name().clone(),
        });
    let back_to_normal = Step::new("back-to-normal".parse()?);
    let scenario = Scenario::new(
        "one-refused-produce".parse()?,
        Mode::Sequence(vec![refuse_once, back_to_normal.clone()]),
        Seed::new(7),
        [],
    )?;
    let control = gaslight.control();
    control.send(CreateDefinition { definition: refuse }).await??;
    control
        .send(CreateDefinition {
            definition: scenario.clone(),
        })
        .await??;
    control
        .send(ActivateScenario {
            environment: gaslight.environment().clone(),
            scenario: scenario.name().clone(),
            expects: None,
        })
        .await??;

    let refused = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;
    let written = producer
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;

    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::TopicAuthorizationFailed
        })
    );
    assert_eq!(written, Ok(Offset::new(0)));
    // The refusal spent the firing of the first step, so the scenario stands at the step after it.
    let mut stream = control.live_stream().await?;
    let StreamMessage::Snapshot(state) = stream.next().await? else {
        anyhow::bail!("the stream started with another message than the snapshot");
    };
    let running = state
        .environments
        .first()
        .map(|running| running.scenario.active.clone());
    assert_eq!(
        running,
        Some(Some(ScenarioPosition {
            scenario: scenario.name().clone(),
            step: Some(back_to_normal.name().clone()),
        }))
    );
    stream.close().await;
    gaslight.stop().await;
    Ok(())
}
