//! A host runs several environments in front of real Kafka brokers, each with a proxy of its own,
//! makes a live rule armed in one step, reads a ⌘K sentence with the names that the traffic of one of them has
//! seen, acts on the presses and the holds of a pad, and keeps a live rule only under a name that no other one has.
//!
//! The tests need Docker.
//! Each test starts the cluster it needs, and a host whose workspace lives in memory.
//! The environments of a test stand in front of that cluster, and every listener of theirs takes a free port,
//! unless the test names one.
//! The client is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address of one
//! environment and nothing else.

use std::net::{IpAddr, Ipv4Addr, TcpListener};

use anyhow::Context;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_engine::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kafka_client_native::{KafkaProducer, ProducerConfiguration};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_quick_inject::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::{Column, Intent, Layer, PadLayout, Row};
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::{Offset, Partition, Record};
use gaslightmq_workspace::prelude::*;

/// How long a test waits for the record of a call that the client already got its answer for.
const PATIENCE: Duration = Duration::from_secs(30);

/// When the orders of these tests were placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

/// The address that the listeners of a test listen on, and that its clients connect to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

#[tokio::test]
async fn a_rule_armed_in_one_environment_leaves_the_other_alone() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build().with(Refusal)?,
    );
    // Two environments stand in front of the same cluster. Every listener of both takes a free port.
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let listen = Listeners::new(any, brokers, LOCALHOST.into())?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen.clone()),
            upstream: upstream.clone(),
        },
    );
    let canary = Environment::new(
        "canary-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen),
            upstream,
        },
    );
    host.create(staging.clone()).await?;
    host.create(canary.clone()).await?;
    // The workspace has a preset that both environments can use.
    let leader_flap = Preset::new(
        "leader-flap".parse()?,
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    host.create(leader_flap.clone()).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    // Someone arms a rule in the canary alone: "tell the 1st Produce that its broker no longer leads the partition".
    let flap = Rule::new(
        "leader-flap-on-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        leader_flap.name().clone(),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1)?));
    host.arm(canary.name(), &flap).await?;
    let mut staging_events = host.live_events(staging.name())?;
    let mut canary_events = host.live_events(canary.name())?;
    let through_staging = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let through_canary = host
        .address_of(canary.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the canary proxy is open")?;
    let placed = Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT);
    let shipped = Record::new("shipped").with_key("o-1").with_timestamp(PLACED_AT);

    let written_through_staging = KafkaProducer::from(through_staging)
        .send(orders.name().as_str(), Partition::new(0), std::slice::from_ref(&placed))
        .await;
    let written_through_canary = KafkaProducer::from(through_canary)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            std::slice::from_ref(&shipped),
        )
        .await;

    assert_eq!(written_through_staging, Ok(Offset::new(0)));
    assert_eq!(written_through_canary, Ok(Offset::new(1)));
    // The staging proxy armed nothing, so its Produce went through at once.
    let staging_produce = loop {
        let event = tokio::time::timeout(PATIENCE, staging_events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            break record;
        }
    };
    assert_eq!(staging_produce.verdict().to_string(), "pass");
    // The canary proxy refused the first Produce, and the client sent the record again.
    let mut canary_produces = Vec::new();
    while canary_produces.len() < 2 {
        let event = tokio::time::timeout(PATIENCE, canary_events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            canary_produces.push(record);
        }
    }
    assert_eq!(
        canary_produces[0].verdict().to_string(),
        "NOT_LEADER_OR_FOLLOWER on orders/0"
    );
    assert_eq!(canary_produces[1].verdict().to_string(), "pass");
    // Each record is in the log once.
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0)), shipped.with_offset(Offset::new(1))]
    );

    host.stop_all().await;
    assert_eq!(host.running(), Vec::<EnvironmentName>::new());
    Ok(())
}

#[tokio::test]
async fn an_environment_runs_once_and_keeps_its_ports_until_it_stops() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    // A port that the system has just handed out, and that the staging proxy names for its bootstrap endpoint.
    let named = ListenAddress::new(LOCALHOST, TcpListener::bind((LOCALHOST, 0))?.local_addr()?.port());
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(named, brokers.clone(), LOCALHOST.into())?),
            upstream: upstream.clone(),
        },
    );
    // The canary names the same port.
    let canary = Environment::new(
        "canary-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(named, brokers, LOCALHOST.into())?),
            upstream,
        },
    );
    host.start(EnvironmentConfiguration::new(&staging)?).await?;

    let twice = host.start(EnvironmentConfiguration::new(&staging)?).await;
    let beside = host.start(EnvironmentConfiguration::new(&canary)?).await;

    assert_eq!(
        twice,
        Err(EnvironmentStartError::AlreadyRunning {
            name: staging.name().clone()
        })
    );
    assert_eq!(
        beside,
        Err(EnvironmentStartError::SharedListener {
            name: canary.name().clone(),
            running: staging.name().clone(),
            address: named,
        })
    );
    assert_eq!(host.running(), [staging.name().clone()]);
    assert_eq!(
        host.address_of(staging.name(), Endpoint::Bootstrap)?,
        Some(named.into())
    );

    // Once the staging proxy stopped, its port is free, and the canary takes it.
    host.stop(staging.name()).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;

    assert_eq!(host.running(), [canary.name().clone()]);
    assert_eq!(host.address_of(canary.name(), Endpoint::Bootstrap)?, Some(named.into()));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_fired_by_hand_lies_on_the_next_call_and_an_incident_needs_a_backend_that_applies_it()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build().with(Refusal)?,
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    // A pad that refuses the next Produce when someone presses it, and nothing before.
    let flap = Rule::new(
        "leader-flap-on-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_trigger(Trigger::Manual);
    // A pad that moves the leader of the first partition of the orders.
    let election = Rule::new(
        "leader-election".parse()?,
        Conditions::new(),
        IncidentDefinition::LeaderMoves {
            partition: Some("orders/0".parse()?),
            to: None,
        },
    )
    .with_trigger(Trigger::Manual);
    let not_armed = host.fire(staging.name(), flap.name()).await;
    host.arm(staging.name(), &flap).await?;
    host.arm(staging.name(), &election).await?;
    let mut events = host.live_events(staging.name())?;
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;

    host.fire(staging.name(), flap.name()).await?;
    let written = KafkaProducer::from(bootstrap)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    let moved = host.fire(staging.name(), election.name()).await;

    assert_eq!(
        not_armed,
        Err(FireRuleError::NotArmed {
            environment: staging.name().clone(),
            rule: flap.name().clone(),
        })
    );
    // The hand firing refused the first Produce, and the client sent the record again.
    assert_eq!(written, Ok(Offset::new(0)));
    let mut produces = Vec::new();
    while produces.len() < 2 {
        let event = tokio::time::timeout(PATIENCE, events.next()).await??;
        if let EngineEvent::CallRecorded(record) = event
            && let Some(api) = record.fields().read(Field::Api).present()
            && api.contains(&ApiKey::PRODUCE.into())
        {
            produces.push(record);
        }
    }
    assert_eq!(produces[0].verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders/0");
    assert_eq!(produces[1].verdict().to_string(), "pass");
    // A proxy in front of a real cluster neither applies nor emulates an incident yet.
    assert_eq!(host.incident_support(staging.name())?, IncidentSupport::none());
    let Err(FireRuleError::IncidentRejected { rule, .. }) = moved else {
        anyhow::bail!("a real cluster applied an incident: {moved:?}");
    };
    assert_eq!(&rule, election.name());
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_live_rule_is_made_armed_in_one_step_or_not_at_all() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    // What Enter in ⌘K gives: a rule that no workspace holds.
    let quiet_produce = Rule::new(
        "blackhole-on-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        LieDefinition::Silence,
    );
    // Someone armed a rule straight in the proxy: no workspace holds it, and it is no live rule.
    let quiet_fetch = Rule::new(
        "blackhole-on-fetch".parse()?,
        Conditions::from(Condition::is(ApiKey::FETCH)),
        LieDefinition::Silence,
    );
    host.arm(staging.name(), &quiet_fetch).await?;

    let made = host.create_live_rule_armed(staging.name(), quiet_produce.clone()).await;
    let refused = host.create_live_rule_armed(staging.name(), quiet_fetch.clone()).await;

    // The rule is a live rule of the environment, and it is armed.
    assert_eq!(made.map(|armed| armed.name().clone()), Ok(quiet_produce.name().clone()));
    // A rule that cannot arm, because one of its name is armed already, is no live rule either.
    assert_eq!(
        refused.map(|armed| armed.name().clone()),
        Err(CreateLiveRuleError::NameTaken {
            environment: staging.name().clone(),
            rule: quiet_fetch.name().clone(),
        })
    );
    assert_eq!(
        host.rules_of(staging.name()).await?,
        [EnvironmentRule {
            rule: quiet_produce,
            live: true,
            state: RuleState::Armed,
        }]
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_sentence_knows_the_names_of_the_workspace_and_those_the_traffic_has_seen() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders.eu").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(Listeners::new(any, brokers, LOCALHOST.into())?),
            upstream: Upstream::new(cluster.brokers()[0].address().to_string().parse()?),
        },
    );
    // The workspace has a preset and a selector with a title.
    host.create(Preset::new(
        "leader-flap".parse()?,
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    ))
    .await?;
    host.create(
        Selector::new(
            "order-producers".parse()?,
            Conditions::from(Condition::is(ApiKey::PRODUCE)),
        )
        .with_title("Order producers".parse()?),
    )
    .await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let order_ingest: ClientId = "order-ingest".parse()?;
    // The traffic has seen the calls of `order-ingest` once its subscription hands them over.
    let mut of_order_ingest = host
        .follow_calls(
            staging.name(),
            &SelectorRef::Inline(Conditions::from(Condition::is(order_ingest.clone()))),
        )
        .await?;
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let mut producing = ProducerConfiguration::new(bootstrap);
    producing.client.client_id = order_ingest;

    // Before `order-ingest` writes, a sentence does not know it.
    let unknown = host
        .read_sentence(staging.name(), "delay produce orders.* 750ms 40% order-ingest")
        .await?;
    let written = KafkaProducer::new(producing)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    tokio::time::timeout(PATIENCE, of_order_ingest.next())
        .await?
        .context("the environment runs until the test stops it")?;
    let typing = host
        .read_sentence(staging.name(), "delay produce orders.* 750ms 40% order-")
        .await?;
    let sentence = host
        .read_sentence(staging.name(), "delay produce orders.* 750ms 40% order-ingest")
        .await?;
    let titled = host.read_sentence(staging.name(), "leader-flap prod").await?;

    assert_eq!(written, Ok(Offset::new(0)));
    let unread: Vec<&str> = unknown.unread().iter().map(Unread::word).collect();
    assert_eq!(unread, ["order-ingest"]);
    // Once it wrote, the sentence completes its client ID, and reads as a whole rule.
    let clients: Vec<&str> = typing
        .completions()
        .iter()
        .filter(|completion| completion.kind() == CompletionKind::Client)
        .map(Completion::word)
        .collect();
    assert_eq!(clients, ["order-ingest"]);
    assert!(sentence.unread().is_empty(), "{:?}", sentence.unread());
    let rule = sentence.rule().context("the sentence names a lie")?;
    assert_eq!(rule.scope().to_string(), "client.id is order-ingest");
    // The names of the workspace stay known: a word of the title of a selector offers its name.
    let selectors: Vec<&str> = titled
        .completions()
        .iter()
        .filter(|completion| completion.kind() == CompletionKind::Selector)
        .map(Completion::sentence)
        .collect();
    assert_eq!(selectors, ["leader-flap order-producers "]);
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn two_presses_that_want_a_pad_armed_arm_it_once() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
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
    host.create(order_producers).await?;
    host.create(delay).await?;
    host.create(desk.clone()).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);

    // Two people see the pad empty, and both press it to arm it.
    let arm = Intent::Arm { pad: pad.clone() };
    let first = host.send_intent(staging.name(), &arm).await?;
    let second = host.send_intent(staging.name(), &arm).await?;

    assert_eq!((first.made, first.changed), (true, true));
    // The second press finds the rule armed and changes nothing, so it leaves nothing to undo.
    assert_eq!((second.made, second.changed), (false, false));
    assert_eq!(state(&second), Some(RuleState::Armed));

    // Two people who press it to disarm it both find it disarmed.
    let disarm = Intent::Disarm { pad };
    let disarmed = host.send_intent(staging.name(), &disarm).await?;
    let again = host.send_intent(staging.name(), &disarm).await?;

    assert!(disarmed.changed);
    assert!(!again.changed);
    assert_eq!(state(&again), Some(RuleState::Disarmed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_held_pad_stays_armed_until_its_last_hold_goes_and_a_disarm_ends_every_hold() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
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
    host.create(order_producers).await?;
    host.create(delay).await?;
    host.create(desk.clone()).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);
    let hold = Intent::Hold { pad: pad.clone() };
    let release = |hold| Intent::Release { pad: pad.clone(), hold };

    // Two people hold the pad. The first hold makes its rule and arms it, and each press adds a hold of its own.
    let mine = host.send_intent(staging.name(), &hold).await?;
    let theirs = host.send_intent(staging.name(), &hold).await?;
    let (Some(my_hold), Some(their_hold)) = (mine.hold, theirs.hold) else {
        anyhow::bail!("a press in hold adds a hold: {mine:?}, {theirs:?}");
    };
    assert_eq!((mine.made, mine.changed), (true, true));
    assert_eq!((theirs.made, theirs.changed), (false, false));
    assert_ne!(my_hold, their_hold);

    // I let go, and the rule stays armed while they still hold it.
    let mine_gone = host.send_intent(staging.name(), &release(my_hold)).await?;
    assert!(!mine_gone.changed);
    assert_eq!(state(&mine_gone), Some(RuleState::Armed));

    // Someone presses the pad in latch to disarm it, which ends their hold too, and holds it again afterwards.
    assert!(
        host.send_intent(staging.name(), &Intent::Disarm { pad: pad.clone() })
            .await?
            .changed
    );
    let anew = host.send_intent(staging.name(), &hold).await?;
    let Some(new_hold) = anew.hold else {
        anyhow::bail!("a press in hold adds a hold: {anew:?}");
    };
    assert!(anew.changed);

    // Their release comes after the disarm ended their hold: it changes nothing.
    let late = host.send_intent(staging.name(), &release(their_hold)).await?;
    assert!(!late.changed);
    assert_eq!(state(&late), Some(RuleState::Armed));
    // The last hold goes, and the rule disarms.
    let last = host.send_intent(staging.name(), &release(new_hold)).await?;
    assert!(last.changed);
    assert_eq!(state(&last), Some(RuleState::Disarmed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_name_that_a_live_rule_of_another_environment_has_is_no_name_for_the_workspace() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    // Two environments stand in front of the same cluster. Every listener of both takes a free port.
    let any = ListenAddress::new(LOCALHOST, 0);
    let brokers = BrokerListeners::pinned(cluster.brokers().iter().map(|broker| (broker.id(), any)))?;
    let listen = Listeners::new(any, brokers, LOCALHOST.into())?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let staging = Environment::new(
        "staging-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen.clone()),
            upstream: upstream.clone(),
        },
    );
    let canary = Environment::new(
        "canary-proxy".parse()?,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen),
            upstream,
        },
    );
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let delay = Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)));
    let slow = Rule::new(
        "slow-order-producers".parse()?,
        order_producers.name().clone(),
        delay.name().clone(),
    );
    host.create(order_producers).await?;
    host.create(delay).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    // Each environment holds a live rule of the same name.
    host.create_live_rule(staging.name(), slow.clone()).await?;
    host.create_live_rule(canary.name(), slow.clone()).await?;

    let kept = host.keep(staging.name(), slow.name()).await;
    let created = host.create(slow.clone()).await;

    // A rule of the workspace is in every environment, so it never stands next to a live rule of its name.
    assert_eq!(
        kept,
        Err(KeepRuleError::NameTaken {
            rule: slow.name().clone(),
            environment: canary.name().clone(),
        })
    );
    assert!(
        matches!(&created, Err(CreateDefinitionError::NameTaken { rule, .. }) if rule == slow.name()),
        "{created:?}"
    );

    // Once the canary deleted its live rule, the staging proxy keeps its own.
    host.delete_live_rule(canary.name(), slow.name())?;
    assert_eq!(host.keep(staging.name(), slow.name()).await?, slow);
    host.stop_all().await;
    Ok(())
}
