//! A host runs several environments in front of real Kafka brokers, each with a proxy of its own,
//! makes a live rule armed in one step, reads a ⌘K sentence with the names that the traffic of one of them has
//! seen, acts on the presses and the holds of a pad, ends the holds of a presence that ended, and keeps a live rule
//! only under a name that no other one has.
//! Whatever someone changes has a revision, and a change says what it expects.
//! The journal of each environment records every change, with the surface it came from.
//! A reload of the files of the workspace takes in what someone edited, and every armed rule follows it.
//!
//! The tests need Docker.
//! Each test starts the cluster it needs, and a host whose workspace lives in memory, or in the files of a
//! temporary directory for a test that reloads them.
//! The environments of a test stand in front of that cluster, and every listener of theirs takes a free port,
//! unless the test names one.
//! The client is the producer of `gaslightmq_kafka_client_native`, which knows the bootstrap address of one
//! environment and nothing else.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, TcpListener};

use anyhow::Context;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_definition_api::prelude::*;
use gaslightmq_engine::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kafka_client_native::{KafkaProducer, ProducerConfiguration};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_quick_inject::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::{Column, Encoder, EncoderTurn, Intent, Layer, PadLayout, PresenceId, Row, Steps};
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::{Offset, Partition, Record};
use gaslightmq_workspace::prelude::*;
use gaslightmq_workspace_fs::prelude::*;

/// How long a test waits for the record of a call that the client already got its answer for.
const PATIENCE: Duration = Duration::from_secs(30);

/// When the orders of these tests were placed, in milliseconds since 1970.
/// A record with a timestamp of its own reads back as it was written.
const PLACED_AT: i64 = 1_700_000_000_000;

/// The presence of Ana's tab of the UI. Whoever lets clients connect makes one from the randomness of the system for
/// each client that comes, and a test names its own.
const ANAS_TAB: PresenceId = PresenceId::from_random_bits(0xa);

/// The presence of Dan's tab.
const DANS_TAB: PresenceId = PresenceId::from_random_bits(0xd);

/// The address that the listeners of a test listen on, and that its clients connect to.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The run of the host of a test. A test names its own, and `gmq` makes one from the randomness of the system.
const RUN: RunId = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);

#[tokio::test]
async fn a_rule_armed_in_one_environment_leaves_the_other_alone() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build().with(Refusal)?,
        RUN,
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
    host.create(staging.clone(), ChangeSurface::Test).await?;
    host.create(canary.clone(), ChangeSurface::Test).await?;
    // The workspace has a preset that both environments can use.
    let leader_flap = Preset::new(
        "leader-flap".parse()?,
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    host.create(leader_flap.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    // Someone arms a rule in the canary alone: "tell the 1st Produce that its broker no longer leads the partition".
    let flap = Rule::new(
        "leader-flap-on-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        leader_flap.name().clone(),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1)?));
    host.arm(canary.name(), &flap, None, ChangeSurface::Test).await?;
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
        RUN,
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
        RUN,
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
    let not_armed = host.fire(staging.name(), flap.name(), ChangeSurface::Test).await;
    host.arm(staging.name(), &flap, None, ChangeSurface::Test).await?;
    host.arm(staging.name(), &election, None, ChangeSurface::Test).await?;
    let mut events = host.live_events(staging.name())?;
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;

    host.fire(staging.name(), flap.name(), ChangeSurface::Test).await?;
    let written = KafkaProducer::from(bootstrap)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    let moved = host.fire(staging.name(), election.name(), ChangeSurface::Test).await;

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
    // The journal records the firing that happened, after the two armings, and nothing of the one that was rejected.
    // A firing is no change of what someone set, so its entry holds no revision.
    let journal = host.journal(staging.name(), None)?;
    let fired = journal.last().context("the journal recorded the firing")?;
    assert_eq!(journal.len(), 3, "{journal:?}");
    assert_eq!(
        fired.change,
        Change::RuleFiredByHand {
            rule: flap.name().as_str().parse()?
        }
    );
    assert_eq!(fired.from, ChangeSource::new(ChangeSurface::Test));
    assert_eq!(fired.revision, None);
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
        RUN,
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
    host.arm(staging.name(), &quiet_fetch, None, ChangeSurface::Test)
        .await?;

    let made = host
        .create_live_rule_armed(staging.name(), quiet_produce.clone(), ChangeSurface::Test)
        .await?;
    let refused = host
        .create_live_rule_armed(staging.name(), quiet_fetch.clone(), ChangeSurface::Test)
        .await;

    // A rule that cannot arm, because one of its name is armed already, is no live rule either.
    assert_eq!(
        refused,
        Err(CreateLiveRuleError::NameTaken {
            environment: staging.name().clone(),
            rule: quiet_fetch.name().clone(),
            holder: Some(RuleHolder::Environment(staging.name().clone())),
        })
    );
    // The other rule is a live rule of the environment, and it is armed. Making it and arming it was one change,
    // so the rule has the revision that the one step answered with.
    assert_eq!(
        host.rules_of(staging.name()).await?,
        [EnvironmentRule {
            rule: quiet_produce,
            live: true,
            state: RuleState::Armed,
            revision: made,
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
        RUN,
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
    host.create(
        Preset::new(
            "leader-flap".parse()?,
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        ),
        ChangeSurface::Test,
    )
    .await?;
    host.create(
        Selector::new(
            "order-producers".parse()?,
            Conditions::from(Condition::is(ApiKey::PRODUCE)),
        )
        .with_title("Order producers".parse()?),
        ChangeSurface::Test,
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
async fn a_rule_of_the_workspace_has_one_revision_in_an_environment_for_its_definition_and_its_state()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let blackhole = Rule::new(
        "blackhole-everything".parse()?,
        Conditions::new(),
        LieDefinition::Silence,
    );
    let of_staging = async || {
        host.rule_of(staging.name(), blackhole.name())
            .await?
            .context("the environment holds every rule of the workspace")
    };
    // The workspace gets the rule while the environment runs, so its last change there is the create.
    let created = host.create(blackhole.clone(), ChangeSurface::Test).await?;
    assert_eq!(
        of_staging().await?,
        EnvironmentRule {
            rule: blackhole.clone(),
            live: false,
            state: RuleState::Disarmed,
            revision: created,
        }
    );

    // One tab arms the rule as it saw it.
    let seen_created = Expects {
        run: host.run(),
        revision: created,
    };
    let armed = host
        .arm(staging.name(), &blackhole, Some(seen_created), ChangeSurface::Test)
        .await?;
    // Another tab gives the rule a title in the workspace.
    let titled = blackhole.clone().with_title("Blackhole everything".parse()?);
    let retitled = host.update(titled.clone(), None, ChangeSurface::Test).await?;
    // The first tab disarms what it armed, and has not seen the title.
    let seen_armed = Expects {
        run: host.run(),
        revision: armed,
    };
    let late = host
        .disarm(staging.name(), blackhole.name(), Some(seen_armed), ChangeSurface::Test)
        .await;

    assert!(
        created < armed && armed < retitled,
        "{created}, {armed} and then {retitled}"
    );
    assert_eq!(
        late,
        Err(DisarmRuleError::Changed {
            environment: staging.name().clone(),
            rule: blackhole.name().clone(),
            revision: retitled,
        })
    );
    // The rule is still armed, and its one revision is the later of its two changes: the title.
    assert_eq!(
        of_staging().await?,
        EnvironmentRule {
            rule: titled.clone(),
            live: false,
            state: RuleState::Armed,
            revision: retitled,
        }
    );
    // Whoever saw the title disarms the rule.
    let seen_titled = Expects {
        run: host.run(),
        revision: retitled,
    };
    let disarmed = host
        .disarm(staging.name(), blackhole.name(), Some(seen_titled), ChangeSurface::Test)
        .await?;
    assert!(retitled < disarmed, "{retitled} and then {disarmed}");
    assert_eq!(
        of_staging().await?,
        EnvironmentRule {
            rule: titled,
            live: false,
            state: RuleState::Disarmed,
            revision: disarmed,
        }
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_live_rule_changes_goes_and_is_kept_only_as_its_sender_saw_it() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    // ⌘K made a live rule: "hold every Produce 20 ms".
    let hold = Rule::new(
        "hold-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let made = host
        .create_live_rule(staging.name(), hold.clone(), ChangeSurface::Test)
        .await?;
    let seen_made = Some(Expects {
        run: host.run(),
        revision: made,
    });
    // Someone else turns an encoder: the rule holds every Produce 50 ms from here on.
    let longer = Rule::new(
        hold.name().clone(),
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(50)),
    );
    let turned = host
        .update_live_rule(staging.name(), longer.clone(), None, ChangeSurface::Test)
        .await?;

    // Whoever still sees the rule of 20 ms changes, deletes and keeps nothing.
    assert_eq!(
        host.update_live_rule(staging.name(), hold.clone(), seen_made, ChangeSurface::Test)
            .await,
        Err(UpdateLiveRuleError::Changed {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
            revision: turned,
        })
    );
    assert_eq!(
        host.delete_live_rule(staging.name(), hold.name(), seen_made, ChangeSurface::Test),
        Err(DeleteLiveRuleError::Changed {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
            revision: turned,
        })
    );
    assert_eq!(
        host.keep(staging.name(), hold.name(), seen_made, ChangeSurface::Test)
            .await,
        Err(KeepRuleError::Changed {
            environment: staging.name().clone(),
            rule: hold.name().clone(),
            revision: turned,
        })
    );
    let live = host
        .rule_of(staging.name(), hold.name())
        .await?
        .context("the environment still holds its live rule")?;
    assert_eq!(
        live,
        EnvironmentRule {
            rule: longer.clone(),
            live: true,
            state: RuleState::Disarmed,
            revision: turned,
        }
    );

    // Whoever saw the turn keeps the rule: the workspace gets the rule that someone looked at.
    let seen_turned = Some(Expects {
        run: host.run(),
        revision: turned,
    });
    let kept = host
        .keep(staging.name(), hold.name(), seen_turned, ChangeSurface::Test)
        .await?;

    assert!(made < turned && turned < kept, "{made}, {turned} and then {kept}");
    // The keep is one change, so the rule has its number in the workspace and in the environment.
    assert_eq!(host.workspace().await.revision(hold.name().clone()), kept);
    let of_the_workspace = host
        .rule_of(staging.name(), hold.name())
        .await?
        .context("the environment holds every rule of the workspace")?;
    assert_eq!(
        of_the_workspace,
        EnvironmentRule {
            rule: longer,
            live: false,
            state: RuleState::Disarmed,
            revision: kept,
        }
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_environment_that_starts_again_gives_every_rule_a_higher_revision() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let blackhole = Rule::new(
        "blackhole-everything".parse()?,
        Conditions::new(),
        LieDefinition::Silence,
    );
    let created = host.create(blackhole.clone(), ChangeSurface::Test).await?;
    let revision_in_staging = async || {
        let rule = host.rule_of(staging.name(), blackhole.name()).await?;
        anyhow::Ok(
            rule.context("the environment holds every rule of the workspace")?
                .revision,
        )
    };

    // The start of an environment is a change of every rule in it.
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let started = revision_in_staging().await?;
    let seen_started = Expects {
        run: host.run(),
        revision: started,
    };
    let armed = host
        .arm(staging.name(), &blackhole, Some(seen_started), ChangeSurface::Test)
        .await?;
    // The environment stops, and nothing is armed in the one that starts under its name.
    host.stop(staging.name()).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let started_again = revision_in_staging().await?;

    assert!(
        created < started && started < armed && armed < started_again,
        "{created}, {started}, {armed} and then {started_again}"
    );
    // A change that expects the rule as it was armed before the stop changes nothing.
    let seen_armed = Expects {
        run: host.run(),
        revision: armed,
    };
    assert_eq!(
        host.disarm(staging.name(), blackhole.name(), Some(seen_armed), ChangeSurface::Test)
            .await,
        Err(DisarmRuleError::Changed {
            environment: staging.name().clone(),
            rule: blackhole.name().clone(),
            revision: started_again,
        })
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_rule_that_spends_its_budget_takes_a_revision_as_it_disarms() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    // "Hold the next Produce 20 ms, once."
    let once = Rule::new(
        "hold-the-next-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    )
    .with_lifetime(Lifetime::Firings(Count::ONE));
    host.create_live_rule(staging.name(), once.clone(), ChangeSurface::Test)
        .await?;
    let armed = host.arm(staging.name(), &once, None, ChangeSurface::Test).await?;
    let mut changes = host.changes();
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;

    let written = KafkaProducer::from(bootstrap)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    // The Produce spent the only firing, so the rule disarmed itself, and whoever follows the host learns of it.
    let disarmed_itself = HostChange::RuleChanged {
        environment: staging.name().clone(),
        rule: once.name().clone(),
    };
    while tokio::time::timeout(PATIENCE, changes.next()).await?? != disarmed_itself {}

    assert_eq!(written, Ok(Offset::new(0)));
    let spent = host
        .rule_of(staging.name(), once.name())
        .await?
        .context("the environment still holds its live rule")?;
    assert_eq!(spent.state, RuleState::Disarmed);
    // Nobody asked for that change, and it took the next number all the same.
    assert!(armed < spent.revision, "{armed} and then {}", spent.revision);
    // The journal says what disarmed the rule: its lifetime, and no surface that someone uses.
    let journal = host.journal(staging.name(), None)?;
    let disarmed = journal.last().context("the journal recorded that the rule disarmed")?;
    assert_eq!(
        disarmed.change,
        Change::RuleDisarmed {
            rule: once.name().as_str().parse()?
        }
    );
    assert_eq!(disarmed.from, ChangeSource::new(ChangeSurface::Lifetime));
    assert_eq!(
        disarmed.revision,
        Some(gaslightmq_journal::Revision::new(spent.revision.number()))
    );
    // Whoever still sees the rule armed arms nothing over it.
    let seen_armed = Expects {
        run: host.run(),
        revision: armed,
    };
    assert_eq!(
        host.arm(staging.name(), &once, Some(seen_armed), ChangeSurface::Test)
            .await,
        Err(ArmRuleError::Changed {
            environment: staging.name().clone(),
            rule: once.name().clone(),
            revision: spent.revision,
        })
    );
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
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);

    // Two people see the pad empty, and both press it to arm it.
    let arm = Intent::Arm { pad: pad.clone() };
    let first = host.send_intent(staging.name(), &arm, ChangeSurface::Pad).await?;
    let second = host.send_intent(staging.name(), &arm, ChangeSurface::Pad).await?;

    assert_eq!((first.made, first.changed), (true, true));
    // The second press finds the rule armed and changes nothing, so it leaves nothing to undo.
    assert_eq!((second.made, second.changed), (false, false));
    assert_eq!(state(&second), Some(RuleState::Armed));
    // Making the rule and arming it was one change. The press that changed nothing took no number:
    // it answers with the rule at the revision that the first press left.
    let revision = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.revision);
    assert!(revision(&first).is_some(), "{first:?}");
    assert_eq!(revision(&second), revision(&first));
    // Each answer says the revision that the press found: none before the rule was there.
    assert_eq!(first.found, None);
    assert_eq!(second.found, revision(&first));

    // Two people who press it to disarm it both find it disarmed.
    let disarm = Intent::Disarm { pad };
    let disarmed = host.send_intent(staging.name(), &disarm, ChangeSurface::Pad).await?;
    let again = host.send_intent(staging.name(), &disarm, ChangeSurface::Pad).await?;

    assert!(disarmed.changed);
    assert!(!again.changed);
    assert_eq!(state(&again), Some(RuleState::Disarmed));
    // The disarm is a later change of the rule, and the press after it changed nothing again.
    assert!(
        revision(&first) < revision(&disarmed),
        "{first:?} and then {disarmed:?}"
    );
    assert_eq!(revision(&again), revision(&disarmed));
    // The disarm found the rule as the presses left it.
    assert_eq!(disarmed.found, revision(&first));
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
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);
    let hold = Intent::Hold { pad: pad.clone() };
    let release = |hold| Intent::Release { pad: pad.clone(), hold };

    // Ana and Dan hold the pad. Ana's hold makes its rule and arms it, and each press adds a hold of its own.
    let ana = host.send_intent(staging.name(), &hold, ChangeSurface::Pad).await?;
    let dan = host.send_intent(staging.name(), &hold, ChangeSurface::Pad).await?;
    let (Some(anas_hold), Some(dans_hold)) = (ana.hold, dan.hold) else {
        anyhow::bail!("a press in hold adds a hold: {ana:?}, {dan:?}");
    };
    assert_eq!((ana.made, ana.changed), (true, true));
    assert_eq!((dan.made, dan.changed), (false, false));
    assert_ne!(anas_hold, dans_hold);

    // Dan lets go, and the rule stays armed while Ana still holds it.
    let dan_gone = host
        .send_intent(staging.name(), &release(dans_hold), ChangeSurface::Pad)
        .await?;
    assert!(!dan_gone.changed);
    assert_eq!(state(&dan_gone), Some(RuleState::Armed));

    // Ben presses the pad in latch to disarm it, which ends Ana's hold. Cleo holds the pad again, which arms it anew.
    let ben = host
        .send_intent(staging.name(), &Intent::Disarm { pad: pad.clone() }, ChangeSurface::Pad)
        .await?;
    assert!(ben.changed);
    let cleo = host.send_intent(staging.name(), &hold, ChangeSurface::Pad).await?;
    let Some(cleos_hold) = cleo.hold else {
        anyhow::bail!("a press in hold adds a hold: {cleo:?}");
    };
    assert!(cleo.changed);

    // Ana lets go after Ben's disarm ended her hold: her release changes nothing, and the rule stays armed for Cleo.
    // A count of holds alone would take Ana's release for Cleo's hold.
    let ana_late = host
        .send_intent(staging.name(), &release(anas_hold), ChangeSurface::Pad)
        .await?;
    assert!(!ana_late.changed);
    assert_eq!(state(&ana_late), Some(RuleState::Armed));
    // Cleo lets go, the last hold goes, and the rule disarms.
    let cleo_gone = host
        .send_intent(staging.name(), &release(cleos_hold), ChangeSurface::Pad)
        .await?;
    assert!(cleo_gone.changed);
    assert_eq!(state(&cleo_gone), Some(RuleState::Disarmed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_release_never_disarms_a_rule_that_a_press_in_latch_armed() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);

    // Ben arms the pad in latch. Ana holds it, which finds it armed, and still answers with a hold.
    let ben = host
        .send_intent(staging.name(), &Intent::Arm { pad: pad.clone() }, ChangeSurface::Pad)
        .await?;
    let ana = host
        .send_intent(staging.name(), &Intent::Hold { pad: pad.clone() }, ChangeSurface::Pad)
        .await?;
    let Some(anas_hold) = ana.hold else {
        anyhow::bail!("a press in hold adds a hold: {ana:?}");
    };
    assert!(ben.changed);
    assert!(!ana.changed);

    // Ana lets go: her hold did not arm the rule, so her release leaves it armed for Ben.
    let ana_gone = host
        .send_intent(
            staging.name(),
            &Intent::Release { pad, hold: anas_hold },
            ChangeSurface::Pad,
        )
        .await?;

    assert!(!ana_gone.changed);
    assert_eq!(state(&ana_gone), Some(RuleState::Armed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_presence_that_ends_takes_its_holds_away_and_the_rule_stays_armed_while_another_presence_holds_it()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let rule = desk.rule_name(&pad).context("the pad points at a rule by its name")?;
    let hold = Intent::Hold { pad };

    // Ana and Dan each play at a tab of their own, and each tab is a presence.
    host.open_presence(ANAS_TAB);
    host.open_presence(DANS_TAB);
    // Both hold the pad. Ana's hold makes its rule and arms it, and each hold belongs to the presence of its press.
    let ana = host
        .send_intent_from(ANAS_TAB, staging.name(), &hold, ChangeSurface::Pad)
        .await?;
    let dan = host
        .send_intent_from(DANS_TAB, staging.name(), &hold, ChangeSurface::Pad)
        .await?;
    assert!(ana.hold.is_some(), "{ana:?}");
    assert!(dan.hold.is_some(), "{dan:?}");
    assert_eq!((ana.made, ana.changed), (true, true));

    // Ana's tab closes while she holds the pad, so her release never comes. Her presence ends, and her hold with it.
    host.end_presence(ANAS_TAB);
    // The rule stays armed while Dan still holds the pad.
    let for_dan = host.rule_of(staging.name(), &rule).await?;
    assert_eq!(for_dan.map(|rule| rule.state), Some(RuleState::Armed));

    // Dan's tab is gone too. That was the last hold of the rule, and holds armed it, so it disarms.
    host.end_presence(DANS_TAB);
    let for_nobody = host.rule_of(staging.name(), &rule).await?;
    assert_eq!(for_nobody.map(|rule| rule.state), Some(RuleState::Disarmed));

    // A presence that ended holds nothing any more: nothing would end the hold of a client that is gone.
    let late = host
        .send_intent_from(ANAS_TAB, staging.name(), &hold, ChangeSurface::Pad)
        .await;
    assert_eq!(
        late,
        Err(IntentError::Refused {
            reason: format!("presence {ANAS_TAB} is not open, so nothing would end its hold"),
        })
    );
    let after = host.rule_of(staging.name(), &rule).await?;
    assert_eq!(after.map(|rule| rule.state), Some(RuleState::Disarmed));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_presence_that_ends_the_last_hold_of_a_rule_gives_the_rule_a_later_revision() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let rule = desk.rule_name(&pad).context("the pad points at a rule by its name")?;
    let hold = Intent::Hold { pad };
    host.open_presence(ANAS_TAB);
    host.open_presence(DANS_TAB);

    // Ana's hold makes the rule of the pad and arms it, which is one change. Dan's hold changes nothing.
    let ana = host
        .send_intent_from(ANAS_TAB, staging.name(), &hold, ChangeSurface::Pad)
        .await?;
    let dan = host
        .send_intent_from(DANS_TAB, staging.name(), &hold, ChangeSurface::Pad)
        .await?;
    let armed = ana.rule.context("a press in hold answers with its rule")?.revision;
    assert_eq!(dan.rule.map(|rule| rule.revision), Some(armed));
    let mut changes = host.changes();

    // Ana's presence ends while Dan still holds the pad: the rule did not change, and keeps its revision.
    host.end_presence(ANAS_TAB);
    let for_dan = host
        .rule_of(staging.name(), &rule)
        .await?
        .context("the environment holds the rule of the pad")?;
    assert_eq!((for_dan.state, for_dan.revision), (RuleState::Armed, armed));

    // Dan's presence ends too, and with it the last hold: the rule disarms.
    // Nobody released anything, and the change is one like any other: whoever follows the host learns of it,
    host.end_presence(DANS_TAB);
    let disarmed = HostChange::RuleChanged {
        environment: staging.name().clone(),
        rule: rule.clone(),
    };
    while tokio::time::timeout(PATIENCE, changes.next()).await?? != disarmed {}
    // and it took the next number.
    let for_nobody = host
        .rule_of(staging.name(), &rule)
        .await?
        .context("the environment still holds its live rule")?;
    assert_eq!(for_nobody.state, RuleState::Disarmed);
    assert!(armed < for_nobody.revision, "{armed} and then {}", for_nobody.revision);
    // The journal records the disarm as the release that Dan's tab never sent: it came from the pads.
    let journal = host.journal(staging.name(), None)?;
    let released = journal.last().context("the journal recorded that the rule disarmed")?;
    assert_eq!(
        released.change,
        Change::RuleDisarmed {
            rule: rule.as_str().parse()?
        }
    );
    assert_eq!(released.from, ChangeSource::new(ChangeSurface::Pad));
    assert_eq!(
        released.revision,
        Some(gaslightmq_journal::Revision::new(for_nobody.revision.number()))
    );
    // Whoever still sees the rule as Ana and Dan held it arms nothing over it.
    let seen_armed = Expects {
        run: host.run(),
        revision: armed,
    };
    assert_eq!(
        host.arm(staging.name(), &for_nobody.rule, Some(seen_armed), ChangeSurface::Test)
            .await,
        Err(ArmRuleError::Changed {
            environment: staging.name().clone(),
            rule,
            revision: for_nobody.revision,
        })
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_presence_that_ends_leaves_a_hold_of_no_presence_and_a_rule_that_a_press_in_latch_armed() -> anyhow::Result<()>
{
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(fetches, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let slow_producers = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row")?;
    let slow_fetches = desk
        .pad(&Layer::all_clients(), Row::from_index(1), Column::from_index(0))
        .context("the desk has a pad at its second row")?;
    let state = |effect: &IntentEffect| effect.rule.as_ref().map(|rule| rule.state);
    host.open_presence(ANAS_TAB);

    // `gmq` holds the pad of the order producers, and it is no presence. Ana holds the same pad at her tab.
    let hold_producers = Intent::Hold {
        pad: slow_producers.clone(),
    };
    let of_the_cli = host
        .send_intent(staging.name(), &hold_producers, ChangeSurface::Cli)
        .await?;
    let Some(hold_of_the_cli) = of_the_cli.hold else {
        anyhow::bail!("a press in hold adds a hold: {of_the_cli:?}");
    };
    host.send_intent_from(ANAS_TAB, staging.name(), &hold_producers, ChangeSurface::Pad)
        .await?;
    // Ben arms the pad of the fetches in latch, and Ana holds that pad too.
    host.send_intent(
        staging.name(),
        &Intent::Arm {
            pad: slow_fetches.clone(),
        },
        ChangeSurface::Pad,
    )
    .await?;
    host.send_intent_from(
        ANAS_TAB,
        staging.name(),
        &Intent::Hold {
            pad: slow_fetches.clone(),
        },
        ChangeSurface::Pad,
    )
    .await?;

    // Ana's tab closes, and her presence ends with both her holds.
    host.end_presence(ANAS_TAB);

    // The pad of the fetches stays armed for Ben: holds did not arm its rule, so no hold that ends disarms it.
    let fetches_now = host
        .send_intent(staging.name(), &Intent::Arm { pad: slow_fetches }, ChangeSurface::Pad)
        .await?;
    assert_eq!(
        (fetches_now.changed, state(&fetches_now)),
        (false, Some(RuleState::Armed))
    );
    // The pad of the order producers stays armed for `gmq`, whose hold ends by its release alone.
    let released = host
        .send_intent(
            staging.name(),
            &Intent::Release {
                pad: slow_producers,
                hold: hold_of_the_cli,
            },
            ChangeSurface::Cli,
        )
        .await?;
    assert_eq!((released.changed, state(&released)), (true, Some(RuleState::Disarmed)));
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
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    // Each environment holds a live rule of the same name.
    host.create_live_rule(staging.name(), slow.clone(), ChangeSurface::Test)
        .await?;
    host.create_live_rule(canary.name(), slow.clone(), ChangeSurface::Test)
        .await?;

    let kept = host.keep(staging.name(), slow.name(), None, ChangeSurface::Test).await;
    let created = host.create(slow.clone(), ChangeSurface::Test).await;

    // A rule of the workspace is in every environment, so it never stands next to a live rule of its name.
    assert_eq!(
        kept,
        Err(KeepRuleError::NameTaken {
            rule: slow.name().clone(),
            holder: RuleHolder::Environment(canary.name().clone()),
        })
    );
    assert!(
        matches!(&created, Err(CreateDefinitionError::NameTaken { rule, .. }) if rule == slow.name()),
        "{created:?}"
    );

    // Once the canary deleted its live rule, the staging proxy keeps its own.
    host.delete_live_rule(canary.name(), slow.name(), None, ChangeSurface::Test)?;
    let kept = host
        .keep(staging.name(), slow.name(), None, ChangeSurface::Test)
        .await?;
    assert_eq!(
        host.rule_of(staging.name(), slow.name()).await?,
        Some(EnvironmentRule {
            rule: slow,
            live: false,
            state: RuleState::Disarmed,
            revision: kept,
        })
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_name_that_a_rule_of_a_scenario_has_is_no_name_for_a_live_rule_or_the_workspace() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let quiet_metadata = Rule::new(
        "quiet-metadata".parse()?,
        Conditions::from(Condition::is(ApiKey::METADATA)),
        LieDefinition::Silence,
    );
    let quiet_fetch = Rule::new(
        "quiet-fetch".parse()?,
        Conditions::from(Condition::is(ApiKey::FETCH)),
        LieDefinition::Silence,
    );
    let outage = Scenario::new(
        "metadata-outage".parse()?,
        Mode::Chain,
        Seed::new(42),
        [quiet_metadata.clone().into()],
    )?;
    host.create(outage.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;

    let live = host
        .create_live_rule(staging.name(), quiet_metadata.clone(), ChangeSurface::Test)
        .await;
    let created = host.create(quiet_metadata.clone(), ChangeSurface::Test).await;

    // Rule names are unique among all rules, and the scenario owns a rule of this one.
    assert_eq!(
        live,
        Err(CreateLiveRuleError::NameTaken {
            environment: staging.name().clone(),
            rule: quiet_metadata.name().clone(),
            holder: Some(RuleHolder::Scenario(outage.name().clone())),
        })
    );
    assert_eq!(
        live.unwrap_err().to_string(),
        "rule \"quiet-metadata\" was not added to environment \"staging-proxy\": \
         scenario \"metadata-outage\" owns a rule of that name"
    );
    assert_eq!(
        created,
        Err(CreateDefinitionError::NameTaken {
            rule: quiet_metadata.name().clone(),
            holder: RuleHolder::Scenario(outage.name().clone()),
        })
    );

    // A scenario can come into the stores behind the back of the host, as a file that someone edits.
    // A live rule of its name that was there before does not go into the workspace.
    host.create_live_rule(staging.name(), quiet_fetch.clone(), ChangeSurface::Test)
        .await?;
    let fetch_outage = Scenario::new(
        "fetch-outage".parse()?,
        Mode::Chain,
        Seed::new(42),
        [quiet_fetch.clone().into()],
    )?;
    host.workspace()
        .await
        .stores()
        .scenarios()
        .create(fetch_outage.clone())
        .await?;
    let kept = host
        .keep(staging.name(), quiet_fetch.name(), None, ChangeSurface::Test)
        .await;
    assert_eq!(
        kept,
        Err(KeepRuleError::NameTaken {
            rule: quiet_fetch.name().clone(),
            holder: RuleHolder::Scenario(fetch_outage.name().clone()),
        })
    );
    assert_eq!(
        kept.unwrap_err().to_string(),
        "rule \"quiet-fetch\" was not kept: scenario \"fetch-outage\" owns a rule of that name"
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn the_journal_of_an_environment_records_every_change_with_where_it_came_from() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let mut changes = host.changes();
    // "Hold every Produce 20 ms."
    let slow = Rule::new(
        "slow-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    // The revision that a change left, as an entry holds it: the journal and the host each have a type for it.
    let left = |revision: gaslightmq_workspace::Revision| Some(gaslightmq_journal::Revision::new(revision.number()));
    let sentence = ChangeSource::new(ChangeSurface::QuickInject);
    let undo = ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo);
    let redo = ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Redo);
    let gmq = ChangeSource::new(ChangeSurface::Cli);

    // Someone types a ⌘K sentence, which makes the live rule armed. They take the arming back with ⌘Z,
    // make it again with ⇧⌘Z, and `gmq` deletes the rule while it is armed.
    let made = host
        .create_live_rule_armed(staging.name(), slow.clone(), sentence)
        .await?;
    let disarmed = host.disarm(staging.name(), slow.name(), None, undo).await?;
    let armed_again = host.arm(staging.name(), &slow, None, redo).await?;
    let deleted = host.delete_live_rule(staging.name(), slow.name(), None, gmq)?;

    // Nothing took an entry out: the journal shows the arming, that someone took it back, and that they made it
    // again. Each entry holds the revision that its change left. Making the rule armed is one change in two
    // entries, and so is deleting it while it is armed: every arming in the journal ends with a disarm.
    let journal = host.journal(staging.name(), None)?;
    let rule: RecordedName = slow.name().as_str().parse()?;
    let said: Vec<(u64, Option<gaslightmq_journal::Revision>, ChangeSource, Change)> = journal
        .iter()
        .map(|entry| (entry.id.value(), entry.revision, entry.from, entry.change.clone()))
        .collect();
    assert_eq!(
        said,
        [
            (1, left(made), sentence, Change::LiveRuleMade { rule: rule.clone() }),
            (2, left(made), sentence, Change::RuleArmed { rule: rule.clone() }),
            (3, left(disarmed), undo, Change::RuleDisarmed { rule: rule.clone() }),
            (4, left(armed_again), redo, Change::RuleArmed { rule: rule.clone() }),
            (5, left(deleted), gmq, Change::RuleDisarmed { rule: rule.clone() }),
            (6, left(deleted), gmq, Change::LiveRuleDeleted { rule }),
        ]
    );
    // The entries tell their time on the clock of the environment, so a later entry is no earlier one.
    assert!(journal.is_sorted_by_key(|entry| entry.at), "{journal:?}");

    // Whoever read the journal up to the redo reads on from there.
    let the_redo = journal[3].id;
    assert_eq!(host.journal(staging.name(), Some(the_redo))?, &journal[4..]);

    // Whoever follows the host got every entry, in the order of their IDs.
    let mut told = Vec::new();
    while told.len() < journal.len() {
        let change = tokio::time::timeout(PATIENCE, changes.next()).await??;
        if let HostChange::Journaled { environment, entry } = change {
            assert_eq!(&environment, staging.name());
            told.push(entry);
        }
    }
    assert_eq!(told, journal);
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_change_of_a_definition_enters_the_journal_of_every_environment_that_runs() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let delay = Preset::new("delay-1s".parse()?, TimingLie::new().with_delay(TimeSpan::from_secs(1)));
    let preset = RecordedDefinition::Preset(delay.name().as_str().parse()?);
    let said = |journal: Vec<JournalEntry>| -> Vec<(u64, Option<gaslightmq_journal::Revision>, ChangeSource, Change)> {
        let said = journal
            .into_iter()
            .map(|entry| (entry.id.value(), entry.revision, entry.from, entry.change));
        said.collect()
    };
    // The revision that a change left, as an entry holds it: the journal and the host each have a type for it.
    let left = |revision: gaslightmq_workspace::Revision| Some(gaslightmq_journal::Revision::new(revision.number()));
    let ui = ChangeSource::new(ChangeSurface::Ui);
    let gmq = ChangeSource::new(ChangeSurface::Cli);

    // `gmq` creates the preset while no environment runs, so no journal records it.
    host.create(delay.clone(), gmq).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    assert_eq!(host.journal(staging.name(), None)?, Vec::<JournalEntry>::new());
    assert_eq!(host.journal(canary.name(), None)?, Vec::<JournalEntry>::new());

    // Someone gives the preset a title in the UI, while both environments run.
    let titled = host
        .update(delay.clone().with_title("Delay 1 s".parse()?), None, ui)
        .await?;

    // A definition belongs to the workspace, so the change is the first entry of both journals.
    // One counter gives every revision, so the entry has the same one in both.
    let changed = Change::DefinitionChanged {
        definition: preset.clone(),
    };
    let in_both = [(1, left(titled), ui, changed)];
    assert_eq!(said(host.journal(staging.name(), None)?), in_both);
    assert_eq!(said(host.journal(canary.name(), None)?), in_both);

    // The canary stops, and its journal ends with it. `gmq` deletes the preset, which the staging proxy records.
    host.stop(canary.name()).await?;
    let deleted = host.delete(delay.name().clone(), None, gmq).await?;
    assert_eq!(
        host.journal(canary.name(), None),
        Err(NotRunningError {
            name: canary.name().clone()
        })
    );
    let of_staging = said(host.journal(staging.name(), None)?);
    let gone = Change::DefinitionDeleted { definition: preset };
    assert_eq!(of_staging.last(), Some(&(2, left(deleted), gmq, gone)));
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_live_rule_that_someone_keeps_goes_on_where_it_lived_and_is_a_new_rule_everywhere_else() -> anyhow::Result<()>
{
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.start(EnvironmentConfiguration::new(&canary)?).await?;
    // "Hold every Produce 20 ms."
    let slow = Rule::new(
        "slow-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let said = |journal: Vec<JournalEntry>| -> Vec<(u64, Option<gaslightmq_journal::Revision>, ChangeSource, Change)> {
        let said = journal
            .into_iter()
            .map(|entry| (entry.id.value(), entry.revision, entry.from, entry.change));
        said.collect()
    };
    // The revision that a change left, as an entry holds it: the journal and the host each have a type for it.
    let left = |revision: gaslightmq_workspace::Revision| Some(gaslightmq_journal::Revision::new(revision.number()));
    let pad = ChangeSource::new(ChangeSurface::Pad);
    let ui = ChangeSource::new(ChangeSurface::Ui);

    // A pad of the staging proxy makes the live rule, and someone keeps it in the UI.
    let made = host.create_live_rule(staging.name(), slow.clone(), pad).await?;
    let kept = host.keep(staging.name(), slow.name(), None, ui).await?;

    // In the staging proxy the rule goes on: the keep is one more entry of its history.
    let rule: RecordedName = slow.name().as_str().parse()?;
    assert_eq!(
        said(host.journal(staging.name(), None)?),
        [
            (1, left(made), pad, Change::LiveRuleMade { rule: rule.clone() }),
            (2, left(kept), ui, Change::LiveRuleKept { rule: rule.clone() }),
        ]
    );
    // The canary never held the rule. For it, the workspace has a new rule, which begins there.
    let created = Change::DefinitionCreated {
        definition: RecordedDefinition::Rule(rule),
    };
    assert_eq!(said(host.journal(canary.name(), None)?), [(1, left(kept), ui, created)]);
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_press_and_a_turn_at_a_pad_leave_what_they_changed_in_the_journal() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(delay, ChangeSurface::Test).await?;
    host.create(desk.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    let pad = desk
        .pad(&Layer::all_clients(), Row::from_index(0), Column::from_index(0))
        .context("the desk has a pad at its first row and column")?;
    let rule: RecordedName = desk
        .rule_name(&pad)
        .context("the pad points at a rule by its name")?
        .as_str()
        .parse()?;
    // The revision that an intent left, as an entry holds it: the journal and the host each have a type for it.
    let left = |effect: &IntentEffect| {
        let rule = effect.rule.as_ref()?;
        Some(gaslightmq_journal::Revision::new(rule.revision.number()))
    };
    let pads = ChangeSource::new(ChangeSurface::Pad);

    // Someone presses the empty pad, which makes its rule and arms it. A second press finds it armed.
    let arm = Intent::Arm { pad: pad.clone() };
    let pressed = host.send_intent(staging.name(), &arm, pads).await?;
    let pressed_again = host.send_intent(staging.name(), &arm, pads).await?;
    // They turn the delay up by two steps of 50 ms, and press the pad to disarm it.
    let two_steps_up = EncoderTurn::new(Encoder::Delay, Steps::new(2));
    let slower = Intent::Turn {
        pad: pad.clone(),
        turn: two_steps_up,
    };
    let turned = host.send_intent(staging.name(), &slower, pads).await?;
    let released = host.send_intent(staging.name(), &Intent::Disarm { pad }, pads).await?;

    // The press that changed nothing left no entry. The turn says which parameter it changed, from what to what.
    assert!(!pressed_again.changed);
    let journal = host.journal(staging.name(), None)?;
    let said: Vec<(u64, Option<gaslightmq_journal::Revision>, ChangeSource, Change)> = journal
        .into_iter()
        .map(|entry| (entry.id.value(), entry.revision, entry.from, entry.change))
        .collect();
    assert_eq!(
        said,
        [
            (1, left(&pressed), pads, Change::LiveRuleMade { rule: rule.clone() }),
            (2, left(&pressed), pads, Change::RuleArmed { rule: rule.clone() }),
            (
                3,
                left(&turned),
                pads,
                Change::ParameterChanged {
                    rule: rule.clone(),
                    parameter: Parameter::Delay,
                    old: "1s".to_owned(),
                    new: "1100ms".to_owned(),
                }
            ),
            (4, left(&released), pads, Change::RuleDisarmed { rule }),
        ]
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_armed_rule_follows_a_reload_and_keeps_what_it_counted() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let directory = tempfile::tempdir()?;
    fs::write(
        directory.path().join("selectors.yaml"),
        "- name: order-producers\n  match:\n    api: Produce\n",
    )?;
    fs::write(
        directory.path().join("presets.yaml"),
        "- name: hold-20ms\n  lie: timing\n  delay: 20ms\n",
    )?;
    let rules = directory.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("producers.yaml"),
        "- name: hold-order-producers\n  selector: order-producers\n  action: hold-20ms\n",
    )?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
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
    let producers: RuleName = "hold-order-producers".parse()?;
    let rule = host.workspace().await.stores().rules().get(&producers).await?;
    let rule = rule.context("the workspace read the rule of the producers")?;
    host.arm(staging.name(), &rule, None, ChangeSurface::Test).await?;
    // One Produce goes through the proxy, and the rule counts it.
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let written = KafkaProducer::from(bootstrap)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    assert_eq!(written, Ok(Offset::new(0)));

    // Someone edits the rule to hold every 2nd Produce, and reloads.
    fs::write(
        rules.join("producers.yaml"),
        "- name: hold-order-producers\n  selector: order-producers\n  action: hold-20ms\n  trigger: { nth: 2 }\n",
    )?;
    let mut changes = host.changes();
    let reloaded = host.reload().await?;

    assert_eq!(reloaded.changes, [DefinitionChange::Updated(producers.clone().into())]);
    // The rule stays armed with the definition of the file, at the revision of the reload.
    let every_2nd = TriggerParts::new().with_nth(MatchNumber::new(2)?);
    let edited = rule.with_trigger(every_2nd);
    assert_eq!(
        host.rule_of(staging.name(), &producers).await?,
        Some(EnvironmentRule {
            rule: edited,
            live: false,
            state: RuleState::Armed,
            revision: reloaded.revision,
        })
    );
    // The proxy holds the edited rule, which still counts the Produce from before the reload.
    let armed = host.armed_rules(staging.name())?;
    let followed = armed.get(&producers).context("the rule is still armed")?;
    assert_eq!(followed.arming().trigger(), Trigger::from(every_2nd));
    assert_eq!(followed.arming().counter().matches(), 1);
    // Whoever follows the host learns of the definition, and that the armed rule follows it.
    let mut told = Vec::new();
    while told.len() < 2 {
        let change = tokio::time::timeout(PATIENCE, changes.next()).await??;
        if matches!(
            change,
            HostChange::DefinitionChanged { .. } | HostChange::RuleChanged { .. }
        ) {
            told.push(change);
        }
    }
    assert_eq!(
        told,
        [
            HostChange::DefinitionChanged {
                name: producers.clone().into()
            },
            HostChange::RuleChanged {
                environment: staging.name().clone(),
                rule: producers,
            },
        ]
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_armed_rule_whose_file_went_disarms_with_the_reload_that_the_journal_records() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let directory = tempfile::tempdir()?;
    fs::write(
        directory.path().join("selectors.yaml"),
        "- name: order-fetches\n  match:\n    api: Fetch\n",
    )?;
    fs::write(
        directory.path().join("presets.yaml"),
        "- name: hold-20ms\n  lie: timing\n  delay: 20ms\n",
    )?;
    let rules = directory.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("fetches.yaml"),
        "- name: hold-order-fetches\n  selector: order-fetches\n  action: hold-20ms\n",
    )?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
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
    let fetches: RuleName = "hold-order-fetches".parse()?;
    let rule = host.workspace().await.stores().rules().get(&fetches).await?;
    let rule = rule.context("the workspace read the rule of the fetches")?;
    host.arm(staging.name(), &rule, None, ChangeSurface::Test).await?;

    // A colleague's branch deletes the file of the rule, and someone reloads.
    fs::remove_file(rules.join("fetches.yaml"))?;
    let mut changes = host.changes();
    let reloaded = host.reload().await?;

    assert_eq!(reloaded.changes, [DefinitionChange::Deleted(fetches.clone().into())]);
    // The rule is armed no more, and the environment holds no rule of its name.
    assert!(host.armed_rules(staging.name())?.get(&fetches).is_none());
    assert_eq!(host.rule_of(staging.name(), &fetches).await?, None);
    // The journal says that the rule disarmed and went, both from the files, with the revision of the reload.
    let file = ChangeSource::new(ChangeSurface::File);
    let left = Some(gaslightmq_journal::Revision::new(reloaded.revision.number()));
    let journal = host.journal(staging.name(), None)?;
    let said: Vec<(Option<gaslightmq_journal::Revision>, ChangeSource, Change)> = journal
        .into_iter()
        .skip_while(|entry| entry.revision != left)
        .map(|entry| (entry.revision, entry.from, entry.change))
        .collect();
    let rule = RecordedName::new(fetches.as_str())?;
    assert_eq!(
        said,
        [
            (left, file, Change::RuleDisarmed { rule: rule.clone() }),
            (
                left,
                file,
                Change::DefinitionDeleted {
                    definition: RecordedDefinition::Rule(rule)
                }
            ),
        ]
    );
    // Whoever follows the host learns that the definition went, which takes the rule out of every environment.
    let deleted = HostChange::DefinitionDeleted { name: fetches.into() };
    while tokio::time::timeout(PATIENCE, changes.next()).await?? != deleted {}
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn a_reload_that_a_live_rule_stands_in_the_way_of_changes_nothing() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let directory = tempfile::tempdir()?;
    let selectors = directory.path().join("selectors.yaml");
    fs::write(&selectors, "- name: order-producers\n  match:\n    api: Produce\n")?;
    fs::write(
        directory.path().join("presets.yaml"),
        "- name: hold-20ms\n  lie: timing\n  delay: 20ms\n",
    )?;
    let stores = FileStores::open(directory.path(), Access::ReadWrite).await?;
    let host = Host::new(
        HostConfiguration::default(),
        FileContext::new(stores),
        Lies::of_every_build(),
        RUN,
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
    // Someone pressed a pad, and the staging proxy holds every Produce with a live rule.
    let order_producers: SelectorName = "order-producers".parse()?;
    let live = Rule::new(
        "hold-the-produces".parse()?,
        order_producers.clone(),
        "hold-20ms".parse::<PresetName>()?,
    );
    host.create_live_rule_armed(staging.name(), live.clone(), ChangeSurface::Pad)
        .await?;

    // A colleague's branch brings a rule of the workspace with the name of the live rule.
    let rules = directory.path().join("rules");
    fs::create_dir(&rules)?;
    fs::write(
        rules.join("produces.yaml"),
        "- name: hold-the-produces\n  selector: order-producers\n  action: hold-20ms\n",
    )?;
    let taken = host.reload().await;

    assert_eq!(
        taken,
        Err(ReloadWorkspaceError::NameTaken {
            rule: live.name().clone(),
            environment: staging.name().clone(),
        })
    );
    assert_eq!(
        host.workspace().await.stores().rules().list().await?,
        Vec::<Rule>::new()
    );

    // Another branch takes away the selector that the armed live rule uses.
    fs::remove_file(rules.join("produces.yaml"))?;
    fs::write(&selectors, "- name: order-fetches\n  match:\n    api: Fetch\n")?;
    let unresolved = host.reload().await;

    assert_eq!(
        unresolved,
        Err(ReloadWorkspaceError::Unresolved {
            environment: staging.name().clone(),
            rule: live.name().clone(),
            reason: "rule \"hold-the-produces\": no selector is named \"order-producers\"".to_owned(),
        })
    );
    let kept = host
        .workspace()
        .await
        .stores()
        .selectors()
        .get(&order_producers)
        .await?;
    assert!(
        kept.is_some(),
        "the workspace holds the selector that the live rule uses"
    );
    let still_armed = host.rule_of(staging.name(), live.name()).await?;
    assert_eq!(still_armed.map(|rule| rule.state), Some(RuleState::Armed));

    // Once the live rule is gone, nothing stands in the way of the files.
    host.delete_live_rule(staging.name(), live.name(), None, ChangeSurface::Pad)?;
    let reloaded = host.reload().await?;
    assert_eq!(
        reloaded.changes,
        [
            DefinitionChange::Created("order-fetches".parse::<SelectorName>()?.into()),
            DefinitionChange::Deleted(order_producers.into()),
        ]
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_armed_rule_follows_an_update_of_its_definition_and_keeps_what_it_counted() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let hold = Preset::new(
        "hold-20ms".parse()?,
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let rule = Rule::new(
        "hold-order-producers".parse()?,
        order_producers.name().clone(),
        hold.name().clone(),
    );
    host.create(order_producers, ChangeSurface::Test).await?;
    host.create(hold, ChangeSurface::Test).await?;
    host.create(rule.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.arm(staging.name(), &rule, None, ChangeSurface::Test).await?;
    // One Produce goes through the proxy, and the rule counts it.
    let bootstrap = host
        .address_of(staging.name(), Endpoint::Bootstrap)?
        .context("the bootstrap endpoint of the staging proxy is open")?;
    let written = KafkaProducer::from(bootstrap)
        .send(
            orders.name().as_str(),
            Partition::new(0),
            &[Record::new("placed").with_key("o-1").with_timestamp(PLACED_AT)],
        )
        .await;
    assert_eq!(written, Ok(Offset::new(0)));
    let mut changes = host.changes();

    // Someone edits the rule in the UI to hold every 2nd Produce.
    let every_2nd = TriggerParts::new().with_nth(MatchNumber::new(2)?);
    let edited = rule.clone().with_trigger(every_2nd);
    let ui = ChangeSource::new(ChangeSurface::Ui);
    let updated = host.update(edited.clone(), None, ui).await?;

    // The rule stays armed with its new definition, and the proxy still counts the Produce from before.
    assert_eq!(
        host.rule_of(staging.name(), rule.name()).await?,
        Some(EnvironmentRule {
            rule: edited,
            live: false,
            state: RuleState::Armed,
            revision: updated,
        })
    );
    let armed = host.armed_rules(staging.name())?;
    let followed = armed.get(rule.name()).context("the rule is still armed")?;
    assert_eq!(followed.arming().trigger(), Trigger::from(every_2nd));
    assert_eq!(followed.arming().counter().matches(), 1);
    let journal = host.journal(staging.name(), None)?;
    let last = journal.last().context("the journal recorded the update")?;
    assert_eq!(
        (last.from, &last.change),
        (
            ui,
            &Change::DefinitionChanged {
                definition: RecordedDefinition::Rule(RecordedName::new(rule.name().as_str())?)
            }
        )
    );
    // Whoever follows the host learns of the definition, and that the armed rule follows it.
    let mut told = Vec::new();
    while told.len() < 2 {
        let change = tokio::time::timeout(PATIENCE, changes.next()).await??;
        if matches!(
            change,
            HostChange::DefinitionChanged { .. } | HostChange::RuleChanged { .. }
        ) {
            told.push(change);
        }
    }
    assert_eq!(
        told,
        [
            HostChange::DefinitionChanged {
                name: rule.name().clone().into()
            },
            HostChange::RuleChanged {
                environment: staging.name().clone(),
                rule: rule.name().clone(),
            },
        ]
    );
    host.stop_all().await;
    Ok(())
}

#[tokio::test]
async fn an_armed_rule_whose_definition_someone_deletes_disarms_and_an_armed_live_rule_keeps_its_selector()
-> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
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
    let order_fetches = Selector::new("order-fetches".parse()?, Conditions::from(Condition::is(ApiKey::FETCH)));
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
    );
    let hold = Preset::new(
        "hold-20ms".parse()?,
        TimingLie::new().with_delay(TimeSpan::from_millis(20)),
    );
    let of_the_workspace = Rule::new(
        "hold-order-fetches".parse()?,
        order_fetches.name().clone(),
        hold.name().clone(),
    );
    let live = Rule::new(
        "hold-the-produces".parse()?,
        order_producers.name().clone(),
        hold.name().clone(),
    );
    host.create(order_fetches, ChangeSurface::Test).await?;
    let producers_created = host.create(order_producers.clone(), ChangeSurface::Test).await?;
    host.create(hold, ChangeSurface::Test).await?;
    host.create(of_the_workspace.clone(), ChangeSurface::Test).await?;
    host.start(EnvironmentConfiguration::new(&staging)?).await?;
    host.arm(staging.name(), &of_the_workspace, None, ChangeSurface::Test)
        .await?;
    host.create_live_rule_armed(staging.name(), live.clone(), ChangeSurface::Pad)
        .await?;

    // A delete that expects what is no more is refused for that first, so an undo that came too late drops its step.
    let earlier = Expects {
        run: host.run(),
        revision: host.workspace().await.loaded(),
    };
    let stale = host
        .delete(order_producers.name().clone(), Some(earlier), ChangeSurface::Ui)
        .await;
    assert_eq!(
        stale,
        Err(DeleteDefinitionError::NotDeleted {
            error: DeleteError::Changed {
                name: order_producers.name().clone().into(),
                revision: producers_created,
            }
        })
    );

    // The armed live rule uses the selector, so the selector stays.
    let kept = host
        .delete(order_producers.name().clone(), None, ChangeSurface::Ui)
        .await;
    assert_eq!(
        kept,
        Err(DeleteDefinitionError::Unresolved {
            name: order_producers.name().clone().into(),
            environment: staging.name().clone(),
            rule: live.name().clone(),
            reason: "rule \"hold-the-produces\": no selector is named \"order-producers\"".to_owned(),
        })
    );

    // The rule of the workspace goes while it is armed, and disarms with it.
    let ui = ChangeSource::new(ChangeSurface::Ui);
    let deleted = host.delete(of_the_workspace.name().clone(), None, ui).await?;

    assert!(host.armed_rules(staging.name())?.get(of_the_workspace.name()).is_none());
    assert_eq!(host.rule_of(staging.name(), of_the_workspace.name()).await?, None);
    let left = Some(gaslightmq_journal::Revision::new(deleted.number()));
    let said: Vec<(Option<gaslightmq_journal::Revision>, ChangeSource, Change)> = host
        .journal(staging.name(), None)?
        .into_iter()
        .skip_while(|entry| entry.revision != left)
        .map(|entry| (entry.revision, entry.from, entry.change))
        .collect();
    let rule = RecordedName::new(of_the_workspace.name().as_str())?;
    assert_eq!(
        said,
        [
            (left, ui, Change::RuleDisarmed { rule: rule.clone() }),
            (
                left,
                ui,
                Change::DefinitionDeleted {
                    definition: RecordedDefinition::Rule(rule)
                }
            ),
        ]
    );
    host.stop_all().await;
    Ok(())
}
