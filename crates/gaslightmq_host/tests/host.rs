//! A host without a real cluster: what it refuses, what it says about an environment that does not run,
//! and what it tells whoever follows it.
//!
//! The tests in `tests/real_cluster` run environments in front of real Kafka brokers.

use std::net::TcpListener;

use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::{Column, Intent, Layer, Pad, Row};
use gaslightmq_workspace::prelude::*;

/// How long the backend waits for a cluster that never answers, before the start gives up.
const PATIENCE: TimeSpan = TimeSpan::from_millis(200);

/// The run of the host of a test. A test names its own, and `gmq` makes one from the randomness of the system.
const RUN: RunId = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);

/// The run of the process before it started again: the one that a client that read back then still names.
const EARLIER_RUN: RunId = RunId::from_random_bits(0x1f0a_52c7_9be3_4d16_8a70_3c5e_d294_6b08);

#[tokio::test]
async fn an_environment_whose_cluster_never_answers_does_not_start() -> anyhow::Result<()> {
    // A server that takes connections and never answers a request.
    let silent = TcpListener::bind("127.0.0.1:0")?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    // The cluster names no broker, so nothing ever listens on these ports.
    let listen = Listeners::new(
        "127.0.0.1:0".parse()?,
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
    let mut configuration = EnvironmentConfiguration::new(&staging)?;
    configuration.upstream.request_timeout = PATIENCE;

    let started = host.start(configuration).await;

    let Err(EnvironmentStartError::BackendNotStarted { name, error }) = started else {
        anyhow::bail!("the environment started without its cluster: {started:?}");
    };
    assert_eq!(&name, staging.name());
    assert!(
        matches!(error, UpstreamBackendError::ClusterUndiscovered { .. }),
        "{error:?}"
    );
    // Nothing of the environment runs.
    assert_eq!(host.running(), Vec::<EnvironmentName>::new());
    assert_eq!(
        host.address_of(staging.name(), Endpoint::Bootstrap),
        Err(NotRunningError { name })
    );
    Ok(())
}

#[tokio::test]
async fn an_environment_of_the_start_that_did_not_start_is_one_of_the_start_that_does_not_run() -> anyhow::Result<()> {
    // A server that takes connections and never answers a request.
    let silent = TcpListener::bind("127.0.0.1:0")?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let listen = Listeners::new(
        "127.0.0.1:0".parse()?,
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
    let mut configuration = EnvironmentConfiguration::new(&staging)?;
    configuration.upstream.request_timeout = PATIENCE;
    assert_eq!(host.started_with(), Vec::<EnvironmentName>::new());

    let started = host.start_with([configuration]).await;

    assert!(
        matches!(started, Err(EnvironmentStartError::BackendNotStarted { .. })),
        "{started:?}"
    );
    // The process started with the environment, and it does not run.
    assert_eq!(host.started_with(), [staging.name().clone()]);
    assert_eq!(host.running(), Vec::<EnvironmentName>::new());
    Ok(())
}

#[tokio::test]
async fn an_environment_that_does_not_run_has_no_listeners_events_rules_or_journal() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let not_running = NotRunningError { name: staging.clone() };

    assert_eq!(host.address_of(&staging, Endpoint::Bootstrap), Err(not_running.clone()));
    assert_eq!(host.endpoints(&staging), Err(not_running.clone()));
    assert!(host.live_events(&staging).is_err());
    assert!(host.every_event(&staging).is_err());
    assert!(host.armed_rules(&staging).is_err());
    assert_eq!(host.journal(&staging, None), Err(not_running.clone()));
    assert_eq!(host.incident_support(&staging), Err(not_running.clone()));
    assert_eq!(
        host.fire(&staging, &"leader-flap".parse()?, ChangeSurface::Test).await,
        Err(FireRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(host.stop(&staging).await, Err(not_running));
    Ok(())
}

#[tokio::test]
async fn a_rule_resolves_against_the_workspace_before_it_is_armed() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    );
    host.create(order_producers.clone(), ChangeSurface::Test).await?;
    let blackhole: PresetName = "blackhole".parse()?;
    let rule = Rule::new(
        "blackhole-order-producers".parse()?,
        order_producers.name().clone(),
        blackhole.clone(),
    );

    // The rule names a preset that the workspace does not have.
    let Err(unresolved) = host.arm(&staging, &rule, None, ChangeSurface::Test).await else {
        anyhow::bail!("a rule was armed with a preset that does not exist");
    };
    assert!(matches!(unresolved, ArmRuleError::Unresolved { .. }), "{unresolved:?}");
    assert_eq!(
        unresolved.to_string(),
        "rule \"blackhole-order-producers\": no preset is named \"blackhole\""
    );

    // Once the workspace has it, the rule resolves, and only an environment that runs can arm it.
    host.create(Preset::new(blackhole, LieDefinition::Silence), ChangeSurface::Test)
        .await?;
    let Err(not_running) = host.arm(&staging, &rule, None, ChangeSurface::Test).await else {
        anyhow::bail!("a rule was armed in an environment that does not run");
    };
    assert_eq!(not_running, ArmRuleError::NotRunning { environment: staging });
    Ok(())
}

#[tokio::test]
async fn a_follower_of_the_host_learns_of_every_change_of_a_definition() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let mut changes = host.changes();
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    );

    host.create(order_producers.clone(), ChangeSurface::Test).await?;
    host.update(
        order_producers.clone().with_title("Order producers".parse()?),
        None,
        ChangeSurface::Test,
    )
    .await?;
    host.delete(order_producers.name().clone(), None, ChangeSurface::Test)
        .await?;

    // No environment runs, so no journal records the changes, and no entry comes between them.
    let name = DefinitionName::from(order_producers.name().clone());
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionChanged { name: name.clone() }
    );
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionChanged { name: name.clone() }
    );
    assert_eq!(changes.next().await?, HostChange::DefinitionDeleted { name });
    Ok(())
}

#[tokio::test]
async fn live_rules_pads_and_sessions_need_an_environment_that_runs() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let silence = Rule::new("blackhole".parse()?, Conditions::new(), LieDefinition::Silence);
    let pad = Pad::new(
        "order-desk".parse()?,
        Layer::all_clients(),
        Row::from_index(0),
        Column::from_index(0),
    );

    assert_eq!(
        host.rules_of(&staging).await,
        Err(EnvironmentRulesError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.create_live_rule(&staging, silence.clone(), ChangeSurface::Test)
            .await,
        Err(CreateLiveRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.create_live_rule_armed(&staging, silence.clone(), ChangeSurface::Test)
            .await,
        Err(CreateLiveRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.update_live_rule(&staging, silence.clone(), None, ChangeSurface::Test)
            .await,
        Err(UpdateLiveRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.delete_live_rule(&staging, silence.name(), None, ChangeSurface::Test),
        Err(DeleteLiveRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.disarm(&staging, silence.name(), None, ChangeSurface::Test).await,
        Err(DisarmRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.keep(&staging, silence.name(), None, ChangeSurface::Test).await,
        Err(KeepRuleError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.send_intent(&staging, &Intent::Arm { pad }, ChangeSurface::Pad)
            .await,
        Err(IntentError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.close_session(&staging, ConnectionNumber::new(7)),
        Err(CloseSessionError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.follow_calls(&staging, &SelectorRef::Inline(Conditions::new()))
            .await
            .map(|_| ()),
        Err(FollowCallsError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(
        host.read_sentence(&staging, "delay produce").await.map(|_| ()),
        Err(ReadSentenceError::NotRunning {
            environment: staging.clone()
        })
    );
    assert_eq!(host.sessions(&staging), Err(NotRunningError { name: staging.clone() }));
    assert_eq!(
        host.statistics(&staging).map(|_| ()),
        Err(NotRunningError { name: staging })
    );
    Ok(())
}

#[tokio::test]
async fn every_change_of_a_definition_answers_with_the_revision_it_left() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);

    let created = host.create(blackhole.clone(), ChangeSurface::Test).await?;
    let updated = host
        .update(
            blackhole.clone().with_title("Blackhole".parse()?),
            None,
            ChangeSurface::Test,
        )
        .await?;

    // Every change takes the next number of the one counter of the process, so a later change has a higher revision.
    assert!(created < updated, "{created} and then {updated}");
    // The answer is the revision that the workspace holds beside the definition.
    assert_eq!(host.workspace().await.revision(blackhole.name().clone()), updated);
    let deleted = host.delete(blackhole.name().clone(), None, ChangeSurface::Test).await?;
    assert!(updated < deleted, "{updated} and then {deleted}");
    assert_eq!(host.run(), RUN);
    Ok(())
}

#[tokio::test]
async fn a_change_that_expects_another_revision_changes_nothing() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let mut changes = host.changes();
    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);
    // Two tabs read the preset as it was created.
    let seen = Expects {
        run: host.run(),
        revision: host.create(blackhole.clone(), ChangeSurface::Test).await?,
    };
    // The first tab gives it a title.
    let titled = blackhole.clone().with_title("Blackhole".parse()?);
    let now = host.update(titled.clone(), Some(seen), ChangeSurface::Test).await?;

    // The second tab changes and deletes what it saw, which is no longer what the workspace holds.
    let late = blackhole.clone().with_title("Silence".parse()?);
    let updated_late = host.update(late, Some(seen), ChangeSurface::Test).await;
    let deleted_late = host
        .delete(blackhole.name().clone(), Some(seen), ChangeSurface::Test)
        .await;

    let name = DefinitionName::from(blackhole.name().clone());
    assert_eq!(
        updated_late,
        Err(UpdateDefinitionError::NotUpdated {
            error: UpdateError::Changed {
                name: name.clone(),
                revision: now,
            },
        })
    );
    assert_eq!(
        deleted_late,
        Err(DeleteDefinitionError::NotDeleted {
            error: DeleteError::Changed {
                name: name.clone(),
                revision: now,
            },
        })
    );
    // The preset is as the first tab left it, and whoever follows the host learned of two changes, and no third.
    let workspace = host.workspace().await;
    assert_eq!(workspace.stores().presets().get(blackhole.name()).await?, Some(titled));
    assert_eq!(workspace.revision(blackhole.name().clone()), now);
    drop(workspace);
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionChanged { name: name.clone() }
    );
    assert_eq!(
        changes.next().await?,
        HostChange::DefinitionChanged { name: name.clone() }
    );
    // The next change that the follower learns of is the delete of whoever saw the title.
    let seen_now = Expects {
        run: host.run(),
        revision: now,
    };
    host.delete(blackhole.name().clone(), Some(seen_now), ChangeSurface::Test)
        .await?;
    assert_eq!(changes.next().await?, HostChange::DefinitionDeleted { name });
    Ok(())
}

#[tokio::test]
async fn a_change_that_expects_a_revision_of_another_run_changes_nothing() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
        RUN,
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let blackhole = Preset::new("blackhole".parse()?, LieDefinition::Silence);
    let silence = Rule::new(
        "blackhole-everything".parse()?,
        Conditions::new(),
        blackhole.name().clone(),
    );
    let created = host.create(blackhole.clone(), ChangeSurface::Test).await?;
    // A client read the preset before the process started again. The load gave it the same number in both runs,
    // so the revision alone fits, and the run does not.
    let before_the_restart = Some(Expects {
        run: EARLIER_RUN,
        revision: created,
    });

    let titled = blackhole.clone().with_title("Blackhole".parse()?);
    assert_eq!(
        host.update(titled, before_the_restart, ChangeSurface::Test).await,
        Err(UpdateDefinitionError::OtherRun { run: RUN })
    );
    assert_eq!(
        host.delete(blackhole.name().clone(), before_the_restart, ChangeSurface::Test)
            .await,
        Err(DeleteDefinitionError::OtherRun { run: RUN })
    );
    // No change of a rule in an environment looks further than the run either.
    assert_eq!(
        host.arm(&staging, &silence, before_the_restart, ChangeSurface::Test)
            .await,
        Err(ArmRuleError::OtherRun { run: RUN })
    );
    assert_eq!(
        host.disarm(&staging, silence.name(), before_the_restart, ChangeSurface::Test)
            .await,
        Err(DisarmRuleError::OtherRun { run: RUN })
    );
    assert_eq!(
        host.update_live_rule(&staging, silence.clone(), before_the_restart, ChangeSurface::Test)
            .await,
        Err(UpdateLiveRuleError::OtherRun { run: RUN })
    );
    assert_eq!(
        host.delete_live_rule(&staging, silence.name(), before_the_restart, ChangeSurface::Test),
        Err(DeleteLiveRuleError::OtherRun { run: RUN })
    );
    assert_eq!(
        host.keep(&staging, silence.name(), before_the_restart, ChangeSurface::Test)
            .await,
        Err(KeepRuleError::OtherRun { run: RUN })
    );
    // The preset is as it was created.
    let workspace = host.workspace().await;
    assert_eq!(
        workspace.stores().presets().get(blackhole.name()).await?,
        Some(blackhole.clone())
    );
    assert_eq!(workspace.revision(blackhole.name().clone()), created);
    Ok(())
}
