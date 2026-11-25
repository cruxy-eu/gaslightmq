//! A host without a real cluster: what it refuses, what it says about an environment that does not run,
//! and what it reads with the names of its workspace.
//!
//! The tests in `tests/real_cluster` run environments in front of real Kafka brokers.

use std::net::TcpListener;

use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_host::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_quick_inject::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_workspace::prelude::*;

/// How long the backend waits for a cluster that never answers, before the start gives up.
const PATIENCE: TimeSpan = TimeSpan::from_millis(200);

#[tokio::test]
async fn an_environment_whose_cluster_never_answers_does_not_start() -> anyhow::Result<()> {
    // A server that takes connections and never answers a request.
    let silent = TcpListener::bind("127.0.0.1:0")?;
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
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
async fn an_environment_that_does_not_run_has_no_listeners_events_or_rules() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let not_running = NotRunningError { name: staging.clone() };

    assert_eq!(host.address_of(&staging, Endpoint::Bootstrap), Err(not_running.clone()));
    assert_eq!(host.endpoints(&staging), Err(not_running.clone()));
    assert!(host.live_events(&staging).is_err());
    assert!(host.every_event(&staging).is_err());
    assert!(host.rules(&staging).is_err());
    assert_eq!(host.stop(&staging).await, Err(not_running));
    Ok(())
}

#[tokio::test]
async fn a_rule_resolves_against_the_workspace_before_it_is_armed() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let staging: EnvironmentName = "staging-proxy".parse()?;
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    );
    host.workspace().await.create(order_producers.clone()).await?;
    let blackhole: PresetName = "blackhole".parse()?;
    let rule = Rule::new(
        "blackhole-order-producers".parse()?,
        order_producers.name().clone(),
        blackhole.clone(),
    );

    // The rule names a preset that the workspace does not have.
    let Err(unresolved) = host.arm(&staging, &rule).await else {
        anyhow::bail!("a rule was armed with a preset that does not exist");
    };
    assert!(matches!(unresolved, ArmRuleError::Unresolved { .. }), "{unresolved:?}");
    assert_eq!(
        unresolved.to_string(),
        "rule \"blackhole-order-producers\": no preset is named \"blackhole\""
    );

    // Once the workspace has it, the rule resolves, and only an environment that runs can arm it.
    host.workspace()
        .await
        .create(Preset::new(blackhole, LieDefinition::Silence))
        .await?;
    let Err(not_running) = host.arm(&staging, &rule).await else {
        anyhow::bail!("a rule was armed in an environment that does not run");
    };
    assert_eq!(not_running, ArmRuleError::NotRunning { environment: staging });
    Ok(())
}

#[tokio::test]
async fn a_sentence_knows_the_presets_and_the_selectors_of_the_workspace() -> anyhow::Result<()> {
    let host = Host::new(
        HostConfiguration::default(),
        MemoryContext::new(),
        Lies::of_every_build(),
    );
    let leader_flap = Preset::new(
        "leader-flap".parse()?,
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    let order_producers = Selector::new(
        "order-producers".parse()?,
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    )
    .with_title("Order producers".parse()?);
    host.workspace().await.create(leader_flap).await?;
    host.workspace().await.create(order_producers).await?;

    // Someone types the start of a word of the title of a selector, and the sentence offers its name.
    let typing = host.read_sentence("leader-flap prod").await?;
    let offered: Vec<&str> = typing
        .completions()
        .iter()
        .filter(|completion| completion.kind() == CompletionKind::Selector)
        .map(Completion::sentence)
        .collect();
    assert_eq!(offered, ["leader-flap order-producers "]);

    // The sentence that names both is a rule with the preset and the selector of the workspace.
    let sentence = host.read_sentence("leader-flap order-producers for 3 firings").await?;
    let rule = sentence
        .rule()
        .ok_or_else(|| anyhow::anyhow!("no rule in {sentence:?}"))?;
    assert_eq!(rule.action().preset().map(PresetName::as_str), Some("leader-flap"));
    assert_eq!(rule.lifetime().to_string(), "3 firings");
    assert!(sentence.unread().is_empty(), "{:?}", sentence.unread());
    Ok(())
}
