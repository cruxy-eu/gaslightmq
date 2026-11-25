//! One process with several environments.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::future::join_all;
use gaslightmq_backend_upstream::UpstreamBackend;
use gaslightmq_clock_waitable::WallClock;
use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_engine::{ArmError, ArmedRules, Engine, EveryEvent, LiveEvents};
use gaslightmq_environment::{EnvironmentName, Listeners};
use gaslightmq_field::Endpoint;
use gaslightmq_lie_api::Lies;
use gaslightmq_quick_inject::{KnownNames, QuickInject, Sentence};
use gaslightmq_rule::{ArmedRule, Preset, PresetSet, Rule};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_workspace::{Stores, Workspace, WorkspaceContext};

use crate::{
    ArmRuleError, EnvironmentConfiguration, EnvironmentStartError, HostConfiguration, NotRunningError,
    ReadSentenceError,
};

/// One process with several environments: it runs the proxy of each, and shares the workspace between them.
///
/// | Part | Is |
/// |---|---|
/// | The [workspace](Self::workspace) | The definitions of every environment, over the context the host was handed. |
/// | The environments that run | Each one by its name, with its own proxy: its listeners, sessions, armed rules and events. |
/// | The lies of the build | One behaviour per kind, which the proxy of every environment tells. |
/// | Quick inject | Reads a ⌘K sentence with the names of the workspace. |
///
/// # Environments
///
/// An environment runs from [`start`](Self::start) until someone [stops](Self::stop) it.
/// Each one is separate: arming a rule in one does not arm it in another,
/// and a client of one never reaches the proxy of another.
/// What runs is asked by the name of the environment, and an environment that does not run is a [`NotRunningError`].
///
/// The host starts and stops one environment at a time, so two starts never claim the same port.
/// It refuses an environment whose listeners meet those of one that runs, before it opens anything.
/// A listener on port 0 takes any free port, and meets nothing.
///
/// A host that someone lets go of lets go of its environments, and each one stops a moment later.
/// Whoever needs every address free again [stops them all](Self::stop_all) first.
///
/// # The workspace
///
/// The host holds the workspace over the context it is handed, and names no store.
/// Whoever puts a process together picks the stores there.
/// [`arm`](Self::arm) resolves a rule against the selectors and the presets of the workspace,
/// and [`read_sentence`](Self::read_sentence) offers their names.
///
/// ```
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ReadSentenceError> {
/// let host = Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build());
/// let quiet = Preset::new("quiet-orders".parse().expect("a preset name"), LieDefinition::Silence);
/// host.workspace().await.create(quiet).await.expect("a workspace in memory keeps it");
///
/// // Someone types in the ⌘K palette, and the sentence knows the presets of the workspace.
/// let sentence = host.read_sentence("quiet-orders for 3 firings").await?;
/// let rule = sentence.rule().expect("the sentence names a preset");
/// assert_eq!(rule.action().preset().map(PresetName::as_str), Some("quiet-orders"));
/// assert!(host.running().is_empty());
/// # Ok(())
/// # }
/// ```
pub struct Host<C: WorkspaceContext> {
    quick_inject: QuickInject,
    /// The workspace. A change takes it exclusively, so its check and its write belong together.
    workspace: tokio::sync::Mutex<Workspace<C>>,
    lies: Arc<Lies>,
    /// The environments that run, by name. Nobody holds the lock across a wait.
    environments: Mutex<BTreeMap<EnvironmentName, Running>>,
    /// Held by a start or a stop, from the check of the listeners until the environment runs or stopped.
    changing: tokio::sync::Mutex<()>,
}

/// One environment that runs: its proxy, and the listeners it was started with.
struct Running {
    engine: Engine<UpstreamBackend, WallClock>,
    listeners: Listeners,
}

impl<C: WorkspaceContext> Host<C> {
    /// A host that runs no environment yet, over the workspace of `context`.
    ///
    /// The proxy of every environment tells the `lies` of this build.
    pub fn new(configuration: HostConfiguration, context: C, lies: Lies) -> Self {
        Self {
            quick_inject: QuickInject::new(configuration.quick_inject),
            workspace: tokio::sync::Mutex::new(Workspace::new(context)),
            lies: Arc::new(lies),
            environments: Mutex::new(BTreeMap::new()),
            changing: tokio::sync::Mutex::new(()),
        }
    }

    /// The workspace, to read and to change.
    ///
    /// Every environment shares it. A change takes it exclusively,
    /// so the workspace waits for whoever holds it, and holding it across a wait for something else holds up the rest.
    pub async fn workspace(&self) -> impl DerefMut<Target = Workspace<C>> {
        self.workspace.lock().await
    }

    /// Starts an environment: its backend in front of the real cluster, and its proxy in front of the backend.
    ///
    /// Where `configuration` comes from is up to the caller:
    /// an environment of the workspace, or one that the flags of a command describe.
    /// The environment runs under its name, and its tasks run on the runtime that the caller runs on.
    ///
    /// The call waits until the backend knows the brokers of its cluster, and every endpoint has a listener.
    /// The proxy accepts connections from the moment the call returns.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentStartError::AlreadyRunning`] when an environment of that name runs.
    /// - [`EnvironmentStartError::SharedListener`] when a listener would take a port that an environment that runs
    ///   has named on the same address.
    /// - [`EnvironmentStartError::BackendNotStarted`] when the backend did not learn the brokers of its cluster,
    ///   or the environment asks it for what it cannot do yet.
    /// - [`EnvironmentStartError::ProxyNotStarted`] when an endpoint of the cluster got no listener.
    ///
    /// Nothing of the environment runs then.
    pub async fn start(&self, configuration: EnvironmentConfiguration) -> Result<(), EnvironmentStartError> {
        let _changing = self.changing.lock().await;
        let EnvironmentConfiguration { name, engine, upstream } = configuration;
        self.check_listeners(&name, &engine.listeners)?;

        let backend = match UpstreamBackend::start(upstream).await {
            Ok(backend) => backend,
            Err(error) => return Err(EnvironmentStartError::BackendNotStarted { name, error }),
        };
        let listeners = engine.listeners.clone();
        let engine = match Engine::start(engine, backend, WallClock::new(), Arc::clone(&self.lies)).await {
            Ok(engine) => engine,
            Err(error) => return Err(EnvironmentStartError::ProxyNotStarted { name, error }),
        };
        self.environments().insert(name, Running { engine, listeners });
        Ok(())
    }

    /// Stops an environment, and waits until its proxy has stopped.
    ///
    /// The proxy takes no longer than the [stop timeout](gaslightmq_engine::EngineConfiguration::stop_timeout)
    /// it was started with. Every address of the environment is free once the call returns,
    /// unless the stop took that whole time: an endpoint that is still open then closes a moment later.
    /// Whoever listens to its events gets no more of them.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub async fn stop(&self, name: &EnvironmentName) -> Result<(), NotRunningError> {
        let _changing = self.changing.lock().await;
        let running = self.environments().remove(name);
        let Some(running) = running else {
            return Err(NotRunningError { name: name.clone() });
        };
        running.engine.stop().await;
        Ok(())
    }

    /// Stops every environment, and waits until each one has stopped.
    ///
    /// The environments stop side by side, so the call takes as long as the slowest of them.
    pub async fn stop_all(&self) {
        let _changing = self.changing.lock().await;
        let running = std::mem::take(&mut *self.environments());
        join_all(running.into_values().map(|running| running.engine.stop())).await;
    }

    /// The names of the environments that run, in the order of their names.
    pub fn running(&self) -> Vec<EnvironmentName> {
        self.environments().keys().cloned().collect()
    }

    /// The address that `endpoint` of an environment accepts connections on,
    /// or `None` while the endpoint is not open.
    ///
    /// It names the port that the system picked, where the listeners asked for port 0.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn address_of(
        &self,
        name: &EnvironmentName,
        endpoint: Endpoint,
    ) -> Result<Option<SocketAddr>, NotRunningError> {
        self.with_running(name, |running| running.engine.address_of(endpoint))
    }

    /// The endpoints of an environment that accept connections at this moment:
    /// `bootstrap` first, then the brokers in the order of their IDs.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn endpoints(&self, name: &EnvironmentName) -> Result<Vec<Endpoint>, NotRunningError> {
        self.with_running(name, |running| running.engine.endpoints())
    }

    /// A listener of what the sessions of an environment hand over from now on, as it happens:
    /// the records of their calls, and the incidents that their rules fire.
    ///
    /// It is for whoever watches, and it misses the oldest events when it falls behind.
    /// See [`Engine::live_events`].
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn live_events(&self, name: &EnvironmentName) -> Result<LiveEvents, NotRunningError> {
        self.with_running(name, |running| running.engine.live_events())
    }

    /// A listener that misses nothing of what the sessions of an environment hand over from now on.
    ///
    /// Every event it did not take yet stays in memory. See [`Engine::every_event`].
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn every_event(&self, name: &EnvironmentName) -> Result<EveryEvent, NotRunningError> {
        self.with_running(name, |running| running.engine.every_event())
    }

    /// The armed rules of an environment: the handle disarms a rule, and arms one that is resolved already.
    ///
    /// The handle stays with the proxy it came from:
    /// once the environment stops, it arms and disarms in a proxy that no session reaches.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn rules(&self, name: &EnvironmentName) -> Result<ArmedRules, NotRunningError> {
        self.with_running(name, |running| running.engine.rules().clone())
    }

    /// Arms `rule` in one environment, and returns the rule as it is armed.
    ///
    /// The rule can be a rule of the workspace, or a live rule that no workspace holds.
    /// It resolves against the selectors and the presets of the workspace,
    /// and arms above every rule that is armed in the environment.
    /// No other environment arms it.
    ///
    /// # Errors
    ///
    /// - [`ArmRuleError::StoreFailed`] when the workspace could not read its selectors or its presets.
    /// - [`ArmRuleError::WorkspaceInvalid`] when they do not hold together, as when selectors include each other in a
    ///   circle.
    /// - [`ArmRuleError::Unresolved`] when the rule names a selector or a preset that the workspace does not have,
    ///   or could never fire.
    /// - [`ArmRuleError::NotRunning`] when no environment of that name runs.
    /// - [`ArmRuleError::AlreadyArmed`] when a rule of that name is armed in the environment.
    pub async fn arm(&self, environment: &EnvironmentName, rule: &Rule) -> Result<ArmedRule, ArmRuleError> {
        let name = rule.name();
        let (selectors, presets) = {
            let workspace = self.workspace.lock().await;
            let stores = workspace.stores();
            let selectors = stores
                .selectors()
                .list()
                .await
                .map_err(|error| ArmRuleError::StoreFailed {
                    rule: name.clone(),
                    reason: unread::<Selector>(error),
                })?;
            let presets = stores
                .presets()
                .list()
                .await
                .map_err(|error| ArmRuleError::StoreFailed {
                    rule: name.clone(),
                    reason: unread::<Preset>(error),
                })?;
            (selectors, presets)
        };
        let invalid = |reason: String| ArmRuleError::WorkspaceInvalid {
            rule: name.clone(),
            reason,
        };
        let selectors = SelectorSet::new(selectors).map_err(|error| invalid(error.to_string()))?;
        let presets = PresetSet::new(presets).map_err(|error| invalid(error.to_string()))?;
        let resolved = rule
            .resolve(&selectors, &presets)
            .map_err(|error| ArmRuleError::Unresolved { error })?;

        let environments = self.environments();
        let Some(running) = environments.get(environment) else {
            return Err(ArmRuleError::NotRunning {
                environment: environment.clone(),
            });
        };
        running
            .engine
            .rules()
            .arm(resolved)
            .map_err(|ArmError::AlreadyArmed { rule }| ArmRuleError::AlreadyArmed {
                environment: environment.clone(),
                rule,
            })
    }

    /// Reads a ⌘K sentence, as someone types it, with the names of the workspace.
    ///
    /// A sentence knows the presets and the selectors of the workspace, with their titles.
    /// It knows no topic, group or client by itself yet: those come from the traffic, which no environment keeps yet.
    /// A sentence names one of those after the word for its kind: `topic orders`.
    ///
    /// Reading changes nothing. The rule of the sentence is a definition, which whoever read it [arms](Self::arm).
    ///
    /// # Errors
    ///
    /// [`ReadSentenceError::StoreFailed`] when the workspace could not read its presets or its selectors.
    pub async fn read_sentence(&self, text: &str) -> Result<Sentence, ReadSentenceError> {
        let (presets, selectors) = {
            let workspace = self.workspace.lock().await;
            let stores = workspace.stores();
            let presets = stores
                .presets()
                .list()
                .await
                .map_err(|error| ReadSentenceError::StoreFailed {
                    reason: unread::<Preset>(error),
                })?;
            let selectors = stores
                .selectors()
                .list()
                .await
                .map_err(|error| ReadSentenceError::StoreFailed {
                    reason: unread::<Selector>(error),
                })?;
            (presets, selectors)
        };
        let known = KnownNames::new()
            .with_presets(
                presets
                    .iter()
                    .map(|preset| (preset.name().clone(), preset.title().cloned())),
            )
            .with_selectors(
                selectors
                    .iter()
                    .map(|selector| (selector.name().clone(), selector.title().cloned())),
            );
        Ok(self.quick_inject.read(text, &known))
    }

    /// Refuses listeners that meet those of an environment that runs, and a name that runs already.
    fn check_listeners(&self, name: &EnvironmentName, listeners: &Listeners) -> Result<(), EnvironmentStartError> {
        let environments = self.environments();
        if environments.contains_key(name) {
            return Err(EnvironmentStartError::AlreadyRunning { name: name.clone() });
        }
        for (running_name, running) in environments.iter() {
            if let Some(address) = listeners.shared_listener(&running.listeners) {
                return Err(EnvironmentStartError::SharedListener {
                    name: name.clone(),
                    running: running_name.clone(),
                    address,
                });
            }
        }
        Ok(())
    }

    /// What `answer` says about the environment of that name, or that it does not run.
    fn with_running<T>(
        &self,
        name: &EnvironmentName,
        answer: impl FnOnce(&Running) -> T,
    ) -> Result<T, NotRunningError> {
        match self.environments().get(name) {
            Some(running) => Ok(answer(running)),
            None => Err(NotRunningError { name: name.clone() }),
        }
    }

    /// The environments that run. A task that panicked while it held them left nothing half done:
    /// every change is one insert or one remove.
    fn environments(&self) -> MutexGuard<'_, BTreeMap<EnvironmentName, Running>> {
        self.environments.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes which environments run.
impl<C: WorkspaceContext> fmt::Debug for Host<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host")
            .field("running", &self.running())
            .finish_non_exhaustive()
    }
}

/// What the store of the definitions `D` said when it could not read, with the word for its kind before it.
fn unread<D: Definition>(error: StoreReadError) -> String {
    let StoreReadError::Failed { reason } = error;
    format!("the {} store could not be read: {reason}", D::LABEL)
}
