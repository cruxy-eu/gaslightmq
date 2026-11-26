//! One process with several environments.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::future::join_all;
use gaslightmq_backend::Backend;
use gaslightmq_backend_upstream::UpstreamBackend;
use gaslightmq_clock::Clock;
use gaslightmq_clock_waitable::WallClock;
use gaslightmq_definition_api::{Definition, Store, StoreReadError};
use gaslightmq_engine::{ArmError, ArmedRules, DisarmError, Engine, EveryEvent, LiveEvents, NotOpenError, OpenSession};
use gaslightmq_environment::{EnvironmentName, Listeners};
use gaslightmq_field::{ConnectionNumber, Endpoint};
use gaslightmq_incident::IncidentSupport;
use gaslightmq_lie_api::Lies;
use gaslightmq_quick_inject::{KnownNames, QuickInject, Sentence};
use gaslightmq_rule::{HandFiring, Preset, PresetSet, ResolvedRule, Rule, RuleError, RuleName};
use gaslightmq_selector::{Selector, SelectorRef, SelectorSet};
use gaslightmq_surface::{EncoderMapping, PresenceId};
use gaslightmq_traffic::{Statistics, Traffic, TrafficConfiguration};
use gaslightmq_workspace::{
    DefinitionName, Revision, RevisionCounter, Stores, Workspace, WorkspaceContext, WorkspaceDefinition,
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::calls::{Watched, lock};
use crate::holds::Holds;
use crate::revisions::RuleRevisions;
use crate::{
    ArmRuleError, CallBacklog, CallSubscription, CloseSessionError, CreateDefinitionError, DeleteDefinitionError,
    DisarmRuleError, EnvironmentConfiguration, EnvironmentStartError, Expects, FireRuleError, FollowCallsError,
    HostChange, HostChanges, HostConfiguration, NotRunningError, ReadSentenceError, RunId, UpdateDefinitionError,
};

mod live;
mod pads;
mod watch;

/// One process with several environments: it runs the proxy of each, and shares the workspace between them.
///
/// | Part | Is |
/// |---|---|
/// | The [workspace](Self::workspace) | The definitions of every environment, over the context the host was handed. |
/// | The environments that run | Each one by its name, with its own proxy: its listeners, sessions, armed rules and events. |
/// | The lies of the build | One behaviour per kind, which the proxy of every environment tells. |
/// | Quick inject | Reads a ⌘K sentence with the names of the workspace, and those that the traffic of an environment has seen. |
/// | The [run](Self::run) | The ID of this life of the process, which the host is handed when it is made. |
/// | The presences that are open | The clients of a control surface that are there. The holds of a presence end with it. |
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
/// and [`read_sentence`](Self::read_sentence) offers their names, next to those of the traffic of the environment.
///
/// # Revisions
///
/// Whatever someone changes through the host has a [`Revision`]: a definition of the workspace,
/// and a rule in an environment. One counter of the process gives all of them, so every change takes the next
/// number, whoever makes it: a change of a definition, a rule that arms or disarms, also when its lifetime ends
/// or its budget is spent, a live rule that someone makes or changes, and an environment that starts.
/// What the traffic does takes no number: not a match, a firing, a counter or a budget.
///
/// - **Every change answers with the revision it left.**
/// - **A rule in an environment has one revision**: the later of the revision of its definition and the number of
///   the last change of its state there. See [`EnvironmentRule`](crate::EnvironmentRule).
/// - **A change can say what it [`Expects`]**: the [run](Self::run) that its sender read from, and the revision
///   that it saw. A change that expects another revision changes nothing, and its error says which revision the
///   thing has now. A change that expects another run changes nothing either, and its error names the run of the
///   host.
/// - **Without it, the last change wins.** A create expects nothing: it refuses a name that is taken.
///
/// A rule whose lifetime ended disarms inside its proxy. The host counts that change when it next looks at the
/// armed rules: a moment later, or when someone reads or changes a rule of the environment before that.
///
/// ```
/// use gaslightmq_environment::EnvironmentName;
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // A test names its run. `gmq` makes the ID from the randomness of the system.
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().expect("a run ID");
/// let host = Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build(), run);
/// let quiet = Preset::new("quiet-orders".parse().expect("a preset name"), LieDefinition::Silence);
/// let created = host.create(quiet.clone()).await.expect("a workspace in memory keeps it");
///
/// // Whoever saw the preset as it was created gives it a title. A second change that saw the same changes nothing.
/// let seen = Expects { run: host.run(), revision: created };
/// let titled = quiet.clone().with_title("Quiet orders".parse().expect("a preset title"));
/// let updated = host.update(titled.clone(), Some(seen)).await.expect("nobody changed the preset since");
/// let late = host.update(titled, Some(seen)).await;
/// assert_eq!(
///     late,
///     Err(UpdateDefinitionError::NotUpdated {
///         error: UpdateError::Changed { name: quiet.name().clone().into(), revision: updated },
///     })
/// );
///
/// // A rule that names the preset resolves against the workspace, and only an environment that runs arms it.
/// let staging: EnvironmentName = "staging-proxy".parse().expect("an environment name");
/// let rule = Rule::new("quiet-produce".parse().expect("a rule name"), Conditions::new(), quiet.name().clone());
/// let armed = host.arm(&staging, &rule, None).await;
/// assert_eq!(armed.err(), Some(ArmRuleError::NotRunning { environment: staging }));
/// # }
/// ```
pub struct Host<C: WorkspaceContext> {
    quick_inject: QuickInject,
    /// The workspace. A change takes it exclusively, so its check and its write belong together.
    workspace: tokio::sync::Mutex<Workspace<C>>,
    /// The run of the process, which a change that expects a revision names.
    run: RunId,
    /// The counter of the process, which the workspace holds too. The start of an environment and every change of
    /// a rule there take their number from it.
    counter: RevisionCounter,
    lies: Arc<Lies>,
    /// The environments that run, by name. Nobody holds the lock across a wait.
    environments: Mutex<BTreeMap<EnvironmentName, Running>>,
    /// Held by a start or a stop, from the check of the listeners until the environment runs or stopped.
    changing: tokio::sync::Mutex<()>,
    /// Every change, for whoever follows the host.
    changes: broadcast::Sender<HostChange>,
    /// What a turn of an encoder is worth.
    encoders: EncoderMapping,
    /// What the traffic of every environment keeps.
    traffic: TrafficConfiguration,
    /// How many calls a follower of the calls of an environment may fall behind.
    call_backlog: CallBacklog,
    /// How many holds the presses of every environment added, which numbers the next one.
    holds_given: AtomicU64,
    /// The presences that are open: the clients of a control surface that are there, whose holds end with them.
    /// Whoever takes it holds the environments already, or needs them for nothing.
    presences: Mutex<BTreeSet<PresenceId>>,
}

/// One environment that runs: its proxy, the backend behind it, the listeners it was started with, and what lives in
/// it alone.
struct Running {
    engine: Engine<Arc<UpstreamBackend>, WallClock>,
    /// The backend that the engine holds too, to apply what a rule fires by hand without holding the environments.
    backend: Arc<UpstreamBackend>,
    listeners: Listeners,
    /// The live rules, by name. They stay in memory until someone keeps or deletes them, or the environment stops.
    live: BTreeMap<RuleName, Rule>,
    /// Who holds the pad of a rule, by the arming of the rule.
    holds: Holds,
    /// The number of the last change of every rule in the environment, which the task that watches it counts too.
    revisions: Arc<Mutex<RuleRevisions>>,
    /// The calls of the environment, as the task that watches it counts them and hands them to their followers.
    traffic: Arc<Mutex<Watched>>,
    /// The task that watches the environment, which ends a moment after its proxy stopped.
    watching: JoinHandle<()>,
}

impl<C: WorkspaceContext> Host<C> {
    /// A host that runs no environment yet, over the workspace of `context`, in the run `run`.
    ///
    /// The proxy of every environment tells the `lies` of this build.
    /// Whoever puts the process together makes `run` from the randomness of the system, once, and a test names its
    /// own. The host loads the workspace, which is the first change of the run.
    pub fn new(configuration: HostConfiguration, context: C, lies: Lies, run: RunId) -> Self {
        let workspace = Workspace::new(context);
        let counter = workspace.counter().clone();
        Self {
            quick_inject: QuickInject::new(configuration.quick_inject),
            workspace: tokio::sync::Mutex::new(workspace),
            run,
            counter,
            lies: Arc::new(lies),
            environments: Mutex::new(BTreeMap::new()),
            changing: tokio::sync::Mutex::new(()),
            changes: broadcast::channel(configuration.change_backlog.changes()).0,
            encoders: configuration.encoders,
            traffic: configuration.traffic,
            call_backlog: configuration.call_backlog,
            holds_given: AtomicU64::new(0),
            presences: Mutex::new(BTreeSet::new()),
        }
    }

    /// The workspace, to read.
    ///
    /// Every environment shares it. A change goes through the host, which tells whoever follows it:
    /// see [`create`](Self::create), [`update`](Self::update) and [`delete`](Self::delete).
    /// Reading takes the workspace as a change does, so holding it across a wait for something else holds up the rest.
    pub async fn workspace(&self) -> impl Deref<Target = Workspace<C>> {
        self.workspace.lock().await
    }

    /// The ID of the run of this host: of this life of the process.
    ///
    /// A revision means something in the run that gave it, and in no other.
    /// Whoever hands out a revision names the run with it, and a change that [`Expects`] a revision names it back.
    pub const fn run(&self) -> RunId {
        self.run
    }

    /// Every change of the host from now on, whoever makes it: see [`HostChange`].
    pub fn changes(&self) -> HostChanges {
        HostChanges::new(self.changes.subscribe())
    }

    /// Adds a definition to the workspace, and returns the revision it has. See [`Workspace::create`].
    ///
    /// A create expects nothing: it refuses a name that is taken.
    /// A rule of the workspace is in every environment, so it never stands next to a live rule of its name:
    /// the host refuses a rule that has the name of a live rule of an environment that runs.
    /// Whoever follows the host learns of it.
    ///
    /// # Errors
    ///
    /// - [`CreateDefinitionError::NameTaken`] when the definition is a rule, and a live rule of an environment that
    ///   runs has its name.
    /// - [`CreateDefinitionError::NotCreated`] with the [`CreateError`](gaslightmq_workspace::CreateError) of the workspace.
    pub async fn create<D: WorkspaceDefinition>(&self, definition: D) -> Result<Revision, CreateDefinitionError> {
        let name: DefinitionName = definition.name().clone().into();
        let revision = {
            // The workspace stays taken from the check to the write, as it does while a live rule goes in.
            let mut workspace = self.workspace.lock().await;
            if let DefinitionName::Rule(rule) = &name
                && let Some(environment) = self.live_rule_of_another(rule, None)
            {
                return Err(CreateDefinitionError::NameTaken {
                    rule: rule.clone(),
                    environment,
                });
            }
            workspace
                .create(definition)
                .await
                .map_err(|error| CreateDefinitionError::NotCreated { error })?
        };
        self.publish(HostChange::DefinitionChanged { name });
        Ok(revision)
    }

    /// Replaces a definition of the workspace, and returns the revision it has from then on.
    /// See [`Workspace::update`].
    ///
    /// With `expects`, the update only changes the definition that its sender saw. Without it, the last change wins.
    /// Whoever follows the host learns of the change.
    ///
    /// # Errors
    ///
    /// - [`UpdateDefinitionError::OtherRun`] when the change expects a revision of another run.
    /// - [`UpdateDefinitionError::NotUpdated`] with the [`UpdateError`](gaslightmq_workspace::UpdateError) of the
    ///   workspace, which is [`Changed`](gaslightmq_workspace::UpdateError::Changed) when the definition has
    ///   another revision than the change expects.
    pub async fn update<D: WorkspaceDefinition>(
        &self,
        definition: D,
        expects: Option<Expects>,
    ) -> Result<Revision, UpdateDefinitionError> {
        let expects = self
            .expected(expects)
            .map_err(|run| UpdateDefinitionError::OtherRun { run })?;
        let name = definition.name().clone().into();
        let updated = self.workspace.lock().await.update(definition, expects).await;
        let revision = updated.map_err(|error| UpdateDefinitionError::NotUpdated { error })?;
        self.publish(HostChange::DefinitionChanged { name });
        Ok(revision)
    }

    /// Removes a definition from the workspace, and returns the revision of the delete.
    /// See [`Workspace::delete`].
    ///
    /// With `expects`, the delete only removes the definition that its sender saw. Without it, the last change wins.
    /// Whoever follows the host learns of the change.
    ///
    /// # Errors
    ///
    /// - [`DeleteDefinitionError::OtherRun`] when the change expects a revision of another run.
    /// - [`DeleteDefinitionError::NotDeleted`] with the [`DeleteError`](gaslightmq_workspace::DeleteError) of the
    ///   workspace, which is [`Changed`](gaslightmq_workspace::DeleteError::Changed) when the definition has
    ///   another revision than the change expects.
    pub async fn delete(
        &self,
        name: impl Into<DefinitionName>,
        expects: Option<Expects>,
    ) -> Result<Revision, DeleteDefinitionError> {
        let expects = self
            .expected(expects)
            .map_err(|run| DeleteDefinitionError::OtherRun { run })?;
        let name = name.into();
        let deleted = self.workspace.lock().await.delete(name.clone(), expects).await;
        let revision = deleted.map_err(|error| DeleteDefinitionError::NotDeleted { error })?;
        self.publish(HostChange::DefinitionDeleted { name });
        Ok(revision)
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
    /// The start is a change: it takes the next number, and every rule has that revision in the environment until
    /// its definition or its state there changes. An environment that stops and starts again therefore gives every
    /// rule a higher revision than it had before.
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
            Ok(backend) => Arc::new(backend),
            Err(error) => return Err(EnvironmentStartError::BackendNotStarted { name, error }),
        };
        let listeners = engine.listeners.clone();
        let started = Engine::start(engine, Arc::clone(&backend), WallClock::new(), Arc::clone(&self.lies)).await;
        let engine = match started {
            Ok(engine) => engine,
            Err(error) => return Err(EnvironmentStartError::ProxyNotStarted { name, error }),
        };
        let traffic = Arc::new(Mutex::new(Watched::new(Traffic::new(self.traffic.clone()))));
        let revisions = Arc::new(Mutex::new(RuleRevisions::started(self.counter.clone())));
        let watching = tokio::spawn(watch::watch(
            name.clone(),
            engine.every_event(),
            engine.rules().clone(),
            engine.rules().changes(),
            Arc::clone(&revisions),
            Arc::clone(&traffic),
            self.changes.clone(),
        ));
        self.environments().insert(
            name.clone(),
            Running {
                engine,
                backend,
                listeners,
                live: BTreeMap::new(),
                holds: Holds::default(),
                revisions,
                traffic,
                watching,
            },
        );
        self.publish(HostChange::EnvironmentStarted { environment: name });
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
        self.end(name.clone(), running).await;
        Ok(())
    }

    /// Stops the proxy of an environment that left the environments, waits until the task that watches it told every
    /// change of its sessions, and tells that the environment stopped.
    async fn end(&self, name: EnvironmentName, running: Running) {
        let Running { engine, watching, .. } = running;
        engine.stop().await;
        // The task ends once it took the last event of the proxy. A task that panicked has nothing more to tell.
        let _ = watching.await;
        self.publish(HostChange::EnvironmentStopped { environment: name });
    }

    /// Stops every environment, and waits until each one has stopped.
    ///
    /// The environments stop side by side, so the call takes as long as the slowest of them.
    pub async fn stop_all(&self) {
        let _changing = self.changing.lock().await;
        let running = std::mem::take(&mut *self.environments());
        join_all(running.into_iter().map(|(name, running)| self.end(name, running))).await;
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

    /// What the backend of an environment does with each kind of incident: apply it, emulate it, or neither.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn incident_support(&self, name: &EnvironmentName) -> Result<IncidentSupport, NotRunningError> {
        self.with_running(name, |running| running.backend.incident_support())
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
    /// It checks no revision and answers with none: [`arm`](Self::arm) and [`disarm`](Self::disarm) do.
    /// The host counts a change that someone makes through the handle when it next looks at the armed rules,
    /// as it counts a lifetime that ended.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn rules(&self, name: &EnvironmentName) -> Result<ArmedRules, NotRunningError> {
        self.with_running(name, |running| running.engine.rules().clone())
    }

    /// Arms `rule` in one environment, and returns the revision that the rule has there from then on.
    ///
    /// The rule can be a rule of the workspace, or a live rule that no workspace holds.
    /// It resolves against the selectors and the presets of the workspace,
    /// and arms above every rule that is armed in the environment.
    /// No other environment arms it. Whoever follows the host learns of it.
    ///
    /// With `expects`, the rule only arms while it has the revision that its sender saw in this environment:
    /// the later of the last change of its definition, and the last change of its state there.
    /// Someone who arms a rule of the workspace after someone else changed its definition arms nothing.
    ///
    /// # Errors
    ///
    /// - [`ArmRuleError::OtherRun`] when the change expects a revision of another run.
    /// - [`ArmRuleError::StoreFailed`] when the workspace could not read its selectors or its presets.
    /// - [`ArmRuleError::WorkspaceInvalid`] when they do not hold together, as when selectors include each other in a
    ///   circle.
    /// - [`ArmRuleError::Unresolved`] when the rule names a selector or a preset that the workspace does not have,
    ///   or could never fire.
    /// - [`ArmRuleError::NotRunning`] when no environment of that name runs.
    /// - [`ArmRuleError::Changed`] when the rule has another revision in the environment than the change expects.
    /// - [`ArmRuleError::AlreadyArmed`] when a rule of that name is armed in the environment.
    pub async fn arm(
        &self,
        environment: &EnvironmentName,
        rule: &Rule,
        expects: Option<Expects>,
    ) -> Result<Revision, ArmRuleError> {
        let expects = self.expected(expects).map_err(|run| ArmRuleError::OtherRun { run })?;
        let name = rule.name();
        // The workspace stays as it is until the rule is armed: nobody changes the definition in between.
        let workspace = self.workspace.lock().await;
        let resolved = resolve(&workspace, rule)
            .await
            .map_err(|unresolvable| match unresolvable {
                Unresolvable::StoreFailed { reason } => ArmRuleError::StoreFailed {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::WorkspaceInvalid { reason } => ArmRuleError::WorkspaceInvalid {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::Unresolved { error } => ArmRuleError::Unresolved { error },
            })?;
        let defined = workspace.revision(name.clone());

        let not_running = |environment| ArmRuleError::NotRunning { environment };
        let revision = self.change_rules(environment, not_running, |running, revisions| {
            let revision = revisions.of(name).max(defined);
            if expects.is_some_and(|expected| expected != revision) {
                return Err(ArmRuleError::Changed {
                    environment: environment.clone(),
                    rule: name.clone(),
                    revision,
                });
            }
            let armed = running.engine.rules();
            armed
                .arm(resolved)
                .map_err(|ArmError::AlreadyArmed { rule }| ArmRuleError::AlreadyArmed {
                    environment: environment.clone(),
                    rule,
                })?;
            Ok(revisions.count(name, &armed.snapshot()))
        })?;
        drop(workspace);
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name.clone(),
        });
        Ok(revision)
    }

    /// Disarms the rule of that name in one environment, and returns the revision that the rule has there from
    /// then on.
    ///
    /// A call in flight that the rule fired on still finishes with it. Whoever follows the host learns of the change.
    ///
    /// With `expects`, the rule only disarms while it has the revision that its sender saw in this environment.
    ///
    /// # Errors
    ///
    /// - [`DisarmRuleError::OtherRun`] when the change expects a revision of another run.
    /// - [`DisarmRuleError::NotRunning`] when no environment of that name runs.
    /// - [`DisarmRuleError::Changed`] when the rule has another revision in the environment than the change expects.
    /// - [`DisarmRuleError::NotArmed`] when no rule of that name is armed in the environment.
    pub async fn disarm(
        &self,
        environment: &EnvironmentName,
        rule: &RuleName,
        expects: Option<Expects>,
    ) -> Result<Revision, DisarmRuleError> {
        let expects = self
            .expected(expects)
            .map_err(|run| DisarmRuleError::OtherRun { run })?;
        // The workspace stays as it is until the rule is disarmed: nobody changes the definition in between.
        let workspace = self.workspace.lock().await;
        let defined = workspace.revision(rule.clone());

        let not_running = |environment| DisarmRuleError::NotRunning { environment };
        let revision = self.change_rules(environment, not_running, |running, revisions| {
            let revision = revisions.of(rule).max(defined);
            if expects.is_some_and(|expected| expected != revision) {
                return Err(DisarmRuleError::Changed {
                    environment: environment.clone(),
                    rule: rule.clone(),
                    revision,
                });
            }
            let armed = running.engine.rules();
            armed
                .disarm(rule)
                .map_err(|DisarmError::NotArmed { rule }| DisarmRuleError::NotArmed {
                    environment: environment.clone(),
                    rule,
                })?;
            Ok(revisions.count(rule, &armed.snapshot()))
        })?;
        drop(workspace);
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: rule.clone(),
        });
        Ok(revision)
    }

    /// Fires a rule that is armed in an environment by hand, which skips its trigger once.
    ///
    /// A rule with a lie lies on the next call that its selector matches.
    /// A rule with an incident fires it at once: the backend of the environment applies it, and the firing counts
    /// once the backend did.
    ///
    /// # Errors
    ///
    /// - [`FireRuleError::NotRunning`] when no environment of that name runs.
    /// - [`FireRuleError::NotArmed`] when no rule of that name is armed in the environment.
    /// - [`FireRuleError::NotFired`] when the lifetime of the rule has ended, or its budget has no firing free.
    /// - [`FireRuleError::NeedsCall`] when the incident of the rule leaves its subject to a call.
    /// - [`FireRuleError::IncidentRejected`] when the backend cannot apply the incident.
    ///   The firing goes back to the budget.
    pub async fn fire(&self, environment: &EnvironmentName, rule: &RuleName) -> Result<(), FireRuleError> {
        let (armed, now, backend) = {
            let environments = self.environments();
            let Some(running) = environments.get(environment) else {
                return Err(FireRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            let snapshot = running.engine.rules().snapshot();
            let Some(armed) = snapshot.get(rule).cloned() else {
                return Err(FireRuleError::NotArmed {
                    environment: environment.clone(),
                    rule: rule.clone(),
                });
            };
            (armed, running.engine.clock().now(), Arc::clone(&running.backend))
        };
        let fired = armed
            .fire_by_hand(now)
            .map_err(|error| FireRuleError::NeedsCall { error })?;
        match fired {
            HandFiring::OnNextMatch => Ok(()),
            HandFiring::NotFired => Err(FireRuleError::NotFired {
                environment: environment.clone(),
                rule: rule.clone(),
            }),
            HandFiring::Incidents { incidents, firing } => {
                for incident in incidents {
                    // The firing goes back to the budget when it is dropped before it is confirmed.
                    if let Err(error) = backend.apply(incident).await {
                        return Err(FireRuleError::IncidentRejected {
                            rule: rule.clone(),
                            error,
                        });
                    }
                }
                firing.confirm();
                Ok(())
            }
        }
    }

    /// Reads a ⌘K sentence, as someone types it, with the names that an environment knows.
    ///
    /// A sentence knows the presets and the selectors of the workspace, with their titles,
    /// and the topics, the consumer groups and the client IDs that the traffic of the environment has seen:
    /// as many of each as the [traffic keeps](gaslightmq_traffic::TrafficConfiguration::seen_names).
    /// It says those without the word for their kind, and offers them as completions.
    /// A sentence names any other one after the word for its kind: `topic orders`.
    ///
    /// Reading changes nothing. The rule of the sentence is a definition, which whoever read it [arms](Self::arm).
    ///
    /// # Errors
    ///
    /// - [`ReadSentenceError::NotRunning`] when no environment of that name runs.
    /// - [`ReadSentenceError::StoreFailed`] when the workspace could not read its presets or its selectors.
    pub async fn read_sentence(
        &self,
        environment: &EnvironmentName,
        text: &str,
    ) -> Result<Sentence, ReadSentenceError> {
        let not_running = || ReadSentenceError::NotRunning {
            environment: environment.clone(),
        };
        if !self.environments().contains_key(environment) {
            return Err(not_running());
        }
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
        let known = self
            .with_running(environment, |running| {
                let watched = lock(&running.traffic);
                let seen = watched.traffic().seen_names();
                known
                    .with_topics(seen.topics().cloned())
                    .with_groups(seen.groups().cloned())
                    .with_clients(seen.clients().cloned())
            })
            .map_err(|_| not_running())?;
        Ok(self.quick_inject.read(text, &known))
    }

    /// The sessions of an environment that are open, in the order of their connection numbers.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn sessions(&self, name: &EnvironmentName) -> Result<Vec<OpenSession>, NotRunningError> {
        self.with_running(name, |running| running.engine.sessions())
    }

    /// Closes a session of an environment as its proxy closes it. The client finds its connection closed, and
    /// whoever follows the host learns that the proxy closed the session, a moment after the call returns.
    ///
    /// # Errors
    ///
    /// - [`CloseSessionError::NotRunning`] when no environment of that name runs.
    /// - [`CloseSessionError::NotOpen`] when no session of the environment is open on the connection.
    pub fn close_session(&self, name: &EnvironmentName, connection: ConnectionNumber) -> Result<(), CloseSessionError> {
        let closed = self.with_running(name, |running| running.engine.close_session(connection));
        match closed {
            Ok(Ok(())) => Ok(()),
            Ok(Err(NotOpenError { connection })) => Err(CloseSessionError::NotOpen {
                environment: name.clone(),
                connection,
            }),
            Err(NotRunningError { name }) => Err(CloseSessionError::NotRunning { environment: name }),
        }
    }

    /// What an environment counted since it started: its calls by API, and what each rule did.
    ///
    /// The numbers come from the calls that the task watching the environment took so far, a moment behind.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn statistics(&self, name: &EnvironmentName) -> Result<Statistics, NotRunningError> {
        self.with_running(name, |running| lock(&running.traffic).traffic().statistics().clone())
    }

    /// The calls of an environment that `selector` matches, from now on: a selector of the workspace by its name,
    /// or conditions of the caller's own.
    ///
    /// The calls come as the task that watches the environment takes them, and the subscription ends when the caller
    /// lets go of it, or the environment stops. The traffic of the environment matches the calls against the selector.
    ///
    /// # Errors
    ///
    /// - [`FollowCallsError::NotRunning`] when no environment of that name runs.
    /// - [`FollowCallsError::StoreFailed`] when the store of the selectors could not be read.
    /// - [`FollowCallsError::Unresolved`] when the selector does not resolve against the selectors of the workspace.
    pub async fn follow_calls(
        &self,
        environment: &EnvironmentName,
        selector: &SelectorRef,
    ) -> Result<CallSubscription, FollowCallsError> {
        let not_running = || FollowCallsError::NotRunning {
            environment: environment.clone(),
        };
        if !self.environments().contains_key(environment) {
            return Err(not_running());
        }
        let selectors = {
            let workspace = self.workspace.lock().await;
            workspace.stores().selectors().list().await
        };
        let selectors = selectors.map_err(|error| FollowCallsError::StoreFailed {
            reason: unread::<Selector>(error),
        })?;
        let unresolved = |error| FollowCallsError::Unresolved { error };
        let selector = SelectorSet::new(selectors)
            .and_then(|selectors| selectors.resolve(selector))
            .map_err(unresolved)?;
        let traffic = self.with_running(environment, |running| Arc::clone(&running.traffic));
        let traffic = traffic.map_err(|_| not_running())?;
        Ok(CallSubscription::new(traffic, selector, self.call_backlog))
    }

    /// The revision that a change expects, when it expects one of this run. A change that expects nothing gets
    /// `None`.
    ///
    /// # Errors
    ///
    /// The run of the host, when the change expects a revision of another run.
    fn expected(&self, expects: Option<Expects>) -> Result<Option<Revision>, RunId> {
        match expects {
            None => Ok(None),
            Some(Expects { run, revision }) if run == self.run => Ok(Some(revision)),
            Some(_) => Err(self.run),
        }
    }

    /// Reads or changes the rules of an environment, with the number of the last change of each.
    ///
    /// `change` gets the environment and the revisions of its rules, which are caught up with the armed rules:
    /// a rule whose lifetime ended since the last look has taken its number.
    /// It holds both for as long as it runs, so what it reads is what it changes.
    /// Whoever follows the host learns of every change that the look counted.
    ///
    /// # Errors
    ///
    /// What `not_running` makes of the name, when no environment of that name runs, and whatever `change` returns.
    fn change_rules<T, E>(
        &self,
        environment: &EnvironmentName,
        not_running: impl FnOnce(EnvironmentName) -> E,
        change: impl FnOnce(&mut Running, &mut RuleRevisions) -> Result<T, E>,
    ) -> Result<T, E> {
        let (changed, uncounted) = {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(not_running(environment.clone()));
            };
            caught_up(running, change)
        };
        for rule in uncounted {
            self.publish(HostChange::RuleChanged {
                environment: environment.clone(),
                rule,
            });
        }
        changed
    }

    /// Tells whoever follows the host. A change that nobody follows is gone.
    fn publish(&self, change: HostChange) {
        let _ = self.changes.send(change);
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

    /// The presences that are open. A task that panicked while it held them left nothing half done:
    /// every change is one insert or one remove.
    fn presences(&self) -> MutexGuard<'_, BTreeSet<PresenceId>> {
        self.presences.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Runs `change` on an environment and the revisions of its rules, which are caught up with the armed rules first.
///
/// Returns what `change` returns, and the rules that the look counted: those that armed or disarmed since the last
/// look without anyone counting it. Whoever calls this tells the followers of the host of them.
fn caught_up<T>(
    running: &mut Running,
    change: impl FnOnce(&mut Running, &mut RuleRevisions) -> T,
) -> (T, Vec<RuleName>) {
    let revisions = Arc::clone(&running.revisions);
    let mut revisions = crate::revisions::lock(&revisions);
    let uncounted = revisions.catch_up(&running.engine.rules().snapshot());
    (change(running, &mut revisions), uncounted)
}

/// Writes which environments run.
impl<C: WorkspaceContext> fmt::Debug for Host<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host")
            .field("running", &self.running())
            .finish_non_exhaustive()
    }
}

/// Resolves `rule` against the selectors and the presets of `workspace`.
async fn resolve<C: WorkspaceContext>(workspace: &Workspace<C>, rule: &Rule) -> Result<ResolvedRule, Unresolvable> {
    let stores = workspace.stores();
    let selectors = stores
        .selectors()
        .list()
        .await
        .map_err(|error| Unresolvable::StoreFailed {
            reason: unread::<Selector>(error),
        })?;
    let presets = stores
        .presets()
        .list()
        .await
        .map_err(|error| Unresolvable::StoreFailed {
            reason: unread::<Preset>(error),
        })?;
    let invalid = |reason: String| Unresolvable::WorkspaceInvalid { reason };
    let selectors = SelectorSet::new(selectors).map_err(|error| invalid(error.to_string()))?;
    let presets = PresetSet::new(presets).map_err(|error| invalid(error.to_string()))?;
    rule.resolve(&selectors, &presets)
        .map_err(|error| Unresolvable::Unresolved { error })
}

/// Why a rule does not resolve against the workspace.
enum Unresolvable {
    /// A store could not read. `reason` says which store, and what it said.
    StoreFailed { reason: String },
    /// The selectors or the presets do not hold together.
    WorkspaceInvalid { reason: String },
    /// The rule names what the workspace does not have, or it could never fire.
    Unresolved { error: RuleError },
}

/// What the store of the definitions `D` said when it could not read, with the word for its kind before it.
fn unread<D: Definition>(error: StoreReadError) -> String {
    let StoreReadError::Failed { reason } = error;
    format!("the {} store could not be read: {reason}", D::LABEL)
}
