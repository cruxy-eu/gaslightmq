//! GaslightMQ that runs in the process of a test, and how it starts and stops.

use std::net::SocketAddr;
use std::sync::Arc;

use std::path::Path;

use gaslightmq_control_api::environment::{EnvironmentName, ServerAddress};
use gaslightmq_control_api::field::Endpoint;
use gaslightmq_control_api::journal::{ChangeSurface, JournalEntry};
use gaslightmq_control_api::rule::RuleName;
use gaslightmq_control_api::{RequestSurface, RunId, ScenarioStatus};
use gaslightmq_control_client::{
    ActiveScenario, ControlClient, ControlClientConfiguration, ControlPlaneAddress, ControlledEnvironment,
    EnvironmentRule, EnvironmentStateError, FollowError, FollowedCalls, JournalError, Prepared, Recordings, Rules,
    Scenarios, SelectorToMatch,
};
use gaslightmq_control_server::ControlServer;
use gaslightmq_definition_api::Store;
use gaslightmq_host::{CreateDefinitionError, Host};
use gaslightmq_lie_api::Lies;
use gaslightmq_lie_refusal::Refusal;
use gaslightmq_workspace::{MemoryContext, Stores, WorkspaceContext, WorkspaceDefinition};
use gaslightmq_workspace_fs::{Access, FileStores};

use crate::{GaslightMqConfiguration, GaslightMqStartError};

/// GaslightMQ in the process of a test: a proxy in front of a real cluster, and a control plane in front of it.
///
/// It runs from [`start`](GaslightMqConfiguration::start) until the test [stops](Self::stop) it.
/// Every listener of the proxy and the control plane takes a free port, so tests run side by side,
/// and each accepts connections from the moment the test holds the value.
/// A test that lets go of it without stopping it stops it a moment later, and waits for nothing.
///
/// The test reaches the proxy through the [control plane](Self::control), as it would reach any other process of
/// GaslightMQ, and its clients connect to the [bootstrap address](Self::bootstrap) of the proxy.
pub struct GaslightMq<C: WorkspaceContext + 'static = MemoryContext> {
    host: Arc<Host<C>>,
    server: ControlServer,
    control: ControlClient,
    environment: ControlledEnvironment,
    bootstrap: SocketAddr,
    run: RunId,
    prepared: Prepared,
}

impl GaslightMq {
    /// A proxy in front of the real cluster whose bootstrap server is `cluster`, with every default.
    ///
    /// [`start`](GaslightMqConfiguration::start) starts it, and the test changes what it wants to first.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> GaslightMqConfiguration {
        GaslightMqConfiguration::proxy_in_front_of(cluster)
    }
}

impl<C: WorkspaceContext + 'static> GaslightMq<C> {
    /// The address that a client of the test bootstraps from: the bootstrap listener of the proxy.
    ///
    /// The proxy names its brokers to the client under the host that its listeners advertise, each on a free port of
    /// its own: see [`GaslightMqConfiguration::listening_on`].
    pub fn bootstrap(&self) -> SocketAddr {
        self.bootstrap
    }

    /// The bootstrap address as a client takes it in `bootstrap.servers`: `127.0.0.1:54321`.
    pub fn bootstrap_servers(&self) -> String {
        self.bootstrap.to_string()
    }

    /// The name of the environment that the proxy runs in, which every request about it names.
    pub fn environment(&self) -> &EnvironmentName {
        self.environment.name()
    }

    /// The environment of the proxy, in the words of a test: its rules, its scenario, its recordings, the calls it
    /// follows and its journal. The calls below are its words, which the instance speaks for the test.
    pub fn controlled(&self) -> &ControlledEnvironment {
        &self.environment
    }

    /// The rules of the proxy: to [arm](Rules::arm) one, written as a [`rule!`](gaslightmq_control_client::rule),
    /// as the YAML of a file, or by the name of a rule of the workspace.
    pub fn rules(&self) -> Rules {
        self.environment.rules()
    }

    /// The rule of this name in the environment of the proxy.
    pub fn rule(&self, name: RuleName) -> EnvironmentRule {
        self.environment.rule(name)
    }

    /// The scenarios of the proxy: to [activate](Scenarios::activate) one.
    pub fn scenarios(&self) -> Scenarios {
        self.environment.scenarios()
    }

    /// The recordings of the proxy: to start one, or to take the one that runs. They keep as much of each call as
    /// the [configuration](GaslightMqConfiguration::recording) says.
    pub fn recordings(&self) -> Recordings {
        self.environment.recordings()
    }

    /// Follows the calls of the proxy that `selector` matches, from now on, so the test [waits](FollowedCalls::wait_for)
    /// for them, and never on the clock.
    ///
    /// # Errors
    ///
    /// The [`FollowError`] of [`ControlledEnvironment::follow`].
    pub async fn follow(&self, selector: impl Into<SelectorToMatch>) -> Result<FollowedCalls, FollowError> {
        self.environment.follow(selector).await
    }

    /// Every entry of the journal of the proxy.
    ///
    /// # Errors
    ///
    /// The errors of [`ControlledEnvironment::journal`].
    pub async fn journal(&self) -> Result<Vec<JournalEntry>, JournalError> {
        self.environment.journal().await
    }

    /// Where the scenario of the proxy stands.
    ///
    /// # Errors
    ///
    /// The errors of [`ControlledEnvironment::scenario_status`].
    pub async fn scenario_status(&self) -> Result<ScenarioStatus, EnvironmentStateError> {
        self.environment.scenario_status().await
    }

    /// The rules that the configuration [armed](GaslightMqConfiguration::arming), in its order.
    pub fn armed(&self) -> &[EnvironmentRule] {
        &self.prepared.armed
    }

    /// The scenario that the configuration [activated](GaslightMqConfiguration::activating), when it named one.
    pub fn scenario(&self) -> Option<&ActiveScenario> {
        self.prepared.scenario.as_ref()
    }

    /// A client of the control plane, whose every request says that it comes from a test: for whatever the words of
    /// the [environment](Self::controlled) do not say.
    ///
    /// It names no presence, so a hold that it adds ends by its release or by a disarm alone.
    /// A test that has the process to itself leaves `expects` out: the last change wins.
    pub fn control(&self) -> &ControlClient {
        &self.control
    }

    /// Where the control plane listens, for another client: the UI in a browser, or one with a configuration of its
    /// own.
    pub fn control_plane(&self) -> ControlPlaneAddress {
        self.server.address().into()
    }

    /// The ID of the run: the one the test named, or the one made for it.
    pub fn run(&self) -> RunId {
        self.run
    }

    /// Stops the control plane and then the proxy, and waits until both have stopped.
    ///
    /// Every address is free once the call returns.
    pub async fn stop(self) {
        let Self { host, server, .. } = self;
        server.stop().await;
        host.stop_all().await;
    }
}

impl<C: WorkspaceContext + 'static> GaslightMqConfiguration<C> {
    /// Starts GaslightMQ in the process of the test, on the runtime that the test runs on.
    ///
    /// The workspace gets the environment of the proxy, the proxy starts with the [configuration](Self::proxy) of its
    /// environment, and the control plane starts in front of it. The proxy tells every lie that GaslightMQ has.
    ///
    /// # Errors
    ///
    /// - [`GaslightMqStartError::NoRandomness`] when the test names no run, and the system gives no randomness.
    /// - [`GaslightMqStartError::EnvironmentNotCreated`] when the workspace does not take the environment.
    /// - [`GaslightMqStartError::ProxyNotStarted`] when the proxy does not reach the cluster, or a broker of the
    ///   cluster gets no listener.
    /// - [`GaslightMqStartError::ControlPlaneNotStarted`] when the control plane cannot listen.
    pub async fn start(self) -> Result<GaslightMq<C>, GaslightMqStartError> {
        let proxy = self.environment();
        let Self {
            proxy: configuration,
            run,
            host,
            control_plane,
            context,
            workspace,
            preparation,
        } = self;
        let run = match run {
            Some(run) => run,
            None => random_run()?,
        };
        let environment = configuration.name().clone();

        let host = Arc::new(Host::new(
            host,
            context,
            every_lie(),
            gaslightmq_host::RunId::from_random_bits(run.bits()),
        ));
        host.create(proxy, ChangeSurface::Test)
            .await
            .map_err(|error| GaslightMqStartError::EnvironmentNotCreated { error })?;
        if let Some(directory) = &workspace {
            take_the_workspace(&host, directory).await?;
        }
        // The environment of the test is the one that the process starts with, so the process is healthy while it runs.
        host.start_with([configuration])
            .await
            .map_err(|error| GaslightMqStartError::ProxyNotStarted { error })?;
        let server = match ControlServer::start(control_plane, Arc::clone(&host)).await {
            Ok(server) => server,
            Err(error) => {
                host.stop_all().await;
                return Err(GaslightMqStartError::ControlPlaneNotStarted { error });
            }
        };
        let bootstrap = bootstrap_of(&host, &environment);
        let mut client = ControlClientConfiguration::new(server.address().into());
        client.surface = RequestSurface::Test;
        let control = ControlClient::new(client);
        let controlled = match preparation.recording_mode() {
            Some(mode) => control.environment(environment).recording_in(mode),
            None => control.environment(environment),
        };
        let prepared = match preparation.prepare(&controlled).await {
            Ok(prepared) => prepared,
            Err(error) => {
                server.stop().await;
                host.stop_all().await;
                return Err(GaslightMqStartError::NotPrepared { error });
            }
        };
        Ok(GaslightMq {
            host,
            server,
            control,
            environment: controlled,
            bootstrap,
            run,
            prepared,
        })
    }
}

/// The workspace of `host` gets the definitions of the files in `directory`: those that others name first.
async fn take_the_workspace<C: WorkspaceContext>(host: &Host<C>, directory: &Path) -> Result<(), GaslightMqStartError> {
    let files = FileStores::open(directory, Access::ReadOnly).await.map_err(|error| {
        GaslightMqStartError::WorkspaceUnreadable {
            directory: directory.to_owned(),
            reason: error.to_string(),
        }
    })?;
    let unreadable = |reason: String| GaslightMqStartError::WorkspaceUnreadable {
        directory: directory.to_owned(),
        reason,
    };
    let not_taken = |error| GaslightMqStartError::WorkspaceNotTaken { error };
    let read = |error: gaslightmq_definition_api::StoreReadError| unreadable(error.to_string());
    take_all(host, files.selectors().list().await.map_err(read)?)
        .await
        .map_err(not_taken)?;
    take_all(host, files.presets().list().await.map_err(read)?)
        .await
        .map_err(not_taken)?;
    take_all(host, files.rules().list().await.map_err(read)?)
        .await
        .map_err(not_taken)?;
    take_all(host, files.scenarios().list().await.map_err(read)?)
        .await
        .map_err(not_taken)?;
    take_all(host, files.layouts().list().await.map_err(read)?)
        .await
        .map_err(not_taken)?;
    Ok(())
}

/// The workspace of `host` gets each of `definitions`.
async fn take_all<C: WorkspaceContext, D: WorkspaceDefinition>(
    host: &Host<C>,
    definitions: Vec<D>,
) -> Result<(), CreateDefinitionError> {
    for definition in definitions {
        host.create(definition, ChangeSurface::Test).await?;
    }
    Ok(())
}

/// The address of the bootstrap listener of `environment`, which runs.
fn bootstrap_of<C: WorkspaceContext>(host: &Host<C>, environment: &EnvironmentName) -> SocketAddr {
    host.address_of(environment, Endpoint::Bootstrap)
        .expect("the environment runs")
        .expect("a proxy starts once its bootstrap endpoint listens")
}

/// A run ID from the randomness of the system.
fn random_run() -> Result<RunId, GaslightMqStartError> {
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| GaslightMqStartError::NoRandomness {
        reason: error.to_string(),
    })?;
    Ok(RunId::from_random_bits(u128::from_be_bytes(random)))
}

/// Every lie that GaslightMQ has: those of every build, and the refusal.
fn every_lie() -> Lies {
    Lies::of_every_build()
        .with(Refusal)
        .expect("no build tells the refusal without its crate")
}
