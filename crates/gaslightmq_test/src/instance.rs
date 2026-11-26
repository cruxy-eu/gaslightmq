//! GaslightMQ that runs in the process of a test, and how it starts and stops.

use std::net::SocketAddr;
use std::sync::Arc;

use gaslightmq_control_api::environment::{EnvironmentName, ServerAddress};
use gaslightmq_control_api::field::Endpoint;
use gaslightmq_control_api::{RequestSurface, RunId};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, ControlPlaneAddress};
use gaslightmq_control_server::ControlServer;
use gaslightmq_host::Host;
use gaslightmq_lie_api::Lies;
use gaslightmq_lie_refusal::Refusal;
use gaslightmq_workspace::{MemoryContext, WorkspaceContext};

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
    environment: EnvironmentName,
    bootstrap: SocketAddr,
    run: RunId,
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
        &self.environment
    }

    /// A client of the control plane, whose every request says that it comes from a test.
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
        host.create(proxy)
            .await
            .map_err(|error| GaslightMqStartError::EnvironmentNotCreated { error })?;
        host.start(configuration)
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
        Ok(GaslightMq {
            host,
            server,
            control: ControlClient::new(client),
            environment,
            bootstrap,
            run,
        })
    }
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
