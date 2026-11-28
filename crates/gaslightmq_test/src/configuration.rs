//! What GaslightMQ in a test is configured with.
//!
//! A property is a public field with a default.
//! The control plane is no Kafka API, so no property is one of Kafka's: each is ours, and says so.
//! What the proxy runs with is the configuration of its environment, whose engine and backend say of each of their
//! properties whether it is Kafka's.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use gaslightmq_control_api::RunId;
use gaslightmq_control_api::environment::RecordingMode;
use gaslightmq_control_api::environment::{
    BrokerListeners, Environment, EnvironmentName, HostName, Instances, ListenAddress, Listeners, ProxyDefinition,
    ServerAddress, Upstream,
};
use gaslightmq_control_api::rule::Rule;
use gaslightmq_control_client::{DefinitionText, Preparation, RuleToArm, ScenarioText, ScenarioToActivate};
use gaslightmq_control_server::ControlServerConfiguration;
use gaslightmq_host::{EnvironmentConfiguration, HostConfiguration};
use gaslightmq_workspace::{MemoryContext, WorkspaceContext};

/// The address that a test listens on, unless it names another: its own machine.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

/// What GaslightMQ in the process of a test is configured with: what the proxy runs with, the workspace it holds,
/// and the control plane in front of it.
///
/// [`GaslightMq::proxy_in_front_of`](crate::GaslightMq::proxy_in_front_of) makes one with every default, and
/// [`start`](Self::start) starts it.
///
/// ```
/// use gaslightmq_control_api::RunId;
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_test::GaslightMq;
///
/// let cluster = "127.0.0.1:9092".parse().expect("a server address");
/// let mut configuration = GaslightMq::proxy_in_front_of(cluster)
///     .named("orders-proxy".parse().expect("an environment name"))
///     .with_run(RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd));
///
/// // Every listener takes a free port of the machine of the test, and the control plane does too.
/// assert_eq!(configuration.proxy.engine.listeners.bootstrap().to_string(), "127.0.0.1:0");
/// assert_eq!(configuration.control_plane.listen.to_string(), "127.0.0.1:0");
///
/// // The proxy stops sooner, and waits less for the cluster, than it would by default.
/// configuration.proxy.engine.stop_timeout = TimeSpan::from_secs(1);
/// configuration.proxy.upstream.request_timeout = TimeSpan::from_secs(5);
///
/// // A test whose clients run in containers listens on every address, and names the host they reach.
/// let configuration = configuration.listening_on(
///     "0.0.0.0".parse().expect("an IP address"),
///     "host.docker.internal".parse().expect("a host name"),
/// );
/// assert_eq!(configuration.proxy.upstream.advertised_host, "host.docker.internal");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct GaslightMqConfiguration<C: WorkspaceContext = MemoryContext> {
    /// Ours. Default: the defaults of a proxy in front of the cluster, in the environment `proxy`, on free ports of
    /// `127.0.0.1`.
    ///
    /// What the proxy runs with: the name of its environment, its engine and its backend in front of the real cluster.
    /// Its engine listens on free ports: one for the bootstrap, and one for each broker that the proxy finds in the
    /// cluster, a broker that joins later too. The test names no broker, however many the cluster has.
    /// The workspace gets the environment that this configuration describes, so it holds no other one of the name.
    pub proxy: EnvironmentConfiguration,
    /// Ours. Default: none.
    ///
    /// The ID of the run. A test that names none gets one from the randomness of the system.
    /// A test that compares what it reads with a run it wrote down names its own.
    pub run: Option<RunId>,
    /// Ours. Default: the defaults of a host.
    ///
    /// What holds for the environment of the test: how it reads a ⌘K sentence, and what its traffic keeps.
    pub host: HostConfiguration,
    /// Ours. Default: the defaults of a control server, on a free port of `127.0.0.1`.
    ///
    /// The control plane that the test reaches the proxy through.
    pub control_plane: ControlServerConfiguration,
    /// Ours. Default: stores in memory that hold nothing.
    ///
    /// The stores of the workspace. A test that wants definitions from the start hands over a context that holds them,
    /// or one over other stores.
    pub context: C,
    /// The directory of a workspace whose definitions the workspace of the test gets when it starts.
    pub(crate) workspace: Option<PathBuf>,
    /// What the test declared that the instance starts with.
    pub(crate) preparation: Preparation,
}

impl GaslightMqConfiguration {
    /// A proxy in front of the real cluster at `cluster`, with every default.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> Self {
        let mut control_plane = ControlServerConfiguration::default();
        control_plane.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
        Self {
            proxy: configured(&described(
                the_proxy(),
                on_free_ports(LOCALHOST, LOCALHOST.into()),
                Upstream::new(cluster),
            )),
            run: None,
            host: HostConfiguration::default(),
            control_plane,
            context: MemoryContext::new(),
            workspace: None,
            preparation: Preparation::default(),
        }
    }
}

impl<C: WorkspaceContext> GaslightMqConfiguration<C> {
    /// The same configuration, with the environment named `environment`. Everything the proxy runs with stays.
    #[must_use]
    pub fn named(mut self, environment: EnvironmentName) -> Self {
        let mut renamed = configured(&self.environment_named(environment));
        renamed.engine = self.proxy.engine;
        renamed.upstream = self.proxy.upstream;
        self.proxy = renamed;
        self
    }

    /// The same configuration, with every listener on a free port of `listen`, and the brokers of the proxy under
    /// `advertised_host` for its clients.
    #[must_use]
    pub fn listening_on(mut self, listen: IpAddr, advertised_host: HostName) -> Self {
        self.proxy.engine.listeners = on_free_ports(listen, advertised_host.clone());
        self.proxy.upstream.advertised_host = advertised_host;
        self
    }

    /// The same configuration, in the run `run`.
    #[must_use]
    pub fn with_run(mut self, run: RunId) -> Self {
        self.run = Some(run);
        self
    }

    /// The same configuration, over the stores of `context`.
    pub fn with_context<D: WorkspaceContext>(self, context: D) -> GaslightMqConfiguration<D> {
        let Self {
            proxy,
            run,
            host,
            control_plane,
            context: _,
            workspace,
            preparation,
        } = self;
        GaslightMqConfiguration {
            proxy,
            run,
            host,
            control_plane,
            context,
            workspace,
            preparation,
        }
    }

    /// The same configuration, whose workspace gets the definitions of the workspace in `directory` when it starts:
    /// its selectors, presets, rules, scenarios and pad layouts.
    ///
    /// The files are read once, and never written: whatever the test changes stays in the workspace of the test.
    /// The environments of the files stay out, since the test runs the environment of its own proxy.
    #[must_use]
    pub fn with_workspace(mut self, directory: impl Into<PathBuf>) -> Self {
        self.workspace = Some(directory.into());
        self
    }

    /// The same configuration, whose workspace gets `rule`, disarmed: a [`rule!`](gaslightmq_control_client::rule),
    /// or its YAML as a file holds it.
    #[must_use]
    pub fn with_rule(mut self, rule: impl Into<DefinitionText<Rule>>) -> Self {
        self.preparation = self.preparation.with_rule(rule);
        self
    }

    /// The same configuration, whose workspace gets `scenario`: a [`scenario!`](gaslightmq_control_client::scenario),
    /// or its YAML as a file holds it.
    #[must_use]
    pub fn with_scenario(mut self, scenario: impl Into<ScenarioText>) -> Self {
        self.preparation = self.preparation.with_scenario(scenario);
        self
    }

    /// The same configuration, which arms `rule` once the proxy runs: a rule of the workspace by its name, or a new live
    /// rule. [`GaslightMq::armed`](crate::GaslightMq::armed) holds it then.
    #[must_use]
    pub fn arming(mut self, rule: impl Into<RuleToArm>) -> Self {
        self.preparation = self.preparation.arming(rule);
        self
    }

    /// The same configuration, which activates `scenario` once the proxy runs, after everything else: a scenario of
    /// the workspace by its name, or one that the workspace gets first.
    #[must_use]
    pub fn activating(mut self, scenario: impl Into<ScenarioToActivate>) -> Self {
        self.preparation = self.preparation.activating(scenario);
        self
    }

    /// The same configuration, whose recordings keep as much of each call as `mode` says: the one from the start, and
    /// each that the test starts. A proxy in front of a real cluster keeps no records otherwise.
    #[must_use]
    pub fn recording(mut self, mode: RecordingMode) -> Self {
        self.preparation = self.preparation.recording_in(mode);
        self
    }

    /// The same configuration, which starts a recording once the proxy runs, before it activates a scenario.
    #[must_use]
    pub fn recording_from_the_start(mut self) -> Self {
        self.preparation = self.preparation.recording_from_the_start();
        self
    }

    /// The environment that the proxy runs in, as the workspace holds it: its listeners and its cluster.
    pub(crate) fn environment(&self) -> Environment {
        self.environment_named(self.proxy.name().clone())
    }

    fn environment_named(&self, name: EnvironmentName) -> Environment {
        described(
            name,
            self.proxy.engine.listeners.clone(),
            self.proxy.upstream.upstream.clone(),
        )
    }
}

/// A proxy in front of a real cluster, with these listeners.
fn described(name: EnvironmentName, listeners: Listeners, upstream: Upstream) -> Environment {
    Environment::new(
        name,
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listeners),
            upstream,
        },
    )
}

/// What a host starts the proxy of `environment` with: what the environment says, and the defaults for the rest.
fn configured(environment: &Environment) -> EnvironmentConfiguration {
    EnvironmentConfiguration::new(environment).expect("a host configures a proxy in front of a real cluster")
}

/// The name of the environment of a proxy that the test names no other for.
fn the_proxy() -> EnvironmentName {
    "proxy".parse().expect("`proxy` is an environment name")
}

/// Listeners on free ports of `listen`, which clients reach under `advertised_host`: the bootstrap listener, and one
/// for each broker that the proxy finds.
fn on_free_ports(listen: IpAddr, advertised_host: HostName) -> Listeners {
    let bootstrap = ListenAddress::new(listen, ANY_PORT);
    // A listener on port 0 shares its port with no other listener.
    Listeners::new(bootstrap, BrokerListeners::any_port(listen), advertised_host)
        .expect("a listener on a free port shares its port with no other")
}

#[cfg(test)]
mod tests {
    use gaslightmq_control_api::clock::TimeSpan;

    use super::*;

    fn cluster() -> ServerAddress {
        "kafka-1.staging:9092".parse().unwrap()
    }

    #[test]
    fn a_proxy_in_front_of_a_cluster_listens_on_free_ports_of_the_machine_of_the_test() {
        let configuration = GaslightMqConfiguration::proxy_in_front_of(cluster());

        assert_eq!(configuration.proxy.name().as_str(), "proxy");
        let listeners = &configuration.proxy.engine.listeners;
        assert_eq!(listeners.bootstrap(), ListenAddress::new(LOCALHOST, ANY_PORT));
        // Every broker that the proxy finds gets a free port: the test names none.
        assert_eq!(listeners.brokers(), &BrokerListeners::any_port(LOCALHOST));
        assert_eq!(listeners.advertised_host(), &HostName::from(LOCALHOST));
        assert_eq!(configuration.proxy.upstream.upstream, Upstream::new(cluster()));
        assert_eq!(configuration.run, None);
        assert_eq!(configuration.host, HostConfiguration::default());
        assert_eq!(configuration.control_plane.listen, SocketAddr::new(LOCALHOST, ANY_PORT));
    }

    #[test]
    fn a_name_and_a_context_of_its_own_keep_what_the_proxy_runs_with() {
        let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
        let mut configuration = GaslightMqConfiguration::proxy_in_front_of(cluster());
        configuration.proxy.engine.stop_timeout = TimeSpan::from_secs(1);
        configuration.proxy.upstream.request_timeout = TimeSpan::from_secs(5);

        let configuration = configuration
            .named("orders-proxy".parse().unwrap())
            .with_run(run)
            .with_context(MemoryContext::new());

        assert_eq!(configuration.proxy.name().as_str(), "orders-proxy");
        assert_eq!(
            configuration.proxy.engine.listeners.brokers(),
            &BrokerListeners::any_port(LOCALHOST)
        );
        assert_eq!(configuration.proxy.engine.stop_timeout, TimeSpan::from_secs(1));
        assert_eq!(configuration.proxy.upstream.request_timeout, TimeSpan::from_secs(5));
        assert_eq!(configuration.run, Some(run));
        assert_eq!(configuration.environment().name().as_str(), "orders-proxy");
    }

    #[test]
    fn listeners_on_another_address_take_free_ports_of_it() {
        let every_address: IpAddr = "0.0.0.0".parse().unwrap();
        let reached: HostName = "host.docker.internal".parse().unwrap();

        let configuration =
            GaslightMqConfiguration::proxy_in_front_of(cluster()).listening_on(every_address, reached.clone());

        let listeners = &configuration.proxy.engine.listeners;
        assert_eq!(listeners.bootstrap(), ListenAddress::new(every_address, ANY_PORT));
        assert_eq!(listeners.advertised_host(), &reached);
        assert_eq!(listeners.brokers(), &BrokerListeners::any_port(every_address));
        assert_eq!(configuration.proxy.upstream.advertised_host, reached);
    }
}
