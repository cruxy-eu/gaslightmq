//! What GaslightMQ in a test is configured with.
//!
//! A property is a public field with a default.
//! The control plane is no Kafka API, so no property is one of Kafka's: each is ours, and says so.
//! What the proxy runs with is the configuration of its environment, whose engine and backend say of each of their
//! properties whether it is Kafka's.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use gaslightmq_control_api::RunId;
use gaslightmq_control_api::environment::{
    BrokerListeners, BrokerListenersError, Environment, EnvironmentName, HostName, Instances, ListenAddress, Listeners,
    ProxyDefinition, ServerAddress, Upstream,
};
use gaslightmq_control_api::kernel::BrokerId;
use gaslightmq_control_server::ControlServerConfiguration;
use gaslightmq_host::{EnvironmentConfiguration, HostConfiguration};
use gaslightmq_workspace::{MemoryContext, WorkspaceContext};

/// The address that a test listens on, unless it names another: its own machine.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

/// The broker of a cluster of one broker, whose ID is 1 in most clusters of a test.
const FIRST_BROKER: u16 = 1;

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
    /// Ours. Default: the defaults of a proxy in front of the cluster, in the environment `proxy`, with a listener for
    /// broker 1, the one broker of most clusters of a test.
    ///
    /// What the proxy runs with: the name of its environment, its engine and its backend in front of the real cluster.
    /// Its engine listens on free ports of `127.0.0.1`: one for the bootstrap, and one for each broker of the cluster.
    /// A broker of the cluster without a listener of its own keeps the proxy from starting, so a test in front of a
    /// cluster of several brokers [names them](Self::with_brokers).
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
}

impl GaslightMqConfiguration {
    /// A proxy in front of the real cluster at `cluster`, with every default.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> Self {
        let mut control_plane = ControlServerConfiguration::default();
        control_plane.listen = SocketAddr::new(LOCALHOST, ANY_PORT);
        Self {
            proxy: configured(&described(the_proxy(), of_one_broker(), Upstream::new(cluster))),
            run: None,
            host: HostConfiguration::default(),
            control_plane,
            context: MemoryContext::new(),
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

    /// The same configuration, in front of a cluster with these brokers, each with a listener on a free port.
    ///
    /// # Errors
    ///
    /// [`BrokerListenersError::NoBroker`] when `brokers` names none.
    pub fn with_brokers(mut self, brokers: impl IntoIterator<Item = BrokerId>) -> Result<Self, BrokerListenersError> {
        let listeners = &self.proxy.engine.listeners;
        let ip = listeners.bootstrap().ip();
        let any = ListenAddress::new(ip, ANY_PORT);
        let brokers = BrokerListeners::pinned(brokers.into_iter().map(|broker| (broker, any)))?;
        self.proxy.engine.listeners = on_free_ports(ip, brokers, listeners.advertised_host().clone());
        Ok(self)
    }

    /// The same configuration, with every listener on a free port of `listen`, and the brokers of the proxy under
    /// `advertised_host` for its clients.
    #[must_use]
    pub fn listening_on(mut self, listen: IpAddr, advertised_host: HostName) -> Self {
        let brokers = self.proxy.engine.listeners.brokers().as_pinned().map(|pinned| {
            let any = ListenAddress::new(listen, ANY_PORT);
            pinned.keys().map(|broker| (*broker, any)).collect::<Vec<_>>()
        });
        let brokers = match brokers {
            Some(pinned) => pinned_anew(pinned),
            None => self.proxy.engine.listeners.brokers().clone(),
        };
        self.proxy.engine.listeners = on_free_ports(listen, brokers, advertised_host.clone());
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
        } = self;
        GaslightMqConfiguration {
            proxy,
            run,
            host,
            control_plane,
            context,
        }
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

/// Listeners on free ports of the machine of the test, for a cluster of one broker.
fn of_one_broker() -> Listeners {
    let any = ListenAddress::new(LOCALHOST, ANY_PORT);
    let brokers = pinned_anew(BTreeSet::from([(BrokerId::from(FIRST_BROKER), any)]));
    on_free_ports(LOCALHOST, brokers, LOCALHOST.into())
}

/// Brokers that were pinned already, each to a free port of their own.
fn pinned_anew(pinned: impl IntoIterator<Item = (BrokerId, ListenAddress)>) -> BrokerListeners {
    BrokerListeners::pinned(pinned).expect("brokers that were pinned once, each on a free port of its own")
}

/// The bootstrap listener on a free port of `listen`, beside `brokers`.
fn on_free_ports(listen: IpAddr, brokers: BrokerListeners, advertised_host: HostName) -> Listeners {
    // A listener on port 0 shares its port with no other listener.
    Listeners::new(ListenAddress::new(listen, ANY_PORT), brokers, advertised_host)
        .expect("a listener on a free port shares its port with no other")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
        assert_eq!(
            listeners
                .brokers()
                .as_pinned()
                .map(|pinned| pinned.keys().copied().collect::<Vec<_>>()),
            Some(vec![BrokerId::from(FIRST_BROKER)])
        );
        assert_eq!(listeners.advertised_host(), &HostName::from(LOCALHOST));
        assert_eq!(configuration.proxy.upstream.upstream, Upstream::new(cluster()));
        assert_eq!(configuration.run, None);
        assert_eq!(configuration.host, HostConfiguration::default());
        assert_eq!(configuration.control_plane.listen, SocketAddr::new(LOCALHOST, ANY_PORT));
    }

    #[test]
    fn a_name_brokers_and_a_context_of_its_own_keep_what_the_proxy_runs_with() {
        let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
        let mut configuration = GaslightMqConfiguration::proxy_in_front_of(cluster());
        configuration.proxy.engine.stop_timeout = TimeSpan::from_secs(1);
        configuration.proxy.upstream.request_timeout = TimeSpan::from_secs(5);

        let configuration = configuration
            .named("orders-proxy".parse().unwrap())
            .with_brokers([1, 2, 3].map(BrokerId::from))
            .unwrap()
            .with_run(run)
            .with_context(MemoryContext::new());

        assert_eq!(configuration.proxy.name().as_str(), "orders-proxy");
        assert_eq!(
            configuration
                .proxy
                .engine
                .listeners
                .brokers()
                .as_pinned()
                .map(BTreeMap::len),
            Some(3)
        );
        assert_eq!(configuration.proxy.engine.stop_timeout, TimeSpan::from_secs(1));
        assert_eq!(configuration.proxy.upstream.request_timeout, TimeSpan::from_secs(5));
        assert_eq!(configuration.run, Some(run));
        assert_eq!(configuration.environment().name().as_str(), "orders-proxy");
    }

    #[test]
    fn a_cluster_without_brokers_gets_no_listeners() {
        let configuration = GaslightMqConfiguration::proxy_in_front_of(cluster()).with_brokers([]);

        assert_eq!(configuration.err(), Some(BrokerListenersError::NoBroker));
    }

    #[test]
    fn listeners_on_another_address_keep_their_brokers() {
        let every_address: IpAddr = "0.0.0.0".parse().unwrap();
        let reached: HostName = "host.docker.internal".parse().unwrap();

        let configuration = GaslightMqConfiguration::proxy_in_front_of(cluster())
            .with_brokers([1, 2].map(BrokerId::from))
            .unwrap()
            .listening_on(every_address, reached.clone());

        let listeners = &configuration.proxy.engine.listeners;
        assert_eq!(listeners.bootstrap(), ListenAddress::new(every_address, ANY_PORT));
        assert_eq!(listeners.advertised_host(), &reached);
        let pinned = listeners.brokers().as_pinned().unwrap();
        assert_eq!(pinned.len(), 2);
        assert!(pinned.values().all(|address| address.ip() == every_address));
        assert_eq!(configuration.proxy.upstream.advertised_host, reached);
    }
}
