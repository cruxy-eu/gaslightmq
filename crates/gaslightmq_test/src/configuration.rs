//! What GaslightMQ in a test is configured with.
//!
//! A property is a public field with a default.
//! The control plane is no Kafka API, so no property is one of Kafka's: each is ours, and says so.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use gaslightmq_control_api::RunId;
use gaslightmq_control_api::environment::{EnvironmentName, HostName, ServerAddress, Upstream};
use gaslightmq_control_api::kernel::BrokerId;
use gaslightmq_control_server::ControlServerConfiguration;
use gaslightmq_host::HostConfiguration;
use gaslightmq_workspace::{MemoryContext, WorkspaceContext};

/// The address that a test listens on, unless it names another: its own machine.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

/// What GaslightMQ in the process of a test is configured with: what it runs, where it listens, and the workspace it
/// holds.
///
/// [`GaslightMq::proxy_in_front_of`](crate::GaslightMq::proxy_in_front_of) makes one with every default, and
/// [`start`](Self::start) starts it.
///
/// ```
/// use gaslightmq_control_api::RunId;
/// use gaslightmq_test::GaslightMq;
///
/// let cluster = "127.0.0.1:9092".parse().expect("a server address");
/// let mut configuration = GaslightMq::proxy_in_front_of(cluster)
///     .named("orders-proxy".parse().expect("an environment name"))
///     .with_run(RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd));
///
/// // Every listener takes a free port of the machine of the test, and the control plane does too.
/// assert_eq!(configuration.listen.to_string(), "127.0.0.1");
/// assert_eq!(configuration.control_plane.listen.to_string(), "127.0.0.1:0");
/// // The cluster has one broker, with the ID 1, until the test says otherwise.
/// assert_eq!(configuration.brokers.len(), 1);
///
/// // A test whose clients run in containers listens on every address, and names the host they reach.
/// configuration.listen = "0.0.0.0".parse().expect("an IP address");
/// configuration.advertised_host = "host.docker.internal".parse().expect("a host name");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct GaslightMqConfiguration<C: WorkspaceContext = MemoryContext> {
    /// Ours. Required.
    ///
    /// The real cluster behind the proxy: its bootstrap server, and how the proxy connects to it.
    pub upstream: Upstream,
    /// Ours. Default: broker 1, the one broker of most clusters of a test.
    ///
    /// The IDs of the brokers of the cluster. The proxy gives each of them a listener of its own, on a free port.
    /// A broker of the cluster that is not among them gets no listener, and the proxy does not start.
    pub brokers: BTreeSet<BrokerId>,
    /// Ours. Default: `proxy`.
    ///
    /// The name of the environment that the proxy runs in. A request to the control plane names it.
    /// The workspace gets an environment of this name, so it holds no other one of the name.
    pub environment: EnvironmentName,
    /// Ours. Default: `127.0.0.1`.
    ///
    /// The address that the listeners of the proxy listen on. Each of them takes a free port there.
    pub listen: IpAddr,
    /// Ours. Default: `127.0.0.1`.
    ///
    /// The host under which a client reaches the brokers of the proxy, which the proxy names in its metadata.
    pub advertised_host: HostName,
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
            upstream: Upstream::new(cluster),
            brokers: BTreeSet::from([BrokerId::from(1)]),
            environment: the_proxy(),
            listen: LOCALHOST,
            advertised_host: LOCALHOST.into(),
            run: None,
            host: HostConfiguration::default(),
            control_plane,
            context: MemoryContext::new(),
        }
    }
}

/// The name of the environment of a proxy that the test names no other for.
fn the_proxy() -> EnvironmentName {
    "proxy".parse().expect("`proxy` is an environment name")
}

impl<C: WorkspaceContext> GaslightMqConfiguration<C> {
    /// The same configuration, with the environment named `environment`.
    #[must_use]
    pub fn named(mut self, environment: EnvironmentName) -> Self {
        self.environment = environment;
        self
    }

    /// The same configuration, in front of a cluster with these brokers.
    #[must_use]
    pub fn with_brokers(mut self, brokers: impl IntoIterator<Item = BrokerId>) -> Self {
        self.brokers = brokers.into_iter().collect();
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
            upstream,
            brokers,
            environment,
            listen,
            advertised_host,
            run,
            host,
            control_plane,
            context: _,
        } = self;
        GaslightMqConfiguration {
            upstream,
            brokers,
            environment,
            listen,
            advertised_host,
            run,
            host,
            control_plane,
            context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster() -> ServerAddress {
        "kafka-1.staging:9092".parse().unwrap()
    }

    #[test]
    fn a_proxy_in_front_of_a_cluster_listens_on_free_ports_of_the_machine_of_the_test() {
        let configuration = GaslightMqConfiguration::proxy_in_front_of(cluster());

        assert_eq!(configuration.upstream, Upstream::new(cluster()));
        assert_eq!(configuration.brokers, BTreeSet::from([BrokerId::from(1)]));
        assert_eq!(configuration.environment.as_str(), "proxy");
        assert_eq!(configuration.listen, LOCALHOST);
        assert_eq!(configuration.advertised_host, HostName::from(LOCALHOST));
        assert_eq!(configuration.run, None);
        assert_eq!(configuration.host, HostConfiguration::default());
        assert_eq!(configuration.control_plane.listen, SocketAddr::new(LOCALHOST, ANY_PORT));
    }

    #[test]
    fn a_context_of_its_own_keeps_everything_else() {
        let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
        let configuration = GaslightMqConfiguration::proxy_in_front_of(cluster())
            .named("orders-proxy".parse().unwrap())
            .with_brokers([1, 2, 3].map(BrokerId::from))
            .with_run(run)
            .with_context(MemoryContext::new());

        assert_eq!(configuration.environment.as_str(), "orders-proxy");
        assert_eq!(configuration.brokers.len(), 3);
        assert_eq!(configuration.run, Some(run));
    }
}
