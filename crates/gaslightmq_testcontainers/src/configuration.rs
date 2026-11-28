//! What GaslightMQ in a container is configured with.
//!
//! A property is a public field with a default.
//! The container and the control plane are no Kafka API, so no property is one of Kafka's: each is ours, and says so.

use std::num::NonZeroU16;
use std::path::PathBuf;

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::{EnvironmentName, ServerAddress};

use crate::image::GaslightMqImage;

/// The port of the bootstrap listener inside the container, which the image exposes.
pub(crate) const BOOTSTRAP_PORT: u16 = 9092;

/// The port of the first broker listener inside the container, which the image exposes. Each broker after it takes
/// the port after the one before it.
pub(crate) const FIRST_BROKER_PORT: u16 = 19092;

/// The port of the control plane inside the container, where the image has it listen.
pub(crate) const CONTROL_PLANE_PORT: u16 = 7092;

/// Where the files of the workspace are inside the container.
pub(crate) const WORKSPACE_IN_THE_CONTAINER: &str = "/workspace";

/// What `--advertised-listeners` says when the process reads its advertised listeners from its input.
const FROM_THE_INPUT: &str = "-";

/// What GaslightMQ in a container next to a test is configured with: the image, the proxy that `gmq proxy` runs
/// there, the network of containers it joins, the workspace it holds, and how long it gets to start and to stop.
///
/// [`GaslightMqContainer::proxy_in_front_of`](crate::GaslightMqContainer::proxy_in_front_of) makes one with every
/// default, and [`start`](Self::start) starts it.
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_testcontainers::{GaslightMqContainer, GaslightMqImage};
///
/// // The cluster as the containers of the network `orders-test` reach it.
/// let cluster = "kafka:19092".parse().expect("a server address");
/// let mut configuration = GaslightMqContainer::proxy_in_front_of(cluster)
///     .named("orders-proxy".parse().expect("an environment name"))
///     .with_network("orders-test")
///     .with_workspace("examples/09-proxy-in-front-of-real-kafka")
///     .with_image(GaslightMqImage::version("latest").expect("an image"));
///
/// // Ten brokers of the cluster get a listener that the test reaches, until the test asks for more.
/// assert_eq!(configuration.brokers.get(), 10);
///
/// // A slow machine gives the process longer to start.
/// configuration.start_timeout = TimeSpan::from_secs(120);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GaslightMqContainerConfiguration {
    /// Ours. Default: none.
    ///
    /// The image the container runs.
    /// Without one, it runs the image that the environment variable [`GASLIGHTMQ_IMAGE`](GaslightMqImage::VARIABLE)
    /// names, and without that variable the [default image](GaslightMqImage::default).
    pub image: Option<GaslightMqImage>,
    /// Ours. Required.
    ///
    /// The bootstrap server of the real cluster, as the container reaches it: on its [network](Self::network), or
    /// anywhere else that the network of the container leads to.
    pub upstream: ServerAddress,
    /// Ours. Default: `proxy`.
    ///
    /// The name of the environment of the proxy, which every request about it names.
    /// The workspace holds no environment of that name, or the proxy does not start.
    pub environment: EnvironmentName,
    /// Ours. Default: none, so the container runs on the network that the container runtime gives it.
    ///
    /// The network of containers that the container joins, such as the one of the cluster.
    /// Whoever runs the test sets the networks up: a network that is not there, the container runtime creates, and
    /// removes again once its last container of the test is gone.
    pub network: Option<String>,
    /// Ours. Default: 10.
    ///
    /// How many brokers of the cluster get a listener that the test reaches: the container publishes a port for
    /// each, and the proxy names each one to its clients under its published port.
    /// A cluster with more brokers does not start the proxy.
    pub brokers: NonZeroU16,
    /// Ours. Default: none, so every definition lives in memory.
    ///
    /// The directory of a workspace on the machine of the test.
    /// Its files are copied into the container before the process starts, so the process reads them, and
    /// whatever it writes, a definition or a recording, stays in the container: the files of the test never change.
    pub workspace: Option<PathBuf>,
    /// Ours. Default: 1 minute.
    ///
    /// How long the process gets to start its environment and its control plane, once the machine holds the image.
    /// Pulling the image takes as long as it takes.
    pub start_timeout: TimeSpan,
    /// Ours. Default: 10 seconds.
    ///
    /// How long the process gets to stop once it is [asked to](crate::GaslightMqContainer::stop), before it is
    /// killed. The container runtime counts it in whole seconds, rounded up.
    pub stop_timeout: TimeSpan,
    /// Ours. Default: 40.
    ///
    /// How many of the last lines that the process wrote an error shows, when it does not get ready.
    pub log_lines_in_errors: usize,
}

impl GaslightMqContainerConfiguration {
    /// A proxy in front of the real cluster at `cluster`, with every default.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> Self {
        Self {
            image: None,
            upstream: cluster,
            environment: the_proxy(),
            network: None,
            brokers: ten_brokers(),
            workspace: None,
            start_timeout: TimeSpan::from_secs(60),
            stop_timeout: TimeSpan::from_secs(10),
            log_lines_in_errors: 40,
        }
    }

    /// The same configuration, with the environment named `environment`.
    #[must_use]
    pub fn named(mut self, environment: EnvironmentName) -> Self {
        self.environment = environment;
        self
    }

    /// The same configuration, on the network of containers `network`.
    #[must_use]
    pub fn with_network(mut self, network: impl Into<String>) -> Self {
        self.network = Some(network.into());
        self
    }

    /// The same configuration, over the files of the workspace in `directory`.
    #[must_use]
    pub fn with_workspace(mut self, directory: impl Into<PathBuf>) -> Self {
        self.workspace = Some(directory.into());
        self
    }

    /// The same configuration, which runs `image` whatever the environment says.
    #[must_use]
    pub fn with_image(mut self, image: GaslightMqImage) -> Self {
        self.image = Some(image);
        self
    }

    /// The ports of the brokers inside the container, one for each broker that gets a listener.
    pub(crate) fn broker_ports(&self) -> impl Iterator<Item = u16> {
        (0..self.brokers.get()).map(|broker| FIRST_BROKER_PORT.saturating_add(broker))
    }

    /// Every port that the container publishes: the bootstrap listener, the brokers and the control plane.
    pub(crate) fn published_ports(&self) -> Vec<u16> {
        let mut ports = vec![BOOTSTRAP_PORT, CONTROL_PLANE_PORT];
        ports.extend(self.broker_ports());
        ports
    }

    /// The arguments of `gmq` in the container: `gmq proxy` in front of the cluster, on the ports that the container
    /// publishes, which reads where its brokers are published from its input, and over the workspace where the crate
    /// copies it.
    pub(crate) fn arguments(&self) -> Vec<String> {
        let last_broker = FIRST_BROKER_PORT.saturating_add(self.brokers.get() - 1);
        let mut arguments = [
            "proxy",
            "--upstream",
            &self.upstream.to_string(),
            "--name",
            self.environment.as_str(),
            "--listen",
            &format!("0.0.0.0:{BOOTSTRAP_PORT}"),
            "--brokers",
            &format!("0.0.0.0:{FIRST_BROKER_PORT}-{last_broker}"),
            "--advertised-listeners",
            FROM_THE_INPUT,
        ]
        .map(str::to_owned)
        .to_vec();
        if self.workspace.is_some() {
            arguments.extend(["--workspace".to_owned(), WORKSPACE_IN_THE_CONTAINER.to_owned()]);
        }
        arguments
    }
}

/// The advertised listeners of the brokers, as `gmq proxy` reads them: each broker port of the container under
/// `host` and the port that `published` says it is published on.
pub(crate) fn advertised_listeners(
    configuration: &GaslightMqContainerConfiguration,
    host: &str,
    published: impl Fn(u16) -> Option<u16>,
) -> Result<String, u16> {
    let mut listeners = Vec::new();
    for port in configuration.broker_ports() {
        let outside = published(port).ok_or(port)?;
        listeners.push(format!("{port}={host}:{outside}"));
    }
    Ok(listeners.join(","))
}

/// How many brokers get a listener that the test reaches, unless the test asks for another number.
fn ten_brokers() -> NonZeroU16 {
    NonZeroU16::new(10).expect("ten is no zero")
}

/// The name of the environment of a proxy that the test names no other for.
fn the_proxy() -> EnvironmentName {
    "proxy".parse().expect("`proxy` is an environment name")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster() -> ServerAddress {
        "kafka:19092".parse().unwrap()
    }

    #[test]
    fn a_proxy_in_front_of_a_cluster_publishes_its_listeners_and_reads_where_from_its_input() {
        let configuration = GaslightMqContainerConfiguration::proxy_in_front_of(cluster());

        assert_eq!(configuration.image, None);
        assert_eq!(configuration.upstream, cluster());
        assert_eq!(configuration.environment.as_str(), "proxy");
        assert_eq!(configuration.network, None);
        assert_eq!(configuration.brokers.get(), 10);
        assert_eq!(configuration.workspace, None);
        assert_eq!(configuration.start_timeout, TimeSpan::from_secs(60));
        assert_eq!(configuration.stop_timeout, TimeSpan::from_secs(10));
        assert_eq!(configuration.log_lines_in_errors, 40);
        assert_eq!(
            configuration.arguments(),
            [
                "proxy",
                "--upstream",
                "kafka:19092",
                "--name",
                "proxy",
                "--listen",
                "0.0.0.0:9092",
                "--brokers",
                "0.0.0.0:19092-19101",
                "--advertised-listeners",
                "-",
            ]
        );
        assert_eq!(configuration.published_ports().len(), 12);
    }

    #[test]
    fn a_workspace_is_read_where_the_crate_copies_it() {
        let configuration = GaslightMqContainerConfiguration::proxy_in_front_of(cluster())
            .named("orders-proxy".parse().unwrap())
            .with_network("orders-test")
            .with_workspace("gaslight");

        let arguments = configuration.arguments();

        assert_eq!(configuration.network.as_deref(), Some("orders-test"));
        assert_eq!(arguments[3..5], ["--name", "orders-proxy"]);
        assert_eq!(arguments[arguments.len() - 2..], ["--workspace", "/workspace"]);
    }

    #[test]
    fn each_broker_is_advertised_under_the_port_that_it_is_published_on() {
        let mut configuration = GaslightMqContainerConfiguration::proxy_in_front_of(cluster());
        configuration.brokers = NonZeroU16::new(2).unwrap();
        let published = |port: u16| Some(port + 40_000);

        let advertised = advertised_listeners(&configuration, "localhost", published);

        assert_eq!(advertised, Ok("19092=localhost:59092,19093=localhost:59093".to_owned()));
        assert_eq!(advertised_listeners(&configuration, "localhost", |_| None), Err(19092));
    }
}
