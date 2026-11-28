//! What GaslightMQ in a container is configured with.
//!
//! A property is a public field with a default.
//! The container and the control plane are no Kafka API, so no property is one of Kafka's: each is ours, and says so.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::{EnvironmentName, HostName, ServerAddress};

use crate::image::GaslightMqImage;

/// The address that the process listens on, unless the test names another: the loopback of the machine of the test.
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Port 0: the system picks a free port.
const ANY_PORT: u16 = 0;

/// Where the files of the workspace are inside the container.
pub(crate) const WORKSPACE_IN_THE_CONTAINER: &str = "/workspace";

/// What GaslightMQ in a container next to a test is configured with: the image, the proxy that `gmq proxy` runs
/// there, the workspace it holds, and how long it gets to start and to stop.
///
/// [`GaslightMqContainer::proxy_in_front_of`](crate::GaslightMqContainer::proxy_in_front_of) makes one with every
/// default, and [`start`](Self::start) starts it.
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_testcontainers::{GaslightMqContainer, GaslightMqImage};
///
/// let cluster = "127.0.0.1:9092".parse().expect("a server address");
/// let mut configuration = GaslightMqContainer::proxy_in_front_of(cluster)
///     .named("orders-proxy".parse().expect("an environment name"))
///     .with_workspace("examples/09-proxy-in-front-of-real-kafka")
///     .with_image(GaslightMqImage::version("latest").expect("an image"));
///
/// // Every listener takes a free port of the loopback of the machine of the test.
/// assert_eq!(configuration.listen.to_string(), "127.0.0.1");
/// assert_eq!(configuration.advertised_host, "127.0.0.1");
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
    /// The bootstrap server of the real cluster, as the test reaches it: the container reaches it under the same
    /// address, and so it reaches every broker under the address that the broker advertises.
    pub upstream: ServerAddress,
    /// Ours. Default: `proxy`.
    ///
    /// The name of the environment of the proxy, which every request about it names.
    /// The workspace holds no environment of that name, or the proxy does not start.
    pub environment: EnvironmentName,
    /// Ours. Default: none, so every definition lives in memory.
    ///
    /// The directory of a workspace on the machine of the test.
    /// Its files are copied into the container before the process starts, so the process reads them, and
    /// whatever it writes, a definition or a recording, stays in the container: the files of the test never change.
    pub workspace: Option<PathBuf>,
    /// Ours. Default: `127.0.0.1`.
    ///
    /// The address that every listener listens on, each on a free port: the bootstrap listener, one listener for each
    /// broker that the proxy finds in the cluster, and the control plane.
    pub listen: IpAddr,
    /// Ours. Default: `127.0.0.1`.
    ///
    /// The host under which the clients of the test reach the brokers of the proxy, which Metadata names to them.
    pub advertised_host: HostName,
    /// Ours. Default: 1 minute.
    ///
    /// How long the process gets to start its environment and its control plane, once the machine holds the image.
    /// Pulling the image takes as long as it takes.
    pub start_timeout: TimeSpan,
    /// Ours. Default: 10 seconds.
    ///
    /// How long the process gets to stop once it is [asked to](crate::GaslightMqContainer::stop), before it is
    /// killed.
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
            workspace: None,
            listen: LOCALHOST,
            advertised_host: LOCALHOST.into(),
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

    /// The same configuration, over the files of the workspace in `directory`.
    #[must_use]
    pub fn with_workspace(mut self, directory: impl Into<PathBuf>) -> Self {
        self.workspace = Some(directory.into());
        self
    }

    /// The same configuration, with every listener on a free port of `listen`, and the brokers of the proxy under
    /// `advertised_host` for its clients.
    #[must_use]
    pub fn listening_on(mut self, listen: IpAddr, advertised_host: HostName) -> Self {
        self.listen = listen;
        self.advertised_host = advertised_host;
        self
    }

    /// The same configuration, which runs `image` whatever the environment says.
    #[must_use]
    pub fn with_image(mut self, image: GaslightMqImage) -> Self {
        self.image = Some(image);
        self
    }

    /// The arguments of `gmq` in the container: `gmq proxy` in front of the cluster, every listener on a free port,
    /// and the workspace where the crate copies it.
    pub(crate) fn arguments(&self) -> Vec<String> {
        let any_port = |address: IpAddr| std::net::SocketAddr::new(address, ANY_PORT).to_string();
        let mut arguments = [
            "proxy",
            "--upstream",
            &self.upstream.to_string(),
            "--name",
            self.environment.as_str(),
            "--listen",
            &any_port(self.listen),
            "--brokers",
            &any_port(self.listen),
            "--advertise",
            self.advertised_host.as_ref(),
            "--control-listen",
            &any_port(self.listen),
        ]
        .map(str::to_owned)
        .to_vec();
        if self.workspace.is_some() {
            arguments.extend(["--workspace".to_owned(), WORKSPACE_IN_THE_CONTAINER.to_owned()]);
        }
        arguments
    }
}

/// The name of the environment of a proxy that the test names no other for.
fn the_proxy() -> EnvironmentName {
    "proxy".parse().expect("`proxy` is an environment name")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster() -> ServerAddress {
        "127.0.0.1:9092".parse().unwrap()
    }

    #[test]
    fn a_proxy_in_front_of_a_cluster_listens_on_free_ports_of_the_loopback() {
        let configuration = GaslightMqContainerConfiguration::proxy_in_front_of(cluster());

        assert_eq!(configuration.image, None);
        assert_eq!(configuration.upstream, cluster());
        assert_eq!(configuration.environment.as_str(), "proxy");
        assert_eq!(configuration.workspace, None);
        assert_eq!(configuration.listen, LOCALHOST);
        assert_eq!(configuration.advertised_host, HostName::from(LOCALHOST));
        assert_eq!(configuration.start_timeout, TimeSpan::from_secs(60));
        assert_eq!(configuration.stop_timeout, TimeSpan::from_secs(10));
        assert_eq!(configuration.log_lines_in_errors, 40);
        assert_eq!(
            configuration.arguments(),
            [
                "proxy",
                "--upstream",
                "127.0.0.1:9092",
                "--name",
                "proxy",
                "--listen",
                "127.0.0.1:0",
                "--brokers",
                "127.0.0.1:0",
                "--advertise",
                "127.0.0.1",
                "--control-listen",
                "127.0.0.1:0",
            ]
        );
    }

    #[test]
    fn a_workspace_is_read_where_the_crate_copies_it() {
        let configuration = GaslightMqContainerConfiguration::proxy_in_front_of(cluster())
            .named("orders-proxy".parse().unwrap())
            .with_workspace("gaslight");

        let arguments = configuration.arguments();

        assert_eq!(configuration.workspace, Some(PathBuf::from("gaslight")));
        assert_eq!(arguments[3..5], ["--name", "orders-proxy"]);
        assert_eq!(arguments[arguments.len() - 2..], ["--workspace", "/workspace"]);
    }

    #[test]
    fn listeners_on_another_address_take_free_ports_of_it() {
        let every_address: IpAddr = "0.0.0.0".parse().unwrap();
        let reached: HostName = "gaslight.test".parse().unwrap();

        let configuration =
            GaslightMqContainerConfiguration::proxy_in_front_of(cluster()).listening_on(every_address, reached);

        assert_eq!(
            configuration.arguments()[6..],
            [
                "--listen",
                "0.0.0.0:0",
                "--brokers",
                "0.0.0.0:0",
                "--advertise",
                "gaslight.test",
                "--control-listen",
                "0.0.0.0:0",
            ]
        );
    }
}
