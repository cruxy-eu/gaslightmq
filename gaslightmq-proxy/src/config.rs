use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use gaslightmq_proxy_api::RuleSet;

use crate::Proxy;

/// How listeners for discovered upstream nodes get their ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePorts {
    /// The n-th discovered node listens on `base + n`.
    Sequential(u16),
    /// Let the OS pick a free port per node (used by tests).
    Ephemeral,
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Upstream bootstrap address, `host:port`.
    pub upstream: String,
    /// Address of the bootstrap listener clients connect to first. Node
    /// listeners bind to the same IP.
    pub bind: SocketAddr,
    pub node_ports: NodePorts,
    /// Host advertised to clients in rewritten Metadata responses. Defaults
    /// to the bind IP, or `127.0.0.1` when binding to all interfaces.
    pub advertise_host: Option<String>,
    /// Address of the HTTP control plane. `None` disables it.
    pub control: Option<SocketAddr>,
    /// Fault rules active from the start.
    pub rules: RuleSet,
    /// Seed for fault probabilities, for reproducible runs.
    pub seed: Option<u64>,
}

impl Default for ProxyConfig {
    /// All listeners on ephemeral localhost ports, no control plane.
    fn default() -> Self {
        Self {
            upstream: String::new(),
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            node_ports: NodePorts::Ephemeral,
            advertise_host: None,
            control: None,
            rules: RuleSet::default(),
            seed: None,
        }
    }
}

impl ProxyConfig {
    pub(crate) fn advertised_host(&self) -> String {
        match &self.advertise_host {
            Some(host) => host.clone(),
            None if self.bind.ip().is_unspecified() => "127.0.0.1".to_string(),
            None => self.bind.ip().to_string(),
        }
    }
}

/// Builds and starts a [`Proxy`].
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// let proxy = gaslightmq_proxy::Proxy::builder()
///     .upstream("localhost:9092")
///     .bind("0.0.0.0:19092".parse()?)
///     .control("0.0.0.0:9000".parse()?)
///     .start()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, Default)]
pub struct ProxyBuilder {
    config: ProxyConfig,
}

impl ProxyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bootstrap address of the upstream cluster, `host:port`.
    pub fn upstream(mut self, addr: impl Into<String>) -> Self {
        self.config.upstream = addr.into();
        self
    }

    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.config.bind = addr;
        self
    }

    pub fn node_ports(mut self, ports: NodePorts) -> Self {
        self.config.node_ports = ports;
        self
    }

    pub fn advertise_host(mut self, host: impl Into<String>) -> Self {
        self.config.advertise_host = Some(host.into());
        self
    }

    pub fn control(mut self, addr: SocketAddr) -> Self {
        self.config.control = Some(addr);
        self
    }

    pub fn rules(mut self, rules: RuleSet) -> Self {
        self.config.rules = rules;
        self
    }

    pub fn seed(mut self, seed: u64) -> Self {
        self.config.seed = Some(seed);
        self
    }

    pub fn build_config(self) -> ProxyConfig {
        self.config
    }

    pub async fn start(self) -> anyhow::Result<Proxy> {
        Proxy::start(self.config).await
    }
}

impl From<ProxyBuilder> for ProxyConfig {
    fn from(b: ProxyBuilder) -> Self {
        b.build_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_host_defaults() {
        let builder = ProxyBuilder::new().upstream("kafka:9092");
        assert_eq!(builder.clone().build_config().advertised_host(), "127.0.0.1");

        let builder = builder.bind("0.0.0.0:19092".parse().unwrap());
        assert_eq!(builder.clone().build_config().advertised_host(), "127.0.0.1");

        let config = builder.advertise_host("proxy.local").build_config();
        assert_eq!(config.advertised_host(), "proxy.local");
        assert_eq!(config.upstream, "kafka:9092");
    }
}
