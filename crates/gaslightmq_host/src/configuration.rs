//! What a host is configured with, and what it starts one environment with.
//!
//! A property is a public field, and its type only holds values that the property allows.

use gaslightmq_backend_upstream::UpstreamBackendConfiguration;
use gaslightmq_engine::EngineConfiguration;
use gaslightmq_environment::{Environment, EnvironmentName, Instances};
use gaslightmq_quick_inject::QuickInjectConfiguration;

/// What a [`Host`](crate::Host) is configured with: what holds for every environment it runs.
///
/// What one environment runs with is an [`EnvironmentConfiguration`], which its caller hands to
/// [`start`](crate::Host::start).
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_host::prelude::*;
///
/// let mut configuration = HostConfiguration::default();
/// assert_eq!(configuration.quick_inject.completion_limit, 5);
///
/// configuration.quick_inject.default_delay = TimeSpan::from_millis(750);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostConfiguration {
    /// Ours. Default: the defaults of quick inject.
    ///
    /// What a ⌘K sentence means where it leaves a value out, and how many completions it offers.
    /// Every environment reads its sentences with it: see [`read_sentence`](crate::Host::read_sentence).
    pub quick_inject: QuickInjectConfiguration,
}

/// What a host starts one environment with: its name, and the configuration of each instance it runs.
///
/// [`new`](Self::new) reads the environment, and gives every instance what the environment says and the defaults
/// for everything else.
/// Whoever starts the environment then changes the properties that a flag, a variable or a file sets,
/// in the [order](gaslightmq_environment::SettingSource) in which they win.
///
/// | Instance | Configuration | From the environment |
/// |---|---|---|
/// | The proxy | [`engine`](Self::engine) | Its listeners: where clients connect, and the host they see. |
/// | The backend in front of a real cluster | [`upstream`](Self::upstream) | The cluster, and the host clients see. |
///
/// What the engine and the backend do with each property, their own configurations say.
/// The host listens where [`engine`](Self::engine) says, and checks those listeners against the environments that
/// run already.
///
/// ```
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:9092".parse().expect("a listen address"),
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "gaslight.staging".parse().expect("a host name"),
/// )
/// .expect("a bootstrap port outside the ports of the brokers");
/// let staging = Environment::new(
///     "staging-proxy".parse().expect("an environment name"),
///     Instances::ProxyInFrontOfCluster {
///         proxy: ProxyDefinition::new(listen.clone()),
///         upstream: Upstream::new("kafka-1.staging:9092".parse().expect("a server address")),
///     },
/// );
///
/// let mut configuration = EnvironmentConfiguration::new(&staging)?;
/// assert_eq!(configuration.name(), staging.name());
/// assert_eq!(configuration.engine.listeners, listen);
/// assert_eq!(configuration.upstream.advertised_host, "gaslight.staging");
///
/// // The workspace settings name a seed for every environment.
/// configuration.engine.seed = Seed::new(1337);
/// # Ok::<(), EnvironmentConfigurationError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnvironmentConfiguration {
    /// The name of the environment, which the host runs it under.
    pub(crate) name: EnvironmentName,
    /// Ours. Required.
    ///
    /// The engine of the proxy: where it listens, and its waits and limits.
    /// [`new`](Self::new) takes its listeners from the environment.
    pub engine: EngineConfiguration,
    /// Ours. Required.
    ///
    /// The backend of the proxy: the real cluster behind it, the host clients see, and its waits and limits.
    /// [`new`](Self::new) takes the cluster from the environment, and the host clients see from its listeners.
    pub upstream: UpstreamBackendConfiguration,
}

impl EnvironmentConfiguration {
    /// The configuration that `environment` says, with the defaults for everything it leaves out.
    ///
    /// # Errors
    ///
    /// [`EnvironmentConfigurationError::UnsupportedBroker`] when the environment runs a broker:
    /// a proxy in front of a broker, or a broker alone.
    pub fn new(environment: &Environment) -> Result<Self, EnvironmentConfigurationError> {
        match environment.instances() {
            Instances::ProxyInFrontOfCluster { proxy, upstream } => {
                let listeners = proxy.listen().clone();
                let advertised_host = listeners.advertised_host().clone();
                Ok(Self {
                    name: environment.name().clone(),
                    engine: EngineConfiguration::new(listeners),
                    upstream: UpstreamBackendConfiguration::new(upstream.clone(), advertised_host),
                })
            }
            Instances::ProxyInFrontOfBroker { .. } | Instances::BrokerAlone { .. } => {
                Err(EnvironmentConfigurationError::UnsupportedBroker {
                    name: environment.name().clone(),
                })
            }
        }
    }

    /// The name of the environment.
    pub fn name(&self) -> &EnvironmentName {
        &self.name
    }
}

/// Why an environment has no [`EnvironmentConfiguration`].
///
/// ```
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_host::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:29092".parse().expect("a listen address"),
///     "0.0.0.0:29093-29192".parse().expect("broker listeners"),
///     "localhost".parse().expect("a host name"),
/// )
/// .expect("a bootstrap port outside the ports of the brokers");
/// let orders = Environment::new(
///     "orders-sim".parse().expect("an environment name"),
///     Instances::ProxyInFrontOfBroker {
///         proxy: ProxyDefinition::new(listen),
///         broker: BrokerDefinition::new("orders-sim".parse().expect("a world name")),
///     },
/// );
///
/// let error = EnvironmentConfiguration::new(&orders).unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "environment \"orders-sim\" runs a broker, and a host cannot run a broker yet"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentConfigurationError {
    /// The environment runs a broker, which a host cannot run yet: a proxy in front of a broker, or a broker alone.
    #[error("environment {:?} runs a broker, and a host cannot run a broker yet", .name.as_str())]
    UnsupportedBroker { name: EnvironmentName },
}

#[cfg(test)]
mod tests {
    use gaslightmq_environment::{BrokerDefinition, Listeners, ProxyDefinition, Upstream};

    use super::*;

    fn listeners() -> Listeners {
        Listeners::new(
            "0.0.0.0:9092".parse().unwrap(),
            "0.0.0.0:19092-19191".parse().unwrap(),
            "gaslight.staging".parse().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn a_proxy_in_front_of_a_cluster_listens_and_connects_where_its_environment_says() {
        let upstream = Upstream::new("kafka-1.staging:9092".parse().unwrap());
        let staging = Environment::new(
            "staging-proxy".parse().unwrap(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listeners()),
                upstream: upstream.clone(),
            },
        );

        let configuration = EnvironmentConfiguration::new(&staging).unwrap();

        assert_eq!(configuration.name(), staging.name());
        assert_eq!(configuration.engine, EngineConfiguration::new(listeners()));
        assert_eq!(
            configuration.upstream,
            UpstreamBackendConfiguration::new(upstream, "gaslight.staging".parse().unwrap())
        );
    }

    #[test]
    fn a_broker_alone_has_no_configuration_yet() {
        let plain = Environment::new(
            "plain-kafka".parse().unwrap(),
            Instances::BrokerAlone {
                broker: BrokerDefinition::new("orders-sim".parse().unwrap()),
                listen: listeners(),
            },
        );

        assert_eq!(
            EnvironmentConfiguration::new(&plain),
            Err(EnvironmentConfigurationError::UnsupportedBroker {
                name: "plain-kafka".parse().unwrap()
            })
        );
    }
}
