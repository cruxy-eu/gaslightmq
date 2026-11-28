//! What a host is configured with, and what it starts one environment with.
//!
//! A property is a public field, and its type only holds values that the property allows.

use std::fmt;

use gaslightmq_backend_upstream::UpstreamBackendConfiguration;
use gaslightmq_engine::EngineConfiguration;
use gaslightmq_environment::{Environment, EnvironmentName, Instances, RecordingMode};
use gaslightmq_quick_inject::QuickInjectConfiguration;
use gaslightmq_surface::EncoderMapping;
use gaslightmq_traffic::TrafficConfiguration;

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
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostConfiguration {
    /// Ours. Default: the defaults of quick inject.
    ///
    /// What a ⌘K sentence means where it leaves a value out, and how many completions it offers.
    /// Every environment reads its sentences with it: see [`read_sentence`](crate::Host::read_sentence).
    pub quick_inject: QuickInjectConfiguration,
    /// Ours. Default: the defaults of the encoder mapping.
    ///
    /// What one step of each encoder is worth, when someone turns one at a pad: see
    /// [`send_intent`](crate::Host::send_intent).
    pub encoders: EncoderMapping,
    /// Ours. Default: the defaults of the traffic.
    ///
    /// What the traffic of every environment keeps: the calls of its live window, and the names that its calls named.
    /// The statistics come from it, and a ⌘K sentence knows the names.
    pub traffic: TrafficConfiguration,
    /// Ours. Default: 1024 changes.
    ///
    /// How many [changes](crate::HostChange) the host keeps for a follower that falls behind.
    /// A follower that falls further behind learns how many it missed.
    pub change_backlog: ChangeBacklog,
    /// Ours. Default: 1024 calls.
    ///
    /// How many calls the host keeps for a follower of the calls of an environment that falls behind:
    /// see [`follow_calls`](crate::Host::follow_calls). A follower that falls further behind misses the oldest calls.
    pub call_backlog: CallBacklog,
}

impl Default for HostConfiguration {
    fn default() -> Self {
        Self {
            quick_inject: QuickInjectConfiguration::default(),
            encoders: EncoderMapping::default(),
            traffic: TrafficConfiguration::default(),
            change_backlog: ChangeBacklog::DEFAULT,
            call_backlog: CallBacklog::DEFAULT,
        }
    }
}

/// How many changes a host keeps for a follower of its [changes](crate::Host::changes) that falls behind.
///
/// A backlog holds at least one change, and at most [`ChangeBacklog::MAX`].
/// The host sets the room for them aside when it starts, so a backlog has a largest size.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let backlog = ChangeBacklog::new(64)?;
/// assert_eq!(backlog.changes(), 64);
/// assert_eq!(backlog.to_string(), "64 changes");
/// assert_eq!(ChangeBacklog::new(0), Err(ChangeBacklogError::Zero));
/// # Ok::<(), ChangeBacklogError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangeBacklog(usize);

impl ChangeBacklog {
    /// The default backlog: 1024 changes.
    pub const DEFAULT: Self = Self(1024);

    /// The largest backlog: 1 048 576 changes, which a host can still set the room aside for.
    pub const MAX: Self = Self(1 << 20);

    /// Check that `changes` is a number of changes that a backlog can hold, and keep it.
    ///
    /// # Errors
    ///
    /// [`ChangeBacklogError::Zero`] when `changes` is zero,
    /// and [`ChangeBacklogError::TooLarge`] when it is more than [`ChangeBacklog::MAX`].
    pub const fn new(changes: usize) -> Result<Self, ChangeBacklogError> {
        if changes == 0 {
            return Err(ChangeBacklogError::Zero);
        }
        if changes > Self::MAX.0 {
            return Err(ChangeBacklogError::TooLarge { changes });
        }
        Ok(Self(changes))
    }

    /// The backlog as a plain number of changes.
    pub const fn changes(self) -> usize {
        self.0
    }
}

/// Writes the number of changes: `1024 changes`.
impl fmt::Display for ChangeBacklog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} changes", self.0)
    }
}

/// Why a number of changes is no [`ChangeBacklog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChangeBacklogError {
    /// A backlog of no change would keep nothing for a follower.
    #[error("a change backlog holds at least one change")]
    Zero,
    /// The backlog is larger than [`ChangeBacklog::MAX`].
    #[error("a change backlog holds at most 1048576 changes, and not {changes}")]
    TooLarge { changes: usize },
}

/// How many calls a host keeps for a follower of the calls of an environment that falls behind.
///
/// A backlog holds at least one call, and at most [`CallBacklog::MAX`].
/// The host sets the room for them aside when someone follows the calls, so a backlog has a largest size.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let backlog = CallBacklog::new(64)?;
/// assert_eq!(backlog.calls(), 64);
/// assert_eq!(backlog.to_string(), "64 calls");
/// assert_eq!(CallBacklog::new(0), Err(CallBacklogError::Zero));
/// # Ok::<(), CallBacklogError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallBacklog(usize);

impl CallBacklog {
    /// The default backlog: 1024 calls.
    pub const DEFAULT: Self = Self(1024);

    /// The largest backlog: 1 048 576 calls, which a host can still set the room aside for.
    pub const MAX: Self = Self(1 << 20);

    /// Check that `calls` is a number of calls that a backlog can hold, and keep it.
    ///
    /// # Errors
    ///
    /// [`CallBacklogError::Zero`] when `calls` is zero,
    /// and [`CallBacklogError::TooLarge`] when it is more than [`CallBacklog::MAX`].
    pub const fn new(calls: usize) -> Result<Self, CallBacklogError> {
        if calls == 0 {
            return Err(CallBacklogError::Zero);
        }
        if calls > Self::MAX.0 {
            return Err(CallBacklogError::TooLarge { calls });
        }
        Ok(Self(calls))
    }

    /// The backlog as a plain number of calls.
    pub const fn calls(self) -> usize {
        self.0
    }
}

/// Writes the number of calls: `1024 calls`.
impl fmt::Display for CallBacklog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} calls", self.0)
    }
}

/// Why a number of calls is no [`CallBacklog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CallBacklogError {
    /// A backlog of no call would keep nothing for a follower.
    #[error("a call backlog holds at least one call")]
    Zero,
    /// The backlog is larger than [`CallBacklog::MAX`].
    #[error("a call backlog holds at most 1048576 calls, and not {calls}")]
    TooLarge { calls: usize },
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
    /// Ours. Default: what the proxy of the environment says, or `metadata-only` in front of a real cluster.
    ///
    /// How much of a call a recording of the environment keeps.
    /// A real cluster can carry production-like data, so a recording in front of one drops the keys, the values and
    /// the headers of records until someone says otherwise.
    pub recording: RecordingMode,
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
                let mut upstream = UpstreamBackendConfiguration::new(upstream.clone(), advertised_host);
                upstream.advertised_listeners = listeners.advertised_listeners().clone();
                Ok(Self {
                    name: environment.name().clone(),
                    engine: EngineConfiguration::new(listeners),
                    upstream,
                    recording: proxy.recording().unwrap_or(RecordingMode::MetadataOnly),
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
