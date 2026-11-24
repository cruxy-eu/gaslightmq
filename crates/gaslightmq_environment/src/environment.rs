use std::fmt;

use crate::address::ListenAddress;
use crate::host::HostName;
use crate::listeners::Listeners;
use crate::name::{EnvironmentName, WorldName};
use crate::ports::Taken;
use crate::recording::RecordingMode;
use crate::schema_registry::SchemaRegistry;
use crate::title::EnvironmentTitle;
use crate::upstream::Upstream;
use crate::uuid::EnvironmentUuid;

/// The proxy of an environment: where it listens, and how much its recordings keep.
///
/// What stands behind the proxy is not a part of it.
/// The [`Instances`] of the environment say that: a real cluster, or the broker of the same environment.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:9092".parse().expect("a listen address"),
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "localhost".parse().expect("a host name"),
/// )?;
/// let proxy = ProxyDefinition::new(listen).with_recording(RecordingMode::MetadataOnly);
///
/// assert_eq!(proxy.recording(), Some(RecordingMode::MetadataOnly));
/// # Ok::<(), ListenersError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDefinition {
    listen: Listeners,
    recording: Option<RecordingMode>,
}

impl ProxyDefinition {
    /// A proxy that listens there, and records in the mode that is the default for what stands behind it.
    pub fn new(listen: Listeners) -> Self {
        Self {
            listen,
            recording: None,
        }
    }

    /// The same proxy, which records in this mode.
    #[must_use]
    pub fn with_recording(mut self, recording: RecordingMode) -> Self {
        self.recording = Some(recording);
        self
    }

    /// Where the proxy listens.
    pub fn listen(&self) -> &Listeners {
        &self.listen
    }

    /// The recording mode that the definition names, or `None` when it leaves the mode to the default.
    pub fn recording(&self) -> Option<RecordingMode> {
        self.recording
    }
}

/// The broker of an environment: the world it answers from.
///
/// A broker behind a proxy has no listeners, because clients reach it through the proxy.
/// A broker alone has its own, and the [`Instances`] of the environment hold them.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let broker = BrokerDefinition::new("orders-sim".parse()?);
/// assert_eq!(broker.world(), &WorldName::new("orders-sim")?);
/// # Ok::<(), WorldNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerDefinition {
    world: WorldName,
}

impl BrokerDefinition {
    /// A broker that answers from the world of this name.
    pub fn new(world: WorldName) -> Self {
        Self { world }
    }

    /// The name of the world the broker answers from.
    pub fn world(&self) -> &WorldName {
        &self.world
    }
}

/// The instances that run together in one environment: one of three shapes.
///
/// | Shape | Clients connect to | Truth comes from |
/// |---|---|---|
/// | [`ProxyInFrontOfCluster`](Self::ProxyInFrontOfCluster) | The proxy. | A real cluster. |
/// | [`ProxyInFrontOfBroker`](Self::ProxyInFrontOfBroker) | The proxy. | The broker, which the proxy reaches without a socket. |
/// | [`BrokerAlone`](Self::BrokerAlone) | The broker. Nothing lies. | The broker. |
///
/// Whatever the shape, there is one set of [`Listeners`]: those that clients connect to.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:39092".parse().expect("a listen address"),
///     "0.0.0.0:39093-39192".parse().expect("broker listeners"),
///     "localhost".parse().expect("a host name"),
/// )
/// .expect("a bootstrap port outside the ports of the brokers");
/// let instances = Instances::BrokerAlone {
///     broker: BrokerDefinition::new("orders-sim".parse()?),
///     listen: listen.clone(),
/// };
///
/// assert_eq!(instances.listeners(), &listen);
/// assert!(instances.proxy().is_none());
/// assert_eq!(instances.to_string(), "a broker alone");
/// # Ok::<(), WorldNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Instances {
    /// A proxy in front of a real cluster.
    ProxyInFrontOfCluster {
        /// The proxy.
        proxy: ProxyDefinition,
        /// The real cluster behind it.
        upstream: Upstream,
    },
    /// A proxy in front of a broker of the same environment: a fake cluster that lies.
    ProxyInFrontOfBroker {
        /// The proxy.
        proxy: ProxyDefinition,
        /// The broker behind it.
        broker: BrokerDefinition,
    },
    /// A broker with its own listeners and no proxy: a truthful fake cluster.
    BrokerAlone {
        /// The broker.
        broker: BrokerDefinition,
        /// Where the broker listens.
        listen: Listeners,
    },
}

impl Instances {
    /// The proxy, when the environment has one.
    pub fn proxy(&self) -> Option<&ProxyDefinition> {
        match self {
            Self::ProxyInFrontOfCluster { proxy, .. } | Self::ProxyInFrontOfBroker { proxy, .. } => Some(proxy),
            Self::BrokerAlone { .. } => None,
        }
    }

    /// The broker, when the environment has one.
    pub fn broker(&self) -> Option<&BrokerDefinition> {
        match self {
            Self::ProxyInFrontOfCluster { .. } => None,
            Self::ProxyInFrontOfBroker { broker, .. } | Self::BrokerAlone { broker, .. } => Some(broker),
        }
    }

    /// The real cluster behind the proxy, when there is one.
    pub fn upstream(&self) -> Option<&Upstream> {
        match self {
            Self::ProxyInFrontOfCluster { upstream, .. } => Some(upstream),
            Self::ProxyInFrontOfBroker { .. } | Self::BrokerAlone { .. } => None,
        }
    }

    /// The listeners that clients connect to: those of the proxy, or those of a broker alone.
    pub fn listeners(&self) -> &Listeners {
        match self {
            Self::ProxyInFrontOfCluster { proxy, .. } | Self::ProxyInFrontOfBroker { proxy, .. } => proxy.listen(),
            Self::BrokerAlone { listen, .. } => listen,
        }
    }

    /// The recording mode when nothing sets one.
    ///
    /// A real cluster can carry production-like data, so a recording in front of it keeps metadata only.
    /// The data of a broker is synthetic or was recorded already, so a recording of it is full.
    pub fn default_recording(&self) -> RecordingMode {
        match self {
            Self::ProxyInFrontOfCluster { .. } => RecordingMode::MetadataOnly,
            Self::ProxyInFrontOfBroker { .. } | Self::BrokerAlone { .. } => RecordingMode::Full,
        }
    }

    fn listeners_mut(&mut self) -> &mut Listeners {
        match self {
            Self::ProxyInFrontOfCluster { proxy, .. } | Self::ProxyInFrontOfBroker { proxy, .. } => &mut proxy.listen,
            Self::BrokerAlone { listen, .. } => listen,
        }
    }
}

/// Writes the shape, for a person: `a proxy in front of a real cluster`.
impl fmt::Display for Instances {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ProxyInFrontOfCluster { .. } => "a proxy in front of a real cluster",
            Self::ProxyInFrontOfBroker { .. } => "a proxy in front of a broker",
            Self::BrokerAlone { .. } => "a broker alone",
        })
    }
}

/// A named definition of instances that run together: a proxy, a broker, or a proxy in front of a broker.
///
/// An environment says where its instances listen and what stands behind them.
/// It can also name a [`SchemaRegistry`].
///
/// An environment holds together:
///
/// - its instances are one of the three shapes of [`Instances`],
/// - a built-in schema registry stands next to a broker, and a real one next to a real cluster,
/// - a built-in schema registry takes no port of a Kafka listener.
///
/// An environment of a workspace also has an [`EnvironmentUuid`], which its file holds as `uuid`.
/// Nobody writes it: the workspace gives an environment its UUID, and tells its environments apart by it.
/// Everything else references an environment by its name.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:9092".parse().expect("a listen address"),
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "gaslight.staging".parse()?,
/// )
/// .expect("a bootstrap port outside the ports of the brokers");
/// let staging = Environment::new(
///     "staging-proxy".parse().expect("an environment name"),
///     Instances::ProxyInFrontOfCluster {
///         proxy: ProxyDefinition::new(listen),
///         upstream: Upstream::new("kafka-1.staging:9092".parse().expect("a server address")),
///     },
/// );
///
/// assert_eq!(staging.name(), "staging-proxy");
/// assert_eq!(staging.recording(), None);
/// assert_eq!(staging.instances().default_recording(), RecordingMode::MetadataOnly);
///
/// // A flag starts the environment with another advertised host than its file names.
/// let local = staging.with_advertised_host("localhost".parse()?);
/// assert_eq!(local.listeners().advertised_host(), "localhost");
/// # Ok::<(), HostNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    name: EnvironmentName,
    uuid: Option<EnvironmentUuid>,
    title: Option<EnvironmentTitle>,
    instances: Instances,
    schema_registry: Option<SchemaRegistry>,
}

impl Environment {
    /// The environment of this name with these instances, without a UUID, a title or a schema registry.
    pub fn new(name: EnvironmentName, instances: Instances) -> Self {
        Self {
            name,
            uuid: None,
            title: None,
            instances,
            schema_registry: None,
        }
    }

    /// The same environment with the UUID that the workspace gives it.
    /// It replaces the UUID the environment had before.
    #[must_use]
    pub fn with_uuid(self, uuid: EnvironmentUuid) -> Self {
        Self {
            uuid: Some(uuid),
            ..self
        }
    }

    /// The same environment without a UUID: an environment that no workspace holds yet, such as the copy of one.
    #[must_use]
    pub fn without_uuid(self) -> Self {
        Self { uuid: None, ..self }
    }

    /// The same environment with a title, which says what the environment is.
    /// It replaces the title the environment had before.
    #[must_use]
    pub fn with_title(self, title: EnvironmentTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same environment without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
    }

    /// The same environment with this schema registry.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentError::BuiltInRegistryWithoutBroker`] for a built-in registry in an environment without a broker.
    /// - [`EnvironmentError::RealRegistryNextToBroker`] for a real registry in an environment with a broker.
    /// - [`EnvironmentError::RegistryAmongListeners`] when the built-in registry takes a port of a Kafka listener.
    pub fn with_schema_registry(mut self, registry: SchemaRegistry) -> Result<Self, EnvironmentError> {
        let name = || self.name.clone();
        match (&registry, self.instances.broker()) {
            (SchemaRegistry::BuiltIn { .. }, None) => {
                return Err(EnvironmentError::BuiltInRegistryWithoutBroker { name: name() });
            }
            (SchemaRegistry::Real { .. }, Some(_)) => {
                return Err(EnvironmentError::RealRegistryNextToBroker { name: name() });
            }
            (SchemaRegistry::BuiltIn { .. }, Some(_)) | (SchemaRegistry::Real { .. }, None) => {}
        }
        let of_registry: Vec<Taken> = registry.listen().and_then(Taken::one).into_iter().collect();
        if let Some(address) = Taken::first_shared(&of_registry, &self.instances.listeners().taken()) {
            return Err(EnvironmentError::RegistryAmongListeners { name: name(), address });
        }
        self.schema_registry = Some(registry);
        Ok(self)
    }

    /// The same environment, whose clients reach its listeners under another host.
    ///
    /// A command that starts an environment can name the host, and it then wins over the one in the file.
    #[must_use]
    pub fn with_advertised_host(mut self, advertise: HostName) -> Self {
        let listeners = self.instances.listeners_mut();
        *listeners = listeners.clone().with_advertised_host(advertise);
        self
    }

    /// The name of the environment, unique among environments. It is the identity of the environment.
    pub fn name(&self) -> &EnvironmentName {
        &self.name
    }

    /// The identity of the environment inside the workspace, once the workspace gave it one.
    pub fn uuid(&self) -> Option<EnvironmentUuid> {
        self.uuid
    }

    /// What the environment is, in free text, when someone wrote it down.
    pub fn title(&self) -> Option<&EnvironmentTitle> {
        self.title.as_ref()
    }

    /// The instances that run together, and the shape they have.
    pub fn instances(&self) -> &Instances {
        &self.instances
    }

    /// The schema registry, when the environment names one.
    pub fn schema_registry(&self) -> Option<&SchemaRegistry> {
        self.schema_registry.as_ref()
    }

    /// The listeners that Kafka clients connect to.
    pub fn listeners(&self) -> &Listeners {
        self.instances.listeners()
    }

    /// The name of the world the broker answers from, when the environment has a broker.
    pub fn world(&self) -> Option<&WorldName> {
        self.instances.broker().map(BrokerDefinition::world)
    }

    /// The recording mode that the environment names, or `None` when it leaves the mode to another
    /// [source](crate::SettingSource).
    pub fn recording(&self) -> Option<RecordingMode> {
        self.instances.proxy().and_then(ProxyDefinition::recording)
    }

    /// The first address that a listener of this environment and a listener of `other` would both listen on.
    ///
    /// The listeners are the Kafka listeners and the built-in schema registry.
    /// A process cannot start two environments that share one.
    ///
    /// ```
    /// use gaslightmq_environment::prelude::*;
    ///
    /// let broker = |name: &str, bootstrap: &str, brokers: &str| {
    ///     let bootstrap = bootstrap.parse().expect("a listen address");
    ///     let brokers = brokers.parse().expect("broker listeners");
    ///     let listen = Listeners::new(bootstrap, brokers, "localhost".parse().expect("a host name"))
    ///         .expect("a bootstrap port outside the ports of the brokers");
    ///     let broker = BrokerDefinition::new("shop".parse().expect("a world name"));
    ///     Environment::new(name.parse().expect("an environment name"), Instances::BrokerAlone { broker, listen })
    /// };
    /// let shop = broker("shop", "0.0.0.0:9092", "0.0.0.0:19092-19094");
    /// let tests = broker("tests", "127.0.0.1:29092", "127.0.0.1:19094-19096");
    ///
    /// assert_eq!(shop.shared_listener(&tests), Some("127.0.0.1:19094".parse()?));
    /// # Ok::<(), ListenAddressError>(())
    /// ```
    pub fn shared_listener(&self, other: &Environment) -> Option<ListenAddress> {
        Taken::first_shared(&self.taken(), &other.taken())
    }

    /// Every port that the listeners of this environment can take.
    fn taken(&self) -> Vec<Taken> {
        let mut taken = self.instances.listeners().taken();
        let registry = self.schema_registry.as_ref().and_then(SchemaRegistry::listen);
        taken.extend(registry.and_then(Taken::one));
        taken
    }
}

/// Why what a file holds is not an [`Environment`], or why an environment cannot take a schema registry.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let listen = Listeners::new(
///     "0.0.0.0:9092".parse()?,
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "localhost".parse().expect("a host name"),
/// )
/// .expect("a bootstrap port outside the ports of the brokers");
/// let staging = Environment::new(
///     "staging".parse().expect("an environment name"),
///     Instances::ProxyInFrontOfCluster {
///         proxy: ProxyDefinition::new(listen),
///         upstream: Upstream::new("kafka-1.staging:9092".parse().expect("a server address")),
///     },
/// );
///
/// let error = staging
///     .with_schema_registry(SchemaRegistry::BuiltIn { listen: "0.0.0.0:8081".parse()? })
///     .unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "environment \"staging\" has no broker, so its schema registry is the real one, with a URL"
/// );
/// # Ok::<(), ListenAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentError {
    /// The environment has neither a proxy nor a broker.
    #[error("environment {:?} has neither a proxy nor a broker", .name.as_str())]
    NoInstance { name: EnvironmentName },
    /// The proxy has no upstream, and the environment has no broker for it to stand in front of.
    #[error(
        "environment {:?} has a proxy without an upstream, and no broker for it to stand in front of",
        .name.as_str()
    )]
    ProxyWithoutBackend { name: EnvironmentName },
    /// The proxy has an upstream, and the environment has a broker too.
    #[error(
        "environment {:?} has a proxy with an upstream, and a broker too: a proxy stands in front of one of them",
        .name.as_str()
    )]
    TwoBackends { name: EnvironmentName },
    /// The environment has a broker and no proxy, and the broker has no listeners.
    #[error("environment {:?} has a broker alone, and a broker alone needs listeners", .name.as_str())]
    BrokerWithoutListeners { name: EnvironmentName },
    /// The broker stands behind a proxy, and it has listeners of its own.
    #[error("environment {:?} has a broker behind a proxy, so only the proxy has listeners", .name.as_str())]
    BrokerListensBehindProxy { name: EnvironmentName },
    /// The schema registry is the built-in one, and the environment has no broker.
    #[error("environment {:?} has no broker, so its schema registry is the real one, with a URL", .name.as_str())]
    BuiltInRegistryWithoutBroker { name: EnvironmentName },
    /// The schema registry is a real one, and the environment has a broker.
    #[error(
        "environment {:?} has a broker, so its schema registry is the built-in one, with an address to listen on",
        .name.as_str()
    )]
    RealRegistryNextToBroker { name: EnvironmentName },
    /// The built-in schema registry would listen on a port of a Kafka listener.
    #[error("environment {:?} has its schema registry and a Kafka listener both on {address}", .name.as_str())]
    RegistryAmongListeners {
        name: EnvironmentName,
        address: ListenAddress,
    },
}

impl EnvironmentError {
    /// The name of the environment that the error is about.
    pub fn name(&self) -> &EnvironmentName {
        match self {
            Self::NoInstance { name }
            | Self::ProxyWithoutBackend { name }
            | Self::TwoBackends { name }
            | Self::BrokerWithoutListeners { name }
            | Self::BrokerListensBehindProxy { name }
            | Self::BuiltInRegistryWithoutBroker { name }
            | Self::RealRegistryNextToBroker { name }
            | Self::RegistryAmongListeners { name, .. } => name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The UUID that a workspace gave the environment `staging-proxy`, and the one of another environment.
    const STAGING_PROXY_UUID: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";
    const ANOTHER_UUID: &str = "0193b6c2-6b07-7d52-8a1e-3c5f7a9b0d24";

    fn listen(bootstrap: &str, brokers: &str) -> Listeners {
        Listeners::new(
            bootstrap.parse().unwrap(),
            brokers.parse().unwrap(),
            "localhost".parse().unwrap(),
        )
        .unwrap()
    }

    fn name(text: &str) -> EnvironmentName {
        text.parse().unwrap()
    }

    fn broker(world: &str) -> BrokerDefinition {
        BrokerDefinition::new(world.parse().unwrap())
    }

    fn in_front_of_cluster() -> Instances {
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen("0.0.0.0:9092", "0.0.0.0:19092-19191")),
            upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
        }
    }

    fn in_front_of_broker() -> Instances {
        Instances::ProxyInFrontOfBroker {
            proxy: ProxyDefinition::new(listen("0.0.0.0:29092", "0.0.0.0:29093-29192")),
            broker: broker("orders-sim"),
        }
    }

    fn alone() -> Instances {
        Instances::BrokerAlone {
            broker: broker("orders-sim"),
            listen: listen("0.0.0.0:39092", "0.0.0.0:39093-39192"),
        }
    }

    fn built_in(address: &str) -> SchemaRegistry {
        SchemaRegistry::BuiltIn {
            listen: address.parse().unwrap(),
        }
    }

    fn real() -> SchemaRegistry {
        SchemaRegistry::Real {
            url: "https://schema-registry.staging.internal:8081".parse().unwrap(),
        }
    }

    #[test]
    fn each_shape_says_what_it_has() {
        let cluster = in_front_of_cluster();
        assert!(cluster.proxy().is_some() && cluster.broker().is_none() && cluster.upstream().is_some());
        assert_eq!(cluster.listeners(), &listen("0.0.0.0:9092", "0.0.0.0:19092-19191"));
        assert_eq!(cluster.to_string(), "a proxy in front of a real cluster");

        let fake = in_front_of_broker();
        assert!(fake.proxy().is_some() && fake.broker().is_some() && fake.upstream().is_none());
        assert_eq!(fake.listeners(), &listen("0.0.0.0:29092", "0.0.0.0:29093-29192"));
        assert_eq!(fake.to_string(), "a proxy in front of a broker");

        let truthful = alone();
        assert!(truthful.proxy().is_none() && truthful.broker().is_some() && truthful.upstream().is_none());
        assert_eq!(truthful.listeners(), &listen("0.0.0.0:39092", "0.0.0.0:39093-39192"));
        assert_eq!(truthful.to_string(), "a broker alone");
    }

    #[test]
    fn a_recording_in_front_of_a_real_cluster_keeps_metadata_only_by_default() {
        assert_eq!(in_front_of_cluster().default_recording(), RecordingMode::MetadataOnly);
        assert_eq!(in_front_of_broker().default_recording(), RecordingMode::Full);
        assert_eq!(alone().default_recording(), RecordingMode::Full);
    }

    #[test]
    fn an_environment_says_what_its_definition_names() {
        let proxy =
            ProxyDefinition::new(listen("0.0.0.0:29092", "0.0.0.0:29093-29192")).with_recording(RecordingMode::Full);
        let fake = Environment::new(
            name("orders-sim"),
            Instances::ProxyInFrontOfBroker {
                proxy,
                broker: broker("orders-sim"),
            },
        );

        assert_eq!(fake.name(), "orders-sim");
        assert_eq!(fake.world(), Some(&"orders-sim".parse().unwrap()));
        assert_eq!(fake.recording(), Some(RecordingMode::Full));
        assert_eq!(fake.schema_registry(), None);
        assert_eq!(fake.listeners().bootstrap(), "0.0.0.0:29092".parse().unwrap());

        let staging = Environment::new(name("staging"), in_front_of_cluster());
        assert_eq!(staging.world(), None);
        assert_eq!(staging.recording(), None);
        assert_eq!(Environment::new(name("plain"), alone()).recording(), None);
    }

    #[test]
    fn another_advertised_host_changes_the_listeners_that_clients_connect_to() {
        let host: HostName = "gaslight.staging".parse().unwrap();

        for instances in [in_front_of_cluster(), in_front_of_broker(), alone()] {
            let before = Environment::new(name("shop"), instances);
            let after = before.clone().with_advertised_host(host.clone());

            assert_eq!(after.listeners().advertised_host(), &host);
            assert_eq!(after.listeners().bootstrap(), before.listeners().bootstrap());
            assert_eq!(after.listeners().brokers(), before.listeners().brokers());
        }
    }

    #[test]
    fn a_built_in_registry_stands_next_to_a_broker() {
        for instances in [in_front_of_broker(), alone()] {
            let environment = Environment::new(name("shop"), instances)
                .with_schema_registry(built_in("0.0.0.0:8081"))
                .unwrap();
            assert_eq!(environment.schema_registry(), Some(&built_in("0.0.0.0:8081")));
        }
        assert_eq!(
            Environment::new(name("staging"), in_front_of_cluster()).with_schema_registry(built_in("0.0.0.0:8081")),
            Err(EnvironmentError::BuiltInRegistryWithoutBroker { name: name("staging") })
        );
    }

    #[test]
    fn a_real_registry_stands_next_to_a_real_cluster() {
        let staging = Environment::new(name("staging"), in_front_of_cluster())
            .with_schema_registry(real())
            .unwrap();
        assert_eq!(staging.schema_registry(), Some(&real()));

        for instances in [in_front_of_broker(), alone()] {
            assert_eq!(
                Environment::new(name("shop"), instances).with_schema_registry(real()),
                Err(EnvironmentError::RealRegistryNextToBroker { name: name("shop") })
            );
        }
    }

    #[test]
    fn a_built_in_registry_takes_no_port_of_a_kafka_listener() {
        let environment = || Environment::new(name("shop"), alone());

        assert_eq!(
            environment().with_schema_registry(built_in("127.0.0.1:39092")),
            Err(EnvironmentError::RegistryAmongListeners {
                name: name("shop"),
                address: "127.0.0.1:39092".parse().unwrap()
            })
        );
        assert_eq!(
            environment().with_schema_registry(built_in("0.0.0.0:39100")),
            Err(EnvironmentError::RegistryAmongListeners {
                name: name("shop"),
                address: "0.0.0.0:39100".parse().unwrap()
            })
        );
        assert!(environment().with_schema_registry(built_in("0.0.0.0:0")).is_ok());
    }

    #[test]
    fn environments_share_a_listener_when_their_ports_meet() {
        let shop = Environment::new(name("shop"), alone())
            .with_schema_registry(built_in("0.0.0.0:8081"))
            .unwrap();
        let on = |bootstrap: &str, brokers: &str| {
            Environment::new(
                name("other"),
                Instances::BrokerAlone {
                    broker: broker("other"),
                    listen: listen(bootstrap, brokers),
                },
            )
        };
        let shared = |other: &Environment| {
            let shared = shop.shared_listener(other);
            assert_eq!(other.shared_listener(&shop).is_some(), shared.is_some());
            shared.map(|address| address.to_string())
        };

        assert_eq!(shared(&on("0.0.0.0:9092", "0.0.0.0:19092-19191")), None);
        assert_eq!(
            shared(&on("0.0.0.0:39092", "0.0.0.0:19092-19191")).as_deref(),
            Some("0.0.0.0:39092")
        );
        assert_eq!(
            shared(&on("0.0.0.0:9092", "0.0.0.0:39192-39200")).as_deref(),
            Some("0.0.0.0:39192")
        );
        // The schema registry is a listener too.
        assert_eq!(
            shared(&on("127.0.0.1:8081", "0.0.0.0:19092-19191")).as_deref(),
            Some("127.0.0.1:8081")
        );
        assert_eq!(
            shared(
                &on("0.0.0.0:9092", "0.0.0.0:19092")
                    .with_schema_registry(built_in("0.0.0.0:8081"))
                    .unwrap()
            )
            .as_deref(),
            Some("0.0.0.0:8081")
        );
        assert_eq!(shared(&shop.clone()).as_deref(), Some("0.0.0.0:39093"));
    }

    #[test]
    fn the_error_names_the_environment() {
        let shop = name("shop");
        let message = |error: EnvironmentError| error.to_string();

        assert_eq!(
            message(EnvironmentError::NoInstance { name: shop.clone() }),
            "environment \"shop\" has neither a proxy nor a broker"
        );
        assert_eq!(
            message(EnvironmentError::ProxyWithoutBackend { name: shop.clone() }),
            "environment \"shop\" has a proxy without an upstream, and no broker for it to stand in front of"
        );
        assert_eq!(
            message(EnvironmentError::TwoBackends { name: shop.clone() }),
            "environment \"shop\" has a proxy with an upstream, and a broker too: a proxy stands in front of one of them"
        );
        assert_eq!(
            message(EnvironmentError::BrokerWithoutListeners { name: shop.clone() }),
            "environment \"shop\" has a broker alone, and a broker alone needs listeners"
        );
        assert_eq!(
            message(EnvironmentError::BrokerListensBehindProxy { name: shop.clone() }),
            "environment \"shop\" has a broker behind a proxy, so only the proxy has listeners"
        );
        assert_eq!(
            message(EnvironmentError::RealRegistryNextToBroker { name: shop.clone() }),
            "environment \"shop\" has a broker, so its schema registry is the built-in one, with an address to listen on"
        );
        assert_eq!(
            message(EnvironmentError::RegistryAmongListeners {
                name: shop,
                address: "0.0.0.0:9092".parse().unwrap()
            }),
            "environment \"shop\" has its schema registry and a Kafka listener both on 0.0.0.0:9092"
        );
    }

    #[test]
    fn an_environment_has_a_title_when_someone_gives_it_one() {
        let title = |text: &str| text.parse::<EnvironmentTitle>().unwrap();
        let staging = Environment::new(name("staging-proxy"), in_front_of_cluster());
        let titled = staging.clone().with_title(title("The proxy in front of staging"));

        assert_eq!(staging.title(), None);
        assert_eq!(titled.title(), Some(&title("The proxy in front of staging")));
        assert_eq!(titled.name(), staging.name());
        assert_eq!(titled.instances(), staging.instances());
        assert_eq!(
            titled.clone().with_title(title("Staging")).title(),
            Some(&title("Staging"))
        );
        assert_eq!(titled.without_title(), staging);
    }

    #[test]
    fn an_environment_has_a_uuid_when_the_workspace_gives_it_one() {
        let uuid = |text: &str| text.parse::<EnvironmentUuid>().unwrap();
        let staging = Environment::new(name("staging-proxy"), in_front_of_cluster());
        let stored = staging.clone().with_uuid(uuid(STAGING_PROXY_UUID));

        assert_eq!(staging.uuid(), None);
        assert_eq!(stored.uuid(), Some(uuid(STAGING_PROXY_UUID)));
        assert_eq!(stored.name(), staging.name());
        assert_eq!(stored.instances(), staging.instances());
        assert_eq!(
            stored.clone().with_uuid(uuid(ANOTHER_UUID)).uuid(),
            Some(uuid(ANOTHER_UUID))
        );
        assert_ne!(stored, stored.clone().with_uuid(uuid(ANOTHER_UUID)));
        assert_eq!(stored.without_uuid(), staging);
    }

    #[test]
    fn an_environment_keeps_its_uuid_when_it_gets_a_schema_registry() {
        let uuid: EnvironmentUuid = STAGING_PROXY_UUID.parse().unwrap();
        let staging = Environment::new(name("staging-proxy"), in_front_of_cluster())
            .with_uuid(uuid)
            .with_schema_registry(real())
            .unwrap();

        assert_eq!(staging.uuid(), Some(uuid));
    }
}
