//! Environments: the instances that run together, where they listen, and what stands behind them.
//!
//! An environment is a definition in a workspace.
//! It names a proxy, a broker, or a proxy in front of a broker, and a process starts one environment or several.
//!
//! | Type | Is |
//! |---|---|
//! | [`Environment`] | A named definition of instances that run together, with a [`SchemaRegistry`] when it has one. |
//! | [`Instances`] | The shape of an environment: a proxy in front of a real cluster, a proxy in front of a broker, or a broker alone. |
//! | [`ProxyDefinition`], [`BrokerDefinition`] | What an environment says about its proxy and about its broker. |
//! | [`Upstream`] | The real cluster behind a proxy: its bootstrap [`ServerAddress`], [`Tls`] and [`Sasl`]. |
//! | [`Listeners`] | Where an instance listens for clients: a bootstrap [`ListenAddress`], [`BrokerListeners`], and the [`HostName`] clients see. |
//! | [`SecretRef`] | Where a secret lives: a [`FilePath`] or a [`VariableName`]. Never the secret. |
//! | [`RecordingMode`] | How much of a call a recording keeps. |
//! | [`EnvironmentSet`] | The environments of a workspace by [`EnvironmentName`], and the ones a process can start together. |
//! | [`Setting`] | One setting with what each [`SettingSource`] gave for it, and the [`EffectiveSetting`] that wins. |
//!
//! ```
//! use gaslightmq_environment::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//!
//! // A proxy in front of a real cluster, as `environments/staging-proxy.yaml` describes it.
//! let upstream = Upstream::new("kafka-1.staging:9092".parse().expect("a server address"))
//!     .with_tls(Tls::new().with_ca("/run/secrets/kafka-ca.pem".parse().expect("a file path")))
//!     .with_sasl(Sasl::new(
//!         SaslMechanism::ScramSha512,
//!         "gaslight".parse().expect("a SASL username"),
//!         SecretRef::File("/run/secrets/kafka-password".parse().expect("a file path")),
//!     ));
//! let listen = Listeners::new(
//!     "0.0.0.0:9092".parse()?,
//!     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
//!     "gaslight.staging".parse().expect("a host name"),
//! )
//! .expect("a bootstrap port outside the ports of the brokers");
//! let staging = Environment::new(
//!     "staging-proxy".parse().expect("an environment name"),
//!     Instances::ProxyInFrontOfCluster {
//!         proxy: ProxyDefinition::new(listen),
//!         upstream,
//!     },
//! );
//!
//! // The proxy discovers the brokers 1, 2 and 3, and each one gets a listener of its own.
//! let listeners = staging.listeners().brokers().assign([1, 2, 3].map(BrokerId::from)).expect("a port for every broker");
//! assert_eq!(listeners[&BrokerId::from(2)], "0.0.0.0:19093".parse()?);
//!
//! // Nothing names a recording mode, so the default for a real cluster holds.
//! let recording = Setting::with_default(staging.instances().default_recording())
//!     .set(SettingSource::Environment, staging.recording())
//!     .into_effective()
//!     .expect("the default sets it");
//! assert_eq!(recording.into_value(), RecordingMode::MetadataOnly);
//! # Ok::<(), ListenAddressError>(())
//! ```
//!
//! # Three shapes
//!
//! An environment has one of three shapes, and [`Instances`] holds nothing else.
//! Each shape has one set of [`Listeners`]: the ones clients connect to.
//!
//! A proxy has no backend of its own to choose.
//! In front of a real cluster it has an [`Upstream`], and in front of a broker it has the broker of its environment.
//!
//! # Listeners that do not meet
//!
//! Two listeners cannot take the same port of the same address.
//! `::` stands for every address, and `0.0.0.0` for every IPv4 address, so they meet those on the same port.
//! Port zero asks the system for a free port, and meets nothing.
//!
//! Each whole checks its own parts:
//!
//! - [`Listeners`]: the bootstrap listener takes no port of a broker listener.
//! - [`BrokerListeners`]: no two pinned brokers have one address.
//! - [`Environment`]: the built-in schema registry takes no port of a Kafka listener.
//! - [`EnvironmentSet::to_start`]: no two environments that start together share a listener.
//!
//! # Secrets
//!
//! A definition holds a [`SecretRef`], and never a secret.
//! Reading a value where a reference belongs fails with [`SecretRefError::Literal`].
//! No error of a secret reference repeats what it read, whatever stands there: a value, a key, a path or a name.
//! [`SecretRefError`], [`FilePathError`] and [`VariableNameError`] hold at most the one character that is wrong.
//! The [`SchemaRegistryUrlError`] of a URL with credentials does not repeat the URL either.
//!
//! This crate never reads a secret.
//! It has no type that holds one, and no code that opens a file or looks at an environment variable.
//!
//! # The order of flags, variables and files
//!
//! A setting can come from several places, and the first one that sets it wins:
//! a flag, an environment variable, the environment, the workspace settings, and the built-in default.
//! [`SettingSource`] is that order, and a [`Setting`] applies it to the values that its caller passes in.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every definition type has a `serde` form.
//! The forms are those of an environment file.
//!
//! | Type | Form |
//! |---|---|
//! | A name, a [`HostName`], an address, a [`PortRange`], a [`SchemaRegistryUrl`], a [`FilePath`], a [`VariableName`] | The text. |
//! | [`RecordingMode`], [`SaslMechanism`], [`SettingSource`] | The name. |
//! | [`SecretRef`] | A map with one entry: `file` and the path, or `env` and the name of the variable. |
//! | [`BrokerListeners`] | The text of a range, or a map from broker IDs to addresses. |
//! | [`Listeners`] | A map with `bootstrap`, `brokers` and `advertise`. |
//! | [`Upstream`] | A map with `bootstrap`, and with `tls` and `sasl` when it has them. |
//! | [`Tls`] | A map with `ca` when it has one. |
//! | [`Sasl`] | A map with `mechanism`, `username` and `password`. |
//! | [`SchemaRegistry`] | A map with `listen`, or a map with `url`. |
//! | [`Environment`] | A map with `name`, and with the `broker`, `proxy` and `schema_registry` blocks it has. |
//! | [`EffectiveSetting`] | A map with `value` and `source`. |
//!
//! The `proxy` block has `listen`, and `upstream` and `recording` when the proxy has them.
//! The `broker` block has `world`, and `listen` when the broker runs alone.
//! Reading checks that the blocks make one of the three shapes, and refuses a key it does not know.
//!
//! ```yaml
//! name: orders-sim
//! broker:
//!   world: orders-sim
//! proxy:
//!   listen:
//!     bootstrap: 0.0.0.0:29092
//!     brokers: 0.0.0.0:29093-29192
//!     advertise: localhost
//! schema_registry:
//!   listen: 0.0.0.0:8081
//! ```
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`EnvironmentName`], [`WorldName`] | [`EnvironmentNameError`], [`WorldNameError`] | The text breaks a part of the rule for names. |
//! | [`HostName`] | [`HostNameError`] | The text is no IP address and no name of a host. |
//! | [`ServerAddress`] | [`ServerAddressError`] | The host or the port is missing or wrong. |
//! | [`ListenAddress`] | [`ListenAddressError`] | The text is not an IP address and a port. |
//! | [`PortRange`] | [`PortRangeError`] | A port is no port, or the range ends before it starts. |
//! | [`BrokerListeners`] | [`BrokerListenersError`] | The text is no range, pins do not hold together, or a broker gets no address. |
//! | [`Listeners`] | [`ListenersError`] | The bootstrap listener takes a port of a broker listener. |
//! | [`SchemaRegistryUrl`] | [`SchemaRegistryUrlError`] | The text breaks a part of the rule for URLs. |
//! | [`FilePath`], [`VariableName`] | [`FilePathError`], [`VariableNameError`] | The text is empty, or holds a character it cannot hold. |
//! | [`SecretRef`] | [`SecretRefError`] | A value stands where a reference belongs, or the reference names no place or two. |
//! | [`SaslMechanism`], [`SaslUsername`] | [`SaslMechanismError`], [`SaslUsernameError`] | No mechanism has this name, or the user name is empty. |
//! | [`RecordingMode`], [`SettingSource`] | [`RecordingModeError`], [`SettingSourceError`] | Nothing has this name. |
//! | [`SchemaRegistry`] | [`SchemaRegistryError`] | The registry names no place, or two. |
//! | [`Environment`] | [`EnvironmentError`] | The blocks make none of the three shapes, or the schema registry does not fit. |
//! | [`EnvironmentSet`] | [`EnvironmentSetError`] | Two environments share a name or a listener, or a name is unknown. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! An error that holds the error of a part writes it once, after the text it read.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_kernel` for the broker ID that a broker listener belongs to.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.
//! An address is a value here: nothing listens, connects or resolves a name.

mod address;
mod environment;
mod host;
mod listeners;
mod name;
mod ports;
mod recording;
mod schema_registry;
mod secret;
#[cfg(feature = "serde")]
mod serde;
mod set;
mod setting;
mod text;
mod upstream;

pub use address::{ListenAddress, ListenAddressError, ServerAddress, ServerAddressError};
pub use environment::{BrokerDefinition, Environment, EnvironmentError, Instances, ProxyDefinition};
pub use host::{HostName, HostNameError};
pub use listeners::{BrokerListeners, BrokerListenersError, Listeners, ListenersError};
pub use name::{EnvironmentName, EnvironmentNameError, WorldName, WorldNameError};
pub use ports::{PortRange, PortRangeError};
pub use recording::{RecordingMode, RecordingModeError};
pub use schema_registry::{SchemaRegistry, SchemaRegistryError, SchemaRegistryUrl, SchemaRegistryUrlError};
pub use secret::{FilePath, FilePathError, SecretRef, SecretRefError, VariableName, VariableNameError};
pub use set::{EnvironmentSet, EnvironmentSetError};
pub use setting::{EffectiveSetting, Setting, SettingSource, SettingSourceError};
pub use upstream::{Sasl, SaslMechanism, SaslMechanismError, SaslUsername, SaslUsernameError, Tls, Upstream};

/// Every item of this crate.
///
/// Import it whole wherever code speaks in environments, next to the prelude of the kernel.
/// No name in it is a name of the standard prelude or of the preludes of the other crates, so the import shadows
/// nothing.
///
/// ```
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let listeners: Result<BrokerListeners, BrokerListenersError> = "0.0.0.0:19092-19191".parse();
/// let addresses = listeners?.assign([BrokerId::from(1)])?;
/// assert_eq!(addresses[&BrokerId::from(1)].port(), 19092);
/// # Ok::<(), gaslightmq_environment::BrokerListenersError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        BrokerDefinition, BrokerListeners, BrokerListenersError, EffectiveSetting, Environment, EnvironmentError,
        EnvironmentName, EnvironmentNameError, EnvironmentSet, EnvironmentSetError, FilePath, FilePathError, HostName,
        HostNameError, Instances, ListenAddress, ListenAddressError, Listeners, ListenersError, PortRange,
        PortRangeError, ProxyDefinition, RecordingMode, RecordingModeError, Sasl, SaslMechanism, SaslMechanismError,
        SaslUsername, SaslUsernameError, SchemaRegistry, SchemaRegistryError, SchemaRegistryUrl,
        SchemaRegistryUrlError, SecretRef, SecretRefError, ServerAddress, ServerAddressError, Setting, SettingSource,
        SettingSourceError, Tls, Upstream, VariableName, VariableNameError, WorldName, WorldNameError,
    };
}
