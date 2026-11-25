//! One process with several environments: a proxy for each, and one workspace for all of them.
//!
//! An [environment](gaslightmq_environment::Environment) describes the instances that run together.
//! A process starts one environment or several, and the host is what runs them side by side.
//!
//! | Type | Is |
//! |---|---|
//! | [`Host`] | The environments of one process, and the workspace they share. |
//! | [`HostConfiguration`] | What holds for every environment of a host: how it reads a ⌘K sentence. |
//! | [`EnvironmentConfiguration`] | What a host starts one environment with: the configuration of its proxy and of its backend. |
//! | [`EnvironmentStartError`], [`NotRunningError`] | Why an environment did not start, and that an environment does not run. |
//! | [`ArmRuleError`], [`ReadSentenceError`] | Why a rule was not armed, and why a sentence was not read. |
//! | [`EnvironmentConfigurationError`] | Why an environment has no configuration that a host can start. |
//!
//! The [`prelude`] holds all of them.
//!
//! ```no_run
//! use gaslightmq_environment::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_host::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_workspace::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), EnvironmentStartError> {
//! let host = Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build());
//!
//! // A proxy in front of the staging cluster, as `environments/staging-proxy.yaml` describes it.
//! let listen = Listeners::new(
//!     "0.0.0.0:9092".parse().expect("a listen address"),
//!     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
//!     "gaslight.staging".parse().expect("a host name"),
//! );
//! let staging = Environment::new(
//!     "staging-proxy".parse().expect("an environment name"),
//!     Instances::ProxyInFrontOfCluster {
//!         proxy: ProxyDefinition::new(listen.expect("a bootstrap port outside the ports of the brokers")),
//!         upstream: Upstream::new("kafka-1.staging:9092".parse().expect("a server address")),
//!     },
//! );
//! let configuration = EnvironmentConfiguration::new(&staging).expect("a proxy in front of a real cluster");
//!
//! // The proxy accepts connections once the start returns.
//! host.start(configuration).await?;
//! let bootstrap = host.address_of(staging.name(), Endpoint::Bootstrap).expect("the environment runs");
//! assert_eq!(bootstrap, Some("0.0.0.0:9092".parse().expect("a socket address")));
//!
//! host.stop(staging.name()).await.expect("the environment runs");
//! # Ok(())
//! # }
//! ```
//!
//! # Several environments in one process
//!
//! - **Each environment is separate.**
//!   It has its own proxy: its own listeners, sessions, armed rules and events.
//!   Arming a rule in one environment does not arm it in another.
//! - **Environments share the workspace.**
//!   The host holds one [`Workspace`](gaslightmq_workspace::Workspace) over the context it is handed, and names no
//!   store: whoever puts the process together picks the stores in that context.
//!   A rule resolves against the selectors and the presets of the workspace in whichever environment it is armed.
//! - **Listeners must not overlap.**
//!   The host refuses to start an environment whose listeners name a port that an environment that runs names on
//!   the same address. A listener on port 0 takes any free port, and meets nothing.
//! - **One start or stop at a time.**
//!   The host checks the listeners and opens them as one step, so two starts never claim one port.
//!   What the environments that run hand over, and their rules, do not wait for a start.
//!
//! The host does not care where an environment comes from.
//! [`EnvironmentConfiguration::new`] takes an environment of the workspace as well as one that the flags of a
//! command describe, and whoever starts it sets what a flag, a variable or a file says first.
//!
//! # A proxy in front of a real cluster
//!
//! Each environment runs as an engine of `gaslightmq_engine`, in front of the backend of `gaslightmq_backend_upstream`,
//! on the wall clock of `gaslightmq_clock_waitable`.
//! The lies that every proxy tells are the lies of the build, which whoever makes the host hands over once.
//!
//! # What is not here yet
//!
//! - **Brokers.** An environment with a broker, behind a proxy or alone, has no configuration yet:
//!   see [`EnvironmentConfigurationError::UnsupportedBroker`]. The broker and its task arrive with the crate `broker`.
//! - **What a process configures.** No workspace setting reaches an environment by itself:
//!   whoever starts one sets its seed and its other properties in its [`EnvironmentConfiguration`].
//! - **The live set, scenarios, the journal, the traffic and recordings** arrive with their crates.
//!   Until then the armed rules of an environment are its live set, and a ⌘K sentence knows the names of the
//!   workspace alone.
//! - **The control plane** stands in front of the host, in its own crate.
//!
//! # Errors
//!
//! Each thing that fails in its own way has an error of its own, so nobody handles a failure that cannot come.
//!
//! | What fails | Error | What is wrong |
//! |---|---|---|
//! | [`EnvironmentConfiguration::new`] | [`EnvironmentConfigurationError`] | The environment runs a broker. |
//! | [`Host::start`] | [`EnvironmentStartError`] | The environment runs already, its listeners meet those of another, or its backend or its proxy did not start. |
//! | [`Host::stop`], and every question about an environment that runs | [`NotRunningError`] | No environment of that name runs. |
//! | [`Host::arm`] | [`ArmRuleError`] | The environment does not run, the workspace could not be read or is not valid, the rule does not resolve, or it is armed already. |
//! | [`Host::read_sentence`] | [`ReadSentenceError`] | The workspace could not be read. |
//!
//! A variant is something its caller decides on, and the rest is text.
//! The error of the backend, of the proxy and of a rule is kept as it is, in a field `error`,
//! and what a store said is text, in a field `reason`.
//! A variant writes either one once, after what it says itself.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it runs on Tokio.
//! It starts no task of its own: each engine and each backend runs its tasks on the runtime of whoever starts it.
//! The configuration and the errors are plain values that never wait.

mod configuration;
mod error;
mod host;

pub use configuration::{EnvironmentConfiguration, EnvironmentConfigurationError, HostConfiguration};
pub use error::{ArmRuleError, EnvironmentStartError, NotRunningError, ReadSentenceError};
pub use host::Host;

/// Every item of this crate.
///
/// Import it whole wherever code runs a host, next to the preludes of the crates a host is made of.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = NotRunningError {
///     name: "staging-proxy".parse().expect("an environment name"),
/// };
/// assert_eq!(error.to_string(), "environment \"staging-proxy\" does not run");
/// assert_eq!(HostConfiguration::default().quick_inject.completion_limit, 5);
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ArmRuleError, EnvironmentConfiguration, EnvironmentConfigurationError, EnvironmentStartError, Host,
        HostConfiguration, NotRunningError, ReadSentenceError,
    };
}
