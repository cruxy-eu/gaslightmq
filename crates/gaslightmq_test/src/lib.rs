//! GaslightMQ inside the process of a test: a proxy that lies to the client under test, and the control plane that
//! the test arms its rules through.
//!
//! | Type | Is |
//! |---|---|
//! | [`GaslightMq`] | GaslightMQ in the process of a test: a proxy in front of a real cluster, and its control plane. It runs for as long as the value lives. |
//! | [`GaslightMqConfiguration`] | What it runs, where it listens, and the workspace it holds. |
//! | [`GaslightMqStartError`] | Why it did not start. |
//!
//! A test starts GaslightMQ in front of a cluster, points its client at the [bootstrap address](GaslightMq::bootstrap),
//! and tells the proxy what to lie about through the [control plane](GaslightMq::control).
//!
//! ```no_run
//! use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
//! use gaslightmq_control_api::lie::RefusalLie;
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_api::rule::Rule;
//! use gaslightmq_control_api::selector::{Condition, Conditions};
//! use gaslightmq_test::{GaslightMq, GaslightMqStartError};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), GaslightMqStartError> {
//! // A proxy in front of the cluster that the test runs against.
//! let cluster = "127.0.0.1:9092".parse().expect("the bootstrap server of the cluster");
//! let gaslight = GaslightMq::proxy_in_front_of(cluster).start().await?;
//!
//! // The client under test connects here.
//! let bootstrap_servers = gaslight.bootstrap_servers();
//! # let _ = bootstrap_servers;
//!
//! // From now on, the proxy refuses every Produce: the client may not write to its topic.
//! let refuse = Rule::new(
//!     "refuse-order-producers".parse().expect("a rule name"),
//!     Conditions::from(Condition::is(ApiKey::PRODUCE)),
//!     RefusalLie::new(ErrorCode::TOPIC_AUTHORIZATION_FAILED),
//! );
//! let armed = gaslight
//!     .control()
//!     .send(CreateLiveRule {
//!         environment: gaslight.environment().clone(),
//!         rule: refuse,
//!         armed: true,
//!     })
//!     .await;
//! assert!(matches!(armed, Ok(Ok(_))), "{armed:?}");
//!
//! gaslight.stop().await;
//! # Ok(())
//! # }
//! ```
//!
//! # What runs
//!
//! - **A proxy in front of a real cluster.**
//!   It runs in an environment of its own, which the workspace holds, so the control plane knows it by its name.
//!   It tells every lie that GaslightMQ has.
//! - **A control plane in front of the proxy.**
//!   The test reaches the proxy through it, as it would reach a process of GaslightMQ in a container.
//!   Its client says that every request comes from a [test](gaslightmq_control_api::RequestSurface::Test),
//!   and names no presence.
//! - **Free ports.**
//!   Every listener of the proxy and the control plane takes a port that the system picks,
//!   so tests run side by side in one binary. The listeners accept connections once the test holds the value.
//!   The proxy gives each broker that it finds in the cluster a listener of its own, so a test names no broker,
//!   however many the cluster has.
//! - **A workspace in memory.**
//!   It holds nothing but the environment of the proxy, until the test creates definitions through the control
//!   plane. A test that wants other stores, or stores that hold definitions already, hands over their
//!   [context](GaslightMqConfiguration::with_context).
//! - **What the proxy runs with is a property.**
//!   The [configuration of its environment](GaslightMqConfiguration::proxy) holds its engine and its backend in front
//!   of the cluster, each with its defaults, and a test changes what it wants to: a timeout, a size or a seed.
//! - **A run of its own.**
//!   A test that names no [run](GaslightMqConfiguration::with_run) gets one from the randomness of the system.
//!   A test that has the process to itself leaves `expects` out of its changes, and the last change wins.
//!
//! # What is not here yet
//!
//! - **Brokers.** GaslightMQ runs no broker yet, so a test starts a proxy in front of a real cluster alone.
//!   A broker, and a proxy in front of one, arrive with the crate `broker`.
//! - **Scenarios and recordings.** A test arms its rules one by one. Activating a scenario and asserting on the
//!   recording arrive with the crates `scenario` and `recording`.
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it runs on Tokio.
//! The proxy and the control plane run their tasks on the runtime of the test.
//! The configuration and the errors are plain values that never wait.

mod configuration;
mod error;
mod instance;

pub use configuration::GaslightMqConfiguration;
pub use error::GaslightMqStartError;
pub use instance::GaslightMq;
