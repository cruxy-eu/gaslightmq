//! GaslightMQ in a container next to a test: the image of GaslightMQ runs a proxy that lies to the client under test,
//! and the test arms its rules through the control plane of the container.
//!
//! | Type | Is |
//! |---|---|
//! | [`GaslightMqContainer`] | GaslightMQ in a container: a proxy in front of a real cluster, and its control plane. It runs until the test stops it. |
//! | [`GaslightMqContainerConfiguration`] | The image, what the proxy runs in front of, where it listens, the workspace it holds, and how long it gets to start and to stop. |
//! | [`GaslightMqImage`] | The image of GaslightMQ, at the release that this crate belongs to. |
//! | [`GaslightMqContainerStartError`] | Why it did not start. |
//! | [`GaslightMqImageError`] | Why a text is no image. |
//!
//! A test starts GaslightMQ in front of a cluster, points its client at the
//! [bootstrap address](GaslightMqContainer::bootstrap), and tells the proxy what to lie about through the
//! [control plane](GaslightMqContainer::control): it arms rules, or it activates a scenario.
//! It works as a test of the crate `gaslightmq_test` does, which starts GaslightMQ in its own process instead.
//!
//! ```no_run
//! use gaslightmq_control_api::kernel::{ApiKey, ErrorCode};
//! use gaslightmq_control_api::lie::RefusalLie;
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_api::rule::Rule;
//! use gaslightmq_control_api::selector::{Condition, Conditions};
//! use gaslightmq_testcontainers::{GaslightMqContainer, GaslightMqContainerStartError};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), GaslightMqContainerStartError> {
//! // A proxy in a container, in front of the cluster that the test runs against.
//! let cluster = "127.0.0.1:9092".parse().expect("the bootstrap server of the cluster");
//! let gaslight = GaslightMqContainer::proxy_in_front_of(cluster)
//!     .with_workspace("gaslight")
//!     .start()
//!     .await?;
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
//! - **`gmq proxy` in front of a real cluster.**
//!   The container runs the image of GaslightMQ, and the binary in it runs a proxy in an environment of its own,
//!   which the control plane knows by its name.
//!   It tells the lies of the build of the image.
//! - **The network of the machine of the test.**
//!   The process reaches what the test reaches, under the same address: a test hands over the bootstrap server of its
//!   cluster as it reaches it, and the proxy reaches every broker under the address that the broker advertises.
//!   A cluster in another container whose brokers advertise addresses of the machine, such as the one of
//!   `gaslightmq_test_kafka`, works as a cluster on the machine itself does.
//! - **Free ports.**
//!   Every listener of the proxy and the control plane takes a port that the system picks, on the loopback of the
//!   machine unless the test names another address, so tests run side by side.
//!   The proxy gives each broker that it finds in the cluster a listener of its own, so a test names no broker.
//! - **The files of a workspace.**
//!   The files of the [workspace](GaslightMqContainerConfiguration::with_workspace) that a test hands over are copied
//!   into the container before the process starts. What the process writes, a definition or a recording, stays in
//!   the container, so the files of the test never change. Without a workspace, every definition lives in memory.
//! - **Ready means ready.**
//!   [`start`](GaslightMqContainerConfiguration::start) returns once the process said where it listens and that it is
//!   ready, and its control plane says that the environment of the proxy runs.
//! - **A run of the process.**
//!   The process makes its [run](GaslightMqContainer::run) when it starts, and the test reads it.
//!
//! # What the machine needs
//!
//! A container runtime that runs a container on the network of the machine: Docker on Linux, or Docker Desktop with
//! its host networking turned on. Without one, [`start`](GaslightMqContainerConfiguration::start) returns an error that
//! says so.
//!
//! # The image
//!
//! The container runs [`GaslightMqImage::default`]: the release of GaslightMQ that this crate belongs to.
//! The environment variable [`GASLIGHTMQ_IMAGE`](GaslightMqImage::VARIABLE) names another image for a whole test run,
//! such as the one that `just image` builds, and [`GaslightMqContainerConfiguration::with_image`] names one for a
//! single container.
//!
//! # What is not here yet
//!
//! - **Brokers.** GaslightMQ runs no broker yet, so a test starts a proxy in front of a real cluster alone.
//!   A broker, and a proxy in front of one, arrive with `gmq broker`.
//! - **The environments of a workspace.** The container runs the proxy that its configuration describes.
//!   Starting the environments of a workspace by their names arrives with `gmq start`.
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it runs on Tokio.
//! The configuration, the image and the errors are plain values that never wait.

mod announcement;
mod configuration;
mod error;
mod image;
mod instance;

pub use configuration::GaslightMqContainerConfiguration;
pub use error::GaslightMqContainerStartError;
pub use image::{GaslightMqImage, GaslightMqImageError};
pub use instance::GaslightMqContainer;
