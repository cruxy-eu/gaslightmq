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
//! | [`rule!`], [`scenario!`], [`selector!`] | A definition as a file of a workspace writes it, inside the code of the test. |
//! | [`RecordingMode`] | How much of each call a recording keeps. |
//!
//! A test starts GaslightMQ in front of a cluster, points its client at the
//! [bootstrap address](GaslightMqContainer::bootstrap), and tells the proxy what to lie about in the words of the files
//! of a workspace: it arms a [`rule!`], activates a [`scenario!`], records, and follows the calls of a [`selector!`].
//! It works as a test of the crate `gaslightmq_test` does, which starts GaslightMQ in its own process instead, and
//! both speak the words of [`ControlledEnvironment`](gaslightmq_control_client::ControlledEnvironment).
//!
//! ```no_run
//! use gaslightmq_testcontainers::{GaslightMqContainer, rule};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> anyhow::Result<()> {
//! // A proxy in a container, in front of the cluster that the test runs against, as the network `orders-test` has it.
//! let cluster = "kafka:19092".parse()?;
//! let gaslight = GaslightMqContainer::proxy_in_front_of(cluster)
//!     .with_network("orders-test")
//!     .with_workspace("gaslight")
//!     // A rule of the workspace, armed by its name before the test begins.
//!     .arming("order-acknowledgements-get-lost")
//!     .start()
//!     .await?;
//!
//! // The client under test connects here, where the container publishes the proxy.
//! let bootstrap_servers = gaslight.bootstrap_servers();
//! # let _ = bootstrap_servers;
//!
//! // From now on, the proxy refuses every Produce: the client may not write to its topic.
//! let refused = gaslight
//!     .rules()
//!     .arm(rule! { selector: { api: Produce }, action: { lie: refusal, error: TOPIC_AUTHORIZATION_FAILED } })
//!     .await?;
//! assert_eq!(refused.name(), "refuse-with-topic-authorization-failed-on-produce");
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
//! - **The network that the test names.**
//!   The container joins the [network of containers](GaslightMqContainerConfiguration::with_network) that the test
//!   names, and reaches the cluster under the bootstrap server that the test hands over: its alias and port on that
//!   network, or any address that the network leads to. The test sets its networks up, and the crate picks none.
//! - **Published listeners.**
//!   The container publishes the bootstrap listener, a listener for each of as many
//!   [brokers](GaslightMqContainerConfiguration::brokers) as the test asks for, and the control plane, each on a free
//!   port of the machine, so tests run side by side. Once the container runs, the crate tells the process where its
//!   brokers are published, and the proxy names them to its clients there, as Kafka's `advertised.listeners` does.
//!   The proxy gives each broker that it finds in the cluster a listener of its own, so a test names no broker.
//! - **The files of a workspace.**
//!   The files of the [workspace](GaslightMqContainerConfiguration::with_workspace) that a test hands over are copied
//!   into the container before the process starts. What the process writes, a definition or a recording, stays in
//!   the container, so the files of the test never change. Without a workspace, every definition lives in memory.
//! - **Ready means ready.**
//!   [`start`](GaslightMqContainerConfiguration::start) returns once the process said that it is ready, and its control
//!   plane says that the environment of the proxy runs.
//! - **A run of the process.**
//!   The process makes its [run](GaslightMqContainer::run) when it starts, and the test reads it.
//!
//! # What the machine needs
//!
//! A container runtime, such as Docker. Without one, [`start`](GaslightMqContainerConfiguration::start) returns an
//! error that says so.
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
//! - **Clients in other containers.** The proxy has one set of listeners, which serves the test through the published
//!   ports. A client in another container of the network needs a listener that the proxy advertises under the alias of
//!   the container, and that arrives with a proxy of several listeners.
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
pub use gaslightmq_control_api::environment::RecordingMode;
pub use gaslightmq_control_client::{rule, scenario, selector};
pub use image::{GaslightMqImage, GaslightMqImageError};
pub use instance::GaslightMqContainer;
