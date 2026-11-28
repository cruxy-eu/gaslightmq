//! GaslightMQ inside the process of a test: a proxy that lies to the client under test, and the control plane that
//! the test arms its rules through.
//!
//! | Type | Is |
//! |---|---|
//! | [`GaslightMq`] | GaslightMQ in the process of a test: a proxy in front of a real cluster, and its control plane. It runs for as long as the value lives. |
//! | [`GaslightMqConfiguration`] | What it runs, where it listens, and the workspace it holds. |
//! | [`GaslightMqStartError`] | Why it did not start. |
//! | [`rule!`], [`scenario!`], [`selector!`] | A definition as a file of a workspace writes it, inside the code of the test. |
//! | [`RecordingMode`] | How much of each call a recording keeps. |
//!
//! A test starts GaslightMQ in front of a cluster, points its client at the [bootstrap address](GaslightMq::bootstrap),
//! and tells the proxy what to lie about in the words of the files of a workspace: it arms a [`rule!`], activates a
//! [`scenario!`], which plays a failure out over its steps, records, and follows the calls of a [`selector!`].
//! Those are the words of [`ControlledEnvironment`](gaslightmq_control_client::ControlledEnvironment), which the crate
//! `gaslightmq_testcontainers` speaks too.
//!
//! ```no_run
//! use gaslightmq_test::{GaslightMq, rule, selector};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> anyhow::Result<()> {
//! // A proxy in front of the cluster that the test runs against, which records from the start.
//! let cluster = "127.0.0.1:9092".parse()?;
//! let gaslight = GaslightMq::proxy_in_front_of(cluster).recording_from_the_start().start().await?;
//!
//! // The client under test connects here.
//! let bootstrap_servers = gaslight.bootstrap_servers();
//! # let _ = bootstrap_servers;
//!
//! // The test waits for the Produce calls that it follows, and never on the clock.
//! let mut produces = gaslight.follow(selector! { api: Produce }).await?;
//! // From now on, the proxy refuses the first Produce: the client is told that the leader moved.
//! let leader_moves = gaslight
//!     .rules()
//!     .arm(rule! {
//!         selector: { api: Produce },
//!         action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER },
//!         trigger: { first: 1 },
//!     })
//!     .await?;
//! // … the client produces, and tries again …
//! let calls = produces.wait_for(2).await?;
//! assert!(calls[0].explanation().applied(&leader_moves));
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
//! - **Recordings in memory.**
//!   The host of the test keeps every recording in the segment layout in memory, for as long as the test runs.
//! - **Assertions on the recording.**
//!   A test starts and stops a recording, and [queries](gaslightmq_control_client::Recording::query) it with a selector:
//!   the answer holds the calls that it matches, each with its verdict and its explanation, and their count over the
//!   whole recording. A test asserts that a rule applied to exactly the calls it should have, and not merely that
//!   its client saw an error.
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
pub use gaslightmq_control_api::environment::RecordingMode;
pub use gaslightmq_control_client::{rule, scenario, selector};
pub use instance::GaslightMq;
