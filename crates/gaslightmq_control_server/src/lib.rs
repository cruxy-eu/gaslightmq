//! The control plane on the network: an HTTP and WebSocket server in front of a host.
//!
//! The UI, the CLI and tests reach a process through the control plane, and nothing else.
//! `gaslightmq_control_api` holds what travels: each operation with its answer and its error, and the messages of
//! the live stream. This crate serves them, over the host that runs the environments of the process.
//!
//! | Type | Is |
//! |---|---|
//! | [`ControlServer`] | The server, from the moment it listens until someone stops it. |
//! | [`ControlServerConfiguration`] | Where it listens, and its limits and waits. |
//! | [`RequestSizeLimit`], [`StreamBacklog`] | The largest request it reads, and how many changes it keeps for a client of the stream. |
//! | [`ControlServerError`] | Why it did not start. |
//!
//! The [`prelude`] holds all of them, with the errors of the values.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use gaslightmq_control_server::prelude::*;
//! use gaslightmq_host::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_workspace::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ControlServerError> {
//! // The binary makes the host, starts what its flags say, and puts the control plane in front of it.
//! let host = Arc::new(Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build()));
//! let server = ControlServer::start(ControlServerConfiguration::default(), Arc::clone(&host)).await?;
//! assert_eq!(server.address(), "127.0.0.1:8080".parse().expect("a socket address"));
//!
//! // On the way out, the control plane stops first, so nobody starts an environment that is about to stop.
//! server.stop().await;
//! host.stop_all().await;
//! # Ok(())
//! # }
//! ```
//!
//! # The wire form
//!
//! The server and `gaslightmq_control_client` share one form.
//! `docs/specs/architecture/control-plane.md` describes it in full, under "Over the network".
//!
//! | Request | Answer |
//! |---|---|
//! | `POST /api/operations` with an `AnyOperation` | `200` with its answer, `422` with its error, `501` or `400` with a `RequestError`. |
//! | `GET /api/stream`, a WebSocket | A `StreamMessage` per text frame, the snapshot first. The client sends a `Subscription` per text frame. |
//!
//! Every body and every frame is the JSON form of a type of `gaslightmq_control_api`.
//!
//! # What the server answers
//!
//! The server answers each operation with the host, and each one that changes something sends a message
//! to every client of the live stream: a definition changed or deleted, a rule armed or disarmed,
//! an environment started or stopped.
//!
//! - **Definitions** change in the workspace of the host, which checks them first.
//! - **An environment starts by its name.** The server reads it from the workspace, makes its
//!   configuration as the environment says, and hands that to the host.
//! - **A rule is a rule of the workspace.** It resolves against the workspace when it is armed,
//!   in the environment that the command names alone.
//!
//! # What the server does not serve yet
//!
//! The host has no live rules, no sessions and no pads yet. The server answers their operations with
//! `RequestError::NotServed`: live rules and keeping one, intents, listing and closing sessions.
//! The sessions of an environment in a snapshot are empty.
//!
//! The live stream carries the changes that go through the server. A rule that its lifetime disarms,
//! an environment that someone starts on the host itself and a store that changes behind the workspace send no
//! message yet: the next snapshot holds them. No statistics travel yet.
//!
//! # Errors
//!
//! | What fails | Error | What is wrong |
//! |---|---|---|
//! | [`ControlServer::start`] | [`ControlServerError`] | The server cannot listen on its address. |
//! | [`RequestSizeLimit::new`] | [`RequestSizeLimitError`] | The limit is zero. |
//! | [`StreamBacklog::new`] | [`StreamBacklogError`] | The backlog is zero, or larger than the largest. |
//!
//! What a request asks fails with the errors of `gaslightmq_control_api`, and travels to the client.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, it runs on Tokio, and it opens a socket.
//! Its configuration and its errors are plain values that never wait.

mod configuration;
mod definitions;
mod error;
mod plane;
mod server;
mod stream;

pub use configuration::{ControlServerConfiguration, RequestSizeLimit, StreamBacklog};
pub use error::{ControlServerError, RequestSizeLimitError, StreamBacklogError};
pub use server::ControlServer;

/// Every item of this crate.
///
/// Import it whole wherever code puts the control plane in front of a host, next to the preludes of the host and of
/// the control plane. No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_control_server::prelude::*;
///
/// let configuration = ControlServerConfiguration::default();
/// assert_eq!(configuration.stream_backlog, StreamBacklog::DEFAULT);
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ControlServer, ControlServerConfiguration, ControlServerError, RequestSizeLimit, RequestSizeLimitError,
        StreamBacklog, StreamBacklogError,
    };
}
