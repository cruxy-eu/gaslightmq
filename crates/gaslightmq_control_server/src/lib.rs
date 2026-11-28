//! The control plane on the network: an HTTP and WebSocket server in front of a host.
//!
//! The UI, the CLI and tests reach a process through the control plane, and nothing else.
//! `gaslightmq_control_api` holds what travels: each operation with its answer and its error, and the messages of
//! the live stream. This crate serves them, over the host that runs the environments of the process.
//!
//! | Type | Is |
//! |---|---|
//! | [`ControlServer`] | The server, from the moment it listens until someone stops it. |
//! | [`ControlServerConfiguration`] | Where it listens, its limits and waits, and how long it keeps the presence of a client that went away. |
//! | [`RequestSizeLimit`], [`StreamBacklog`], [`StatsInterval`], [`PingInterval`] | The largest request it reads, how many changes it keeps for a client of the stream, how often the statistics come, and how often it asks a client whether it is still there. |
//! | [`ControlServerError`] | Why it did not start. |
//!
//! The [`prelude`] holds all of them, with the errors of the values.
//!
//! ```no_run
//! use std::net::SocketAddr;
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
//! // It reads the randomness of the system once, for the ID of the run. This one is written out.
//! let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
//! let host = Arc::new(Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build(), run));
//! let server = ControlServer::start(ControlServerConfiguration::default(), Arc::clone(&host)).await?;
//! assert_eq!(server.address(), "127.0.0.1:7092".parse::<SocketAddr>().expect("a socket address"));
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
//! | `POST /api/operations` with an `OperationRequest` | `200` with its answer, `422` with its error, `400` with a `RequestError`. |
//! | `GET /api/stream`, a WebSocket | A `StreamMessage` per text frame: the presence of the client first, and the snapshot next. The client sends a `Subscription` per text frame. A client that comes back names the presence it was in the query: `?presence=…`. |
//! | `GET /health` | `200` with the environments that the process was started with when every one of them runs, and `503` with the same list when one does not. A probe asks it without a request body. |
//! | `GET` of any other path | A file of the web UI, or the page of the UI, which opens the view of the path. |
//!
//! Every body and every frame is the JSON form of a type of `gaslightmq_control_api`.
//! The UI and the control plane share one origin: `just ui` builds the files of the UI into the folder `ui/` of this
//! crate, a release build holds them, and a debug build reads them from the folder while it runs.
//!
//! # What the server answers
//!
//! The server answers every operation of `gaslightmq_control_api` with the host.
//! A request says where it comes from, next to its operation: the surface that sends it, and the cause of an undo
//! or a redo. A request that does not say so, or that claims a surface which only the process names, is unreadable.
//! The server hands the surface and the cause to the host with every change, and the host records them in the
//! journal of the environment.
//!
//! - **Whatever a request changes has a revision**, and the server hands it out beside the thing:
//!   with every definition, with every rule of an environment, and in the snapshot, which names the run of the host.
//!   A change answers with the revision it left. A change that says what it expects reaches the host with it,
//!   and a change that expects a revision of another run learns that before anything else.
//! - **Definitions** change in the workspace, through the host, which checks them first.
//! - **An environment starts by its name.** The server reads it from the workspace, makes its
//!   configuration as the environment says, and hands that to the host.
//! - **A rule of an environment** is a rule of the workspace or a live rule of that environment.
//!   A live rule that is created armed is there and armed, or neither.
//!   Pads, live rules, keeping one and the sessions of an environment are the host's.
//! - **The journal of an environment** is the host's too: a query answers with the entries that the environment
//!   keeps in memory, or with those after the entry that the client read last.
//!
//! # The live stream follows the host
//!
//! The server follows every change of the host, whoever makes it, and sends each one to every client with the state
//! and the revision that its subject has then: a definition changed or deleted, a rule armed, disarmed, changed or removed, a session
//! that opened, said who its client is or closed, and an environment that started or stopped.
//! Every entry that the journal of an environment gets comes too, as it is, in the order of the entries.
//! The statistics of every environment that runs come at the
//! [interval](ControlServerConfiguration::stats_interval) of the configuration.
//! A server that falls behind the host sends every client a new snapshot.
//! A store that changes behind the workspace, as a file that someone edits, sends nothing yet.
//!
//! # The presence of a client
//!
//! A client that follows the live stream is there, and the server holds a presence for it.
//! The server makes the ID of a presence from the randomness of the system, names it in the first message of the
//! stream, and opens it in the host. A request that names the presence adds holds that belong to it.
//!
//! - **A presence outlives its stream.** When the last stream of a presence ends, the server keeps the presence
//!   for the [grace period](ControlServerConfiguration::presence_grace) of its configuration.
//!   A client that opens a stream as that presence within it goes on as it, and the pads it holds stay held.
//! - **A client that stays away is gone.** The server ends its presence in the host, which ends every hold of it:
//!   a tab that closed never sends its release.
//! - **A client that goes silent is gone too.** A connection can go silent without closing: the network of its
//!   client dropped, or its laptop sleeps. The server pings every client of the stream at the
//!   [interval](ControlServerConfiguration::ping_interval) of its configuration.
//!   A client that sent nothing between one ping and the next has gone silent, and so has one that takes no message
//!   for as long. The server ends its stream as if the connection had closed, and the grace period begins.
//! - **A server that stops ends every presence at once.** No client comes back to it.
//!
//! The grace period and the pings run on the clock of the server: real time for [`ControlServer::start`],
//! and the clock that [`ControlServer::start_on`] is handed, which a test steps.
//!
//! # Errors
//!
//! | What fails | Error | What is wrong |
//! |---|---|---|
//! | [`ControlServer::start`] | [`ControlServerError`] | The server cannot listen on its address. |
//! | [`RequestSizeLimit::new`] | [`RequestSizeLimitError`] | The limit is zero. |
//! | [`StreamBacklog::new`] | [`StreamBacklogError`] | The backlog is zero, or larger than the largest. |
//! | [`StatsInterval::new`] | [`StatsIntervalError`] | The interval is no time at all. |
//! | [`PingInterval::new`] | [`PingIntervalError`] | The interval is no time at all. |
//!
//! What a request asks fails with the errors of `gaslightmq_control_api`, and travels to the client.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, it runs on Tokio, it opens a socket,
//! and it reads the randomness of the system for the ID of each presence.
//! Its configuration and its errors are plain values that never wait, and so is what it decides about a presence:
//! which one a client is, and when one has ended.

mod attendance;
mod configuration;
mod definitions;
mod error;
mod feed;
mod plane;
mod presence;
mod revisions;
mod server;
mod stream;
mod ui;

pub use configuration::{ControlServerConfiguration, PingInterval, RequestSizeLimit, StatsInterval, StreamBacklog};
pub use error::{ControlServerError, PingIntervalError, RequestSizeLimitError, StatsIntervalError, StreamBacklogError};
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
        ControlServer, ControlServerConfiguration, ControlServerError, PingInterval, PingIntervalError,
        RequestSizeLimit, RequestSizeLimitError, StatsInterval, StatsIntervalError, StreamBacklog, StreamBacklogError,
    };
}
