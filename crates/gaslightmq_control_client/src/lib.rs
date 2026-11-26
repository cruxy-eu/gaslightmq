//! A client of the control plane, for a native target and for WebAssembly in a browser.
//!
//! The UI, the CLI and tests reach a process through the control plane.
//! This crate sends the operations of [`gaslightmq_control_api`] and follows its live stream,
//! over HTTP and a WebSocket.
//!
//! | Type | Is |
//! |---|---|
//! | [`ControlClient`] | A client of one control plane. It sends operations and opens live streams. |
//! | [`LiveStream`] | The snapshot of the process, then every change, and the calls it follows. |
//! | [`ControlClientConfiguration`] | Where the control plane is, which surface the client is, and how long it waits. |
//! | [`ControlPlaneAddress`], [`ControlPlaneAddressError`] | Where a control plane listens. |
//! | [`SendError`] | Why an operation got no answer of its own. |
//! | [`StreamError`] | Why a live stream did not open, or ended. |
//!
//! # An operation gets its answer or its error
//!
//! A caller sends an [`Operation`](gaslightmq_control_api::Operation), and gets back what the control plane
//! answered: the answer of the operation, or its error, each typed by the operation.
//! The error of the operation is an answer too, so the call returns it as its value.
//! A [`SendError`] says that no answer came, that the control plane did not take the request,
//! or that the client cannot read what came.
//!
//! Every request says [where it comes from](gaslightmq_control_api::OperationRequest).
//! A client is one surface, which its configuration names, and [`ControlClient::send`] sends from there.
//! [`ControlClient::send_from`] names the source of one request: another surface, or the cause of an undo or a redo.
//!
//! ```no_run
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_client::{ControlClient, SendError};
//!
//! # async fn arm(client: ControlClient) -> Result<(), SendError> {
//! let arm = ArmRule {
//!     environment: "orders-sim".parse().expect("an environment name"),
//!     rule: "delay-1s-on-order-producers".parse().expect("a rule name"),
//! };
//! match client.send(arm).await? {
//!     Ok(()) => {}
//!     // The environment does not run: start it, and arm the rule again.
//!     Err(RuleCommandError::NotRunning { environment }) => {
//!         let _ = client.send(StartEnvironment { environment }).await?;
//!     }
//!     Err(other) => eprintln!("{other}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # The live stream
//!
//! A [`LiveStream`] starts with the [snapshot](gaslightmq_control_api::StreamMessage::Snapshot) of the process,
//! and every message after it is one change, or one call that the stream follows.
//! A client that falls behind gets a new snapshot, and goes on from there.
//!
//! ```no_run
//! use gaslightmq_control_api::prelude::*;
//! use gaslightmq_control_api::selector::{Conditions, SelectorRef};
//! use gaslightmq_control_client::{ControlClient, StreamError};
//!
//! # async fn follow(client: ControlClient) -> Result<(), StreamError> {
//! let mut stream = client.live_stream().await?;
//! stream
//!     .follow(Subscription::Calls {
//!         environment: "orders-sim".parse().expect("an environment name"),
//!         selector: SelectorRef::Inline(Conditions::new()),
//!     })
//!     .await;
//! loop {
//!     match stream.next().await? {
//!         StreamMessage::Snapshot(state) => println!("{} environments run", state.environments.len()),
//!         StreamMessage::Call { call, .. } => println!("{call:?}"),
//!         _ => {}
//!     }
//! }
//! # }
//! ```
//!
//! # Transports
//!
//! The crate has one feature per transport, and each takes effect on its own target alone.
//! A build with every feature therefore has the transport of the target it builds for.
//!
//! | Feature | Target | Transport |
//! |---|---|---|
//! | `native`, on by default | Every target but `wasm32` | HTTP and WebSocket on Tokio. The runtime has its timers on. |
//! | `wasm` | `wasm32` | The browser's own fetch and WebSocket, and its timers. |
//!
//! The client speaks plain HTTP and plain WebSocket: an address is an `http` URL.

#[cfg(not(any(
    all(feature = "native", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32"),
)))]
compile_error!("the client needs a transport: the feature `native` on a native target, or `wasm` on wasm32");

mod address;
mod client;
mod configuration;
mod error;
mod exchange;
mod stream;
mod transport;

pub use address::{ControlPlaneAddress, ControlPlaneAddressError};
pub use client::ControlClient;
pub use configuration::ControlClientConfiguration;
pub use error::{SendError, StreamError};
pub use stream::LiveStream;
