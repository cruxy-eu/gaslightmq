//! Whatever handles a connection.

use std::future::Future;
use std::sync::Arc;

use crate::Transport;

/// Whatever handles the connections that a [`Listener`](crate::Listener) accepts.
///
/// The listener calls [`handle`](Self::handle) once for every connection, in a task of that connection alone.
/// The handler owns the [`Transport`] from there on, and the connection lives as long as the call runs.
///
/// The listener knows nothing about what a handler does with a frame.
/// A proxy puts the task of a session behind it, and a broker that runs alone answers the requests itself.
///
/// One handler serves every connection of its listener at once, so it is shared, and `handle` takes `&self`.
/// An [`Arc`] of a handler is a handler too, so several listeners share one.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// /// Sends every frame back to the client that sent it.
/// struct Echo;
///
/// impl ConnectionHandler for Echo {
///     async fn handle(&self, mut transport: Transport) {
///         while let Ok(frame) = transport.receive().await {
///             if transport.send(&frame.to_bytes()).await.is_err() {
///                 return;
///             }
///         }
///     }
/// }
/// ```
pub trait ConnectionHandler: Send + Sync + 'static {
    /// Runs one connection, from the moment the client connected until the connection ends.
    ///
    /// The connection ends when the call returns, whatever the client still wanted to say.
    /// A handler that wants the client to read its last response [closes](Transport::close) the transport first.
    ///
    /// A call that panics ends its own connection only, in a build that unwinds on a panic.
    /// In a build that aborts on a panic, it ends the process.
    fn handle(&self, transport: Transport) -> impl Future<Output = ()> + Send;
}

impl<H: ConnectionHandler> ConnectionHandler for Arc<H> {
    fn handle(&self, transport: Transport) -> impl Future<Output = ()> + Send {
        (**self).handle(transport)
    }
}
