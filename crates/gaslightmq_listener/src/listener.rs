//! One endpoint on a socket: it accepts connections, and runs each one in a task of its own.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use gaslightmq_field::{ClientAddress, Endpoint};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::{debug, error, warn};

use crate::{ConnectionHandler, ConnectionNumbers, ListenerConfiguration, Transport};

/// One endpoint, open on a socket.
///
/// A listener accepts the connections of Kafka clients on one address.
/// It gives every connection a number and a [`Transport`],
/// and runs its [`ConnectionHandler`] for that connection, in a task of the connection alone.
///
/// One process holds as many listeners as it has endpoints.
/// The listeners of one proxy share their [`ConnectionNumbers`], and usually their handler.
///
/// # Closing
///
/// A listener is open from [`bind`](Self::bind) until someone [closes](Self::close) it or lets go of it.
/// Closing ends the endpoint as a whole:
///
/// - The address refuses new connections.
/// - Every [`Transport`] that is still open tells its handler so:
///   its [`receive`](Transport::receive) returns [`ReceiveError::ListenerClosed`](crate::ReceiveError::ListenerClosed),
///   and a [`send`](Transport::send) that waits for its client gives up with
///   [`SendError::ListenerClosed`](crate::SendError::ListenerClosed).
///   The handler decides what happens to the connection, and it usually closes it.
///
/// Once [`close`](Self::close) returns, a new listener can [bind](Self::bind) to the address.
/// A listener that someone lets go of closes the same way, a moment later, and nobody waits for its handlers.
///
/// # A handler that panics
///
/// Every connection runs in a task of its own, so a panic in a handler ends that task and closes that connection.
/// The listener reports it and goes on, and the other connections do not notice.
/// That holds for a build that unwinds on a panic, as debug and test builds do.
/// A build that aborts on a panic, as the release build of this workspace does, ends the whole process instead.
#[derive(Debug)]
pub struct Listener {
    address: SocketAddr,
    endpoint: Endpoint,
    /// The listener is open while this lives. Nothing is ever sent through it.
    open: watch::Sender<()>,
    accepting: JoinHandle<()>,
}

impl Listener {
    /// Opens the endpoint of `configuration`, and accepts connections from then on.
    ///
    /// Every connection gets its number from `numbers`, and `handler` runs it.
    /// The listener accepts in a task of the runtime that the caller runs on, and a connection runs there too.
    ///
    /// # Errors
    ///
    /// [`ListenerError::Bind`] when the address cannot take a listener:
    /// another socket holds the port, or the address is not one of this machine.
    pub async fn bind<H: ConnectionHandler>(
        configuration: ListenerConfiguration,
        numbers: ConnectionNumbers,
        handler: H,
    ) -> Result<Self, ListenerError> {
        let requested = configuration.address;
        let failed = |error| ListenerError::Bind {
            address: requested,
            error,
        };
        let socket = TcpListener::bind(requested).await.map_err(failed)?;
        let address = socket.local_addr().map_err(failed)?;
        let (open, closing) = watch::channel(());
        let endpoint = configuration.endpoint;
        let accepting = Accepting {
            socket,
            configuration,
            numbers,
            handler: Arc::new(handler),
            closing: ListenerClosing(closing),
            connections: JoinSet::new(),
        };
        Ok(Self {
            address,
            endpoint,
            open,
            accepting: tokio::spawn(accepting.run()),
        })
    }

    /// The address the listener accepts connections on.
    ///
    /// It names the port the system picked, when the configuration asked for port 0.
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// The endpoint the listener stands for.
    pub const fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Closes the endpoint, and waits until every connection has ended.
    ///
    /// The address refuses connections from here on, and every open [`Transport`] tells its handler that the listener closed.
    /// The call returns once every handler returned, so nothing of the listener is left behind.
    ///
    /// A client cannot hold the call up: a transport of a closed listener waits for its client no longer than
    /// the [`close_timeout`](ListenerConfiguration::close_timeout), and only when its handler [closes](Transport::close) it.
    /// A handler can: one that waits for something else than its transport, and never returns, keeps the call waiting.
    /// A handler must not make the call itself.
    pub async fn close(self) {
        let Self { open, accepting, .. } = self;
        drop(open);
        // The task ends by itself once the listener is closed. It has no result to hand over.
        let _ = accepting.await;
    }
}

/// Tells the task of a listener and every transport when the listener closes.
#[derive(Debug, Clone)]
pub(crate) struct ListenerClosing(watch::Receiver<()>);

impl ListenerClosing {
    /// Waits until the listener closed. Returns at once when it already has.
    ///
    /// Giving up the wait loses nothing.
    pub async fn closed(&mut self) {
        // The channel carries no value. It ends when the listener closes or someone lets go of it.
        while self.0.changed().await.is_ok() {}
    }
}

/// The task of one listener: it accepts connections until the listener closes.
struct Accepting<H> {
    socket: TcpListener,
    configuration: ListenerConfiguration,
    numbers: ConnectionNumbers,
    handler: Arc<H>,
    closing: ListenerClosing,
    /// The tasks of the connections that are open.
    connections: JoinSet<()>,
}

impl<H: ConnectionHandler> Accepting<H> {
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                () = self.closing.closed() => break,
                // A task that ended leaves before the next connection comes in, so the tasks never pile up.
                Some(ended) = self.connections.join_next() => report(self.configuration.endpoint, ended),
                accepted = self.socket.accept() => match accepted {
                    Ok((stream, address)) => self.open(stream, address),
                    Err(error) => match AcceptFailure::of(&error) {
                        // The connection is gone, and the next one is not worse off for it.
                        AcceptFailure::OfOneConnection => {
                            debug!(endpoint = %self.configuration.endpoint, %error, "a connection failed before the listener accepted it");
                        }
                        // Trying again at once would fail again, and keep the task busy with nothing.
                        AcceptFailure::OfTheListener => {
                            warn!(endpoint = %self.configuration.endpoint, %error, "accepting a connection failed");
                            tokio::select! {
                                () = self.closing.closed() => break,
                                () = tokio::time::sleep(self.configuration.accept_retry_backoff.duration()) => {}
                            }
                        }
                    },
                },
            }
        }
        // Letting go of the socket frees the address, so that it refuses connections while the handlers wind down.
        drop(self.socket);
        while let Some(ended) = self.connections.join_next().await {
            report(self.configuration.endpoint, ended);
        }
    }

    /// Runs the handler for a connection that the socket accepted, in a task of its own.
    fn open(&mut self, stream: TcpStream, address: SocketAddr) {
        // A response is small and its client waits for it, so it leaves at once, as with a Kafka broker.
        // A socket that refuses the option still works.
        let _ = stream.set_nodelay(true);
        let transport = Transport::new(
            self.numbers.assign(),
            ClientAddress::new(address),
            stream,
            &self.configuration,
            self.closing.clone(),
        );
        let handler = Arc::clone(&self.handler);
        self.connections.spawn(async move { handler.handle(transport).await });
    }
}

/// What a failed accept says about the accepts that follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptFailure {
    /// One connection failed on its way in: its client gave up, or the network lost it.
    /// The listener accepts the next connection at once.
    OfOneConnection,
    /// The listener itself cannot accept: the process has run out of sockets or of memory, or nobody knows why.
    /// The listener waits before it accepts again.
    OfTheListener,
}

impl AcceptFailure {
    /// Reads `error` by its kind.
    ///
    /// The failures of one connection have a kind of their own.
    /// Running out of sockets has none, so every other error is a failure of the listener:
    /// an error that nobody knows makes the listener wait, and never spin.
    fn of(error: &io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::Interrupted
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::TimedOut => Self::OfOneConnection,
            _ => Self::OfTheListener,
        }
    }
}

/// Says so when the handler of a connection panicked. The connection closed with it, and the listener goes on.
/// A build that aborts on a panic never gets here: the process ends with the handler.
fn report(endpoint: Endpoint, ended: Result<(), JoinError>) {
    if let Err(panicked) = ended
        && panicked.is_panic()
    {
        error!(%endpoint, "the handler of a connection panicked, and the connection closed");
    }
}

/// Why a [`Listener`] could not open its endpoint.
#[derive(Debug)]
#[non_exhaustive]
pub enum ListenerError {
    /// The address cannot take a listener: another socket holds the port, or the address is not one of this machine.
    Bind { address: SocketAddr, error: io::Error },
}

impl fmt::Display for ListenerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind { address, error } => write!(f, "cannot listen on {address}: {error}"),
        }
    }
}

/// The message is complete, so the error has no source.
impl std::error::Error for ListenerError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_connection_that_failed_on_its_way_in_does_not_make_the_listener_wait() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::Interrupted,
            io::ErrorKind::NetworkDown,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::TimedOut,
        ] {
            assert_eq!(
                AcceptFailure::of(&io::Error::from(kind)),
                AcceptFailure::OfOneConnection,
                "{kind}"
            );
        }
    }

    /// The numbers the system gives its errors are the same on Linux and macOS for these three.
    #[cfg(unix)]
    #[test]
    fn a_process_out_of_sockets_or_memory_makes_the_listener_wait() {
        const OUT_OF_MEMORY: i32 = 12;
        const TOO_MANY_OPEN_FILES_IN_THE_SYSTEM: i32 = 23;
        const TOO_MANY_OPEN_FILES: i32 = 24;

        for code in [OUT_OF_MEMORY, TOO_MANY_OPEN_FILES_IN_THE_SYSTEM, TOO_MANY_OPEN_FILES] {
            let error = io::Error::from_raw_os_error(code);
            assert_eq!(AcceptFailure::of(&error), AcceptFailure::OfTheListener, "{error}");
        }
    }

    #[test]
    fn an_error_that_nobody_knows_makes_the_listener_wait() {
        let error = io::Error::other("something new");

        assert_eq!(AcceptFailure::of(&error), AcceptFailure::OfTheListener);
    }

    #[test]
    fn a_listener_error_names_the_address_and_the_cause_once() {
        let error = ListenerError::Bind {
            address: "127.0.0.1:9092".parse().unwrap(),
            error: io::Error::new(io::ErrorKind::AddrInUse, "address in use"),
        };

        assert_eq!(error.to_string(), "cannot listen on 127.0.0.1:9092: address in use");
        assert!(error.source().is_none());
    }
}
