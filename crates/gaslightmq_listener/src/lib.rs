//! Kafka on a socket: accept connections, frame bytes, and run whatever handles a connection.
//!
//! | Type | Is |
//! |---|---|
//! | [`Listener`] | One endpoint, open on a socket. It accepts connections, and runs each one in a task of its own. |
//! | [`ListenerConfiguration`] | What a listener is configured with: its address, its endpoint and its limits. |
//! | [`FrameSizeLimit`] | The largest frame a listener takes from a client. |
//! | [`ConnectionNumbers`] | The numbers of the connections that the listeners of one instance accept. |
//! | [`ConnectionHandler`] | Whatever handles a connection: the task of a proxy's session, or a broker that runs alone. |
//! | [`Transport`] | The connection of one client, as its handler holds it: frames in, bytes out, and the end in both directions. |
//!
//! The crate owns sockets and framing, and nothing else.
//! It knows no lie, no rule and no session: what happens to a frame is the handler's business.
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_listener::prelude::*;
//! use gaslightmq_wire::{Frame, FrameDecoder};
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use tokio::net::TcpStream;
//!
//! /// Sends every frame back to the client that sent it.
//! struct Echo;
//!
//! impl ConnectionHandler for Echo {
//!     async fn handle(&self, mut transport: Transport) {
//!         while let Ok(frame) = transport.receive().await {
//!             if transport.send(&frame.to_bytes()).await.is_err() {
//!                 return;
//!             }
//!         }
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), ListenerError> {
//! // Port 0 asks for any free port, and the listener names the one it got.
//! let any_port = "127.0.0.1:0".parse().expect("an address");
//! let configuration = ListenerConfiguration::new(any_port, Endpoint::Bootstrap);
//! let listener = Listener::bind(configuration, ConnectionNumbers::new(), Echo).await?;
//!
//! // A client sends one frame in two pieces, and reads it back whole.
//! let mut client = TcpStream::connect(listener.address()).await.expect("a listener that accepts");
//! let sent = Frame::new(&b"hello"[..]).to_bytes();
//! client.write_all(&sent[..3]).await.expect("a connection that takes bytes");
//! client.write_all(&sent[3..]).await.expect("a connection that takes bytes");
//!
//! let mut frames = FrameDecoder::new();
//! let echoed = loop {
//!     if let Some(frame) = frames.next_frame().expect("a frame") {
//!         break frame;
//!     }
//!     client.read_buf(frames.buffer_mut()).await.expect("the echo of the listener");
//! };
//! assert_eq!(echoed.payload().as_ref(), b"hello");
//!
//! // Closing the listener ends its connections, and returns once every handler returned.
//! listener.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # One connection, one task
//!
//! A [`Listener`] accepts on one address, for one [`Endpoint`](gaslightmq_field::Endpoint).
//! For every connection it calls [`ConnectionHandler::handle`] with a [`Transport`], in a task of that connection alone.
//! The transport carries what is known the moment a client connects:
//! the connection number, the address of the client and the endpoint.
//!
//! One process holds several listeners: a proxy has one for its bootstrap endpoint and one per broker.
//! They share one [`ConnectionNumbers`], so a number names one connection of the proxy.
//! A listener that binds to port 0 gets a free port, and [`Listener::address`] names it,
//! so tests run side by side.
//!
//! # Frames
//!
//! The bytes of a client arrive in pieces of any size.
//! [`Transport::receive`] hands out whole frames, one per call, and never a part of one.
//! A client that announces a frame over the [limit](ListenerConfiguration::socket_request_max_bytes) sends no frame,
//! and the transport says so before the payload arrives.
//!
//! [`Transport::send`] writes bytes as they are.
//! A response travels with its length in front, and whoever builds the response puts it there.
//!
//! # How a connection ends
//!
//! | Who ends it | The handler sees |
//! |---|---|
//! | The client closes its socket. | [`ReceiveError::ClientClosed`], or [`ReceiveError::FrameCutShort`] inside a frame. |
//! | The connection breaks. | [`ReceiveError::ConnectionLost`], or [`SendError::ConnectionLost`]. |
//! | The client sends bytes that are no frame. | [`ReceiveError::NotAFrame`]. The handler closes the connection. |
//! | The handler [closes](Transport::close) the transport. | Nothing more. The client reads everything that was sent, and then the end. |
//! | The handler lets go of the transport, or returns. | Nothing more. The connection is gone at once, and the client may lose bytes that were on their way. |
//! | Someone [closes](Listener::close) the listener. | [`ReceiveError::ListenerClosed`], or [`SendError::ListenerClosed`] from a send that waits for its client. The handler closes the connection. |
//!
//! Closing a transport waits until the client has closed its side,
//! for at most the [`close_timeout`](ListenerConfiguration::close_timeout) of the listener.
//! Without that wait the system drops the last response when the client sent something that nobody read,
//! such as its next request.
//!
//! A handler that panics ends its own connection, and the listener and the other connections go on.
//!
//! # Errors
//!
//! The three things the crate does fail in different ways, so each has an error of its own.
//! No caller meets a reason that cannot come from what it called.
//!
//! | What failed | Error | What is wrong |
//! |---|---|---|
//! | [`Listener::bind`] | [`ListenerError`] | The address cannot take a listener. |
//! | [`Transport::receive`] | [`ReceiveError`] | No further frame comes from the client. |
//! | [`Transport::send`] | [`SendError`] | The connection takes no bytes, or the listener closed before the client took them. |
//!
//! [`FrameSizeLimit`] checks its value, so it has an error of its own, [`FrameSizeLimitError`]:
//! the number of bytes is zero, or more than a frame can hold.
//!
//! An error names what failed first, and then says what the system or the reader of frames said about it.
//! It keeps that as text, in a field `reason`, and never as an error of the system.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # IO
//!
//! This crate is part of the IO edge: it is async, and it owns the sockets of the clients.
//! It holds no [`Clock`](gaslightmq_clock::Clock), and it gives no frame an instant:
//! a handler that needs the time of a frame reads its own clock when the frame arrives.
//! Its own two waits are real time, on the timer of the runtime, whatever clock the instance behind it runs on:
//! the [wait after a failed accept](ListenerConfiguration::accept_retry_backoff),
//! and the [wait for a client](ListenerConfiguration::close_timeout) when a connection closes.
//! It depends on `gaslightmq_wire` for frames, on `gaslightmq_field` for the connection fields,
//! and on `gaslightmq_clock` for the time spans of a configuration.
//!
//! [`Listener::bind`] starts tasks, so it runs inside the runtime.
//! The configuration and the connection numbers are plain values, and need none.

mod configuration;
mod error;
mod handler;
mod listener;
mod numbers;
mod transport;

pub use configuration::{FrameSizeLimit, FrameSizeLimitError, ListenerConfiguration};
pub use error::{ListenerError, ReceiveError, SendError};
pub use handler::ConnectionHandler;
pub use listener::Listener;
pub use numbers::ConnectionNumbers;
pub use transport::Transport;

/// Every item of this crate.
///
/// Import it whole wherever code opens listeners or handles connections, next to the prelude of the fields.
/// No name in it is a name of that prelude, of the kernel's prelude or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_listener::prelude::*;
///
/// let address = "127.0.0.1:9092".parse().expect("an address");
/// let configuration = ListenerConfiguration::new(address, Endpoint::Bootstrap);
/// assert_eq!(configuration.socket_request_max_bytes, FrameSizeLimit::default());
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ConnectionHandler, ConnectionNumbers, FrameSizeLimit, FrameSizeLimitError, Listener, ListenerConfiguration,
        ListenerError, ReceiveError, SendError, Transport,
    };
}
