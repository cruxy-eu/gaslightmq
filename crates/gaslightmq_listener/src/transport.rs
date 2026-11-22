//! The connection of one client, as its handler holds it.

use std::fmt;
use std::io;

use gaslightmq_clock::Duration;
use gaslightmq_field::{ClientAddress, ConnectionNumber, Endpoint};
use gaslightmq_wire::{Frame, FrameDecoder, WireError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::ListenerConfiguration;
use crate::listener::ListenerClosing;

/// The connection of one client: it reads the client's bytes, and writes the bytes the client receives.
///
/// A [`Listener`](crate::Listener) makes one for every connection it accepts,
/// and hands it to its [`ConnectionHandler`](crate::ConnectionHandler).
///
/// # What a session opens with
///
/// The transport knows what is known the moment a client connects:
/// the [number](Self::number) of the connection, the [address](Self::address) of the client,
/// and the [endpoint](Self::endpoint) the client connected to.
///
/// # Frames in, bytes out
///
/// [`receive`](Self::receive) waits for the next whole frame, however the bytes of the client arrive:
/// one frame in many pieces, or many frames in one piece.
/// [`send`](Self::send) writes bytes as they are, so whoever writes a response puts its length in front.
///
/// # How a connection ends
///
/// | Who ends it | How | The other side |
/// |---|---|---|
/// | The client | It closes its socket. | [`receive`](Self::receive) returns [`ReceiveError::ClientClosed`]. |
/// | The handler | It calls [`close`](Self::close). | The client reads everything that was sent, and then the end of the connection. |
/// | The handler | It lets go of the transport. | The connection is gone at once, and the client may lose bytes that were on their way. |
/// | The listener | Someone [closes](crate::Listener::close) it. | [`receive`](Self::receive) returns [`ReceiveError::ListenerClosed`], and the handler closes the transport or lets go of it. |
///
/// # After the listener closed
///
/// No call holds a handler up for long once its listener closed, so the listener can wait for its handlers.
/// [`receive`](Self::receive) returns at once, and so does a [`send`](Self::send) that the client does not take.
/// [`close`](Self::close) waits for the client for a limited time, as it always does.
#[derive(Debug)]
pub struct Transport {
    number: ConnectionNumber,
    address: ClientAddress,
    endpoint: Endpoint,
    stream: TcpStream,
    /// The bytes of the client that no frame has taken yet.
    frames: FrameDecoder,
    /// How long a closing connection waits for its client.
    close_timeout: Duration,
    listener: ListenerClosing,
}

impl Transport {
    /// The transport of the connection that a listener with that configuration accepted as `stream`.
    pub(crate) fn new(
        number: ConnectionNumber,
        address: ClientAddress,
        stream: TcpStream,
        configuration: &ListenerConfiguration,
        listener: ListenerClosing,
    ) -> Self {
        Self {
            number,
            address,
            endpoint: configuration.endpoint,
            stream,
            frames: FrameDecoder::with_max_len(configuration.socket_request_max_bytes.bytes()),
            close_timeout: configuration.close_timeout.duration(),
            listener,
        }
    }

    /// The number of the connection, which is the field `connection`.
    pub const fn number(&self) -> ConnectionNumber {
        self.number
    }

    /// The address the client connects from, which is the field `address`.
    pub const fn address(&self) -> ClientAddress {
        self.address
    }

    /// The endpoint the client connected to, which is the field `endpoint`.
    pub const fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Waits for the next frame of the client.
    ///
    /// A frame that arrived whole leaves before the transport reads again,
    /// so the frames of one piece come out one call after the other.
    ///
    /// Giving up the wait loses nothing: the bytes that arrived stay in the transport, and the next call goes on.
    /// A handler therefore waits for a frame and for whatever else can happen in one `select!`.
    ///
    /// # Errors
    ///
    /// A [`ReceiveError`] when no further frame comes. It says why.
    /// The transport has no frame after that, and every later call returns an error again.
    pub async fn receive(&mut self) -> Result<Frame, ReceiveError> {
        loop {
            match self.frames.next_frame() {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {}
                Err(error) => return Err(ReceiveError::NotAFrame { error }),
            }
            let read = tokio::select! {
                biased;
                () = self.listener.closed() => return Err(ReceiveError::ListenerClosed),
                read = self.stream.read_buf(self.frames.buffer_mut()) => read,
            };
            match read {
                Ok(0) => {
                    return Err(match self.frames.buffered() {
                        0 => ReceiveError::ClientClosed,
                        buffered => ReceiveError::FrameCutShort { buffered },
                    });
                }
                Ok(_) => {}
                Err(error) => return Err(ReceiveError::ConnectionLost { error }),
            }
        }
    }

    /// Writes `bytes` to the client, as they are.
    ///
    /// The call returns once the system took every byte.
    /// A client that does not read holds it up, as it holds up a broker.
    /// Giving up the wait may leave a part of the bytes written, so a handler lets the call finish.
    ///
    /// The wait ends when the listener closes: a client that stopped reading must not keep a closed endpoint alive.
    /// Bytes that the system takes at once still leave after the listener closed, so a handler can say its last words.
    ///
    /// # Errors
    ///
    /// [`SendError::ConnectionLost`] when the connection does not take the bytes.
    /// [`SendError::ListenerClosed`] when the listener closed before the client took them all.
    /// After either, a part of the bytes may be written, and the connection is of no use for sending.
    pub async fn send(&mut self, bytes: &[u8]) -> Result<(), SendError> {
        tokio::select! {
            biased;
            written = self.stream.write_all(bytes) => written.map_err(|error| SendError::ConnectionLost { error }),
            () = self.listener.closed() => Err(SendError::ListenerClosed),
        }
    }

    /// Closes the connection, and lets the client read everything that was [sent](Self::send).
    ///
    /// The transport tells the client that nothing more comes.
    /// Then it waits until the client closed its side, and throws away what the client still sends.
    /// It waits for at most the [`close_timeout`](ListenerConfiguration::close_timeout) of its listener.
    /// A client that has not read everything by then loses the rest.
    ///
    /// Letting go of the transport without this call ends the connection at once.
    /// When the client sent bytes that the handler never read, such as a next request, or still sends some,
    /// the system resets the connection, and the client loses what it has not received yet.
    /// A handler that wants its last response to arrive closes the transport.
    pub async fn close(mut self) {
        // The connection ends either way: when the socket cannot say so, letting go of it does.
        let _ = self.stream.shutdown().await;
        let client_closed = async {
            let unread = self.frames.buffer_mut();
            loop {
                unread.clear();
                if matches!(self.stream.read_buf(unread).await, Ok(0) | Err(_)) {
                    return;
                }
            }
        };
        // A client that keeps its side open is cut off when the time is over.
        let _ = tokio::time::timeout(self.close_timeout, client_closed).await;
    }
}

/// Why a [`Transport`] has no next frame.
///
/// A handler decides for every reason who ended the connection, so the list is complete:
/// a new reason is a new decision for every handler.
#[derive(Debug)]
pub enum ReceiveError {
    /// The client closed the connection after a whole frame, or before its first one.
    ClientClosed,
    /// The client closed the connection inside a frame. `buffered` bytes of that frame arrived.
    FrameCutShort { buffered: usize },
    /// The bytes of the client are no frame: they announce a frame of no bytes, or one over the limit of the listener.
    /// Nothing says where the next frame starts, so the connection is of no use.
    NotAFrame { error: WireError },
    /// The connection broke while the transport read from it.
    ConnectionLost { error: io::Error },
    /// The listener that accepted the connection closed.
    ListenerClosed,
}

impl fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientClosed => f.write_str("the client closed the connection"),
            Self::FrameCutShort { buffered } => {
                write!(f, "the client closed the connection {buffered} bytes into a frame")
            }
            Self::NotAFrame { error } => write!(f, "the bytes of the client are no frame: {error}"),
            Self::ConnectionLost { error } => write!(f, "the connection broke while reading from the client: {error}"),
            Self::ListenerClosed => f.write_str("the listener of the connection closed"),
        }
    }
}

/// The message is complete, so the error has no source.
impl std::error::Error for ReceiveError {}

/// Why a [`Transport`] could not write to its client.
#[derive(Debug)]
#[non_exhaustive]
pub enum SendError {
    /// The connection broke, or the client closed it, so it takes no bytes.
    ConnectionLost { error: io::Error },
    /// The listener that accepted the connection closed while the client did not take the bytes.
    ListenerClosed,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionLost { error } => write!(f, "the connection broke while writing to the client: {error}"),
            Self::ListenerClosed => {
                f.write_str("the listener of the connection closed before the client took the bytes")
            }
        }
    }
}

/// The message is complete, so the error has no source.
impl std::error::Error for SendError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_receive_error_says_everything_in_one_message() {
        let errors = [
            (ReceiveError::ClientClosed, "the client closed the connection"),
            (
                ReceiveError::FrameCutShort { buffered: 6 },
                "the client closed the connection 6 bytes into a frame",
            ),
            (
                ReceiveError::NotAFrame {
                    error: WireError::FrameTooLarge { len: 9, max: 8 },
                },
                "the bytes of the client are no frame: frame of 9 bytes exceeds the limit of 8",
            ),
            (
                ReceiveError::ConnectionLost {
                    error: io::Error::new(io::ErrorKind::ConnectionReset, "reset by peer"),
                },
                "the connection broke while reading from the client: reset by peer",
            ),
            (ReceiveError::ListenerClosed, "the listener of the connection closed"),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }

    #[test]
    fn a_send_error_says_everything_in_one_message() {
        let error = SendError::ConnectionLost {
            error: io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"),
        };

        assert_eq!(
            error.to_string(),
            "the connection broke while writing to the client: broken pipe"
        );
        assert!(error.source().is_none());
        assert_eq!(
            SendError::ListenerClosed.to_string(),
            "the listener of the connection closed before the client took the bytes"
        );
    }
}
