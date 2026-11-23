//! What a listener or the connection of a client cannot do.

use std::net::SocketAddr;

/// Why a listener could not open its endpoint, or why the connection of a client has no next frame or takes no bytes.
///
/// It is the one error of what this crate does.
///
/// | The variants | Come from |
/// |---|---|
/// | `CannotListen` | [`Listener::bind`](crate::Listener::bind) |
/// | `ClientClosed`, `FrameCutShort`, `NotAFrame`, `ConnectionLost`, `ListenerClosed` | [`Transport::receive`](crate::Transport::receive) |
/// | `ConnectionLost`, `ListenerClosed` | [`Transport::send`](crate::Transport::send) |
///
/// A handler decides for every reason who ended the connection, so the list is complete:
/// a new reason is a new decision for every handler.
///
/// A variant about something that the system or the reader of frames reported keeps what it said as text, in `reason`,
/// and its message writes that text once.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let error = ListenerError::FrameCutShort { buffered: 6 };
/// assert_eq!(error.to_string(), "the client closed the connection 6 bytes into a frame");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListenerError {
    /// The address cannot take a listener: another socket holds the port, or the address is not one of this machine.
    /// `reason` says which.
    #[error("cannot listen on {address}: {reason}")]
    CannotListen { address: SocketAddr, reason: String },
    /// The client closed the connection after a whole frame, or before its first one.
    #[error("the client closed the connection")]
    ClientClosed,
    /// The client closed the connection inside a frame. `buffered` bytes of that frame arrived.
    #[error("the client closed the connection {buffered} bytes into a frame")]
    FrameCutShort { buffered: usize },
    /// The bytes of the client are no frame: they announce a frame of no bytes, or one over the limit of the listener.
    /// Nothing says where the next frame starts, so the connection is of no use.
    #[error("the bytes of the client are no frame: {reason}")]
    NotAFrame { reason: String },
    /// The connection broke while the transport read from it or wrote to it, or the client closed it before it took the
    /// bytes. `reason` says how it broke.
    #[error("the connection to the client broke: {reason}")]
    ConnectionLost { reason: String },
    /// The listener that accepted the connection closed.
    /// A send that waited for its client stopped there, and a part of its bytes may be written.
    #[error("the listener of the connection closed")]
    ListenerClosed,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_listener_error_says_everything_in_one_message() {
        let errors = [
            (
                ListenerError::CannotListen {
                    address: "127.0.0.1:9092".parse().unwrap(),
                    reason: "address in use".to_owned(),
                },
                "cannot listen on 127.0.0.1:9092: address in use",
            ),
            (ListenerError::ClientClosed, "the client closed the connection"),
            (
                ListenerError::FrameCutShort { buffered: 6 },
                "the client closed the connection 6 bytes into a frame",
            ),
            (
                ListenerError::NotAFrame {
                    reason: "frame of 9 bytes exceeds the limit of 8".to_owned(),
                },
                "the bytes of the client are no frame: frame of 9 bytes exceeds the limit of 8",
            ),
            (
                ListenerError::ConnectionLost {
                    reason: "reset by peer".to_owned(),
                },
                "the connection to the client broke: reset by peer",
            ),
            (ListenerError::ListenerClosed, "the listener of the connection closed"),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }
}
