//! What a listener or the connection of a client cannot do.

use std::net::SocketAddr;

/// Why a [`Listener`](crate::Listener) could not open its endpoint.
///
/// [`Listener::bind`](crate::Listener::bind) returns it.
/// The variant keeps what the system said as text, in `reason`, and its message writes that text once.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let error = ListenerError::CannotListen {
///     address: "127.0.0.1:9092".parse().expect("an address"),
///     reason: "address in use".to_owned(),
/// };
/// assert_eq!(error.to_string(), "cannot listen on 127.0.0.1:9092: address in use");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListenerError {
    /// The address cannot take a listener: another socket holds the port, or the address is not one of this machine.
    /// `reason` says which.
    #[error("cannot listen on {address}: {reason}")]
    CannotListen { address: SocketAddr, reason: String },
}

/// Why a [`Transport`](crate::Transport) has no next frame.
///
/// [`Transport::receive`](crate::Transport::receive) returns it.
/// A handler decides for every reason who ended the connection, so the list is complete:
/// a new reason is a new decision for every handler.
///
/// A variant about something that the system or the reader of frames reported keeps what it said as text, in `reason`,
/// and its message writes that text once.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let error = ReceiveError::FrameCutShort { buffered: 6 };
/// assert_eq!(error.to_string(), "the client closed the connection 6 bytes into a frame");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReceiveError {
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
    /// The connection broke while the transport read from it. `reason` says how it broke.
    #[error("the connection to the client broke: {reason}")]
    ConnectionLost { reason: String },
    /// The listener that accepted the connection closed.
    #[error("the listener of the connection closed")]
    ListenerClosed,
}

/// Why a [`Transport`](crate::Transport) could not write to its client.
///
/// [`Transport::send`](crate::Transport::send) returns it.
/// After either, a part of the bytes may be written, and the connection is of no use for sending.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let error = SendError::ConnectionLost { reason: "broken pipe".to_owned() };
/// assert_eq!(error.to_string(), "the connection to the client broke: broken pipe");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    /// The connection broke while the transport wrote to it, or the client closed it before it took the bytes.
    /// `reason` says how it broke.
    #[error("the connection to the client broke: {reason}")]
    ConnectionLost { reason: String },
    /// The listener that accepted the connection closed while the client did not take the bytes.
    #[error("the listener of the connection closed")]
    ListenerClosed,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_listener_error_names_the_address_and_what_the_system_said_once() {
        let error = ListenerError::CannotListen {
            address: "127.0.0.1:9092".parse().unwrap(),
            reason: "address in use".to_owned(),
        };

        assert_eq!(error.to_string(), "cannot listen on 127.0.0.1:9092: address in use");
        assert!(error.source().is_none());
    }

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
                    reason: "frame of 9 bytes exceeds the limit of 8".to_owned(),
                },
                "the bytes of the client are no frame: frame of 9 bytes exceeds the limit of 8",
            ),
            (
                ReceiveError::ConnectionLost {
                    reason: "reset by peer".to_owned(),
                },
                "the connection to the client broke: reset by peer",
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
        let errors = [
            (
                SendError::ConnectionLost {
                    reason: "broken pipe".to_owned(),
                },
                "the connection to the client broke: broken pipe",
            ),
            (SendError::ListenerClosed, "the listener of the connection closed"),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }
}
