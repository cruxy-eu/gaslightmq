//! Why a control server did not start, and why a value of its configuration is not one.

use std::net::SocketAddr;

/// Why a [`ControlServer`](crate::ControlServer) did not start.
///
/// ```
/// use gaslightmq_control_server::prelude::*;
///
/// let error = ControlServerError::ListenerNotOpened {
///     address: "127.0.0.1:8080".parse().expect("a socket address"),
///     reason: "address in use".to_owned(),
/// };
/// assert_eq!(error.to_string(), "the control plane cannot listen on 127.0.0.1:8080: address in use");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlServerError {
    /// The server could not listen on the address of its configuration: another process listens there,
    /// or the address is none of this machine. `reason` is what the system said.
    #[error("the control plane cannot listen on {address}: {reason}")]
    ListenerNotOpened { address: SocketAddr, reason: String },
}

/// Why a number of bytes is no [`RequestSizeLimit`](crate::RequestSizeLimit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RequestSizeLimitError {
    /// A limit of zero bytes would read no request.
    #[error("a request size limit is at least one byte")]
    Zero,
}

/// Why a number of messages is no [`StreamBacklog`](crate::StreamBacklog).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamBacklogError {
    /// A backlog of no message would keep no change for a client.
    #[error("a stream backlog holds at least one message")]
    Zero,
    /// The backlog is larger than [`StreamBacklog::MAX`](crate::StreamBacklog::MAX).
    #[error("a stream backlog holds at most 1048576 messages, and not {messages}")]
    TooLarge { messages: usize },
}

/// Why a time span is no [`StatsInterval`](crate::StatsInterval).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StatsIntervalError {
    /// The statistics cannot come all the time: an interval is longer than no time at all.
    #[error("a stats interval is longer than no time at all")]
    Zero,
}
