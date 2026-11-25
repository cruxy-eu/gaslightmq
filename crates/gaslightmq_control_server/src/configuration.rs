//! What a control server is configured with.
//!
//! A property is a public field, and its type only holds values that the property allows.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};

use gaslightmq_control_api::clock::TimeSpan;

use crate::error::{RequestSizeLimitError, StatsIntervalError, StreamBacklogError};

/// What a [`ControlServer`](crate::ControlServer) is configured with.
///
/// ```
/// use gaslightmq_control_server::prelude::*;
///
/// let mut configuration = ControlServerConfiguration::default();
/// assert_eq!(configuration.listen, "127.0.0.1:7092".parse().expect("a socket address"));
///
/// // A test takes any free port, and asks the server which one it got.
/// configuration.listen = "127.0.0.1:0".parse().expect("a socket address");
/// configuration.stream_backlog = StreamBacklog::new(64)?;
/// # Ok::<(), StreamBacklogError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ControlServerConfiguration {
    /// Ours. Default: `127.0.0.1:7092`, which the UI, the CLI and tests reach on the same machine.
    ///
    /// The address the control plane listens on, which `control.listen` of `gaslight.yaml` names.
    /// Port 0 takes any free port, which [`ControlServer::address`](crate::ControlServer::address) names.
    pub listen: SocketAddr,
    /// Ours. Default: 1 MiB.
    ///
    /// The largest body of a request that the server reads. A larger one gets a `400` with
    /// [`RequestError::Unreadable`](gaslightmq_control_api::RequestError::Unreadable).
    pub request_limit: RequestSizeLimit,
    /// Ours. Default: 1024 messages.
    ///
    /// How many changes the server keeps for a client of the live stream that falls behind.
    /// A client that falls further behind gets a new snapshot instead of the changes it missed.
    pub stream_backlog: StreamBacklog,
    /// Ours. Default: 1 second.
    ///
    /// How often every client of the live stream gets the statistics of every environment that runs.
    pub stats_interval: StatsInterval,
    /// Ours. Default: 5 seconds.
    ///
    /// How long [`stop`](crate::ControlServer::stop) waits for the requests that are being answered.
    /// A request that takes longer ends without an answer.
    pub stop_timeout: TimeSpan,
}

impl Default for ControlServerConfiguration {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 7092)),
            request_limit: RequestSizeLimit::DEFAULT,
            stream_backlog: StreamBacklog::DEFAULT,
            stats_interval: StatsInterval::DEFAULT,
            stop_timeout: TimeSpan::from_secs(5),
        }
    }
}

/// The largest body of a request that a control server reads, in bytes. It is at least one byte.
///
/// ```
/// use gaslightmq_control_server::prelude::*;
///
/// let limit = RequestSizeLimit::new(64 * 1024)?;
/// assert_eq!(limit.bytes(), 65_536);
/// assert_eq!(limit.to_string(), "65536 bytes");
/// assert_eq!(RequestSizeLimit::new(0), Err(RequestSizeLimitError::Zero));
/// # Ok::<(), RequestSizeLimitError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestSizeLimit(usize);

impl RequestSizeLimit {
    /// The default limit: 1 MiB, which holds the largest definition with room to spare.
    pub const DEFAULT: Self = Self(1 << 20);

    /// Check that `bytes` is a size that a request can have, and keep it.
    ///
    /// # Errors
    ///
    /// [`RequestSizeLimitError::Zero`] when `bytes` is zero: no request would be read.
    pub const fn new(bytes: usize) -> Result<Self, RequestSizeLimitError> {
        if bytes == 0 {
            return Err(RequestSizeLimitError::Zero);
        }
        Ok(Self(bytes))
    }

    /// The limit as a plain number of bytes.
    pub const fn bytes(self) -> usize {
        self.0
    }
}

/// Writes the number of bytes: `1048576 bytes`.
impl fmt::Display for RequestSizeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0)
    }
}

/// How many changes a control server keeps for a client of the live stream that falls behind.
///
/// A backlog holds at least one message, and at most [`StreamBacklog::MAX`].
/// The server sets the room for them aside when it starts, so a backlog has a largest size.
///
/// ```
/// use gaslightmq_control_server::prelude::*;
///
/// let backlog = StreamBacklog::new(64)?;
/// assert_eq!(backlog.messages(), 64);
/// assert_eq!(backlog.to_string(), "64 messages");
/// assert_eq!(StreamBacklog::new(0), Err(StreamBacklogError::Zero));
/// # Ok::<(), StreamBacklogError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamBacklog(usize);

impl StreamBacklog {
    /// The default backlog: 1024 messages.
    pub const DEFAULT: Self = Self(1024);

    /// The largest backlog: 1 048 576 messages, which a server can still set the room aside for.
    pub const MAX: Self = Self(1 << 20);

    /// Check that `messages` is a number of messages that a backlog can hold, and keep it.
    ///
    /// # Errors
    ///
    /// [`StreamBacklogError::Zero`] when `messages` is zero,
    /// and [`StreamBacklogError::TooLarge`] when it is more than [`StreamBacklog::MAX`].
    pub const fn new(messages: usize) -> Result<Self, StreamBacklogError> {
        if messages == 0 {
            return Err(StreamBacklogError::Zero);
        }
        if messages > Self::MAX.0 {
            return Err(StreamBacklogError::TooLarge { messages });
        }
        Ok(Self(messages))
    }

    /// The backlog as a plain number of messages.
    pub const fn messages(self) -> usize {
        self.0
    }
}

/// Writes the number of messages: `1024 messages`.
impl fmt::Display for StreamBacklog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} messages", self.0)
    }
}

/// How often a control server sends the statistics of every environment that runs: longer than no time at all.
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_control_server::prelude::*;
///
/// let interval = StatsInterval::new(TimeSpan::from_millis(250))?;
/// assert_eq!(interval.span(), TimeSpan::from_millis(250));
/// assert_eq!(StatsInterval::new(TimeSpan::ZERO), Err(StatsIntervalError::Zero));
/// # Ok::<(), StatsIntervalError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatsInterval(TimeSpan);

impl StatsInterval {
    /// The default interval: one second, which a person reads a rate at.
    pub const DEFAULT: Self = Self(TimeSpan::from_secs(1));

    /// Check that `span` is longer than no time at all, and keep it.
    ///
    /// # Errors
    ///
    /// [`StatsIntervalError::Zero`] when `span` is no time at all.
    pub fn new(span: TimeSpan) -> Result<Self, StatsIntervalError> {
        if span == TimeSpan::ZERO {
            return Err(StatsIntervalError::Zero);
        }
        Ok(Self(span))
    }

    /// The interval as a time span.
    pub const fn span(self) -> TimeSpan {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backlog_is_one_message_at_least_and_the_largest_at_most() {
        assert_eq!(StreamBacklog::new(1).map(StreamBacklog::messages), Ok(1));
        assert_eq!(
            StreamBacklog::new(StreamBacklog::MAX.messages()),
            Ok(StreamBacklog::MAX)
        );
        assert_eq!(
            StreamBacklog::new(StreamBacklog::MAX.messages() + 1),
            Err(StreamBacklogError::TooLarge { messages: 1_048_577 })
        );
    }

    #[test]
    fn the_defaults_are_valid_values() {
        assert_eq!(
            StreamBacklog::new(StreamBacklog::DEFAULT.messages()),
            Ok(StreamBacklog::DEFAULT)
        );
        assert_eq!(
            RequestSizeLimit::new(RequestSizeLimit::DEFAULT.bytes()),
            Ok(RequestSizeLimit::DEFAULT)
        );
    }
}
