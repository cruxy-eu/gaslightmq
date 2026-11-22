//! What a listener is configured with.
//!
//! A property is a public field, and its type only holds values that the property allows.
//! A property that a Kafka broker has keeps the name, the meaning and the default of that property,
//! and the documentation of the field names it.
//! A property that Kafka does not have says that it is ours.

use std::fmt;
use std::net::SocketAddr;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::Endpoint;
use gaslightmq_wire::Frame;

/// What one [`Listener`](crate::Listener) is configured with.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_listener::prelude::*;
///
/// // Port 0 asks for any free port. The listener says which one it got.
/// let mut configuration = ListenerConfiguration::new("127.0.0.1:0".parse()?, Endpoint::Bootstrap);
/// assert_eq!(configuration.socket_request_max_bytes.bytes(), 100 * 1024 * 1024);
///
/// configuration.socket_request_max_bytes = FrameSizeLimit::new(1024 * 1024)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListenerConfiguration {
    /// Kafka: the host and the port of one entry of `listeners`. Required.
    ///
    /// The address the listener accepts connections on.
    /// With port 0 the system picks a free port, and [`Listener::address`](crate::Listener::address) names it.
    pub address: SocketAddr,
    /// Ours. Required.
    ///
    /// The endpoint the listener stands for: the bootstrap endpoint, or one broker of the cluster.
    /// Every connection it accepts is a connection to that endpoint.
    pub endpoint: Endpoint,
    /// Kafka: `socket.request.max.bytes`. Default: 100 MiB.
    ///
    /// The largest frame a client may send.
    /// A client that announces a larger one sends no frame at all, and its connection is of no use from there on.
    pub socket_request_max_bytes: FrameSizeLimit,
    /// Ours. Default: 100 milliseconds.
    ///
    /// How long the listener waits after it failed to accept a connection, before it accepts again.
    /// It waits when the process has run out of sockets or of memory, because trying again at once would fail again.
    /// It does not wait when a single connection failed on its way in: the next one is accepted at once.
    /// The wait is real time, whatever clock the instance behind the listener runs on.
    pub accept_retry_backoff: TimeSpan,
    /// Ours. Default: 1 second.
    ///
    /// How long a connection that its handler [closes](crate::Transport::close) waits for the client to close its side.
    /// Until then the client can read everything that was sent to it.
    /// A client that takes longer loses what it has not read yet.
    /// The wait is real time, whatever clock the instance behind the listener runs on.
    pub close_timeout: TimeSpan,
}

impl ListenerConfiguration {
    /// The defaults, for a listener that accepts connections to `endpoint` on `address`.
    pub fn new(address: SocketAddr, endpoint: Endpoint) -> Self {
        Self {
            address,
            endpoint,
            socket_request_max_bytes: FrameSizeLimit::default(),
            accept_retry_backoff: TimeSpan::from_millis(100),
            close_timeout: TimeSpan::from_secs(1),
        }
    }
}

/// The largest frame a listener takes from a client, in bytes of payload.
///
/// The length of a frame travels as a signed 32-bit number, so no limit is above [`FrameSizeLimit::MAX`].
/// A frame holds at least one byte, so no limit is below one.
/// The default is the default of a Kafka broker: 100 MiB.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let limit = FrameSizeLimit::new(1024)?;
/// assert_eq!(limit.bytes(), 1024);
/// assert_eq!(limit.to_string(), "1024 bytes");
/// assert_eq!(FrameSizeLimit::new(0), Err(FrameSizeLimitError::Zero));
/// # Ok::<(), FrameSizeLimitError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameSizeLimit(usize);

impl FrameSizeLimit {
    /// The largest limit: the most bytes the length of a frame can say.
    pub const MAX: Self = Self(0x7FFF_FFFF);

    /// Check that `bytes` is a size a frame can have, and keep it.
    ///
    /// # Errors
    ///
    /// [`FrameSizeLimitError::Zero`] when `bytes` is zero,
    /// and [`FrameSizeLimitError::TooLarge`] when it is more than [`FrameSizeLimit::MAX`].
    pub const fn new(bytes: usize) -> Result<Self, FrameSizeLimitError> {
        if bytes == 0 {
            return Err(FrameSizeLimitError::Zero);
        }
        if bytes > Self::MAX.0 {
            return Err(FrameSizeLimitError::TooLarge { bytes });
        }
        Ok(Self(bytes))
    }

    /// The limit as a plain number of bytes.
    pub const fn bytes(self) -> usize {
        self.0
    }
}

/// The default of a Kafka broker: 100 MiB.
impl Default for FrameSizeLimit {
    fn default() -> Self {
        Self(Frame::MAX_LEN)
    }
}

impl TryFrom<usize> for FrameSizeLimit {
    type Error = FrameSizeLimitError;

    fn try_from(bytes: usize) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl From<FrameSizeLimit> for usize {
    fn from(limit: FrameSizeLimit) -> Self {
        limit.bytes()
    }
}

/// Writes the number of bytes: `1024 bytes`.
impl fmt::Display for FrameSizeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0)
    }
}

/// Why a number of bytes is not a [`FrameSizeLimit`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameSizeLimitError {
    /// The limit is zero, and a frame holds at least one byte.
    Zero,
    /// The limit is more than the length of a frame can say.
    TooLarge { bytes: usize },
}

impl fmt::Display for FrameSizeLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("a frame size limit is at least one byte"),
            Self::TooLarge { bytes } => write!(
                f,
                "frame size limit of {bytes} bytes is more than the {} that the length of a frame can say",
                FrameSizeLimit::MAX
            ),
        }
    }
}

impl std::error::Error for FrameSizeLimitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_starts_with_the_defaults() {
        let configuration = ListenerConfiguration::new("127.0.0.1:9092".parse().unwrap(), Endpoint::Bootstrap);

        assert_eq!(configuration.address.port(), 9092);
        assert_eq!(configuration.endpoint, Endpoint::Bootstrap);
        assert_eq!(configuration.socket_request_max_bytes, FrameSizeLimit::default());
        assert_eq!(configuration.accept_retry_backoff, TimeSpan::from_millis(100));
        assert_eq!(configuration.close_timeout, TimeSpan::from_secs(1));
    }

    #[test]
    fn the_default_limit_is_the_default_of_a_kafka_broker() {
        assert_eq!(FrameSizeLimit::default().bytes(), 104_857_600);
        assert_eq!(FrameSizeLimit::default().bytes(), Frame::MAX_LEN);
    }

    #[test]
    fn a_limit_is_a_size_a_frame_can_have() {
        assert_eq!(FrameSizeLimit::new(1).map(FrameSizeLimit::bytes), Ok(1));
        assert_eq!(FrameSizeLimit::new(2_147_483_647), Ok(FrameSizeLimit::MAX));
        assert_eq!(FrameSizeLimit::new(0), Err(FrameSizeLimitError::Zero));
        assert_eq!(
            FrameSizeLimit::new(2_147_483_648),
            Err(FrameSizeLimitError::TooLarge { bytes: 2_147_483_648 })
        );
        assert_eq!(FrameSizeLimit::try_from(0), Err(FrameSizeLimitError::Zero));
        assert_eq!(usize::from(FrameSizeLimit::try_from(512).unwrap()), 512);
    }

    #[test]
    fn each_limit_error_says_what_is_wrong() {
        assert_eq!(
            FrameSizeLimitError::Zero.to_string(),
            "a frame size limit is at least one byte"
        );
        assert_eq!(
            FrameSizeLimitError::TooLarge { bytes: 2_147_483_648 }.to_string(),
            "frame size limit of 2147483648 bytes is more than the 2147483647 bytes that the length of a frame can say"
        );
    }
}
