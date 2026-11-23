//! What an Upstream backend is configured with.
//!
//! A property is a public field, and its type only holds values that the property allows.
//! A property that Kafka's own clients have keeps the name and the meaning of that property,
//! and the documentation of the field names it.
//! A property that Kafka does not have says that it is ours.

use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_environment::{HostName, Upstream};
use gaslightmq_kernel::ClientId;
use gaslightmq_wire::Frame;

/// What one [`UpstreamBackend`](crate::UpstreamBackend) is configured with.
///
/// The cluster and the advertised host come from the environment of the proxy.
/// Every other property has a default.
///
/// No property bounds the wait for the answer to a client's request.
/// A Fetch waits for records and a `JoinGroup` for the other members of its group,
/// the client has a timeout of its own, and a broker that never answers keeps the call in flight.
///
/// ```
/// use gaslightmq_backend_upstream::prelude::*;
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_environment::prelude::*;
///
/// let upstream = Upstream::new("kafka-1.staging:9092".parse().expect("a server address"));
/// let advertised_host = "gaslight.staging".parse().expect("a host name");
/// let mut configuration = UpstreamBackendConfiguration::new(upstream, advertised_host);
/// assert_eq!(configuration.socket_connection_setup_timeout, TimeSpan::from_secs(10));
///
/// configuration.metadata_max_age = TimeSpan::from_secs(30);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UpstreamBackendConfiguration {
    /// Ours. Required.
    ///
    /// The real cluster: the server the backend asks for the brokers, and how it connects.
    /// Its bootstrap server is Kafka's `bootstrap.servers`, with one server.
    pub upstream: Upstream,
    /// Ours. Required.
    ///
    /// The host under which clients reach every listener of the proxy.
    /// Wherever the cluster tells the address of a broker, the backend tells clients this host,
    /// and the port on which the engine [listens](gaslightmq_backend::Backend::listens) for that broker.
    /// A broker of Kafka has `advertised.listeners` for this, which names a port for each listener too.
    pub advertised_host: HostName,
    /// Kafka: `client.id`. Default: empty.
    ///
    /// The name the backend sends with its own requests, which ask the cluster for its brokers.
    /// A backend without one names itself `gaslightmq`, as Kafka's own clients name themselves after their role.
    /// The request of a client carries the client's own name, and the backend leaves it there.
    pub client_id: ClientId,
    /// Kafka: `socket.connection.setup.timeout.ms`. Default: 10 seconds.
    ///
    /// How long the backend waits for a connection to a broker to open.
    /// A call whose connection does not open in that time fails as
    /// [`Unreachable`](gaslightmq_backend::BackendFailure::Unreachable).
    /// The wait is real time, whatever clock the instance runs on.
    pub socket_connection_setup_timeout: TimeSpan,
    /// Kafka: `request.timeout.ms`. Default: 30 seconds.
    ///
    /// How long the backend waits for the answer to one of its own requests.
    /// The request of a client never waits against it.
    /// The wait is real time, whatever clock the instance runs on.
    pub request_timeout: TimeSpan,
    /// Kafka: `metadata.max.age.ms`. Default: 5 minutes.
    ///
    /// How long the backend trusts what it knows about the brokers of the cluster, before it asks again.
    /// It asks earlier when a call loses its connection or does not get one,
    /// and when an answer names a broker that has no listener.
    pub metadata_max_age: TimeSpan,
    /// Kafka: `retry.backoff.ms`. Default: 100 milliseconds.
    ///
    /// How long the backend waits after it asked the cluster for its brokers, before it asks again.
    /// However many calls fail in that time, the cluster gets one question.
    pub retry_backoff: TimeSpan,
    /// Ours. Default: 100 MiB.
    ///
    /// The largest response the backend takes from a broker.
    /// A broker that announces a larger one sends no response at all, and the call ends as
    /// [`NotAResponse`](gaslightmq_backend::BackendAnswer::NotAResponse).
    pub response_max_bytes: ResponseSizeLimit,
}

impl UpstreamBackendConfiguration {
    /// The defaults, for a backend in front of `upstream` whose proxy clients reach under `advertised_host`.
    pub fn new(upstream: Upstream, advertised_host: HostName) -> Self {
        Self {
            upstream,
            advertised_host,
            client_id: no_client_id(),
            socket_connection_setup_timeout: TimeSpan::from_secs(10),
            request_timeout: TimeSpan::from_secs(30),
            metadata_max_age: TimeSpan::from_secs(5 * 60),
            retry_backoff: TimeSpan::from_millis(100),
            response_max_bytes: ResponseSizeLimit::default(),
        }
    }

    /// The name the backend sends with its own requests: its `client_id`, or `gaslightmq` when it has none.
    pub(crate) fn own_client_id(&self) -> ClientId {
        if self.client_id.as_str().is_empty() {
            product_name()
        } else {
            self.client_id.clone()
        }
    }
}

/// The empty client ID, which is Kafka's default.
fn no_client_id() -> ClientId {
    ClientId::new("").expect("a client ID may be empty")
}

/// The client ID of a backend that nobody named: the name of the product.
fn product_name() -> ClientId {
    ClientId::new("gaslightmq").expect("a short text is a client ID")
}

/// The largest response a backend takes from a broker, in bytes of payload.
///
/// The length of a response travels as a signed 32-bit number, so no limit is above [`ResponseSizeLimit::MAX`].
/// A response holds at least one byte, so no limit is below one.
/// The default is 100 MiB.
///
/// ```
/// use gaslightmq_backend_upstream::prelude::*;
///
/// let limit = ResponseSizeLimit::new(1024)?;
/// assert_eq!(limit.bytes(), 1024);
/// assert_eq!(limit.to_string(), "1024 bytes");
/// assert_eq!(ResponseSizeLimit::new(0), Err(ResponseSizeLimitError::Zero));
/// # Ok::<(), ResponseSizeLimitError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResponseSizeLimit(usize);

impl ResponseSizeLimit {
    /// The largest limit: the most bytes the length of a response can say.
    pub const MAX: Self = Self(0x7FFF_FFFF);

    /// Check that `bytes` is a size a response can have, and keep it.
    ///
    /// # Errors
    ///
    /// [`ResponseSizeLimitError::Zero`] when `bytes` is zero,
    /// and [`ResponseSizeLimitError::TooLarge`] when it is more than [`ResponseSizeLimit::MAX`].
    pub const fn new(bytes: usize) -> Result<Self, ResponseSizeLimitError> {
        if bytes == 0 {
            return Err(ResponseSizeLimitError::Zero);
        }
        if bytes > Self::MAX.0 {
            return Err(ResponseSizeLimitError::TooLarge { bytes });
        }
        Ok(Self(bytes))
    }

    /// The limit as a plain number of bytes.
    pub const fn bytes(self) -> usize {
        self.0
    }
}

/// 100 MiB, which holds the largest Fetch response a client asks for by default.
impl Default for ResponseSizeLimit {
    fn default() -> Self {
        Self(Frame::MAX_LEN)
    }
}

impl TryFrom<usize> for ResponseSizeLimit {
    type Error = ResponseSizeLimitError;

    fn try_from(bytes: usize) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

impl From<ResponseSizeLimit> for usize {
    fn from(limit: ResponseSizeLimit) -> Self {
        limit.bytes()
    }
}

/// Writes the number of bytes: `1024 bytes`.
impl fmt::Display for ResponseSizeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0)
    }
}

/// Why a number of bytes is not a [`ResponseSizeLimit`].
///
/// The message is complete, so no variant has a source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ResponseSizeLimitError {
    /// The limit is zero, and a response holds at least one byte.
    #[error("a response size limit is at least one byte")]
    Zero,
    /// The limit is more than the length of a response can say.
    #[error(
        "response size limit of {bytes} bytes is more than the {max} that the length of a response can say",
        max = ResponseSizeLimit::MAX
    )]
    TooLarge { bytes: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> UpstreamBackendConfiguration {
        UpstreamBackendConfiguration::new(
            Upstream::new("kafka-1.staging:9092".parse().unwrap()),
            "gaslight.staging".parse().unwrap(),
        )
    }

    #[test]
    fn a_configuration_starts_with_the_defaults() {
        let configuration = configuration();

        assert_eq!(configuration.upstream.bootstrap().to_string(), "kafka-1.staging:9092");
        assert_eq!(configuration.advertised_host, "gaslight.staging");
        assert_eq!(configuration.client_id, "");
        assert_eq!(configuration.socket_connection_setup_timeout, TimeSpan::from_secs(10));
        assert_eq!(configuration.request_timeout, TimeSpan::from_secs(30));
        assert_eq!(configuration.metadata_max_age, TimeSpan::from_secs(300));
        assert_eq!(configuration.retry_backoff, TimeSpan::from_millis(100));
        assert_eq!(configuration.response_max_bytes, ResponseSizeLimit::default());
    }

    #[test]
    fn a_backend_without_a_client_id_names_itself_after_the_product() {
        let mut configuration = configuration();
        assert_eq!(configuration.own_client_id(), "gaslightmq");

        configuration.client_id = ClientId::new("gaslight-staging").unwrap();
        assert_eq!(configuration.own_client_id(), "gaslight-staging");
    }

    #[test]
    fn the_default_limit_is_a_hundred_mebibytes() {
        assert_eq!(ResponseSizeLimit::default().bytes(), 104_857_600);
    }

    #[test]
    fn a_limit_is_a_size_a_response_can_have() {
        assert_eq!(ResponseSizeLimit::new(1).map(ResponseSizeLimit::bytes), Ok(1));
        assert_eq!(ResponseSizeLimit::new(2_147_483_647), Ok(ResponseSizeLimit::MAX));
        assert_eq!(ResponseSizeLimit::new(0), Err(ResponseSizeLimitError::Zero));
        assert_eq!(
            ResponseSizeLimit::new(2_147_483_648),
            Err(ResponseSizeLimitError::TooLarge { bytes: 2_147_483_648 })
        );
        assert_eq!(ResponseSizeLimit::try_from(0), Err(ResponseSizeLimitError::Zero));
        assert_eq!(usize::from(ResponseSizeLimit::try_from(512).unwrap()), 512);
    }

    #[test]
    fn each_limit_error_says_what_is_wrong() {
        assert_eq!(
            ResponseSizeLimitError::Zero.to_string(),
            "a response size limit is at least one byte"
        );
        assert_eq!(
            ResponseSizeLimitError::TooLarge { bytes: 2_147_483_648 }.to_string(),
            "response size limit of 2147483648 bytes is more than the 2147483647 bytes that the length of a response can say"
        );
    }
}
