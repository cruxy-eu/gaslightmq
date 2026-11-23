//! The connection of a session: what is known the moment a client connects.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use gaslightmq_field::prelude::*;

/// What a session knows from the moment its client connects: the connection fields of the Opened state.
///
/// The engine numbers the connection, and the transport reports the address and the endpoint.
/// The `host` is the address without its port.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let connection = Connection::new(
///     ConnectionNumber::new(7),
///     "10.0.4.27:51459".parse()?,
///     Endpoint::Broker(BrokerId::from(2)),
/// );
/// assert_eq!(connection.host().to_string(), "10.0.4.27");
/// assert_eq!(connection.to_string(), "#7 from 10.0.4.27:51459 to broker 2");
/// # Ok::<(), ClientAddressError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Connection {
    number: ConnectionNumber,
    address: ClientAddress,
    endpoint: Endpoint,
}

impl Connection {
    /// The connection with that number, from that address to that endpoint.
    pub const fn new(number: ConnectionNumber, address: ClientAddress, endpoint: Endpoint) -> Self {
        Self {
            number,
            address,
            endpoint,
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

    /// The IP address the client connects from, which is the field `host`.
    pub fn host(&self) -> ClientHost {
        self.address.host()
    }

    /// The endpoint the client connected to, which is the field `endpoint`.
    pub const fn endpoint(&self) -> Endpoint {
        self.endpoint
    }
}

/// Writes the number, the address and the endpoint: `#7 from 10.0.4.27:51459 to broker 2`.
impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} from {} to {}", self.number, self.address, self.endpoint)
    }
}

/// Which connection from its host to its endpoint a connection is: the 1st, the 2nd, the 3rd.
///
/// The ordinal is the last part of a [`SessionKey`](crate::SessionKey).
/// Unlike a connection number, it does not depend on what other hosts do,
/// so two runs of one test give each connection the same ordinal.
///
/// The text form is `#1`.
///
/// ```
/// use gaslightmq_session::prelude::*;
///
/// let second = ConnectionOrdinal::new(2)?;
/// assert_eq!(second.to_string(), "#2");
/// assert_eq!("#2".parse::<ConnectionOrdinal>()?, second);
/// assert_eq!(ConnectionOrdinal::new(0), Err(ConnectionOrdinalError::Zero));
/// # Ok::<(), ConnectionOrdinalError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionOrdinal(NonZeroU64);

impl ConnectionOrdinal {
    /// The first connection from a host to an endpoint.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Check that `value` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`ConnectionOrdinalError::Zero`] when `value` is zero.
    pub fn new(value: u64) -> Result<Self, ConnectionOrdinalError> {
        NonZeroU64::new(value).map(Self).ok_or(ConnectionOrdinalError::Zero)
    }

    /// The ordinal as a plain number.
    pub const fn value(self) -> u64 {
        self.0.get()
    }

    /// The ordinal after `connections` earlier connections.
    fn following(connections: u64) -> Self {
        Self(NonZeroU64::new(connections.saturating_add(1)).unwrap_or(NonZeroU64::MAX))
    }
}

impl TryFrom<u64> for ConnectionOrdinal {
    type Error = ConnectionOrdinalError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ConnectionOrdinal> for u64 {
    fn from(ordinal: ConnectionOrdinal) -> Self {
        ordinal.value()
    }
}

/// Reads `#1`.
impl FromStr for ConnectionOrdinal {
    type Err = ConnectionOrdinalError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text.strip_prefix('#').unwrap_or_default();
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ConnectionOrdinalError::Malformed { input: text.to_owned() });
        }
        let value = digits
            .parse()
            .map_err(|_| ConnectionOrdinalError::OutOfRange { input: text.to_owned() })?;
        Self::new(value)
    }
}

impl fmt::Display for ConnectionOrdinal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Why a number or a text is not a [`ConnectionOrdinal`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionOrdinalError {
    /// The number is zero, and the first connection has the ordinal one.
    #[error("a connection ordinal is at least one")]
    Zero,
    /// The text is not a `#` and digits.
    #[error("connection ordinal {input:?} does not have the form #N")]
    Malformed { input: String },
    /// The number is more than a `u64` holds.
    #[error("connection ordinal {input} is not between 1 and {}", u64::MAX)]
    OutOfRange { input: String },
}

/// The ordinals of the connections a proxy accepted, per host and endpoint.
///
/// Whoever accepts connections owns one of these, and asks it for the ordinal of every new connection.
/// It counts from the moment it was made, and a connection that closes does not give its ordinal back.
/// A client that reconnects therefore gets the next ordinal, and so a new [`SessionKey`](crate::SessionKey).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let host: ClientHost = "10.0.4.27".parse()?;
/// let broker = Endpoint::Broker(BrokerId::from(2));
/// let mut ordinals = ConnectionOrdinals::new();
///
/// assert_eq!(ordinals.assign(host, Endpoint::Bootstrap).value(), 1);
/// assert_eq!(ordinals.assign(host, broker).value(), 1);
/// assert_eq!(ordinals.assign(host, broker).value(), 2);
/// # Ok::<(), ClientHostError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConnectionOrdinals {
    /// How many connections each host opened to each endpoint.
    connections: BTreeMap<(ClientHost, Endpoint), u64>,
}

impl ConnectionOrdinals {
    /// The ordinals of a proxy that accepted no connection yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts one more connection from `host` to `endpoint`, and returns its ordinal.
    pub fn assign(&mut self, host: ClientHost, endpoint: Endpoint) -> ConnectionOrdinal {
        let connections = self.connections.entry((host, endpoint)).or_default();
        let ordinal = ConnectionOrdinal::following(*connections);
        *connections = ordinal.value();
        ordinal
    }

    /// How many connections `host` opened to `endpoint`.
    pub fn assigned(&self, host: ClientHost, endpoint: Endpoint) -> u64 {
        self.connections.get(&(host, endpoint)).copied().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn host(text: &str) -> ClientHost {
        text.parse().unwrap()
    }

    #[test]
    fn a_connection_has_the_fields_of_the_opened_state() {
        let connection = Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Bootstrap,
        );

        assert_eq!(connection.number(), ConnectionNumber::new(7));
        assert_eq!(connection.address().to_string(), "10.0.4.27:51459");
        assert_eq!(connection.host(), host("10.0.4.27"));
        assert_eq!(connection.endpoint(), Endpoint::Bootstrap);
        assert_eq!(connection.to_string(), "#7 from 10.0.4.27:51459 to bootstrap");
    }

    #[test]
    fn an_ordinal_is_at_least_one() {
        assert_eq!(ConnectionOrdinal::new(1), Ok(ConnectionOrdinal::FIRST));
        assert_eq!(ConnectionOrdinal::new(0), Err(ConnectionOrdinalError::Zero));
        assert_eq!(ConnectionOrdinal::try_from(0), Err(ConnectionOrdinalError::Zero));
        assert_eq!(u64::from(ConnectionOrdinal::try_from(3).unwrap()), 3);
        assert_eq!(ConnectionOrdinal::following(u64::MAX).value(), u64::MAX);
    }

    #[test]
    fn an_ordinal_writes_and_reads_a_hash_and_digits() {
        assert_eq!(ConnectionOrdinal::FIRST.to_string(), "#1");
        assert_eq!("#12".parse(), Ok(ConnectionOrdinal::new(12).unwrap()));
        assert_eq!("#0".parse::<ConnectionOrdinal>(), Err(ConnectionOrdinalError::Zero));
        for text in ["", "#", "1", "##1", "#-1", "# 1", "#1 ", "#one"] {
            assert_eq!(
                text.parse::<ConnectionOrdinal>(),
                Err(ConnectionOrdinalError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
        assert_eq!(
            "#18446744073709551616".parse::<ConnectionOrdinal>(),
            Err(ConnectionOrdinalError::OutOfRange {
                input: "#18446744073709551616".to_owned()
            })
        );
    }

    #[test]
    fn each_ordinal_error_says_what_is_wrong() {
        assert_eq!(
            ConnectionOrdinalError::Zero.to_string(),
            "a connection ordinal is at least one"
        );
        assert_eq!(
            "1".parse::<ConnectionOrdinal>().unwrap_err().to_string(),
            "connection ordinal \"1\" does not have the form #N"
        );
        assert_eq!(
            "#18446744073709551616"
                .parse::<ConnectionOrdinal>()
                .unwrap_err()
                .to_string(),
            "connection ordinal #18446744073709551616 is not between 1 and 18446744073709551615"
        );
    }

    #[test]
    fn ordinals_count_per_host_and_endpoint() {
        let broker = Endpoint::Broker(BrokerId::from(2));
        let mut ordinals = ConnectionOrdinals::new();

        assert_eq!(ordinals.assigned(host("10.0.4.27"), broker), 0);
        assert_eq!(ordinals.assign(host("10.0.4.27"), broker).value(), 1);
        assert_eq!(ordinals.assign(host("10.0.4.27"), broker).value(), 2);
        assert_eq!(ordinals.assign(host("10.0.4.27"), Endpoint::Bootstrap).value(), 1);
        assert_eq!(ordinals.assign(host("10.0.4.28"), broker).value(), 1);
        assert_eq!(ordinals.assign(host("10.0.4.27"), broker).value(), 3);
        assert_eq!(ordinals.assigned(host("10.0.4.27"), broker), 3);
    }

    #[test]
    fn the_ordinals_of_one_host_do_not_depend_on_the_other_hosts() {
        let broker = Endpoint::Broker(BrokerId::from(2));
        let mut alone = ConnectionOrdinals::new();
        let mut crowded = ConnectionOrdinals::new();

        crowded.assign(host("10.0.4.28"), broker);
        crowded.assign(host("10.0.4.29"), broker);

        assert_eq!(
            alone.assign(host("10.0.4.27"), broker),
            crowded.assign(host("10.0.4.27"), broker)
        );
    }
}
