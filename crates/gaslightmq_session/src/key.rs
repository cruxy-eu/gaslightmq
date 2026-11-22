//! The session key: what tells one session from another in every run.

use std::fmt;
use std::str::FromStr;

use gaslightmq_field::prelude::*;
use gaslightmq_trigger::CounterKey;

use crate::{Connection, ConnectionOrdinal, ConnectionOrdinalError};

/// The key of a session: the host, the endpoint, and the ordinal of the connection among those of that pair.
///
/// A connection number depends on the order in which every client connects.
/// The session key does not: the second connection from `10.0.4.27` to `broker 2` has the same key in every run.
/// A rule that counts `per: session` rolls its dice from the key, so every run gives a connection the same rolls.
///
/// The text form is the three parts with a space between them: `10.0.4.27 broker 2 #1`.
/// That text is the [counter key](Self::counter_key) of the session.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let key = SessionKey::new(
///     "10.0.4.27".parse()?,
///     Endpoint::Broker(BrokerId::from(2)),
///     ConnectionOrdinal::FIRST,
/// );
/// assert_eq!(key.to_string(), "10.0.4.27 broker 2 #1");
/// assert_eq!("10.0.4.27 broker 2 #1".parse::<SessionKey>()?, key);
/// assert_eq!(key.counter_key().as_str(), "10.0.4.27 broker 2 #1");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionKey {
    host: ClientHost,
    endpoint: Endpoint,
    ordinal: ConnectionOrdinal,
}

impl SessionKey {
    /// The key of the connection with that ordinal among the connections from `host` to `endpoint`.
    pub const fn new(host: ClientHost, endpoint: Endpoint, ordinal: ConnectionOrdinal) -> Self {
        Self {
            host,
            endpoint,
            ordinal,
        }
    }

    /// The key of `connection`, which has that ordinal among the connections from its host to its endpoint.
    pub fn of(connection: &Connection, ordinal: ConnectionOrdinal) -> Self {
        Self::new(connection.host(), connection.endpoint(), ordinal)
    }

    /// The IP address the client connects from.
    pub const fn host(&self) -> ClientHost {
        self.host
    }

    /// The endpoint the client connected to.
    pub const fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Which connection from the host to the endpoint this is.
    pub const fn ordinal(&self) -> ConnectionOrdinal {
        self.ordinal
    }

    /// The counter key of the session: the text of the session key.
    ///
    /// The dice of a rule that counts `per: session` derive from it.
    pub fn counter_key(&self) -> CounterKey {
        match CounterKey::new(self.to_string()) {
            Ok(key) => key,
            Err(_) => unreachable!("the text of a session key has a host, an endpoint and an ordinal"),
        }
    }
}

/// Writes the host, the endpoint and the ordinal: `10.0.4.27 broker 2 #1`.
impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.host, self.endpoint, self.ordinal)
    }
}

/// Reads a host, an endpoint and an ordinal with one space between them.
impl FromStr for SessionKey {
    type Err = SessionKeyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || SessionKeyError::Malformed { input: text.to_owned() };
        let (host, rest) = text.split_once(' ').ok_or_else(malformed)?;
        let (endpoint, ordinal) = rest.rsplit_once(' ').ok_or_else(malformed)?;
        let input = || text.to_owned();
        Ok(Self {
            host: host
                .parse()
                .map_err(|error| SessionKeyError::InvalidHost { input: input(), error })?,
            endpoint: endpoint
                .parse()
                .map_err(|error| SessionKeyError::InvalidEndpoint { input: input(), error })?,
            ordinal: ordinal
                .parse()
                .map_err(|error| SessionKeyError::InvalidOrdinal { input: input(), error })?,
        })
    }
}

/// Why a text is not a [`SessionKey`].
///
/// A variant that names a part keeps the error of that part.
/// Its message names the whole text first, and then says what is wrong with the part.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionKeyError {
    /// The text is not three parts with a space between them.
    Malformed { input: String },
    /// The first part is not a host.
    InvalidHost { input: String, error: ClientHostError },
    /// The middle part is not an endpoint.
    InvalidEndpoint { input: String, error: EndpointError },
    /// The last part is not an ordinal.
    InvalidOrdinal {
        input: String,
        error: ConnectionOrdinalError,
    },
}

impl fmt::Display for SessionKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { input } => write!(
                f,
                "session key {input:?} does not have the form a host, an endpoint and an ordinal, \
                 such as 10.0.4.27 broker 2 #1"
            ),
            Self::InvalidHost { input, error } => write!(f, "session key {input:?}: {error}"),
            Self::InvalidEndpoint { input, error } => write!(f, "session key {input:?}: {error}"),
            Self::InvalidOrdinal { input, error } => write!(f, "session key {input:?}: {error}"),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for SessionKeyError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn key(host: &str, endpoint: Endpoint, ordinal: u64) -> SessionKey {
        SessionKey::new(
            host.parse().unwrap(),
            endpoint,
            ConnectionOrdinal::new(ordinal).unwrap(),
        )
    }

    #[test]
    fn a_session_key_writes_and_reads_its_three_parts() {
        let broker = key("10.0.4.27", Endpoint::Broker(BrokerId::from(2)), 1);
        let bootstrap = key("2001:db8::1", Endpoint::Bootstrap, 12);

        assert_eq!(broker.to_string(), "10.0.4.27 broker 2 #1");
        assert_eq!(bootstrap.to_string(), "2001:db8::1 bootstrap #12");
        for key in [broker, bootstrap] {
            assert_eq!(key.to_string().parse(), Ok(key));
        }
        assert_eq!(broker.host().to_string(), "10.0.4.27");
        assert_eq!(broker.endpoint(), Endpoint::Broker(BrokerId::from(2)));
        assert_eq!(broker.ordinal(), ConnectionOrdinal::FIRST);
    }

    #[test]
    fn the_key_of_a_connection_takes_its_host_and_its_endpoint() {
        let connection = Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Broker(BrokerId::from(2)),
        );

        assert_eq!(
            SessionKey::of(&connection, ConnectionOrdinal::FIRST),
            key("10.0.4.27", Endpoint::Broker(BrokerId::from(2)), 1)
        );
    }

    #[test]
    fn the_counter_key_is_the_text_of_the_session_key() {
        let key = key("10.0.4.27", Endpoint::Broker(BrokerId::from(2)), 3);

        assert_eq!(key.counter_key().as_str(), "10.0.4.27 broker 2 #3");
    }

    #[test]
    fn a_session_key_needs_three_parts() {
        for text in ["", "10.0.4.27", "10.0.4.27 bootstrap", "bootstrap#1"] {
            assert_eq!(
                text.parse::<SessionKey>(),
                Err(SessionKeyError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_session_key_says_which_part_is_wrong() {
        let cases = [
            (
                "localhost bootstrap #1",
                "session key \"localhost bootstrap #1\": \
                 client host \"localhost\" does not have the form an IP address, such as 10.0.4.27",
            ),
            (
                "10.0.4.27 broker -1 #1",
                "session key \"10.0.4.27 broker -1 #1\": endpoint \"broker -1\": broker ID cannot be negative: -1",
            ),
            (
                "10.0.4.27 broker 2 #0",
                "session key \"10.0.4.27 broker 2 #0\": a connection ordinal is at least one",
            ),
            (
                "10.0.4.27 broker 2 1",
                "session key \"10.0.4.27 broker 2 1\": connection ordinal \"1\" does not have the form #N",
            ),
        ];

        for (text, message) in cases {
            let error = text.parse::<SessionKey>().unwrap_err();
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none());
        }
        assert!(matches!(
            "localhost bootstrap #1".parse::<SessionKey>(),
            Err(SessionKeyError::InvalidHost { .. })
        ));
        assert!(matches!(
            "10.0.4.27 broker -1 #1".parse::<SessionKey>(),
            Err(SessionKeyError::InvalidEndpoint { .. })
        ));
        assert!(matches!(
            "10.0.4.27 broker 2 #0".parse::<SessionKey>(),
            Err(SessionKeyError::InvalidOrdinal {
                error: ConnectionOrdinalError::Zero,
                ..
            })
        ));
    }

    #[test]
    fn session_keys_order_by_host_then_endpoint_then_ordinal() {
        let mut keys = [
            key("10.0.4.28", Endpoint::Bootstrap, 1),
            key("10.0.4.27", Endpoint::Broker(BrokerId::from(2)), 2),
            key("10.0.4.27", Endpoint::Broker(BrokerId::from(2)), 1),
            key("10.0.4.27", Endpoint::Bootstrap, 1),
        ];
        keys.sort();

        assert_eq!(
            keys.map(|key| key.to_string()),
            [
                "10.0.4.27 bootstrap #1",
                "10.0.4.27 broker 2 #1",
                "10.0.4.27 broker 2 #2",
                "10.0.4.28 bootstrap #1"
            ]
        );
    }
}
