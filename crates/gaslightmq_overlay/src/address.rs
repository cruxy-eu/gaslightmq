use std::fmt;
use std::str::FromStr;

/// The address of a broker: the host and the port a client connects to.
///
/// A cluster tells it in Metadata, `FindCoordinator` and `DescribeCluster` responses.
/// A real broker has one, and so has the listener that stands for that broker in front of a client.
///
/// The host is a name or an IP address, as the cluster writes it.
/// It has at least one character and at most [`MAX_HOST_LEN`](Self::MAX_HOST_LEN) bytes,
/// and no whitespace or control character.
/// The port is between 1 and 65535.
///
/// The text form is the host, a colon and the port: `kafka-2.internal:9092`.
/// An IPv6 address stands in brackets: `[2001:db8::2]:9092`.
///
/// ```
/// use gaslightmq_overlay::prelude::*;
///
/// let address = BrokerAddress::new("kafka-2.internal", 9092)?;
/// assert_eq!(address.host(), "kafka-2.internal");
/// assert_eq!(address.port(), 9092);
/// assert_eq!(address.to_string(), "kafka-2.internal:9092");
///
/// let listener: BrokerAddress = "[2001:db8::2]:19094".parse()?;
/// assert_eq!(listener.host(), "2001:db8::2");
/// # Ok::<(), BrokerAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BrokerAddress {
    host: String,
    port: u16,
}

impl BrokerAddress {
    /// The longest host, in bytes: the longest name the domain name system has.
    pub const MAX_HOST_LEN: usize = 255;

    /// Check the host and the port, and keep them.
    ///
    /// # Errors
    ///
    /// - [`BrokerAddressError::EmptyHost`] when the host has no character.
    /// - [`BrokerAddressError::HostTooLong`] when it has more than [`MAX_HOST_LEN`](Self::MAX_HOST_LEN) bytes.
    /// - [`BrokerAddressError::InvalidHostCharacter`] when it holds whitespace or a control character.
    /// - [`BrokerAddressError::PortZero`] when the port is zero.
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, BrokerAddressError> {
        let host = host.into();
        if host.is_empty() {
            return Err(BrokerAddressError::EmptyHost);
        }
        if host.len() > Self::MAX_HOST_LEN {
            return Err(BrokerAddressError::HostTooLong {
                len: host.len(),
                max: Self::MAX_HOST_LEN,
            });
        }
        let invalid = |character: &char| character.is_whitespace() || character.is_control();
        if let Some((position, character)) = host.chars().enumerate().find(|(_, character)| invalid(character)) {
            return Err(BrokerAddressError::InvalidHostCharacter {
                host,
                character,
                position: position + 1,
            });
        }
        if port == 0 {
            return Err(BrokerAddressError::PortZero);
        }
        Ok(Self { host, port })
    }

    /// The name or the IP address of the host, without brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port, between 1 and 65535.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The address that a response writes as `host` and `port`, or `None` when the two are no address.
    ///
    /// A response writes "no broker" as an empty host and the port `-1`.
    pub(crate) fn from_wire(host: &str, port: i32) -> Option<Self> {
        Self::new(host, u16::try_from(port).ok()?).ok()
    }
}

/// Reads a host, a colon and a port, such as `kafka-2.internal:9092` or `[2001:db8::2]:9092`.
impl FromStr for BrokerAddress {
    type Err = BrokerAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || BrokerAddressError::Malformed { input: text.to_owned() };
        let (host, port) = text.rsplit_once(':').ok_or_else(malformed)?;
        let host = match host.strip_prefix('[') {
            Some(bracketed) => bracketed.strip_suffix(']').ok_or_else(malformed)?,
            // Without brackets, nobody can tell where an IPv6 address ends and the port starts.
            None if host.contains(':') => return Err(malformed()),
            None => host,
        };
        if host.is_empty() {
            return Err(malformed());
        }
        let digits = !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit());
        let port = match port.parse::<u16>() {
            Ok(port) if digits && port > 0 => port,
            _ => return Err(BrokerAddressError::InvalidPort { input: text.to_owned() }),
        };
        Self::new(host, port)
    }
}

impl TryFrom<&str> for BrokerAddress {
    type Error = BrokerAddressError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl TryFrom<String> for BrokerAddress {
    type Error = BrokerAddressError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

/// Writes the text form, such as `kafka-2.internal:9092`. An IPv6 address stands in brackets.
impl fmt::Display for BrokerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Why a host and a port, or a text, are not a [`BrokerAddress`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BrokerAddressError {
    /// The host has no character.
    EmptyHost,
    /// The host has more bytes than a host name can have.
    HostTooLong { len: usize, max: usize },
    /// The host holds whitespace or a control character, at this position, counted from one.
    InvalidHostCharacter {
        host: String,
        character: char,
        position: usize,
    },
    /// The port is zero, which no client connects to.
    PortZero,
    /// The text is not a host, a colon and a port.
    Malformed { input: String },
    /// What follows the last colon of the text is not a port between 1 and 65535.
    InvalidPort { input: String },
}

impl fmt::Display for BrokerAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHost => f.write_str("the host of a broker address cannot be empty"),
            Self::HostTooLong { len, max } => {
                write!(
                    f,
                    "the host of a broker address is {len} bytes long, and the limit is {max}"
                )
            }
            Self::InvalidHostCharacter {
                host,
                character,
                position,
            } => write!(
                f,
                "the host {host:?} of a broker address has the invalid character {character:?} at position {position}"
            ),
            Self::PortZero => f.write_str("the port of a broker address cannot be zero"),
            Self::Malformed { input } => write!(
                f,
                "broker address {input:?} does not have the form of a host and a port, such as kafka-2.internal:9092"
            ),
            Self::InvalidPort { input } => {
                write!(f, "broker address {input:?} does not end in a port between 1 and 65535")
            }
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for BrokerAddressError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_keeps_its_host_and_its_port() {
        let address = BrokerAddress::new("kafka-2.internal", 9092).unwrap();

        assert_eq!(address.host(), "kafka-2.internal");
        assert_eq!(address.port(), 9092);
        assert_eq!(address.to_string(), "kafka-2.internal:9092");
    }

    #[test]
    fn an_address_reads_what_it_writes() {
        for text in [
            "kafka-2.internal:9092",
            "10.0.4.27:1",
            "localhost:65535",
            "[2001:db8::2]:9092",
        ] {
            let address: BrokerAddress = text.parse().unwrap();
            assert_eq!(address.to_string(), text);
            assert_eq!(BrokerAddress::try_from(text), Ok(address.clone()));
            assert_eq!(BrokerAddress::try_from(text.to_owned()), Ok(address));
        }
    }

    #[test]
    fn an_ipv6_host_stands_in_brackets_in_the_text_form_only() {
        let address: BrokerAddress = "[::1]:9092".parse().unwrap();

        assert_eq!(address.host(), "::1");
        assert_eq!(address, BrokerAddress::new("::1", 9092).unwrap());
    }

    #[test]
    fn a_host_is_not_empty_not_too_long_and_has_no_whitespace() {
        assert_eq!(BrokerAddress::new("", 9092), Err(BrokerAddressError::EmptyHost));
        assert_eq!(
            BrokerAddress::new("k".repeat(256), 9092),
            Err(BrokerAddressError::HostTooLong { len: 256, max: 255 })
        );
        assert!(BrokerAddress::new("k".repeat(255), 9092).is_ok());
        assert_eq!(
            BrokerAddress::new("kafka 2", 9092),
            Err(BrokerAddressError::InvalidHostCharacter {
                host: "kafka 2".to_owned(),
                character: ' ',
                position: 6
            })
        );
        assert!(BrokerAddress::new("kafka\n", 9092).is_err());
    }

    #[test]
    fn a_port_is_not_zero() {
        assert_eq!(BrokerAddress::new("kafka-2", 0), Err(BrokerAddressError::PortZero));
    }

    #[test]
    fn a_text_without_a_host_and_a_port_is_malformed() {
        for text in ["", "kafka-2", ":9092", "[::1:9092", "::1:9092", "[]:9092"] {
            assert_eq!(
                text.parse::<BrokerAddress>(),
                Err(BrokerAddressError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_text_ends_in_a_port_between_one_and_the_highest() {
        for text in [
            "kafka-2:",
            "kafka-2:0",
            "kafka-2:65536",
            "kafka-2:+9092",
            "kafka-2:-1",
            "kafka-2:kafka",
        ] {
            assert_eq!(
                text.parse::<BrokerAddress>(),
                Err(BrokerAddressError::InvalidPort { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_response_writes_no_broker_as_no_address() {
        assert_eq!(
            BrokerAddress::from_wire("kafka-2.internal", 9092),
            Some(BrokerAddress::new("kafka-2.internal", 9092).unwrap())
        );
        assert_eq!(BrokerAddress::from_wire("", -1), None);
        assert_eq!(BrokerAddress::from_wire("kafka-2.internal", -1), None);
        assert_eq!(BrokerAddress::from_wire("kafka-2.internal", 0), None);
        assert_eq!(BrokerAddress::from_wire("kafka-2.internal", 65536), None);
    }

    #[test]
    fn every_error_says_what_is_wrong_in_one_message() {
        let errors = [
            (
                BrokerAddressError::EmptyHost,
                "the host of a broker address cannot be empty",
            ),
            (
                BrokerAddressError::HostTooLong { len: 256, max: 255 },
                "the host of a broker address is 256 bytes long, and the limit is 255",
            ),
            (
                BrokerAddress::new("kafka 2", 9092).unwrap_err(),
                "the host \"kafka 2\" of a broker address has the invalid character ' ' at position 6",
            ),
            (
                BrokerAddressError::PortZero,
                "the port of a broker address cannot be zero",
            ),
            (
                "kafka-2".parse::<BrokerAddress>().unwrap_err(),
                "broker address \"kafka-2\" does not have the form of a host and a port, such as kafka-2.internal:9092",
            ),
            (
                "kafka-2:0".parse::<BrokerAddress>().unwrap_err(),
                "broker address \"kafka-2:0\" does not end in a port between 1 and 65535",
            ),
        ];

        for (error, message) in errors {
            assert_eq!(error.to_string(), message);
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
