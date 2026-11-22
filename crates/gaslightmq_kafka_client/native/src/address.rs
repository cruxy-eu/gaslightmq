//! Where a broker listens, and the addresses a client starts from.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

/// The host and port a broker listens on, written as `host:port`.
///
/// The host is a name or an IP address, and it is never empty.
/// An IPv6 address stands in brackets in the text form: `[::1]:9092`.
///
/// ```
/// use gaslightmq_kafka_client_native::BrokerAddress;
///
/// let address: BrokerAddress = "kafka-1.internal:9092".parse()?;
/// assert_eq!(address.host(), "kafka-1.internal");
/// assert_eq!(address.port(), 9092);
/// assert_eq!(address.to_string(), "kafka-1.internal:9092");
/// # Ok::<(), gaslightmq_kafka_client_native::BrokerAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BrokerAddress {
    host: String,
    port: u16,
}

impl BrokerAddress {
    /// Keep `host` and `port` as the address of a broker.
    /// An IPv6 address may stand in brackets, as a broker may advertise it: `[::1]` is the host `::1`.
    ///
    /// # Errors
    ///
    /// [`BrokerAddressError::MissingHost`] when `host` is empty.
    pub fn new(host: impl AsRef<str>, port: u16) -> Result<Self, BrokerAddressError> {
        let host = without_brackets(host.as_ref());
        if host.is_empty() {
            return Err(BrokerAddressError::MissingHost {
                input: format!(":{port}"),
            });
        }
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }

    /// The name or IP address, without brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl From<SocketAddr> for BrokerAddress {
    fn from(address: SocketAddr) -> Self {
        Self {
            host: address.ip().to_string(),
            port: address.port(),
        }
    }
}

impl FromStr for BrokerAddress {
    type Err = BrokerAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let input = || text.to_owned();
        let (host, port) = if let Some(bracketed) = text.strip_prefix('[') {
            let (host, rest) = bracketed
                .split_once(']')
                .ok_or_else(|| BrokerAddressError::MissingBrackets { input: input() })?;
            let port = rest
                .strip_prefix(':')
                .ok_or_else(|| BrokerAddressError::MissingPort { input: input() })?;
            (host, port)
        } else {
            let (host, port) = text
                .rsplit_once(':')
                .ok_or_else(|| BrokerAddressError::MissingPort { input: input() })?;
            if host.contains(':') {
                return Err(BrokerAddressError::MissingBrackets { input: input() });
            }
            (host, port)
        };
        if host.is_empty() {
            return Err(BrokerAddressError::MissingHost { input: input() });
        }
        let port = port
            .parse()
            .map_err(|_| BrokerAddressError::InvalidPort { input: input() })?;
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

/// `host` without the pair of brackets that an IPv6 address may stand in.
fn without_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

impl fmt::Display for BrokerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Why a text is not a [`BrokerAddress`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BrokerAddressError {
    /// The text has no `:` between a host and a port.
    MissingPort { input: String },
    /// Nothing stands before the port.
    MissingHost { input: String },
    /// The host holds a `:`, as an IPv6 address does, and does not stand in a pair of brackets.
    MissingBrackets { input: String },
    /// The port is not a number from 0 to 65535.
    InvalidPort { input: String },
}

impl fmt::Display for BrokerAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::MissingPort { input }
        | Self::MissingHost { input }
        | Self::MissingBrackets { input }
        | Self::InvalidPort { input }) = self;
        write!(f, "invalid broker address `{input}`: ")?;
        match self {
            Self::MissingPort { .. } => f.write_str("missing port, expected host:port"),
            Self::MissingHost { .. } => f.write_str("missing host, expected host:port"),
            Self::MissingBrackets { .. } => f.write_str("a host with `:` in it stands in brackets, as in [::1]:9092"),
            Self::InvalidPort { .. } => f.write_str("the port is not a number from 0 to 65535"),
        }
    }
}

impl std::error::Error for BrokerAddressError {}

/// The addresses a client asks first, to learn which brokers the cluster has.
///
/// There is always at least one.
/// The text form is the one of Kafka's `bootstrap.servers`: addresses with commas between them.
///
/// ```
/// use gaslightmq_kafka_client_native::BootstrapServers;
///
/// let bootstrap: BootstrapServers = "kafka-1:9092, kafka-2:9092".parse()?;
/// assert_eq!(bootstrap.iter().count(), 2);
/// assert_eq!(bootstrap.to_string(), "kafka-1:9092,kafka-2:9092");
/// # Ok::<(), gaslightmq_kafka_client_native::BootstrapServersError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapServers {
    first: BrokerAddress,
    others: Vec<BrokerAddress>,
}

impl BootstrapServers {
    /// Keep `addresses`, in their order.
    ///
    /// # Errors
    ///
    /// [`BootstrapServersError::Empty`] when there is no address.
    pub fn new(addresses: impl IntoIterator<Item = BrokerAddress>) -> Result<Self, BootstrapServersError> {
        let mut addresses = addresses.into_iter();
        let first = addresses.next().ok_or(BootstrapServersError::Empty)?;
        Ok(Self {
            first,
            others: addresses.collect(),
        })
    }

    /// The addresses, in the order they were given.
    pub fn iter(&self) -> impl Iterator<Item = &BrokerAddress> {
        std::iter::once(&self.first).chain(&self.others)
    }

    pub(crate) fn first(&self) -> &BrokerAddress {
        &self.first
    }
}

impl From<BrokerAddress> for BootstrapServers {
    fn from(address: BrokerAddress) -> Self {
        Self {
            first: address,
            others: Vec::new(),
        }
    }
}

impl From<SocketAddr> for BootstrapServers {
    fn from(address: SocketAddr) -> Self {
        Self::from(BrokerAddress::from(address))
    }
}

impl FromStr for BootstrapServers {
    type Err = BootstrapServersError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let addresses = text
            .split(',')
            .map(str::trim)
            .filter(|address| !address.is_empty())
            .map(|address| {
                address.parse().map_err(|error| BootstrapServersError::InvalidAddress {
                    input: text.to_owned(),
                    error,
                })
            })
            .collect::<Result<Vec<BrokerAddress>, _>>()?;
        Self::new(addresses)
    }
}

impl fmt::Display for BootstrapServers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.first)?;
        self.others.iter().try_for_each(|address| write!(f, ",{address}"))
    }
}

/// Why a list is not a [`BootstrapServers`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BootstrapServersError {
    /// The list holds no address.
    Empty,
    /// One address of the text `input` is not a broker address.
    InvalidAddress { input: String, error: BrokerAddressError },
}

impl fmt::Display for BootstrapServersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("bootstrap servers need at least one address"),
            Self::InvalidAddress { input, error } => write!(f, "bootstrap servers `{input}`: {error}"),
        }
    }
}

/// The message names the address that is wrong, so the error has no source.
impl std::error::Error for BootstrapServersError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn an_address_reads_a_host_and_a_port() {
        let address: BrokerAddress = "localhost:9092".parse().unwrap();

        assert_eq!(address, BrokerAddress::new("localhost", 9092).unwrap());
        assert_eq!(address.to_string(), "localhost:9092");
    }

    #[test]
    fn an_ipv6_address_stands_in_brackets() {
        let address: BrokerAddress = "[::1]:9092".parse().unwrap();

        assert_eq!(address.host(), "::1");
        assert_eq!(address.to_string(), "[::1]:9092");
        assert_eq!(
            BrokerAddress::from("[::1]:9092".parse::<SocketAddr>().unwrap()),
            address
        );
    }

    #[test]
    fn a_host_in_brackets_is_the_same_host_however_the_address_is_built() {
        let parsed: BrokerAddress = "[::1]:9092".parse().unwrap();
        let built = BrokerAddress::new("[::1]", 9092).unwrap();

        assert_eq!(built, parsed);
        assert_eq!(built, BrokerAddress::new("::1", 9092).unwrap());
        assert_eq!(built.host(), "::1");
        assert_eq!(built.to_string(), "[::1]:9092");
        assert_eq!(
            BrokerAddress::new("[]", 9092),
            Err(BrokerAddressError::MissingHost { input: ":9092".into() })
        );
    }

    #[test]
    fn an_ipv6_address_without_brackets_is_not_an_address() {
        let message = |text: &str| text.parse::<BrokerAddress>().unwrap_err().to_string();

        assert_eq!(
            message("::1:9092"),
            "invalid broker address `::1:9092`: a host with `:` in it stands in brackets, as in [::1]:9092"
        );
        assert_eq!(
            "[::1:9092".parse::<BrokerAddress>(),
            Err(BrokerAddressError::MissingBrackets {
                input: "[::1:9092".into()
            })
        );
        assert_eq!(
            message("[::1]"),
            "invalid broker address `[::1]`: missing port, expected host:port"
        );
    }

    #[test]
    fn an_address_says_what_is_wrong_with_its_text() {
        let message = |text: &str| text.parse::<BrokerAddress>().unwrap_err().to_string();

        assert_eq!(
            message("localhost"),
            "invalid broker address `localhost`: missing port, expected host:port"
        );
        assert_eq!(
            message(":9092"),
            "invalid broker address `:9092`: missing host, expected host:port"
        );
        assert_eq!(
            message("localhost:kafka"),
            "invalid broker address `localhost:kafka`: the port is not a number from 0 to 65535"
        );
        assert_eq!(
            BrokerAddress::new("", 9092),
            Err(BrokerAddressError::MissingHost { input: ":9092".into() })
        );
    }

    #[test]
    fn bootstrap_addresses_keep_their_order() {
        let bootstrap: BootstrapServers = "b:2,a:1".parse().unwrap();

        let hosts: Vec<&str> = bootstrap.iter().map(BrokerAddress::host).collect();
        assert_eq!(hosts, ["b", "a"]);
        assert_eq!(bootstrap.first().host(), "b");
    }

    #[test]
    fn bootstrap_addresses_need_one_address() {
        assert_eq!(BootstrapServers::new([]), Err(BootstrapServersError::Empty));
        assert_eq!("".parse::<BootstrapServers>(), Err(BootstrapServersError::Empty));
        assert_eq!(" , ".parse::<BootstrapServers>(), Err(BootstrapServersError::Empty));
    }

    #[test]
    fn bootstrap_addresses_name_the_address_that_is_wrong_once() {
        let error = "a:1,b".parse::<BootstrapServers>().unwrap_err();

        assert_eq!(
            error.to_string(),
            "bootstrap servers `a:1,b`: invalid broker address `b`: missing port, expected host:port"
        );
        assert!(error.source().is_none());
    }
}
