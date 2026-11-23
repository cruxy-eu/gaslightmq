use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use crate::host::{HostName, HostNameError};
use crate::text::text_form;

/// Where a server answers: a host and a port, such as `kafka-1.staging:9092`.
///
/// It is the address the proxy connects to, such as the bootstrap server of a real cluster.
/// The port is between 1 and 65535.
/// An IPv6 address stands in brackets: `[2001:db8::7]:9092`.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let bootstrap: ServerAddress = "kafka-1.staging:9092".parse()?;
/// assert_eq!(bootstrap.host(), &HostName::new("kafka-1.staging").expect("a host name"));
/// assert_eq!(bootstrap.port(), 9092);
/// assert_eq!(bootstrap.to_string(), "kafka-1.staging:9092");
///
/// assert!("kafka-1.staging".parse::<ServerAddress>().is_err());
/// # Ok::<(), ServerAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServerAddress {
    host: HostName,
    port: u16,
}

impl ServerAddress {
    /// The address of the server on `host` that answers on `port`.
    ///
    /// # Errors
    ///
    /// [`ServerAddressError::InvalidPort`] for port zero, which no server answers on.
    pub fn new(host: HostName, port: u16) -> Result<Self, ServerAddressError> {
        let address = Self { host, port };
        if port == 0 {
            return Err(ServerAddressError::InvalidPort {
                input: address.to_string(),
            });
        }
        Ok(address)
    }

    /// The host of the server.
    pub fn host(&self) -> &HostName {
        &self.host
    }

    /// The port the server answers on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl FromStr for ServerAddress {
    type Err = ServerAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let input = || text.to_owned();
        let (host, Some(port)) = split_port(text) else {
            return Err(ServerAddressError::MissingPort { input: input() });
        };
        let host = host_of_address(host).map_err(|error| ServerAddressError::InvalidHost { input: input(), error })?;
        let port = connect_port(port).ok_or_else(|| ServerAddressError::InvalidPort { input: input() })?;
        Ok(Self { host, port })
    }
}

impl fmt::Display for ServerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_host(f, &self.host)?;
        write!(f, ":{}", self.port)
    }
}

text_form!(ServerAddress, ServerAddressError);

/// Why a text is not a [`ServerAddress`].
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let error = "kafka 1:9092".parse::<ServerAddress>().unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "server address \"kafka 1:9092\": host name \"kafka 1\" holds the character ' '"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServerAddressError {
    /// The text names a host and no port.
    #[error("server address {input:?} has no port: write host:port")]
    MissingPort { input: String },
    /// The port is not a number between 1 and 65535.
    #[error("server address {input:?} has a port that is no number between 1 and 65535")]
    InvalidPort { input: String },
    /// The part before the port is not a host.
    #[error("server address {input:?}: {error}")]
    InvalidHost { input: String, error: HostNameError },
}

/// Where an instance listens: an IP address and a port, such as `0.0.0.0:9092`.
///
/// `0.0.0.0` stands for every IPv4 address of the machine, and `::` for every address, IPv4 and IPv6.
/// An IPv6 address stands in brackets: `[::1]:9092`.
///
/// Port zero asks the system for any free port.
/// The instance learns the port when it starts to listen, so such an address [shares](Self::shares) no port with
/// another one.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let bootstrap: ListenAddress = "0.0.0.0:9092".parse()?;
/// assert_eq!(bootstrap.port(), 9092);
/// assert!(bootstrap.ip().is_unspecified());
///
/// // A name is not an address to listen on.
/// assert!("localhost:9092".parse::<ListenAddress>().is_err());
/// # Ok::<(), ListenAddressError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListenAddress(SocketAddr);

impl ListenAddress {
    /// The address `ip` with `port`.
    pub fn new(ip: IpAddr, port: u16) -> Self {
        Self(SocketAddr::new(ip, port))
    }

    /// The IP address.
    pub fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    /// The port, or zero for any free port.
    pub fn port(&self) -> u16 {
        self.0.port()
    }

    /// Whether the system picks the port, because the address names port zero.
    pub fn takes_any_port(&self) -> bool {
        self.0.port() == 0
    }

    /// Whether a listener on this address and one on `other` would take the same port of the same address.
    ///
    /// Both name the same port, and they name the same IP address, or one of them names every address that the
    /// other can be: `::` names every address, and `0.0.0.0` every IPv4 address.
    /// An IPv4 address that is written as an IPv6 address, such as `::ffff:10.0.4.27`, is that IPv4 address.
    /// An address that [takes any port](Self::takes_any_port) shares nothing.
    ///
    /// ```
    /// use gaslightmq_environment::prelude::*;
    ///
    /// let every: ListenAddress = "0.0.0.0:9092".parse()?;
    /// let local: ListenAddress = "127.0.0.1:9092".parse()?;
    /// let other: ListenAddress = "10.0.4.27:9092".parse()?;
    ///
    /// assert!(every.shares(&local));
    /// assert!(!local.shares(&other));
    /// # Ok::<(), ListenAddressError>(())
    /// ```
    pub fn shares(&self, other: &ListenAddress) -> bool {
        !self.takes_any_port() && self.port() == other.port() && same_ip(self.ip(), other.ip())
    }
}

/// Whether two listeners on these IP addresses can be on the same address: the addresses are one,
/// or one of them stands for every address that the other can be.
///
/// `::` stands for every address, and `0.0.0.0` for every IPv4 address and for no IPv6 address.
/// An IPv4 address that is written as an IPv6 address, such as `::ffff:10.0.4.27`, is that IPv4 address.
pub(crate) fn same_ip(one: IpAddr, other: IpAddr) -> bool {
    match (one.to_canonical(), other.to_canonical()) {
        (one, other) if one == other => true,
        (IpAddr::V6(every), _) | (_, IpAddr::V6(every)) if every.is_unspecified() => true,
        (IpAddr::V4(one), IpAddr::V4(other)) => one.is_unspecified() || other.is_unspecified(),
        _ => false,
    }
}

impl From<SocketAddr> for ListenAddress {
    fn from(address: SocketAddr) -> Self {
        Self(address)
    }
}

impl From<ListenAddress> for SocketAddr {
    fn from(address: ListenAddress) -> Self {
        address.0
    }
}

impl FromStr for ListenAddress {
    type Err = ListenAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse()
            .map(Self)
            .map_err(|_| ListenAddressError::NotAnAddress { input: text.to_owned() })
    }
}

impl fmt::Display for ListenAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

text_form!(ListenAddress, ListenAddressError);

/// Why a text is not a [`ListenAddress`].
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let error = "localhost:9092".parse::<ListenAddress>().unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "listen address \"localhost:9092\" is not an IP address and a port, such as 0.0.0.0:9092"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListenAddressError {
    /// The text is not an IP address, a colon and a port.
    #[error("listen address {input:?} is not an IP address and a port, such as 0.0.0.0:9092")]
    NotAnAddress { input: String },
}

/// Splits `host:port` into the host and the port, at the last colon outside the brackets of an IPv6 address.
pub(crate) fn split_port(text: &str) -> (&str, Option<&str>) {
    if text.ends_with(']') {
        return (text, None);
    }
    match text.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (text, None),
    }
}

/// The host of an address, in which an IPv6 address stands in brackets.
pub(crate) fn host_of_address(text: &str) -> Result<HostName, HostNameError> {
    let bracketed = text.strip_prefix('[').and_then(|rest| rest.strip_suffix(']'));
    if let Some(ip) = bracketed.and_then(|inner| inner.parse::<Ipv6Addr>().ok()) {
        return Ok(HostName::from(IpAddr::V6(ip)));
    }
    if text.contains(':') {
        // Without brackets, nobody can tell the last part of an IPv6 address from a port.
        return Err(HostNameError::InvalidCharacter {
            input: text.to_owned(),
            character: ':',
        });
    }
    HostName::new(text)
}

/// A port to connect to or to listen on: digits that make a number between 1 and 65535.
pub(crate) fn connect_port(text: &str) -> Option<u16> {
    let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    text.parse().ok().filter(|port| digits && *port != 0)
}

/// Writes a host as an address has it: an IPv6 address in brackets.
pub(crate) fn write_host(f: &mut fmt::Formatter<'_>, host: &HostName) -> fmt::Result {
    match host.ip() {
        Some(IpAddr::V6(ip)) => write!(f, "[{ip}]"),
        _ => write!(f, "{host}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(text: &str) -> HostName {
        HostName::new(text).unwrap()
    }

    #[test]
    fn a_server_address_is_a_host_and_a_port() {
        for (text, name, port) in [
            ("kafka-1.staging:9092", "kafka-1.staging", 9092),
            ("localhost:1", "localhost", 1),
            ("10.0.4.27:65535", "10.0.4.27", 65535),
            ("[2001:db8::7]:9092", "2001:db8::7", 9092),
        ] {
            let address: ServerAddress = text.parse().unwrap();
            assert_eq!(address.host(), &host(name));
            assert_eq!(address.port(), port);
            assert_eq!(address.to_string(), text);
            assert_eq!(ServerAddress::new(host(name), port), Ok(address));
        }
    }

    #[test]
    fn one_ipv6_host_is_one_value_however_it_is_written() {
        let short: ServerAddress = "[2001:db8::7]:9092".parse().unwrap();
        let long: ServerAddress = "[2001:0DB8:0:0:0:0:0:7]:9092".parse().unwrap();

        assert_eq!(long, short);
        assert_eq!(long.to_string(), "[2001:db8::7]:9092");
        // The host of an address is the host that the same text makes alone.
        assert_eq!(long.host(), &host("2001:0DB8:0:0:0:0:0:7"));
        assert_eq!(ServerAddress::new(host("2001:0db8::7"), 9092), Ok(short));
    }

    #[test]
    fn a_server_address_needs_a_host_and_a_port() {
        let input = |text: &str| text.to_owned();
        let parse = |text: &str| text.parse::<ServerAddress>();

        assert_eq!(
            parse("kafka-1.staging"),
            Err(ServerAddressError::MissingPort {
                input: input("kafka-1.staging")
            })
        );
        assert_eq!(
            parse("[::1]"),
            Err(ServerAddressError::MissingPort { input: input("[::1]") })
        );
        for text in [
            "kafka:0",
            "kafka:",
            "kafka:65536",
            "kafka:+80",
            "kafka:kafka",
            "kafka:-1",
        ] {
            assert_eq!(
                parse(text),
                Err(ServerAddressError::InvalidPort { input: input(text) }),
                "{text}"
            );
        }
        assert_eq!(
            ServerAddress::new(host("kafka"), 0),
            Err(ServerAddressError::InvalidPort {
                input: input("kafka:0")
            })
        );
        assert_eq!(
            parse(":9092"),
            Err(ServerAddressError::InvalidHost {
                input: input(":9092"),
                error: HostNameError::Empty
            })
        );
        // An IPv6 address without brackets: its last part could be the port.
        assert_eq!(
            parse("2001:db8::7:9092"),
            Err(ServerAddressError::InvalidHost {
                input: input("2001:db8::7:9092"),
                error: HostNameError::InvalidCharacter {
                    input: input("2001:db8::7"),
                    character: ':'
                }
            })
        );
        // Brackets are for an IPv6 address alone.
        assert!(matches!(
            parse("[kafka]:9092"),
            Err(ServerAddressError::InvalidHost { .. })
        ));
    }

    #[test]
    fn the_error_of_a_server_address_names_the_whole_text() {
        let message = |text: &str| text.parse::<ServerAddress>().unwrap_err().to_string();

        assert_eq!(
            message("kafka-1"),
            "server address \"kafka-1\" has no port: write host:port"
        );
        assert_eq!(
            message("kafka-1:0"),
            "server address \"kafka-1:0\" has a port that is no number between 1 and 65535"
        );
        assert_eq!(
            message("kafka/1:9092"),
            "server address \"kafka/1:9092\": host name \"kafka/1\" holds the character '/'"
        );
    }

    #[test]
    fn a_listen_address_is_an_ip_address_and_a_port() {
        for text in ["0.0.0.0:9092", "127.0.0.1:0", "[::]:9092", "[::1]:19092"] {
            let address: ListenAddress = text.parse().unwrap();
            assert_eq!(address.to_string(), text);
            assert_eq!(ListenAddress::new(address.ip(), address.port()), address);
            assert_eq!(ListenAddress::from(SocketAddr::from(address)), address);
        }
        for text in ["localhost:9092", "0.0.0.0", "9092", "", "0.0.0.0:65536", "::1:9092"] {
            assert_eq!(
                text.parse::<ListenAddress>(),
                Err(ListenAddressError::NotAnAddress { input: text.to_owned() }),
                "{text}"
            );
        }
    }

    #[test]
    fn listen_addresses_share_a_port_of_an_address() {
        let address = |text: &str| text.parse::<ListenAddress>().unwrap();
        let shares = |one: &str, other: &str| {
            let shares = address(one).shares(&address(other));
            assert_eq!(address(other).shares(&address(one)), shares, "{one} and {other}");
            shares
        };

        assert!(shares("127.0.0.1:9092", "127.0.0.1:9092"));
        assert!(shares("0.0.0.0:9092", "127.0.0.1:9092"));
        assert!(shares("[::]:9092", "127.0.0.1:9092"));
        assert!(shares("0.0.0.0:9092", "0.0.0.0:9092"));
        assert!(!shares("127.0.0.1:9092", "10.0.4.27:9092"));
        assert!(!shares("0.0.0.0:9092", "0.0.0.0:9093"));
    }

    #[test]
    fn every_ipv4_address_is_no_ipv6_address() {
        let address = |text: &str| text.parse::<ListenAddress>().unwrap();
        let shares = |one: &str, other: &str| {
            let shares = address(one).shares(&address(other));
            assert_eq!(address(other).shares(&address(one)), shares, "{one} and {other}");
            shares
        };

        // `0.0.0.0` stands for every IPv4 address, and for no IPv6 address.
        assert!(shares("0.0.0.0:9092", "10.0.4.27:9092"));
        assert!(!shares("0.0.0.0:9092", "[::1]:9092"));
        assert!(!shares("0.0.0.0:9092", "[2001:db8::7]:9092"));
        assert!(!shares("127.0.0.1:9092", "[::1]:9092"));
        // `::` stands for every address of both families.
        assert!(shares("[::]:9092", "[::1]:9092"));
        assert!(shares("[::]:9092", "10.0.4.27:9092"));
        assert!(shares("[::]:9092", "0.0.0.0:9092"));
        assert!(shares("[::]:9092", "[::]:9092"));
    }

    #[test]
    fn an_ipv4_address_inside_an_ipv6_address_is_that_ipv4_address() {
        let address = |text: &str| text.parse::<ListenAddress>().unwrap();
        let shares = |one: &str, other: &str| {
            let shares = address(one).shares(&address(other));
            assert_eq!(address(other).shares(&address(one)), shares, "{one} and {other}");
            shares
        };

        assert!(shares("[::ffff:127.0.0.1]:9092", "127.0.0.1:9092"));
        assert!(!shares("[::ffff:127.0.0.1]:9092", "10.0.4.27:9092"));
        assert!(shares("[::ffff:127.0.0.1]:9092", "0.0.0.0:9092"));
        // Every IPv4 address, written as an IPv6 address: it still stands for no IPv6 address.
        assert!(shares("[::ffff:0.0.0.0]:9092", "10.0.4.27:9092"));
        assert!(!shares("[::ffff:0.0.0.0]:9092", "[::1]:9092"));
    }

    #[test]
    fn an_address_that_takes_any_port_shares_none() {
        let any: ListenAddress = "127.0.0.1:0".parse().unwrap();

        assert!(any.takes_any_port());
        assert!(!any.shares(&any));
        assert!(!"127.0.0.1:9092".parse::<ListenAddress>().unwrap().takes_any_port());
    }
}
