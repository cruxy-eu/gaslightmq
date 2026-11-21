//! The values of the connection fields that the kernel has no type for.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use gaslightmq_kernel::prelude::*;

use crate::FieldError;
use crate::text::{number_form, text_form};

/// The number of a connection, which is the value of the field `connection`.
///
/// The engine numbers the connections it accepts.
/// A number only lives as long as its connection: a client that reconnects gets a new one.
///
/// The text form is `#7`.
/// Reading also takes the bare number `7`, because `#` starts a comment in YAML.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let connection = ConnectionNumber::new(7);
/// assert_eq!(connection.to_string(), "#7");
/// assert_eq!("#7".parse::<ConnectionNumber>()?, connection);
/// assert_eq!("7".parse::<ConnectionNumber>()?, connection);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionNumber(u64);

impl ConnectionNumber {
    const WHAT: &'static str = "connection number";

    /// The connection with that number.
    pub const fn new(number: u64) -> Self {
        Self(number)
    }

    /// The connection number as a plain number.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl From<u64> for ConnectionNumber {
    fn from(number: u64) -> Self {
        Self::new(number)
    }
}

impl From<ConnectionNumber> for u64 {
    fn from(connection: ConnectionNumber) -> Self {
        connection.0
    }
}

/// Reads `#7` or `7`.
impl FromStr for ConnectionNumber {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text.strip_prefix('#').unwrap_or(text);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(FieldError::malformed(Self::WHAT, text, "#N"));
        }
        digits.parse().map(Self).map_err(|_| FieldError::out_of_range(Self::WHAT, text, 0, u64::MAX))
    }
}

impl fmt::Display for ConnectionNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

number_form!(ConnectionNumber, u64, "a connection number such as 7 or \"#7\"");

/// The address a client connects from, which is the value of the field `address`.
///
/// The text form is `10.0.4.27:51459`, or `[2001:db8::1]:51459` for IPv6.
///
/// An address only keeps what its text form shows, so two addresses that read the same are equal.
/// A listener on both IP versions reports an IPv4 client as `::ffff:10.0.4.27`,
/// and the address keeps that as `10.0.4.27`.
/// The flow label and the scope of an IPv6 address are dropped.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let address: ClientAddress = "10.0.4.27:51459".parse()?;
/// assert_eq!(address.host().to_string(), "10.0.4.27");
/// assert_eq!(address.port(), 51459);
///
/// let mapped: ClientAddress = "[::ffff:10.0.4.27]:51459".parse()?;
/// assert_eq!(mapped, address);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientAddress(SocketAddr);

impl ClientAddress {
    /// The address of a client, from the address of its socket.
    pub fn new(address: SocketAddr) -> Self {
        Self(SocketAddr::new(address.ip().to_canonical(), address.port()))
    }

    /// The address as the standard library holds it.
    pub const fn socket_addr(self) -> SocketAddr {
        self.0
    }

    /// The IP address of the client, without the port.
    pub fn host(self) -> ClientHost {
        ClientHost(self.0.ip())
    }

    /// The port the client connects from.
    pub const fn port(self) -> u16 {
        self.0.port()
    }
}

impl From<SocketAddr> for ClientAddress {
    fn from(address: SocketAddr) -> Self {
        Self::new(address)
    }
}

impl From<ClientAddress> for SocketAddr {
    fn from(address: ClientAddress) -> Self {
        address.0
    }
}

/// Reads an IP address and a port, such as `10.0.4.27:51459`.
impl FromStr for ClientAddress {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse::<SocketAddr>().map(Self::new).map_err(|_| {
            FieldError::malformed("client address", text, "an IP address and a port, such as 10.0.4.27:51459")
        })
    }
}

impl fmt::Display for ClientAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

text_form!(ClientAddress);

/// The IP address a client connects from, which is the value of the field `host`.
///
/// A host is an IP address and never a DNS name: it is what the socket reports.
/// A `client.id` with a host identifies one client instance across reconnects.
///
/// The text form is `10.0.4.27`, or `2001:db8::1` for IPv6.
/// A host keeps an IPv4 address that arrives mapped into IPv6 as the IPv4 address,
/// as a [`ClientAddress`] does.
///
/// ```
/// use std::net::{IpAddr, Ipv4Addr};
///
/// use gaslightmq_field::prelude::*;
///
/// let host: ClientHost = "10.0.4.27".parse()?;
/// assert_eq!(host.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 4, 27)));
/// assert_eq!("::ffff:10.0.4.27".parse::<ClientHost>()?, host);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientHost(IpAddr);

impl ClientHost {
    /// The host of a client, from the IP address of its socket.
    pub fn new(ip: IpAddr) -> Self {
        Self(ip.to_canonical())
    }

    /// The host as the standard library holds it.
    pub const fn ip(self) -> IpAddr {
        self.0
    }
}

impl From<IpAddr> for ClientHost {
    fn from(ip: IpAddr) -> Self {
        Self::new(ip)
    }
}

impl From<ClientHost> for IpAddr {
    fn from(host: ClientHost) -> Self {
        host.0
    }
}

/// Reads an IP address, such as `10.0.4.27`.
impl FromStr for ClientHost {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse::<IpAddr>()
            .map(Self::new)
            .map_err(|_| FieldError::malformed("client host", text, "an IP address, such as 10.0.4.27"))
    }
}

impl fmt::Display for ClientHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

text_form!(ClientHost);

/// The endpoint a client connected to, which is the value of the field `endpoint`.
///
/// GaslightMQ listens on one endpoint for the first connection of a client,
/// and on one endpoint for each broker of the cluster.
///
/// The text form is `bootstrap` or `broker 2`.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let endpoint: Endpoint = "broker 2".parse()?;
/// assert_eq!(endpoint, Endpoint::Broker(BrokerId::from(2)));
/// assert_eq!(endpoint.broker(), Some(BrokerId::from(2)));
/// assert_eq!(Endpoint::Bootstrap.to_string(), "bootstrap");
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Endpoint {
    /// The endpoint a client connects to first, to learn about the cluster.
    Bootstrap,
    /// The endpoint that stands for one broker of the cluster.
    Broker(BrokerId),
}

impl Endpoint {
    const BOOTSTRAP: &'static str = "bootstrap";
    const BROKER: &'static str = "broker ";

    /// The broker the endpoint stands for, if it stands for one.
    pub const fn broker(self) -> Option<BrokerId> {
        match self {
            Endpoint::Bootstrap => None,
            Endpoint::Broker(broker) => Some(broker),
        }
    }
}

impl From<BrokerId> for Endpoint {
    fn from(broker: BrokerId) -> Self {
        Endpoint::Broker(broker)
    }
}

/// Reads `bootstrap`, or `broker` and a broker ID with one space between them.
impl FromStr for Endpoint {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == Self::BOOTSTRAP {
            return Ok(Endpoint::Bootstrap);
        }
        match text.strip_prefix(Self::BROKER) {
            Some(broker) => Ok(Endpoint::Broker(broker.parse()?)),
            None => Err(FieldError::malformed("endpoint", text, "bootstrap or broker N")),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Bootstrap => f.write_str(Self::BOOTSTRAP),
            Endpoint::Broker(broker) => write!(f, "{}{broker}", Self::BROKER),
        }
    }
}

text_form!(Endpoint);

/// The client library of a connection, which is the value of the field `client.software`.
///
/// A client names its library and the version of it in its `ApiVersions` request.
/// Both have at least one character, and no whitespace or control character.
/// Kafka itself only allows letters, digits, dots and hyphens.
///
/// The text form is the name, one space and the version: `librdkafka 2.6.1`.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let software = ClientSoftware::new("apache-kafka-java", "3.8.0")?;
/// assert_eq!(software.to_string(), "apache-kafka-java 3.8.0");
///
/// let software: ClientSoftware = "librdkafka 2.6.1".parse()?;
/// assert_eq!(software.name(), "librdkafka");
/// assert_eq!(software.version(), "2.6.1");
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientSoftware {
    /// The text form.
    text: Arc<str>,
    /// The number of bytes of the name, which the text starts with.
    name_len: usize,
}

impl ClientSoftware {
    const WHAT: &'static str = "client software";
    const FORM: &'static str = "a name and a version, such as librdkafka 2.6.1";

    /// Check the name and the version, and keep them.
    pub fn new(name: impl AsRef<str>, version: impl AsRef<str>) -> Result<Self, FieldError> {
        let (name, version) = (name.as_ref(), version.as_ref());
        let text = format!("{name} {version}");
        let valid = |part: &str| !part.is_empty() && !part.chars().any(|c| c.is_whitespace() || c.is_control());
        if !valid(name) || !valid(version) {
            return Err(FieldError::malformed(Self::WHAT, &text, Self::FORM));
        }
        Ok(Self { text: text.into(), name_len: name.len() })
    }

    /// The name of the client library, such as `librdkafka`.
    pub fn name(&self) -> &str {
        self.text.split_at(self.name_len).0
    }

    /// The version of the client library, such as `2.6.1`.
    pub fn version(&self) -> &str {
        // The text is the name, one space and the version.
        self.text.split_at(self.name_len + 1).1
    }

    /// The name, one space and the version.
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

/// Reads a name and a version with one space between them.
impl FromStr for ClientSoftware {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.split_once(' ') {
            Some((name, version)) => Self::new(name, version),
            None => Err(FieldError::malformed(Self::WHAT, text, Self::FORM)),
        }
    }
}

impl fmt::Display for ClientSoftware {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.text)
    }
}

impl fmt::Debug for ClientSoftware {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientSoftware({:?})", self.as_str())
    }
}

text_form!(ClientSoftware);

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

    use super::*;

    #[test]
    fn a_connection_number_writes_a_hash_and_reads_with_or_without_it() {
        assert_eq!(ConnectionNumber::new(7).to_string(), "#7");
        assert_eq!(ConnectionNumber::from(0).to_string(), "#0");
        assert_eq!("#7".parse(), Ok(ConnectionNumber::new(7)));
        assert_eq!("7".parse(), Ok(ConnectionNumber::new(7)));
        assert_eq!("#18446744073709551615".parse(), Ok(ConnectionNumber::new(u64::MAX)));
        assert_eq!(u64::from(ConnectionNumber::new(7)), 7);
        assert_eq!(ConnectionNumber::new(7).value(), 7);
    }

    #[test]
    fn a_connection_number_is_a_whole_number() {
        for text in ["", "#", "##7", "# 7", "#-7", "-7", "#7 ", "seven", "#7.0"] {
            assert_eq!(
                text.parse::<ConnectionNumber>(),
                Err(FieldError::malformed("connection number", text, "#N")),
                "{text:?}"
            );
        }
        assert_eq!(
            "#18446744073709551616".parse::<ConnectionNumber>(),
            Err(FieldError::out_of_range("connection number", "#18446744073709551616", 0, u64::MAX))
        );
    }

    #[test]
    fn connections_order_by_number() {
        assert!(ConnectionNumber::new(7) < ConnectionNumber::new(10));
    }

    #[test]
    fn a_client_address_is_an_ip_address_and_a_port() {
        let address: ClientAddress = "10.0.4.27:51459".parse().unwrap();

        assert_eq!(address.to_string(), "10.0.4.27:51459");
        assert_eq!(address.host(), ClientHost::new(IpAddr::V4(Ipv4Addr::new(10, 0, 4, 27))));
        assert_eq!(address.port(), 51459);
        assert_eq!(SocketAddr::from(address), "10.0.4.27:51459".parse::<SocketAddr>().unwrap());
        assert_eq!(address.socket_addr(), SocketAddr::from(address));
    }

    #[test]
    fn a_client_address_takes_ipv6() {
        let address: ClientAddress = "[2001:db8::1]:51459".parse().unwrap();

        assert_eq!(address.to_string(), "[2001:db8::1]:51459");
        assert_eq!(address.host().to_string(), "2001:db8::1");
    }

    #[test]
    fn an_ipv4_address_mapped_into_ipv6_is_the_ipv4_address() {
        let mapped = SocketAddr::new(IpAddr::V6(Ipv4Addr::new(10, 0, 4, 27).to_ipv6_mapped()), 51459);

        assert_eq!(ClientAddress::new(mapped).to_string(), "10.0.4.27:51459");
        assert_eq!(ClientAddress::from(mapped), "10.0.4.27:51459".parse().unwrap());
        assert_eq!(ClientHost::from(mapped.ip()).to_string(), "10.0.4.27");
        assert_eq!("::ffff:10.0.4.27".parse::<ClientHost>(), "10.0.4.27".parse());
    }

    #[test]
    fn a_client_address_keeps_what_its_text_shows() {
        let scoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 51459, 5, 3));
        let address = ClientAddress::new(scoped);

        assert_eq!(address.to_string(), "[::1]:51459");
        assert_eq!(address.to_string().parse(), Ok(address));
    }

    #[test]
    fn a_client_address_needs_an_ip_address_and_a_port() {
        for text in ["", "10.0.4.27", "10.0.4.27:", ":51459", "localhost:9092", "10.0.4.27:99999", "2001:db8::1:51459x"]
        {
            assert_eq!(
                text.parse::<ClientAddress>(),
                Err(FieldError::malformed("client address", text, "an IP address and a port, such as 10.0.4.27:51459")),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_client_host_is_an_ip_address() {
        let host: ClientHost = "10.0.4.27".parse().unwrap();

        assert_eq!(host.to_string(), "10.0.4.27");
        assert_eq!(IpAddr::from(host), IpAddr::V4(Ipv4Addr::new(10, 0, 4, 27)));
        for text in ["", "localhost", "10.0.4", "10.0.4.27:51459", "10.0.4.*"] {
            assert_eq!(
                text.parse::<ClientHost>(),
                Err(FieldError::malformed("client host", text, "an IP address, such as 10.0.4.27")),
                "{text:?}"
            );
        }
    }

    #[test]
    fn an_endpoint_is_the_bootstrap_or_a_broker() {
        assert_eq!("bootstrap".parse(), Ok(Endpoint::Bootstrap));
        assert_eq!("broker 2".parse(), Ok(Endpoint::Broker(BrokerId::from(2))));
        assert_eq!(Endpoint::Bootstrap.to_string(), "bootstrap");
        assert_eq!(Endpoint::from(BrokerId::from(2)).to_string(), "broker 2");
        assert_eq!(Endpoint::Bootstrap.broker(), None);
        assert_eq!(Endpoint::Broker(BrokerId::from(2)).broker(), Some(BrokerId::from(2)));
    }

    #[test]
    fn an_endpoint_has_one_of_two_forms() {
        for text in ["", "Bootstrap", "broker", "broker2", "2", "bootstrap ", "Broker 2"] {
            assert_eq!(
                text.parse::<Endpoint>(),
                Err(FieldError::malformed("endpoint", text, "bootstrap or broker N")),
                "{text:?}"
            );
        }
    }

    #[test]
    fn the_broker_of_an_endpoint_is_a_broker_id() {
        assert_eq!(
            "broker -1".parse::<Endpoint>(),
            Err(FieldError::Kernel(KernelError::Negative { what: "broker ID", value: -1 }))
        );
        assert_eq!(
            "broker  2".parse::<Endpoint>(),
            Err(FieldError::Kernel(KernelError::NotANumber { what: "broker ID", input: " 2".to_owned() }))
        );
    }

    #[test]
    fn the_bootstrap_endpoint_comes_before_the_brokers() {
        let mut endpoints =
            [Endpoint::Broker(BrokerId::from(2)), Endpoint::Bootstrap, Endpoint::Broker(BrokerId::from(1))];
        endpoints.sort();

        assert_eq!(endpoints.map(|endpoint| endpoint.to_string()), ["bootstrap", "broker 1", "broker 2"]);
    }

    #[test]
    fn client_software_is_a_name_and_a_version() {
        let software = ClientSoftware::new("librdkafka", "2.6.1").unwrap();

        assert_eq!(software.name(), "librdkafka");
        assert_eq!(software.version(), "2.6.1");
        assert_eq!(software.as_str(), "librdkafka 2.6.1");
        assert_eq!(software.to_string(), "librdkafka 2.6.1");
        assert_eq!("librdkafka 2.6.1".parse(), Ok(software.clone()));
        assert_eq!(format!("{software:?}"), "ClientSoftware(\"librdkafka 2.6.1\")");
    }

    #[test]
    fn client_software_needs_a_name_and_a_version_without_whitespace() {
        let malformed = |text: &str| {
            Err(FieldError::malformed("client software", text, "a name and a version, such as librdkafka 2.6.1"))
        };

        for text in ["", "librdkafka", " 2.6.1", "librdkafka ", "librdkafka  2.6.1", "librdkafka 2.6.1 beta", "a\tb c"]
        {
            assert_eq!(text.parse::<ClientSoftware>(), malformed(text), "{text:?}");
        }
        assert_eq!(ClientSoftware::new("lib rdkafka", "2.6.1"), malformed("lib rdkafka 2.6.1"));
        assert_eq!(ClientSoftware::new("librdkafka", ""), malformed("librdkafka "));
        assert_eq!(ClientSoftware::new("librdkafka", "2.6\u{0}"), malformed("librdkafka 2.6\u{0}"));
    }

    #[test]
    fn client_software_orders_by_its_text() {
        let java = ClientSoftware::new("apache-kafka-java", "3.8.0").unwrap();
        let rdkafka = ClientSoftware::new("librdkafka", "2.6.1").unwrap();
        let older = ClientSoftware::new("librdkafka", "2.5.0").unwrap();

        assert!(java < older && older < rdkafka);
    }
}
