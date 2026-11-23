use std::fmt;
use std::net::IpAddr;
use std::ops::RangeInclusive;
use std::str::FromStr;

use crate::address::{ListenAddress, connect_port, same_ip};
use crate::text::text_form;

/// Ports that follow each other, such as `19092-19191`, or one port, such as `19092`.
///
/// Every port is between 1 and 65535, and the range has at least one port.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let ports: PortRange = "19092-19191".parse()?;
/// assert_eq!((ports.first(), ports.last()), (19092, 19191));
/// assert_eq!(ports.count(), 100);
/// assert!(ports.contains(19100));
///
/// let one: PortRange = "19092".parse()?;
/// assert_eq!(one, PortRange::new(19092, 19092)?);
/// assert_eq!(one.to_string(), "19092");
/// # Ok::<(), PortRangeError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortRange {
    first: u16,
    last: u16,
}

impl PortRange {
    /// The ports from `first` to `last`, both included.
    ///
    /// # Errors
    ///
    /// [`PortRangeError::InvalidPort`] for port zero, and [`PortRangeError::Backwards`] when `last` is below `first`.
    pub fn new(first: u16, last: u16) -> Result<Self, PortRangeError> {
        if first == 0 || last == 0 {
            return Err(PortRangeError::InvalidPort {
                input: format!("{first}-{last}"),
            });
        }
        if last < first {
            return Err(PortRangeError::Backwards { first, last });
        }
        Ok(Self { first, last })
    }

    /// The lowest port.
    pub fn first(&self) -> u16 {
        self.first
    }

    /// The highest port.
    pub fn last(&self) -> u16 {
        self.last
    }

    /// How many ports the range has, which is at least one.
    pub fn count(&self) -> usize {
        usize::from(self.last - self.first) + 1
    }

    /// Whether the range holds this port.
    pub fn contains(&self, port: u16) -> bool {
        (self.first..=self.last).contains(&port)
    }

    /// The ports from the lowest to the highest.
    pub fn ports(&self) -> RangeInclusive<u16> {
        self.first..=self.last
    }

    /// The lowest port that this range and `other` both hold.
    pub fn first_shared(&self, other: &PortRange) -> Option<u16> {
        let port = self.first.max(other.first);
        (port <= self.last.min(other.last)).then_some(port)
    }
}

impl FromStr for PortRange {
    type Err = PortRangeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || PortRangeError::InvalidPort { input: text.to_owned() };
        let (first, last) = text.split_once('-').unwrap_or((text, text));
        let first = connect_port(first).ok_or_else(invalid)?;
        let last = connect_port(last).ok_or_else(invalid)?;
        Self::new(first, last)
    }
}

impl fmt::Display for PortRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.first == self.last {
            write!(f, "{}", self.first)
        } else {
            write!(f, "{}-{}", self.first, self.last)
        }
    }
}

text_form!(PortRange, PortRangeError);

/// Why ports are not a [`PortRange`].
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let error = "19191-19092".parse::<PortRange>().unwrap_err();
/// assert_eq!(error, PortRangeError::Backwards { first: 19191, last: 19092 });
/// assert_eq!(error.to_string(), "port range 19191-19092 ends before it starts");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortRangeError {
    /// A port is not a number between 1 and 65535.
    InvalidPort { input: String },
    /// The last port is below the first.
    Backwards { first: u16, last: u16 },
}

impl fmt::Display for PortRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPort { input } => write!(
                f,
                "port range {input:?} has a port that is no number between 1 and 65535"
            ),
            Self::Backwards { first, last } => write!(f, "port range {first}-{last} ends before it starts"),
        }
    }
}

impl std::error::Error for PortRangeError {}

/// The ports that listeners take on one IP address.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Taken {
    ip: IpAddr,
    ports: PortRange,
}

impl Taken {
    /// The ports of a range on one address.
    pub(crate) fn range(ip: IpAddr, ports: PortRange) -> Self {
        Self { ip, ports }
    }

    /// The port of one address, or nothing when the system picks the port.
    pub(crate) fn one(address: ListenAddress) -> Option<Self> {
        let ports = PortRange::new(address.port(), address.port()).ok()?;
        Some(Self::range(address.ip(), ports))
    }

    /// The first address that both would listen on.
    fn shared(&self, other: &Taken) -> Option<ListenAddress> {
        let port = self.ports.first_shared(&other.ports)?;
        let ip = if self.ip.is_unspecified() { other.ip } else { self.ip };
        same_ip(self.ip, other.ip).then(|| ListenAddress::new(ip, port))
    }

    /// The first address that a listener of `one` and a listener of `other` would both listen on.
    pub(crate) fn first_shared(one: &[Taken], other: &[Taken]) -> Option<ListenAddress> {
        one.iter()
            .find_map(|taken| other.iter().find_map(|other| taken.shared(other)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ports(text: &str) -> PortRange {
        text.parse().unwrap()
    }

    #[test]
    fn a_range_runs_from_its_first_port_to_its_last() {
        let range = ports("19092-19094");

        assert_eq!(range, PortRange::new(19092, 19094).unwrap());
        assert_eq!(range.ports().collect::<Vec<_>>(), [19092, 19093, 19094]);
        assert_eq!(range.count(), 3);
        assert!(range.contains(19092) && range.contains(19094));
        assert!(!range.contains(19091) && !range.contains(19095));
        assert_eq!(range.to_string(), "19092-19094");
    }

    #[test]
    fn one_port_is_a_range() {
        let range = ports("19092");

        assert_eq!(range, ports("19092-19092"));
        assert_eq!(range.count(), 1);
        assert_eq!(range.to_string(), "19092");
        assert_eq!(ports("1-65535").count(), 65535);
    }

    #[test]
    fn ports_that_make_no_range_are_an_error() {
        for text in [
            "", "0", "0-5", "5-0", "65536", "19092-", "-19092", "a-b", "1-2-3", "+1", " 1",
        ] {
            assert_eq!(
                text.parse::<PortRange>(),
                Err(PortRangeError::InvalidPort { input: text.to_owned() }),
                "{text:?}"
            );
        }
        assert_eq!(
            "19094-19092".parse::<PortRange>(),
            Err(PortRangeError::Backwards {
                first: 19094,
                last: 19092
            })
        );
        assert_eq!(
            PortRange::new(0, 5),
            Err(PortRangeError::InvalidPort {
                input: "0-5".to_owned()
            })
        );
        assert_eq!(
            PortRange::new(0, 5).unwrap_err().to_string(),
            "port range \"0-5\" has a port that is no number between 1 and 65535"
        );
    }

    #[test]
    fn two_ranges_share_their_lowest_common_port() {
        let shared = |one: &str, other: &str| {
            let shared = ports(one).first_shared(&ports(other));
            assert_eq!(ports(other).first_shared(&ports(one)), shared);
            shared
        };

        assert_eq!(shared("19092-19191", "19191-19200"), Some(19191));
        assert_eq!(shared("19092-19191", "19100"), Some(19100));
        assert_eq!(shared("19092-19191", "9092-29092"), Some(19092));
        assert_eq!(shared("19092-19191", "19192-19200"), None);
        assert_eq!(shared("9092", "9093"), None);
    }

    #[test]
    fn listeners_share_an_address_when_their_ports_and_their_ip_addresses_meet() {
        let taken = |ip: &str, range: &str| Taken::range(ip.parse().unwrap(), ports(range));
        let address = |text: &str| Some(text.parse::<ListenAddress>().unwrap());
        let shared = |one: Taken, other: Taken| Taken::first_shared(&[one], &[other]);

        assert_eq!(
            shared(taken("0.0.0.0", "19092-19191"), taken("0.0.0.0", "19100-19300")),
            address("0.0.0.0:19100")
        );
        // The address that names one IP address says more than the one that names all of them.
        assert_eq!(
            shared(taken("0.0.0.0", "9092"), taken("127.0.0.1", "9000-9999")),
            address("127.0.0.1:9092")
        );
        assert_eq!(
            shared(taken("127.0.0.1", "9092"), taken("::", "9092")),
            address("127.0.0.1:9092")
        );
        assert_eq!(shared(taken("127.0.0.1", "9092"), taken("10.0.4.27", "9092")), None);
        assert_eq!(shared(taken("0.0.0.0", "9092"), taken("0.0.0.0", "9093")), None);
        assert!(Taken::one("127.0.0.1:0".parse().unwrap()).is_none());
    }
}
