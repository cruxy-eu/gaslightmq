use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use gaslightmq_kernel::BrokerId;

use crate::address::{ListenAddress, split_port};
use crate::host::HostName;
use crate::ports::{PortRange, PortRangeError, Taken};

/// Where the brokers of a cluster listen: one listener per broker.
///
/// There are two ways to say it.
///
/// | Way | Says | Example |
/// |---|---|---|
/// | A range | One IP address and a range of ports. The brokers take the ports in the order of their IDs. | `0.0.0.0:19092-19191` |
/// | Pinned | The address of each broker, for ports that are published one by one. | `1` on `0.0.0.0:19092`, `2` on `0.0.0.0:19093` |
///
/// Nobody lists the brokers of a real cluster: the proxy discovers them.
/// [`assign`](Self::assign) then gives each broker its address.
///
/// ```
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let listeners: BrokerListeners = "0.0.0.0:19092-19191".parse()?;
///
/// // The cluster has the brokers 1, 2 and 5. Each one takes the next port, in the order of the IDs.
/// let addresses = listeners.assign([5, 1, 2].map(BrokerId::from))?;
/// assert_eq!(addresses[&BrokerId::from(1)], "0.0.0.0:19092".parse().expect("a listen address"));
/// assert_eq!(addresses[&BrokerId::from(2)], "0.0.0.0:19093".parse().expect("a listen address"));
/// assert_eq!(addresses[&BrokerId::from(5)], "0.0.0.0:19094".parse().expect("a listen address"));
/// # Ok::<(), BrokerListenersError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerListeners(Way);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Way {
    Range { ip: IpAddr, ports: PortRange },
    Pinned(BTreeMap<BrokerId, ListenAddress>),
}

impl BrokerListeners {
    /// One listener per broker on `ip`, on the ports of the range in the order of the broker IDs.
    pub fn range(ip: IpAddr, ports: PortRange) -> Self {
        Self(Way::Range { ip, ports })
    }

    /// Each broker on the address it is pinned to.
    ///
    /// # Errors
    ///
    /// - [`BrokerListenersError::NoBroker`] when no broker is pinned.
    /// - [`BrokerListenersError::PinnedTwice`] when one broker has two addresses.
    /// - [`BrokerListenersError::SharedAddress`] when two brokers would listen on the same port of the same address.
    pub fn pinned(brokers: impl IntoIterator<Item = (BrokerId, ListenAddress)>) -> Result<Self, BrokerListenersError> {
        let mut pinned = BTreeMap::new();
        for (broker, address) in brokers {
            if pinned.insert(broker, address).is_some() {
                return Err(BrokerListenersError::PinnedTwice { broker });
            }
        }
        if pinned.is_empty() {
            return Err(BrokerListenersError::NoBroker);
        }
        for (position, (first, address)) in pinned.iter().enumerate() {
            let shares = |(_, other): &(&BrokerId, &ListenAddress)| address.shares(other);
            if let Some((second, _)) = pinned.iter().skip(position + 1).find(shares) {
                return Err(BrokerListenersError::SharedAddress {
                    first: *first,
                    second: *second,
                    address: *address,
                });
            }
        }
        Ok(Self(Way::Pinned(pinned)))
    }

    /// The IP address and the ports, when the listeners are a range.
    pub fn as_range(&self) -> Option<(IpAddr, PortRange)> {
        match &self.0 {
            Way::Range { ip, ports } => Some((*ip, *ports)),
            Way::Pinned(_) => None,
        }
    }

    /// The address of each broker, when the brokers are pinned.
    pub fn as_pinned(&self) -> Option<&BTreeMap<BrokerId, ListenAddress>> {
        match &self.0 {
            Way::Range { .. } => None,
            Way::Pinned(pinned) => Some(pinned),
        }
    }

    /// The address that each of these brokers listens on.
    ///
    /// A range gives its ports in the order of the broker IDs,
    /// so a broker keeps its port for as long as the set of brokers stays the same.
    /// Pinned brokers keep their address whatever the set is, and a pin for a broker that is not there is left out.
    ///
    /// # Errors
    ///
    /// - [`BrokerListenersError::TooFewPorts`] when there are more brokers than ports in the range.
    /// - [`BrokerListenersError::NotPinned`] when the brokers are pinned, and one of these has no address.
    pub fn assign(
        &self,
        brokers: impl IntoIterator<Item = BrokerId>,
    ) -> Result<BTreeMap<BrokerId, ListenAddress>, BrokerListenersError> {
        let brokers: BTreeSet<BrokerId> = brokers.into_iter().collect();
        match &self.0 {
            Way::Range { ip, ports } => {
                if brokers.len() > ports.count() {
                    return Err(BrokerListenersError::TooFewPorts {
                        brokers: brokers.len(),
                        ports: *ports,
                    });
                }
                let addresses = ports.ports().map(|port| ListenAddress::new(*ip, port));
                Ok(brokers.into_iter().zip(addresses).collect())
            }
            Way::Pinned(pinned) => brokers
                .into_iter()
                .map(|broker| match pinned.get(&broker) {
                    Some(address) => Ok((broker, *address)),
                    None => Err(BrokerListenersError::NotPinned { broker }),
                })
                .collect(),
        }
    }

    /// The ports that these listeners can take.
    pub(crate) fn taken(&self) -> Vec<Taken> {
        match &self.0 {
            Way::Range { ip, ports } => vec![Taken::range(*ip, *ports)],
            Way::Pinned(pinned) => pinned.values().copied().filter_map(Taken::one).collect(),
        }
    }
}

/// Reads a range, such as `0.0.0.0:19092-19191`. An IPv6 address stands in brackets: `[::]:19092-19191`.
impl FromStr for BrokerListeners {
    type Err = BrokerListenersError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let input = || text.to_owned();
        let invalid_address = || BrokerListenersError::InvalidAddress { input: input() };
        let (ip, Some(ports)) = split_port(text) else {
            return Err(invalid_address());
        };
        let ip = match ip.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
            Some(inner) => inner.parse::<Ipv6Addr>().map(IpAddr::V6).ok(),
            None => ip.parse::<Ipv4Addr>().map(IpAddr::V4).ok(),
        };
        let ip = ip.ok_or_else(invalid_address)?;
        let ports = ports
            .parse()
            .map_err(|error| BrokerListenersError::InvalidPorts { input: input(), error })?;
        Ok(Self::range(ip, ports))
    }
}

impl TryFrom<&str> for BrokerListeners {
    type Error = BrokerListenersError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        text.parse()
    }
}

/// Writes a range as it is read, and pinned brokers as `1 on 0.0.0.0:19092, 2 on 0.0.0.0:19093`.
impl fmt::Display for BrokerListeners {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Way::Range {
                ip: IpAddr::V4(ip),
                ports,
            } => write!(f, "{ip}:{ports}"),
            Way::Range {
                ip: IpAddr::V6(ip),
                ports,
            } => write!(f, "[{ip}]:{ports}"),
            Way::Pinned(pinned) => {
                for (position, (broker, address)) in pinned.iter().enumerate() {
                    let separator = if position == 0 { "" } else { ", " };
                    write!(f, "{separator}{broker} on {address}")?;
                }
                Ok(())
            }
        }
    }
}

/// Why broker listeners cannot be, or cannot give a broker its address.
///
/// ```
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let listeners: BrokerListeners = "0.0.0.0:19092-19093".parse()?;
/// let error = listeners.assign([1, 2, 3].map(BrokerId::from)).unwrap_err();
/// assert_eq!(error.to_string(), "3 brokers need a port each, and the range 19092-19093 has 2");
/// # Ok::<(), BrokerListenersError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BrokerListenersError {
    /// The text does not start with an IP address and a colon.
    InvalidAddress { input: String },
    /// What follows the IP address is not a range of ports.
    InvalidPorts { input: String, error: PortRangeError },
    /// No broker is pinned.
    NoBroker,
    /// One broker is pinned to two addresses.
    PinnedTwice { broker: BrokerId },
    /// Two pinned brokers would listen on the same port of the same address.
    SharedAddress {
        first: BrokerId,
        second: BrokerId,
        address: ListenAddress,
    },
    /// The cluster has more brokers than the range has ports.
    TooFewPorts { brokers: usize, ports: PortRange },
    /// The brokers are pinned, and the cluster has a broker that is not.
    NotPinned { broker: BrokerId },
}

impl fmt::Display for BrokerListenersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAddress { input } => write!(
                f,
                "broker listeners {input:?} are not an IP address and a range of ports, such as 0.0.0.0:19092-19191"
            ),
            Self::InvalidPorts { input, error } => write!(f, "broker listeners {input:?}: {error}"),
            Self::NoBroker => f.write_str("broker listeners pin no broker to an address"),
            Self::PinnedTwice { broker } => write!(f, "broker {broker} is pinned to two addresses"),
            Self::SharedAddress { first, second, address } => {
                write!(f, "brokers {first} and {second} are both pinned to {address}")
            }
            Self::TooFewPorts { brokers, ports } => write!(
                f,
                "{brokers} brokers need a port each, and the range {ports} has {}",
                ports.count()
            ),
            Self::NotPinned { broker } => write!(f, "broker {broker} is not pinned to an address"),
        }
    }
}

impl std::error::Error for BrokerListenersError {}

/// Where an instance listens for Kafka clients, and the host those clients see.
///
/// | Part | Is |
/// |---|---|
/// | [`bootstrap`](Self::bootstrap) | The address clients connect to first. |
/// | [`brokers`](Self::brokers) | One listener per broker of the cluster behind. |
/// | [`advertised_host`](Self::advertised_host) | The host that Metadata names to clients, under which they reach every listener. |
///
/// The bootstrap listener and the broker listeners take different ports.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let listeners = Listeners::new(
///     "0.0.0.0:9092".parse().expect("a listen address"),
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "gaslight.staging".parse().expect("a host name"),
/// )?;
/// assert_eq!(listeners.advertised_host(), &HostName::new("gaslight.staging").expect("a host name"));
///
/// // The bootstrap listener cannot take a port of the brokers.
/// let error = Listeners::new(
///     "0.0.0.0:19092".parse().expect("a listen address"),
///     "0.0.0.0:19092-19191".parse().expect("broker listeners"),
///     "localhost".parse().expect("a host name"),
/// );
/// assert_eq!(
///     error.unwrap_err().to_string(),
///     "the bootstrap listener and a broker listener both listen on 0.0.0.0:19092"
/// );
/// # Ok::<(), ListenersError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "crate::serde::ListenersForm", into = "crate::serde::ListenersForm")
)]
pub struct Listeners {
    bootstrap: ListenAddress,
    brokers: BrokerListeners,
    advertise: HostName,
}

impl Listeners {
    /// Listeners on those addresses, which clients reach under the host `advertise`.
    ///
    /// # Errors
    ///
    /// [`ListenersError::BootstrapAmongBrokers`] when the bootstrap listener takes a port of the broker listeners.
    pub fn new(
        bootstrap: ListenAddress,
        brokers: BrokerListeners,
        advertise: HostName,
    ) -> Result<Self, ListenersError> {
        let of_bootstrap: Vec<Taken> = Taken::one(bootstrap).into_iter().collect();
        if let Some(address) = Taken::first_shared(&of_bootstrap, &brokers.taken()) {
            return Err(ListenersError::BootstrapAmongBrokers { address });
        }
        Ok(Self {
            bootstrap,
            brokers,
            advertise,
        })
    }

    /// The address clients connect to first.
    pub fn bootstrap(&self) -> ListenAddress {
        self.bootstrap
    }

    /// Where the brokers listen.
    pub fn brokers(&self) -> &BrokerListeners {
        &self.brokers
    }

    /// The host that clients see in Metadata.
    pub fn advertised_host(&self) -> &HostName {
        &self.advertise
    }

    /// The same listeners, which clients reach under another host.
    #[must_use]
    pub fn with_advertised_host(mut self, advertise: HostName) -> Self {
        self.advertise = advertise;
        self
    }

    /// Every port that these listeners can take.
    pub(crate) fn taken(&self) -> Vec<Taken> {
        let mut taken = self.brokers.taken();
        taken.extend(Taken::one(self.bootstrap));
        taken
    }
}

/// Why addresses are not [`Listeners`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListenersError {
    /// The bootstrap listener and a broker listener would listen on the same port of the same address.
    BootstrapAmongBrokers { address: ListenAddress },
}

impl fmt::Display for ListenersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BootstrapAmongBrokers { address } => write!(
                f,
                "the bootstrap listener and a broker listener both listen on {address}"
            ),
        }
    }
}

impl std::error::Error for ListenersError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(text: &str) -> ListenAddress {
        text.parse().unwrap()
    }

    fn range(text: &str) -> BrokerListeners {
        text.parse().unwrap()
    }

    fn brokers<const N: usize>(ids: [u16; N]) -> [BrokerId; N] {
        ids.map(BrokerId::from)
    }

    fn pinned<const N: usize>(pins: [(u16, &str); N]) -> Result<BrokerListeners, BrokerListenersError> {
        BrokerListeners::pinned(pins.map(|(broker, text)| (BrokerId::from(broker), address(text))))
    }

    #[test]
    fn a_range_is_an_ip_address_and_ports() {
        for text in ["0.0.0.0:19092-19191", "127.0.0.1:19092", "[::]:19092-19094"] {
            let listeners = range(text);
            assert_eq!(listeners.to_string(), text);
            assert!(listeners.as_pinned().is_none());
            assert_eq!(BrokerListeners::try_from(text), Ok(listeners));
        }
        assert_eq!(
            range("0.0.0.0:19092-19191").as_range(),
            Some(("0.0.0.0".parse().unwrap(), "19092-19191".parse().unwrap()))
        );
    }

    #[test]
    fn a_text_that_is_no_range_is_an_error() {
        for text in [
            "",
            "19092-19191",
            "localhost:19092",
            "0.0.0.0",
            "[::]",
            "::1:19092",
            "[0.0.0.0]:19092",
        ] {
            assert_eq!(
                text.parse::<BrokerListeners>(),
                Err(BrokerListenersError::InvalidAddress { input: text.to_owned() }),
                "{text:?}"
            );
        }
        let error = "0.0.0.0:19191-19092".parse::<BrokerListeners>().unwrap_err();
        assert_eq!(
            error,
            BrokerListenersError::InvalidPorts {
                input: "0.0.0.0:19191-19092".to_owned(),
                error: PortRangeError::Backwards {
                    first: 19191,
                    last: 19092
                }
            }
        );
        assert_eq!(
            error.to_string(),
            "broker listeners \"0.0.0.0:19191-19092\": port range 19191-19092 ends before it starts"
        );
    }

    #[test]
    fn a_range_gives_its_ports_in_the_order_of_the_broker_ids() {
        let listeners = range("0.0.0.0:19092-19094");

        let assigned = listeners.assign(brokers([7, 1, 3])).unwrap();
        assert_eq!(
            assigned.into_iter().collect::<Vec<_>>(),
            [
                (BrokerId::from(1), address("0.0.0.0:19092")),
                (BrokerId::from(3), address("0.0.0.0:19093")),
                (BrokerId::from(7), address("0.0.0.0:19094")),
            ]
        );
        // A broker that is named twice is one broker.
        assert_eq!(listeners.assign(brokers([1, 1, 1, 1])).unwrap().len(), 1);
        assert!(listeners.assign([]).unwrap().is_empty());
    }

    #[test]
    fn a_range_with_fewer_ports_than_brokers_is_an_error() {
        let error = range("0.0.0.0:19092-19094").assign(brokers([1, 2, 3, 4])).unwrap_err();

        assert_eq!(
            error,
            BrokerListenersError::TooFewPorts {
                brokers: 4,
                ports: "19092-19094".parse().unwrap()
            }
        );
        assert_eq!(
            error.to_string(),
            "4 brokers need a port each, and the range 19092-19094 has 3"
        );
    }

    #[test]
    fn pinned_brokers_keep_their_address() {
        let listeners = pinned([(1, "0.0.0.0:19092"), (2, "0.0.0.0:19093"), (3, "0.0.0.0:19094")]).unwrap();

        assert!(listeners.as_range().is_none());
        assert_eq!(listeners.as_pinned().unwrap().len(), 3);
        assert_eq!(
            listeners.to_string(),
            "1 on 0.0.0.0:19092, 2 on 0.0.0.0:19093, 3 on 0.0.0.0:19094"
        );
        // Broker 2 is gone, and broker 3 keeps its address.
        let assigned = listeners.assign(brokers([3, 1])).unwrap();
        assert_eq!(assigned[&BrokerId::from(1)], address("0.0.0.0:19092"));
        assert_eq!(assigned[&BrokerId::from(3)], address("0.0.0.0:19094"));
        assert_eq!(assigned.len(), 2);
    }

    #[test]
    fn a_broker_without_a_pin_is_an_error() {
        let listeners = pinned([(1, "0.0.0.0:19092")]).unwrap();
        let error = listeners.assign(brokers([1, 4])).unwrap_err();

        assert_eq!(
            error,
            BrokerListenersError::NotPinned {
                broker: BrokerId::from(4)
            }
        );
        assert_eq!(error.to_string(), "broker 4 is not pinned to an address");
    }

    #[test]
    fn pins_that_do_not_hold_together_are_an_error() {
        assert_eq!(pinned([]), Err(BrokerListenersError::NoBroker));
        assert_eq!(
            pinned([(1, "0.0.0.0:19092"), (1, "0.0.0.0:19093")]),
            Err(BrokerListenersError::PinnedTwice {
                broker: BrokerId::from(1)
            })
        );
        let shared = pinned([(1, "0.0.0.0:19092"), (2, "0.0.0.0:19093"), (3, "127.0.0.1:19092")]).unwrap_err();
        assert_eq!(
            shared,
            BrokerListenersError::SharedAddress {
                first: BrokerId::from(1),
                second: BrokerId::from(3),
                address: address("0.0.0.0:19092")
            }
        );
        assert_eq!(shared.to_string(), "brokers 1 and 3 are both pinned to 0.0.0.0:19092");
        // The system picks both ports, so they differ.
        assert!(pinned([(1, "127.0.0.1:0"), (2, "127.0.0.1:0")]).is_ok());
    }

    #[test]
    fn listeners_hold_a_bootstrap_address_broker_listeners_and_a_host() {
        let host = |text: &str| HostName::new(text).unwrap();
        let listeners =
            Listeners::new(address("0.0.0.0:9092"), range("0.0.0.0:19092-19191"), host("localhost")).unwrap();

        assert_eq!(listeners.bootstrap(), address("0.0.0.0:9092"));
        assert_eq!(listeners.brokers(), &range("0.0.0.0:19092-19191"));
        assert_eq!(listeners.advertised_host(), &host("localhost"));

        let elsewhere = listeners.clone().with_advertised_host(host("gaslight.staging"));
        assert_eq!(elsewhere.advertised_host(), &host("gaslight.staging"));
        assert_eq!(elsewhere.bootstrap(), listeners.bootstrap());
    }

    #[test]
    fn the_bootstrap_listener_takes_no_port_of_a_broker() {
        let host = HostName::new("localhost").unwrap();
        let listeners =
            |bootstrap: &str, brokers: BrokerListeners| Listeners::new(address(bootstrap), brokers, host.clone());

        assert_eq!(
            listeners("127.0.0.1:19100", range("0.0.0.0:19092-19191")),
            Err(ListenersError::BootstrapAmongBrokers {
                address: address("127.0.0.1:19100")
            })
        );
        assert_eq!(
            listeners(
                "0.0.0.0:19093",
                pinned([(1, "0.0.0.0:19092"), (2, "0.0.0.0:19093")]).unwrap()
            ),
            Err(ListenersError::BootstrapAmongBrokers {
                address: address("0.0.0.0:19093")
            })
        );
        // Another IP address, or a port that the system picks, is free.
        assert!(listeners("127.0.0.1:19100", range("10.0.4.27:19092-19191")).is_ok());
        assert!(listeners("0.0.0.0:0", range("0.0.0.0:19092-19191")).is_ok());
    }
}
