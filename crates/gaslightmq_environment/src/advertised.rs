use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::address::{ServerAddress, ServerAddressError};

/// The address that Metadata names for a broker listener, by the port it listens on, as Kafka's
/// `advertised.listeners` names one for each listener: `19092=localhost:55002,19093=localhost:55003`.
///
/// A listener whose port it does not name is reached under the [advertised host](crate::Listeners::advertised_host)
/// and the port it listens on.
/// A listener whose clients reach it through another port, such as a port that a container runtime publishes, needs
/// an advertised listener: the proxy listens on `19092`, and its clients reach it at `localhost:55002`.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let advertised: AdvertisedListeners = "19092=localhost:55002,19093=localhost:55003".parse()?;
/// assert_eq!(advertised.of_port(19093), Some(&"localhost:55003".parse().expect("a server address")));
/// assert_eq!(advertised.of_port(19094), None);
/// assert_eq!(advertised.to_string(), "19092=localhost:55002,19093=localhost:55003");
///
/// assert!(AdvertisedListeners::none().is_empty());
/// # Ok::<(), AdvertisedListenersError>(())
/// ```
///
/// With `serde`, it is a map from the port to the address: `{ 19092: "localhost:55002" }`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "BTreeMap<u16, ServerAddress>", into = "BTreeMap<u16, ServerAddress>")
)]
pub struct AdvertisedListeners(BTreeMap<u16, ServerAddress>);

impl AdvertisedListeners {
    /// No advertised listener: every listener is reached under the advertised host and the port it listens on.
    pub fn none() -> Self {
        Self::default()
    }

    /// The listeners on these ports, advertised at these addresses.
    ///
    /// # Errors
    ///
    /// - [`AdvertisedListenersError::FreePort`] for port 0, whose listener takes a port that nobody knows up front.
    /// - [`AdvertisedListenersError::PortTwice`] when one port has two addresses.
    pub fn new(listeners: impl IntoIterator<Item = (u16, ServerAddress)>) -> Result<Self, AdvertisedListenersError> {
        let mut advertised = BTreeMap::new();
        for (port, address) in listeners {
            if port == 0 {
                return Err(AdvertisedListenersError::FreePort { address });
            }
            if advertised.insert(port, address).is_some() {
                return Err(AdvertisedListenersError::PortTwice { port });
            }
        }
        Ok(Self(advertised))
    }

    /// The address that Metadata names for the listener on `port`, when one is advertised.
    pub fn of_port(&self, port: u16) -> Option<&ServerAddress> {
        self.0.get(&port)
    }

    /// Whether no listener is advertised.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every advertised listener: its port, and the address that Metadata names for it.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &ServerAddress)> {
        self.0.iter().map(|(port, address)| (*port, address))
    }
}

impl TryFrom<BTreeMap<u16, ServerAddress>> for AdvertisedListeners {
    type Error = AdvertisedListenersError;

    fn try_from(listeners: BTreeMap<u16, ServerAddress>) -> Result<Self, Self::Error> {
        Self::new(listeners)
    }
}

impl From<AdvertisedListeners> for BTreeMap<u16, ServerAddress> {
    fn from(listeners: AdvertisedListeners) -> Self {
        listeners.0
    }
}

impl FromStr for AdvertisedListeners {
    type Err = AdvertisedListenersError;

    /// Reads `port=host:port`, one for each listener, separated by commas. An empty text advertises none.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut listeners = Vec::new();
        for entry in text.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
            let invalid = || AdvertisedListenersError::InvalidEntry {
                input: entry.to_owned(),
            };
            let (port, address) = entry.split_once('=').ok_or_else(invalid)?;
            let port = port.trim().parse().map_err(|_| invalid())?;
            let address = address
                .trim()
                .parse()
                .map_err(|error| AdvertisedListenersError::InvalidAddress { port, error })?;
            listeners.push((port, address));
        }
        Self::new(listeners)
    }
}

impl fmt::Display for AdvertisedListeners {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, (port, address)) in self.iter().enumerate() {
            let separator = if position == 0 { "" } else { "," };
            write!(f, "{separator}{port}={address}")?;
        }
        Ok(())
    }
}

/// Why listeners are not [`AdvertisedListeners`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvertisedListenersError {
    /// An entry is not `port=host:port`, with a port between 0 and 65535.
    #[error("advertised listener {input:?}: write port=host:port")]
    InvalidEntry { input: String },
    /// The address of a listener is no server address.
    #[error("advertised listener on port {port}: {error}")]
    InvalidAddress { port: u16, error: ServerAddressError },
    /// Port 0 stands for a free port that nobody knows up front, so no address can be advertised for it.
    #[error("advertised listener {address} names port 0, which no listener is known by")]
    FreePort { address: ServerAddress },
    /// One port has two addresses.
    #[error("the listener on port {port} is advertised twice")]
    PortTwice { port: u16 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(text: &str) -> ServerAddress {
        text.parse().unwrap()
    }

    #[test]
    fn listeners_read_back_from_their_text_form() {
        let advertised: AdvertisedListeners = "19093=localhost:55003, 19092=localhost:55002".parse().unwrap();

        assert_eq!(
            advertised.iter().collect::<Vec<_>>(),
            [
                (19092, &address("localhost:55002")),
                (19093, &address("localhost:55003"))
            ]
        );
        assert_eq!(advertised.to_string().parse(), Ok(advertised));
        assert_eq!("".parse(), Ok(AdvertisedListeners::none()));
    }

    #[test]
    fn an_entry_says_what_is_wrong_with_it() {
        assert_eq!(
            "19092".parse::<AdvertisedListeners>().unwrap_err().to_string(),
            "advertised listener \"19092\": write port=host:port"
        );
        assert_eq!(
            "19092=localhost"
                .parse::<AdvertisedListeners>()
                .unwrap_err()
                .to_string(),
            "advertised listener on port 19092: server address \"localhost\" has no port: write host:port"
        );
        assert_eq!(
            "0=localhost:55002"
                .parse::<AdvertisedListeners>()
                .unwrap_err()
                .to_string(),
            "advertised listener localhost:55002 names port 0, which no listener is known by"
        );
        assert_eq!(
            "19092=localhost:55002,19092=localhost:55003"
                .parse::<AdvertisedListeners>()
                .unwrap_err(),
            AdvertisedListenersError::PortTwice { port: 19092 }
        );
    }
}
