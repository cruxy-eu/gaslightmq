use std::collections::BTreeMap;

use gaslightmq_kernel::prelude::*;

use crate::BrokerAddress;

/// The address clients are told for each broker of the real cluster.
///
/// The Upstream backend runs one listener per real broker.
/// A client must connect to that listener and never to the broker behind it,
/// so the [overlay](crate::Overlay) tells clients the address of the listener wherever the cluster tells its own.
/// The backend knows these addresses: its environment names the host clients see and the ports of the listeners.
///
/// No two brokers share an address.
/// A client that is sent to one address for two brokers could not tell them apart.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_overlay::prelude::*;
///
/// let at = |address: &str| address.parse::<BrokerAddress>().expect("an address");
/// let advertised = AdvertisedAddresses::new()
///     .with(BrokerId::from(1), at("gaslight.staging:19092"))?
///     .with(BrokerId::from(2), at("gaslight.staging:19093"))?;
///
/// assert_eq!(advertised.address_of(BrokerId::from(2)), Some(&at("gaslight.staging:19093")));
/// assert_eq!(advertised.broker_at(&at("gaslight.staging:19092")), Some(BrokerId::from(1)));
/// assert_eq!(advertised.address_of(BrokerId::from(3)), None);
///
/// // Broker 3 cannot stand at the address of broker 2.
/// let shared = advertised.with(BrokerId::from(3), at("gaslight.staging:19093"));
/// assert_eq!(
///     shared.unwrap_err().to_string(),
///     "broker 3 cannot be advertised at gaslight.staging:19093: broker 2 is advertised there",
/// );
/// # Ok::<(), AdvertisedAddressesError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdvertisedAddresses {
    by_broker: BTreeMap<BrokerId, BrokerAddress>,
}

impl AdvertisedAddresses {
    /// No broker is advertised.
    pub fn new() -> Self {
        Self::default()
    }

    /// The addresses of those brokers.
    ///
    /// # Errors
    ///
    /// [`AdvertisedAddressesError::SharedAddress`] when two brokers have the same address.
    pub fn of(
        addresses: impl IntoIterator<Item = (BrokerId, BrokerAddress)>,
    ) -> Result<Self, AdvertisedAddressesError> {
        let mut advertised = Self::new();
        for (broker, address) in addresses {
            advertised.advertise(broker, address)?;
        }
        Ok(advertised)
    }

    /// Tell clients from now on that `broker` is at `address`.
    ///
    /// A broker that was advertised before moves to the new address.
    ///
    /// # Errors
    ///
    /// [`AdvertisedAddressesError::SharedAddress`] when another broker is advertised at `address`.
    /// Nothing changes then.
    pub fn advertise(&mut self, broker: BrokerId, address: BrokerAddress) -> Result<(), AdvertisedAddressesError> {
        match self.broker_at(&address) {
            Some(advertised_there) if advertised_there != broker => Err(AdvertisedAddressesError::SharedAddress {
                address,
                broker,
                advertised_there,
            }),
            _ => {
                self.by_broker.insert(broker, address);
                Ok(())
            }
        }
    }

    /// The same addresses, and `broker` at `address`.
    ///
    /// # Errors
    ///
    /// [`AdvertisedAddressesError::SharedAddress`] when another broker is advertised at `address`.
    pub fn with(mut self, broker: BrokerId, address: BrokerAddress) -> Result<Self, AdvertisedAddressesError> {
        self.advertise(broker, address)?;
        Ok(self)
    }

    /// The address clients are told for `broker`, or `None` when the broker is not advertised.
    pub fn address_of(&self, broker: BrokerId) -> Option<&BrokerAddress> {
        self.by_broker.get(&broker)
    }

    /// The broker that is advertised at `address`, if there is one.
    pub fn broker_at(&self, address: &BrokerAddress) -> Option<BrokerId> {
        self.by_broker
            .iter()
            .find(|(_, advertised)| *advertised == address)
            .map(|(broker, _)| *broker)
    }

    /// The advertised brokers with their addresses, in the order of the broker IDs.
    pub fn iter(&self) -> impl Iterator<Item = (BrokerId, &BrokerAddress)> {
        self.by_broker.iter().map(|(broker, address)| (*broker, address))
    }

    /// How many brokers are advertised.
    pub fn len(&self) -> usize {
        self.by_broker.len()
    }

    /// Whether no broker is advertised.
    pub fn is_empty(&self) -> bool {
        self.by_broker.is_empty()
    }
}

/// Why [`AdvertisedAddresses`] cannot hold an address.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdvertisedAddressesError {
    /// Another broker is advertised at the address already.
    #[error("broker {broker} cannot be advertised at {address}: broker {advertised_there} is advertised there")]
    SharedAddress {
        address: BrokerAddress,
        /// The broker that was to be advertised.
        broker: BrokerId,
        /// The broker that is advertised at the address.
        advertised_there: BrokerId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(text: &str) -> BrokerAddress {
        text.parse().unwrap()
    }

    fn broker(id: u16) -> BrokerId {
        BrokerId::from(id)
    }

    #[test]
    fn nothing_is_advertised_at_first() {
        let advertised = AdvertisedAddresses::new();

        assert!(advertised.is_empty());
        assert_eq!(advertised.len(), 0);
        assert_eq!(advertised.address_of(broker(1)), None);
        assert_eq!(advertised.broker_at(&address("gaslight.staging:19092")), None);
    }

    #[test]
    fn a_broker_is_found_by_its_id_and_by_its_address() {
        let advertised = AdvertisedAddresses::of([
            (broker(2), address("gaslight.staging:19093")),
            (broker(1), address("gaslight.staging:19092")),
        ])
        .unwrap();

        assert_eq!(advertised.len(), 2);
        assert_eq!(
            advertised.address_of(broker(1)),
            Some(&address("gaslight.staging:19092"))
        );
        assert_eq!(
            advertised.broker_at(&address("gaslight.staging:19093")),
            Some(broker(2))
        );
        let in_order: Vec<(BrokerId, String)> = advertised.iter().map(|(id, at)| (id, at.to_string())).collect();
        assert_eq!(
            in_order,
            [
                (broker(1), "gaslight.staging:19092".to_owned()),
                (broker(2), "gaslight.staging:19093".to_owned())
            ]
        );
    }

    #[test]
    fn two_brokers_do_not_share_an_address() {
        let mut advertised = AdvertisedAddresses::new()
            .with(broker(1), address("gaslight.staging:19092"))
            .unwrap();
        let before = advertised.clone();

        assert_eq!(
            advertised.advertise(broker(2), address("gaslight.staging:19092")),
            Err(AdvertisedAddressesError::SharedAddress {
                address: address("gaslight.staging:19092"),
                broker: broker(2),
                advertised_there: broker(1),
            })
        );
        assert_eq!(advertised, before);
        assert!(
            AdvertisedAddresses::of([
                (broker(1), address("gaslight.staging:19092")),
                (broker(2), address("gaslight.staging:19092")),
            ])
            .is_err()
        );
    }

    #[test]
    fn a_broker_moves_to_another_address() {
        let mut advertised = AdvertisedAddresses::new()
            .with(broker(1), address("gaslight.staging:19092"))
            .unwrap();

        advertised
            .advertise(broker(1), address("gaslight.staging:19192"))
            .unwrap();
        // The address it left is free again.
        advertised
            .advertise(broker(2), address("gaslight.staging:19092"))
            .unwrap();

        assert_eq!(
            advertised.address_of(broker(1)),
            Some(&address("gaslight.staging:19192"))
        );
        assert_eq!(
            advertised.broker_at(&address("gaslight.staging:19092")),
            Some(broker(2))
        );
    }

    #[test]
    fn advertising_a_broker_where_it_stands_changes_nothing() {
        let mut advertised = AdvertisedAddresses::new()
            .with(broker(1), address("gaslight.staging:19092"))
            .unwrap();
        let before = advertised.clone();

        advertised
            .advertise(broker(1), address("gaslight.staging:19092"))
            .unwrap();

        assert_eq!(advertised, before);
    }

    #[test]
    fn the_error_names_both_brokers_and_the_address() {
        let error = AdvertisedAddressesError::SharedAddress {
            address: address("gaslight.staging:19092"),
            broker: broker(2),
            advertised_there: broker(1),
        };

        assert_eq!(
            error.to_string(),
            "broker 2 cannot be advertised at gaslight.staging:19092: broker 1 is advertised there"
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}
