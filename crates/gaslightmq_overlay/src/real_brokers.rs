use std::collections::BTreeMap;

use gaslightmq_kernel::prelude::*;

use crate::BrokerAddress;

/// The brokers of the real cluster, each with the address the cluster tells for it.
///
/// Nobody configures these addresses: the [overlay](crate::Overlay) reads them from the answers of the cluster,
/// before it writes the advertised ones over them.
/// The last answer stands, so a broker that moved is known at its new address.
/// A broker that the cluster no longer names stays known.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RealBrokers {
    addresses: BTreeMap<BrokerId, BrokerAddress>,
}

impl RealBrokers {
    /// Remembers that the cluster reaches `broker` at `address`.
    pub(crate) fn learn(&mut self, broker: BrokerId, address: BrokerAddress) {
        self.addresses.insert(broker, address);
    }

    /// The address the cluster tells for `broker`, if the cluster named the broker.
    pub(crate) fn address_of(&self, broker: BrokerId) -> Option<&BrokerAddress> {
        self.addresses.get(&broker)
    }

    /// The known brokers with their addresses, in the order of the broker IDs.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (BrokerId, &BrokerAddress)> {
        self.addresses.iter().map(|(broker, address)| (*broker, address))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(text: &str) -> BrokerAddress {
        text.parse().unwrap()
    }

    #[test]
    fn the_last_address_of_a_broker_stands() {
        let mut brokers = RealBrokers::default();
        assert_eq!(brokers.address_of(BrokerId::from(2)), None);

        brokers.learn(BrokerId::from(2), address("kafka-2.internal:9092"));
        brokers.learn(BrokerId::from(1), address("kafka-1.internal:9092"));
        brokers.learn(BrokerId::from(2), address("kafka-2b.internal:9092"));

        assert_eq!(
            brokers.address_of(BrokerId::from(2)),
            Some(&address("kafka-2b.internal:9092"))
        );
        let known: Vec<BrokerId> = brokers.iter().map(|(broker, _)| broker).collect();
        assert_eq!(known, [BrokerId::from(1), BrokerId::from(2)]);
    }
}
