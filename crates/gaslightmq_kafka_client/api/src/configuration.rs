//! What a raw client is configured with.

use std::net::SocketAddr;

use gaslightmq_kernel::ClientId;

/// What a [`RawKafkaClient`](crate::RawKafkaClient) is configured with.
///
/// A property that Kafka's own clients have keeps its name, its meaning and its default,
/// and the documentation of the field names it.
///
/// ```
/// use gaslightmq_kafka_client_api::{RawClientConfiguration, RawKafkaClient};
/// use gaslightmq_kernel::{ClientId, ClientIdError};
///
/// let broker = "127.0.0.1:9092".parse().expect("an address");
/// let mut configuration = RawClientConfiguration::new(broker);
/// configuration.client_id = ClientId::new("order-ingest")?;
///
/// // The client connects when it first sends.
/// let client = RawKafkaClient::new(configuration);
/// assert_eq!(client.configuration().client_id, "order-ingest");
/// # Ok::<(), ClientIdError>(())
/// ```
///
/// An address alone is a configuration with Kafka's defaults:
///
/// ```
/// use gaslightmq_kafka_client_api::RawKafkaClient;
///
/// let broker: std::net::SocketAddr = "127.0.0.1:9092".parse().expect("an address");
/// let client = RawKafkaClient::new(broker);
/// assert_eq!(client.configuration().client_id, "");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RawClientConfiguration {
    /// Ours. Kafka's clients have `bootstrap.servers`, and learn the brokers of the cluster from there.
    ///
    /// The one address the client sends every request to.
    pub address: SocketAddr,
    /// Kafka: `client.id`. Default: empty.
    ///
    /// The name the client sends in every request.
    /// The client sends it as it is configured, also when it is empty:
    /// it does not name itself, as a producer, a consumer and an admin of Kafka do.
    pub client_id: ClientId,
}

impl RawClientConfiguration {
    /// Kafka's defaults, for a client that sends every request to `address`.
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            client_id: no_client_id(),
        }
    }
}

/// The empty client ID, which is Kafka's default.
fn no_client_id() -> ClientId {
    ClientId::new("").expect("a client ID may be empty")
}

impl From<SocketAddr> for RawClientConfiguration {
    fn from(address: SocketAddr) -> Self {
        Self::new(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_those_of_kafkas_clients() {
        let address: SocketAddr = "127.0.0.1:9092".parse().unwrap();

        let configuration = RawClientConfiguration::new(address);

        assert_eq!(configuration.address, address);
        assert_eq!(configuration.client_id, "");
        assert_eq!(RawClientConfiguration::from(address), configuration);
    }
}
