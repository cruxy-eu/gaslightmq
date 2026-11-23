use std::fmt;

use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::{Request, Response};

use crate::BrokerAddress;

/// What the overlay decides about a request before the real cluster sees it.
///
/// [`Overlay::forward`](crate::Overlay::forward) returns it, and the backend does it.
/// The overlay opens no connection: it only says where the request goes, and in which form.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_overlay::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::ApiVersionsRequest;
///
/// let overlay = Overlay::new(AdvertisedAddresses::new());
/// let request = Request::new(3, 1, Some("order-ingest"), &ApiVersionsRequest::default())?;
///
/// let forwarding = overlay.forward(Endpoint::Bootstrap, &request)?;
/// assert_eq!(forwarding.to_string(), "forward ApiVersions v3 to any broker");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub enum Forwarding {
    /// Send `request` to the broker `to`, and give its answer to [`Overlay::answer`](crate::Overlay::answer).
    ///
    /// The request is the client's, byte for byte, or the same request without its fetch session.
    /// It carries the correlation ID of the client's request.
    Forward { to: Destination, request: Request },
    /// The overlay answers the request itself, and the real cluster never sees it.
    ///
    /// This response is the truth of the call, and no answer of the real cluster stands next to it.
    Answer(Response),
}

/// Writes what happens to the request, such as `forward Fetch v12 to broker 2 at kafka-2.internal:9092`.
impl fmt::Display for Forwarding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forward { to, request } => write!(f, "forward {} to {to}", request.api()),
            Self::Answer(response) => write!(f, "answer {}", response.api()),
        }
    }
}

/// Which broker of the real cluster a request goes to.
///
/// A client reaches the cluster through the endpoint it connected to, and the endpoint decides the destination.
///
/// | The client connected to | The request goes to |
/// |---|---|
/// | The bootstrap endpoint | [`AnyBroker`](Self::AnyBroker) |
/// | The endpoint of a broker | [`Broker`](Self::Broker): that broker, at its real address |
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Destination {
    /// Any broker of the cluster.
    ///
    /// The bootstrap endpoint stands for no broker, and a client only asks it what every broker knows.
    /// The backend picks the broker, such as the one behind the bootstrap address of its environment.
    AnyBroker,
    /// This broker, which the real cluster reaches at `address`.
    Broker { broker: BrokerId, address: BrokerAddress },
}

impl Destination {
    /// The broker the request goes to, if it goes to one broker and no other.
    pub const fn broker(&self) -> Option<BrokerId> {
        match self {
            Self::AnyBroker => None,
            Self::Broker { broker, .. } => Some(*broker),
        }
    }

    /// The real address of that broker.
    pub const fn address(&self) -> Option<&BrokerAddress> {
        match self {
            Self::AnyBroker => None,
            Self::Broker { address, .. } => Some(address),
        }
    }
}

/// Writes `any broker`, or `broker 2 at kafka-2.internal:9092`.
impl fmt::Display for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnyBroker => f.write_str("any broker"),
            Self::Broker { broker, address } => write!(f, "broker {broker} at {address}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};

    use super::*;

    #[test]
    fn a_destination_names_its_broker_and_its_address() {
        let address: BrokerAddress = "kafka-2.internal:9092".parse().unwrap();
        let broker = Destination::Broker {
            broker: BrokerId::from(2),
            address: address.clone(),
        };

        assert_eq!(broker.broker(), Some(BrokerId::from(2)));
        assert_eq!(broker.address(), Some(&address));
        assert_eq!(broker.to_string(), "broker 2 at kafka-2.internal:9092");

        assert_eq!(Destination::AnyBroker.broker(), None);
        assert_eq!(Destination::AnyBroker.address(), None);
        assert_eq!(Destination::AnyBroker.to_string(), "any broker");
    }

    #[test]
    fn a_forwarding_says_what_happens_to_the_request() {
        let request = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default()).unwrap();
        let response = request.response(&HeartbeatResponse::default()).unwrap();

        let forward = Forwarding::Forward {
            to: Destination::AnyBroker,
            request,
        };
        assert_eq!(forward.to_string(), "forward Heartbeat v4 to any broker");
        assert_eq!(Forwarding::Answer(response).to_string(), "answer Heartbeat v4");
    }
}
