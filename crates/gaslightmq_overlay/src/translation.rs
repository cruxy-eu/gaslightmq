use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::{
    DescribeClusterResponse, FetchResponse, FindCoordinatorResponse, MetadataResponse, ProduceResponse,
    ShareAcknowledgeResponse, ShareFetchResponse,
};
use gaslightmq_wire::protocol::StrBytes;

use crate::real_brokers::RealBrokers;
use crate::{AdvertisedAddresses, BrokerAddress};

/// The translation of the broker addresses of one response of the real cluster.
///
/// Wherever the response tells the address of a broker, the translation does two things:
///
/// - it [learns](RealBrokers::learn) the address, which is where the backend reaches the broker,
/// - and it writes the [advertised address](AdvertisedAddresses) of the broker in its place.
///
/// A broker that is not advertised has no address a client may know.
/// Its real address never stays in the response:
///
/// | The response names it | The translation |
/// |---|---|
/// | In a list of brokers or of leaders | Leaves the broker out of the list. |
/// | As the coordinator that the client asked for | Says that the coordinator is not available. |
///
/// Everything else of the response stays as the cluster wrote it: the broker IDs, the racks, the leaders.
#[derive(Debug)]
pub(crate) struct AddressTranslation<'a> {
    advertised: &'a AdvertisedAddresses,
    real: &'a mut RealBrokers,
    changed: bool,
}

impl<'a> AddressTranslation<'a> {
    /// What a broker writes for a coordinator that is not available.
    const UNAVAILABLE: &'static str = "The coordinator is not available.";
    /// The endpoint type of a `DescribeCluster` response that lists controllers, and not brokers.
    const CONTROLLERS: i8 = 2;
    /// The first version of `FindCoordinator` that answers a list of keys, each with a coordinator.
    const FIRST_VERSION_WITH_KEYS: i16 = 4;

    /// A translation that writes the addresses of `advertised`, and that teaches `real` what it reads.
    pub(crate) fn new(advertised: &'a AdvertisedAddresses, real: &'a mut RealBrokers) -> Self {
        Self {
            advertised,
            real,
            changed: false,
        }
    }

    /// Whether the translation changed anything of what it was given so far.
    pub(crate) const fn changed(&self) -> bool {
        self.changed
    }

    /// Translate the brokers of a Metadata response.
    pub(crate) fn metadata(&mut self, response: &mut MetadataResponse) {
        response
            .brokers
            .retain_mut(|broker| self.address(broker.node_id.0, &mut broker.host, &mut broker.port));
    }

    /// Translate the brokers of a `DescribeCluster` response.
    ///
    /// A response that lists controllers stays as it is.
    /// A controller is no broker: it has no listener, and its ID can be the ID of a broker.
    pub(crate) fn describe_cluster(&mut self, response: &mut DescribeClusterResponse) {
        if response.endpoint_type == Self::CONTROLLERS {
            return;
        }
        response
            .brokers
            .retain_mut(|broker| self.address(broker.broker_id.0, &mut broker.host, &mut broker.port));
    }

    /// Translate the coordinator of a `FindCoordinator` response of `version`, or the coordinator of each key.
    pub(crate) fn find_coordinator(&mut self, response: &mut FindCoordinatorResponse, version: i16) {
        if version < Self::FIRST_VERSION_WITH_KEYS {
            if !self.address(response.node_id.0, &mut response.host, &mut response.port) {
                response.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE.value();
                // Version 0 has no place for the message, and leaves it out.
                response.error_message = Some(StrBytes::from_static_str(Self::UNAVAILABLE));
                response.node_id = BrokerId::to_wire(None).into();
                response.host = StrBytes::default();
                response.port = -1;
            }
            return;
        }
        for coordinator in &mut response.coordinators {
            if !self.address(coordinator.node_id.0, &mut coordinator.host, &mut coordinator.port) {
                coordinator.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE.value();
                coordinator.error_message = Some(StrBytes::from_static_str(Self::UNAVAILABLE));
                coordinator.node_id = BrokerId::to_wire(None).into();
                coordinator.host = StrBytes::default();
                coordinator.port = -1;
            }
        }
    }

    /// Translate the leaders that a Produce response names for the partitions that moved.
    pub(crate) fn produce(&mut self, response: &mut ProduceResponse) {
        response
            .node_endpoints
            .retain_mut(|leader| self.address(leader.node_id.0, &mut leader.host, &mut leader.port));
    }

    /// Translate the leaders that a Fetch response names for the partitions that moved.
    pub(crate) fn fetch(&mut self, response: &mut FetchResponse) {
        response
            .node_endpoints
            .retain_mut(|leader| self.address(leader.node_id.0, &mut leader.host, &mut leader.port));
    }

    /// Translate the leaders that a `ShareFetch` response names for the partitions that moved.
    pub(crate) fn share_fetch(&mut self, response: &mut ShareFetchResponse) {
        response
            .node_endpoints
            .retain_mut(|leader| self.address(leader.node_id.0, &mut leader.host, &mut leader.port));
    }

    /// Translate the leaders that a `ShareAcknowledge` response names for the partitions that moved.
    pub(crate) fn share_acknowledge(&mut self, response: &mut ShareAcknowledgeResponse) {
        response
            .node_endpoints
            .retain_mut(|leader| self.address(leader.node_id.0, &mut leader.host, &mut leader.port));
    }

    /// Translate one address of the response: the `host` and the `port` it tells for the broker `node_id`.
    ///
    /// It returns whether the address may stay in the response.
    /// The address of a broker that is not advertised may not, and the caller takes it out.
    /// A response writes "no broker" as `-1`, and that entry stays as it is.
    fn address(&mut self, node_id: i32, host: &mut StrBytes, port: &mut i32) -> bool {
        let Ok(Some(broker)) = BrokerId::from_wire(node_id) else {
            return true;
        };
        if let Some(real) = BrokerAddress::from_wire(host, *port) {
            self.real.learn(broker, real);
        }
        let Some(advertised) = self.advertised.address_of(broker) else {
            self.changed = true;
            return false;
        };
        let advertised_port = i32::from(advertised.port());
        if host.as_str() != advertised.host() || *port != advertised_port {
            *host = StrBytes::from_string(advertised.host().to_owned());
            *port = advertised_port;
            self.changed = true;
        }
        true
    }
}
