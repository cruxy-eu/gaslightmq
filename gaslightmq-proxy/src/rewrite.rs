//! Response rewrites that keep clients talking to the proxy.
//!
//! Every broker address the cluster hands out is replaced with the address of
//! the proxy listener standing in for that broker, so clients never learn
//! where the real brokers are.

use gaslightmq_proxy_api::catalog::api;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeConfigsResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    MetadataRequest, MetadataResponse,
};
use kafka_protocol::protocol::{Message, StrBytes};

/// Maps an upstream broker to the address clients should use instead.
pub(crate) trait Advertise {
    fn advertise(&self, node_id: i32, host: &str, port: i32) -> (StrBytes, i32);
}

pub(crate) fn metadata(resp: &mut MetadataResponse, a: &impl Advertise) {
    for b in &mut resp.brokers {
        (b.host, b.port) = a.advertise(b.node_id.0, &b.host, b.port);
    }
}

pub(crate) fn find_coordinator(resp: &mut FindCoordinatorResponse, a: &impl Advertise) {
    // v0-3 carry a single coordinator in top-level fields, v4+ a list.
    if resp.node_id.0 >= 0 && resp.error_code == 0 {
        (resp.host, resp.port) = a.advertise(resp.node_id.0, &resp.host, resp.port);
    }
    for c in &mut resp.coordinators {
        if c.node_id.0 >= 0 && c.error_code == 0 {
            (c.host, c.port) = a.advertise(c.node_id.0, &c.host, c.port);
        }
    }
}

pub(crate) fn describe_cluster(resp: &mut DescribeClusterResponse, a: &impl Advertise) {
    for b in &mut resp.brokers {
        (b.host, b.port) = a.advertise(b.broker_id.0, &b.host, b.port);
    }
}

/// APIs removed from ApiVersions responses: their responses carry broker
/// endpoints the proxy doesn't rewrite.
const HIDDEN_APIS: &[i16] = &[
    78, // ShareFetch
    79, // ShareAcknowledge
];

fn min2(a: i16, b: i16) -> i16 {
    a.min(b)
}

/// Highest version of each API the proxy is willing to let through.
///
/// Produce and Fetch are capped below the versions that add `node_endpoints`
/// (KIP-951: Produce v10, Fetch v16). Every API the proxy decodes is capped
/// to what `kafka-protocol` understands.
pub(crate) fn version_cap(api_key: i16) -> Option<i16> {
    Some(match api_key {
        api::PRODUCE => 9,
        api::FETCH => 15,
        api::METADATA => min2(MetadataRequest::VERSIONS.max, MetadataResponse::VERSIONS.max),
        api::FIND_COORDINATOR => min2(FindCoordinatorRequest::VERSIONS.max, FindCoordinatorResponse::VERSIONS.max),
        api::DESCRIBE_CLUSTER => min2(DescribeClusterRequest::VERSIONS.max, DescribeClusterResponse::VERSIONS.max),
        api::DESCRIBE_CONFIGS => min2(DescribeConfigsRequest::VERSIONS.max, DescribeConfigsResponse::VERSIONS.max),
        api::API_VERSIONS => min2(ApiVersionsRequest::VERSIONS.max, ApiVersionsResponse::VERSIONS.max),
        _ => return None,
    })
}

pub(crate) fn cap_api_versions(resp: &mut ApiVersionsResponse) {
    resp.api_keys.retain(|k| !HIDDEN_APIS.contains(&k.api_key));
    for k in &mut resp.api_keys {
        if let Some(cap) = version_cap(k.api_key) {
            k.max_version = k.max_version.min(cap);
        }
    }
    resp.api_keys.retain(|k| k.min_version <= k.max_version);
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::messages::find_coordinator_response::Coordinator;
    use kafka_protocol::messages::metadata_response::MetadataResponseBroker;

    struct Shift;
    impl Advertise for Shift {
        fn advertise(&self, node_id: i32, _host: &str, _port: i32) -> (StrBytes, i32) {
            (StrBytes::from_static_str("proxy"), 20000 + node_id)
        }
    }

    #[test]
    fn metadata_brokers_point_at_proxy() {
        let mut resp = MetadataResponse::default().with_brokers(vec![
            MetadataResponseBroker::default().with_node_id(BrokerId(1)).with_host(StrBytes::from_static_str("kafka-1")).with_port(9092),
            MetadataResponseBroker::default().with_node_id(BrokerId(2)).with_host(StrBytes::from_static_str("kafka-2")).with_port(9092),
        ]);
        metadata(&mut resp, &Shift);
        let got: Vec<(&str, i32)> = resp.brokers.iter().map(|b| (b.host.as_str(), b.port)).collect();
        assert_eq!(got, [("proxy", 20001), ("proxy", 20002)]);
    }

    #[test]
    fn find_coordinator_skips_errors() {
        let mut resp = FindCoordinatorResponse::default()
            .with_node_id(BrokerId(-1))
            .with_error_code(15)
            .with_coordinators(vec![
                Coordinator::default().with_node_id(BrokerId(3)).with_host(StrBytes::from_static_str("k")).with_port(1),
                Coordinator::default().with_node_id(BrokerId(-1)).with_error_code(15),
            ]);
        find_coordinator(&mut resp, &Shift);
        assert_eq!(resp.port, 0);
        assert_eq!(resp.coordinators[0].port, 20003);
        assert_eq!(resp.coordinators[1].port, 0);
    }

    #[test]
    fn api_versions_are_capped_and_share_apis_hidden() {
        let key = |k: i16, min: i16, max: i16| ApiVersion::default().with_api_key(k).with_min_version(min).with_max_version(max);
        let mut resp = ApiVersionsResponse::default().with_api_keys(vec![
            key(api::PRODUCE, 3, 12),
            key(api::FETCH, 4, 17),
            key(api::LIST_OFFSETS, 1, 9),
            key(78, 1, 1),
        ]);
        cap_api_versions(&mut resp);
        let got: Vec<(i16, i16)> = resp.api_keys.iter().map(|k| (k.api_key, k.max_version)).collect();
        assert_eq!(got, [(api::PRODUCE, 9), (api::FETCH, 15), (api::LIST_OFFSETS, 9)]);
    }
}
