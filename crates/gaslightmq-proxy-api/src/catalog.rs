//! The Kafka APIs and error codes the UI offers, independent of
//! `kafka-protocol` so this crate stays wasm-friendly.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCodeInfo {
    pub code: i16,
    pub name: String,
    pub retriable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiCatalogEntry {
    pub api_key: i16,
    pub name: String,
    /// The proxy can synthesize an error response for this API.
    pub supports_error: bool,
    /// Rows for this API can be scoped to a single topic.
    pub supports_topic: bool,
    /// Error codes offered for this API, most useful first.
    pub error_codes: Vec<ErrorCodeInfo>,
}

pub mod api {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const SYNC_GROUP: i16 = 14;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const INIT_PRODUCER_ID: i16 = 22;
    pub const DESCRIBE_CONFIGS: i16 = 32;
    pub const DESCRIBE_CLUSTER: i16 = 60;
}

/// Rows of the grid, in display order.
const APIS: &[(i16, &str)] = &[
    (api::DESCRIBE_CONFIGS, "DescribeConfigs"),
    (api::METADATA, "Metadata"),
    (api::PRODUCE, "Produce"),
    (api::FETCH, "Fetch"),
    (api::LIST_OFFSETS, "ListOffsets"),
    (api::API_VERSIONS, "ApiVersions"),
    (api::FIND_COORDINATOR, "FindCoordinator"),
    (api::INIT_PRODUCER_ID, "InitProducerId"),
    (api::OFFSET_COMMIT, "OffsetCommit"),
    (api::OFFSET_FETCH, "OffsetFetch"),
    (api::JOIN_GROUP, "JoinGroup"),
    (api::SYNC_GROUP, "SyncGroup"),
    (api::HEARTBEAT, "Heartbeat"),
    (api::CREATE_TOPICS, "CreateTopics"),
    (api::DESCRIBE_CLUSTER, "DescribeCluster"),
];

const ERROR_CODES: &[(i16, &str, bool)] = &[
    (-1, "UNKNOWN_SERVER_ERROR", false),
    (3, "UNKNOWN_TOPIC_OR_PARTITION", true),
    (5, "LEADER_NOT_AVAILABLE", true),
    (6, "NOT_LEADER_OR_FOLLOWER", true),
    (7, "REQUEST_TIMED_OUT", true),
    (10, "MESSAGE_TOO_LARGE", false),
    (17, "INVALID_TOPIC_EXCEPTION", false),
    (18, "RECORD_LIST_TOO_LARGE", false),
    (19, "NOT_ENOUGH_REPLICAS", true),
    (20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", true),
    (29, "TOPIC_AUTHORIZATION_FAILED", false),
    (31, "CLUSTER_AUTHORIZATION_FAILED", false),
    (42, "INVALID_REQUEST", false),
    (56, "KAFKA_STORAGE_ERROR", true),
];

fn codes_for(api_key: i16) -> &'static [i16] {
    match api_key {
        api::PRODUCE => &[6, 7, 19, 20, 56, 10, 18, 3, 29, -1],
        api::METADATA => &[5, 3, 17, 29, -1],
        api::DESCRIBE_CONFIGS => &[7, 3, 29, 31, 42, -1],
        _ => &[],
    }
}

/// Name of an error code, or `None` if it isn't in the catalog.
pub fn error_name(code: i16) -> Option<&'static str> {
    ERROR_CODES.iter().find(|(c, _, _)| *c == code).map(|(_, n, _)| *n)
}

/// Name of an API key, or `None` if it isn't in the catalog.
pub fn api_name(api_key: i16) -> Option<&'static str> {
    APIS.iter().find(|(k, _)| *k == api_key).map(|(_, n)| *n)
}

/// Look up an API key by its name, ignoring case.
pub fn api_key_by_name(name: &str) -> Option<i16> {
    APIS.iter()
        .find(|(_, n)| n.eq_ignore_ascii_case(name))
        .map(|(k, _)| *k)
}

/// The full catalog in display order.
pub fn catalog() -> Vec<ApiCatalogEntry> {
    APIS.iter()
        .map(|&(api_key, name)| {
            let error_codes: Vec<ErrorCodeInfo> = codes_for(api_key)
                .iter()
                .filter_map(|code| ERROR_CODES.iter().find(|(c, _, _)| c == code))
                .map(|&(code, name, retriable)| ErrorCodeInfo {
                    code,
                    name: name.to_string(),
                    retriable,
                })
                .collect();
            let supports_error = !error_codes.is_empty();
            ApiCatalogEntry {
                api_key,
                name: name.to_string(),
                supports_error,
                supports_topic: supports_error,
                error_codes,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_apis_come_first_and_support_errors() {
        let catalog = catalog();
        let first: Vec<&str> = catalog[..3].iter().map(|e| e.name.as_str()).collect();
        assert_eq!(first, ["DescribeConfigs", "Metadata", "Produce"]);
        assert!(catalog[..3].iter().all(|e| e.supports_error && e.supports_topic));
        assert!(catalog[3..].iter().all(|e| !e.supports_error));
    }

    #[test]
    fn every_offered_code_has_a_name() {
        for api in [api::PRODUCE, api::METADATA, api::DESCRIBE_CONFIGS] {
            for code in codes_for(api) {
                assert!(error_name(*code).is_some(), "code {code}");
            }
        }
    }

    #[test]
    fn api_lookup_ignores_case() {
        assert_eq!(api_key_by_name("produce"), Some(api::PRODUCE));
        assert_eq!(api_name(api::DESCRIBE_CONFIGS), Some("DescribeConfigs"));
        assert_eq!(api_key_by_name("Nope"), None);
    }
}
