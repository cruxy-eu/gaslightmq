use serde::{Deserialize, Deserializer, Serialize};

use crate::catalog;

/// Probability in percent, `0..=100`.
pub type Percent = u8;

/// A fault toggle with a firing probability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Pad {
    pub enabled: bool,
    pub probability: Percent,
}

impl Default for Pad {
    fn default() -> Self {
        Self { enabled: false, probability: 100 }
    }
}

/// Answer with a synthesized error response instead of forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorPad {
    pub enabled: bool,
    pub probability: Percent,
    /// Kafka error code applied to every topic, partition or resource.
    pub code: i16,
}

impl Default for ErrorPad {
    fn default() -> Self {
        Self { enabled: false, probability: 100, code: -1 }
    }
}

/// Hold the response back for `ms` before returning it to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DelayPad {
    pub enabled: bool,
    pub probability: Percent,
    pub ms: u64,
}

impl Default for DelayPad {
    fn default() -> Self {
        Self { enabled: false, probability: 100, ms: 500 }
    }
}

/// The fault configuration of one Kafka API: one row of the pad grid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRow {
    /// Kafka API key. In YAML, the API name (`Produce`) works too.
    #[serde(alias = "api", deserialize_with = "deserialize_api_key")]
    pub api_key: i16,
    /// Only requests touching this topic are faulted. Only honoured for APIs
    /// whose catalog entry has `supports_topic`.
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub error: ErrorPad,
    #[serde(default)]
    pub delay: DelayPad,
    #[serde(default)]
    pub blackhole: Pad,
}

impl ApiRow {
    pub fn new(api_key: i16) -> Self {
        Self {
            api_key,
            topic: None,
            error: ErrorPad::default(),
            delay: DelayPad::default(),
            blackhole: Pad::default(),
        }
    }

    /// Whether any pad on this row can fire.
    pub fn is_active(&self) -> bool {
        (self.error.enabled && self.error.probability > 0)
            || (self.delay.enabled && self.delay.probability > 0)
            || (self.blackhole.enabled && self.blackhole.probability > 0)
    }
}

/// A set of rows, e.g. loaded with `gmq proxy --rules faults.yaml`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleSet {
    #[serde(default)]
    pub rows: Vec<ApiRow>,
}

impl RuleSet {
    pub fn row(&self, api_key: i16) -> Option<&ApiRow> {
        self.rows.iter().find(|r| r.api_key == api_key)
    }

    /// Insert or replace the row for `row.api_key`, keeping rows sorted by
    /// api key. Rows equal to the default are removed; anything else is
    /// kept, so knob settings on disabled pads survive.
    pub fn upsert(&mut self, row: ApiRow) {
        self.rows.retain(|r| r.api_key != row.api_key);
        if row != ApiRow::new(row.api_key) {
            let pos = self.rows.partition_point(|r| r.api_key < row.api_key);
            self.rows.insert(pos, row);
        }
    }
}

fn deserialize_api_key<'de, D: Deserializer<'de>>(d: D) -> Result<i16, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum KeyOrName {
        Key(i16),
        Name(String),
    }
    match KeyOrName::deserialize(d)? {
        KeyOrName::Key(k) => Ok(k),
        KeyOrName::Name(name) => catalog::api_key_by_name(&name)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown Kafka API `{name}`"))),
    }
}

/// What a proxied broker does with client traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// Traffic flows normally (subject to the pad grid).
    #[default]
    Live,
    /// Connections are accepted but nothing is read or answered.
    Unresponsive,
    /// The listener is closed (connection refused) and existing
    /// connections are dropped.
    Down,
}

impl NodeState {
    /// The next state when cycling a broker pad.
    pub fn next(self) -> Self {
        match self {
            Self::Live => Self::Unresponsive,
            Self::Unresponsive => Self::Down,
            Self::Down => Self::Live,
        }
    }
}

/// An upstream broker and the proxy listener standing in for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: i32,
    /// Upstream `host:port` as advertised by the cluster.
    pub upstream: String,
    /// Port clients are pointed at for this node.
    pub proxy_port: u16,
    pub state: NodeState,
    pub connections: u32,
    /// Last listener error, e.g. when the port could not be rebound.
    pub error: Option<String>,
}

/// The listener clients bootstrap from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapInfo {
    pub upstream: String,
    pub proxy_port: u16,
    pub connections: u32,
}

/// Full proxy state, sent when a UI connects and returned by `GET /api/state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub bootstrap: BootstrapInfo,
    pub rows: Vec<ApiRow>,
    pub nodes: Vec<NodeInfo>,
    pub catalog: Vec<catalog::ApiCatalogEntry>,
}

/// Activity during one stats tick, for one API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ApiTick {
    pub api_key: i16,
    pub requests: u64,
    pub errors: u64,
    pub delays: u64,
    pub blackholes: u64,
}

/// Activity during one stats tick, for one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeTick {
    pub node_id: i32,
    pub connections: u32,
    pub requests: u64,
}

/// Counter deltas since the previous tick. Only APIs with activity are listed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StatsTick {
    pub seq: u64,
    pub interval_ms: u32,
    /// Open connections on the bootstrap listener.
    pub bootstrap_connections: u32,
    pub apis: Vec<ApiTick>,
    pub nodes: Vec<NodeTick>,
}

/// Request body for `PUT /api/nodes/{node_id}/state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetNodeState {
    pub state: NodeState,
}
