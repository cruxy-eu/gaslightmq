use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use gaslightmq_proxy_api::{ApiRow, BootstrapInfo, NodeState, RuleSet, ServerMsg, Snapshot};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{broadcast, watch};

use crate::config::ProxyConfig;
use crate::entity::ClusterInfo;
use crate::nodes::NodeEntry;
use crate::stats::Stats;

/// Cluster-wide details learned from upstream Metadata responses.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClusterMeta {
    pub cluster_id: Option<StrBytes>,
    pub controller_id: i32,
}

/// State shared by listeners, connections and the control plane.
pub(crate) struct Shared {
    pub config: ProxyConfig,
    pub advertised_host: String,
    pub bootstrap_port: u16,
    pub bootstrap_conns: Arc<AtomicU32>,
    /// Used for bootstrap connections when the bootstrap address isn't one
    /// of the known brokers. Always live.
    pub bootstrap_state: watch::Sender<NodeState>,
    pub rules: watch::Sender<Arc<RuleSet>>,
    pub nodes: Mutex<BTreeMap<i32, NodeEntry>>,
    pub cluster: Mutex<ClusterMeta>,
    pub stats: Stats,
    pub events: broadcast::Sender<ServerMsg>,
    pub shutdown: watch::Receiver<bool>,
    conn_seq: AtomicU64,
}

impl Shared {
    pub fn new(config: ProxyConfig, bootstrap_port: u16, shutdown: watch::Receiver<bool>) -> Self {
        let rules = Arc::new(config.rules.clone());
        Self {
            advertised_host: config.advertised_host(),
            config,
            bootstrap_port,
            bootstrap_conns: Arc::default(),
            bootstrap_state: watch::Sender::new(NodeState::Live),
            rules: watch::Sender::new(rules),
            nodes: Mutex::default(),
            cluster: Mutex::default(),
            stats: Stats::default(),
            events: broadcast::channel(1024).0,
            shutdown,
            conn_seq: AtomicU64::new(0),
        }
    }

    /// Seed for a new connection's RNG, derived from the configured seed so
    /// seeded runs are reproducible.
    pub fn next_conn_seed(&self) -> Option<u64> {
        let n = self.conn_seq.fetch_add(1, Ordering::Relaxed);
        self.config.seed.map(|s| s.wrapping_add(n))
    }

    pub fn publish(&self, msg: ServerMsg) {
        // No subscribers is fine.
        let _ = self.events.send(msg);
    }

    pub fn publish_nodes(&self) {
        self.publish(ServerMsg::Nodes { nodes: self.node_infos() });
    }

    pub fn set_row(&self, row: ApiRow) {
        self.rules.send_modify(|rules| Arc::make_mut(rules).upsert(row.clone()));
        self.publish(ServerMsg::RowChanged(row));
    }

    pub fn set_rules(&self, rules: RuleSet) {
        self.rules.send_replace(Arc::new(rules.clone()));
        self.publish(ServerMsg::RowsCleared);
        for row in rules.rows {
            self.publish(ServerMsg::RowChanged(row));
        }
    }

    pub fn cluster_info(&self) -> ClusterInfo {
        let meta = self.cluster.lock().unwrap().clone();
        ClusterInfo {
            brokers: self.advertised_brokers(),
            cluster_id: meta.cluster_id,
            controller_id: meta.controller_id,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            bootstrap: BootstrapInfo {
                upstream: self.config.upstream.clone(),
                proxy_port: self.bootstrap_port,
                connections: self.bootstrap_conns.load(Ordering::Relaxed),
            },
            rows: self.rules.borrow().rows.clone(),
            nodes: self.node_infos(),
            catalog: gaslightmq_proxy_api::catalog(),
        }
    }
}
