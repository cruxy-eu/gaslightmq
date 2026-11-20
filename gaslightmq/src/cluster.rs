//! The broker's view of the cluster it advertises in Metadata responses.
//!
//! A single broker advertises itself as node 0. Several in-process brokers
//! can pretend to be one multi-node cluster by sharing a view that lists
//! all of them, see [`crate::Broker::set_cluster`].

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl Node {
    pub fn new(node_id: i32, addr: SocketAddr) -> Self {
        Self { node_id, host: addr.ip().to_string(), port: addr.port() as i32 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterView {
    /// Node id of the broker holding this view.
    pub node_id: i32,
    /// All nodes in the cluster, including this broker.
    pub nodes: Vec<Node>,
}

impl ClusterView {
    /// A cluster consisting of just this broker as node 0.
    pub fn single(addr: SocketAddr) -> Self {
        Self { node_id: 0, nodes: vec![Node::new(0, addr)] }
    }

    /// This broker's own entry.
    pub fn this_node(&self) -> &Node {
        self.nodes
            .iter()
            .find(|n| n.node_id == self.node_id)
            .unwrap_or(&self.nodes[0])
    }

    /// The node leading `partition`, assigned round-robin over node ids.
    pub fn leader_for(&self, partition: i32) -> i32 {
        let mut ids: Vec<i32> = self.nodes.iter().map(|n| n.node_id).collect();
        ids.sort_unstable();
        ids[partition.rem_euclid(ids.len() as i32) as usize]
    }

    /// The lowest node id, advertised as controller.
    pub fn controller_id(&self) -> i32 {
        self.nodes.iter().map(|n| n.node_id).min().unwrap_or(self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three_nodes() -> ClusterView {
        ClusterView {
            node_id: 1,
            nodes: vec![
                Node { node_id: 2, host: "h".into(), port: 3 },
                Node { node_id: 0, host: "h".into(), port: 1 },
                Node { node_id: 1, host: "h".into(), port: 2 },
            ],
        }
    }

    #[test]
    fn leaders_are_round_robin_over_sorted_ids() {
        let view = three_nodes();
        let leaders: Vec<i32> = (0..4).map(|p| view.leader_for(p)).collect();
        assert_eq!(leaders, vec![0, 1, 2, 0]);
    }

    #[test]
    fn this_node_and_controller() {
        let view = three_nodes();
        assert_eq!(view.this_node().port, 2);
        assert_eq!(view.controller_id(), 0);
    }
}
