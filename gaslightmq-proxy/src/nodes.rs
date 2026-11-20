//! Listeners standing in for upstream brokers.
//!
//! Plaintext Kafka has no way to tell which broker a client meant to reach,
//! so every upstream node gets its own proxy listener. Nodes are discovered
//! from Metadata responses, and their listener is bound before the response
//! that advertises it reaches the client.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use gaslightmq_proxy_api::{NodeInfo, NodeState, NodeTick};
use kafka_protocol::messages::BrokerId;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::protocol::StrBytes;
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::NodePorts;
use crate::rewrite::Advertise;
use crate::shared::Shared;

/// Where a proxy listener forwards its connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    Bootstrap,
    Node(i32),
}

pub(crate) struct NodeEntry {
    pub node_id: i32,
    pub upstream: String,
    pub proxy_port: u16,
    pub state: watch::Sender<NodeState>,
    pub conns: Arc<AtomicU32>,
    pub requests: Arc<AtomicU64>,
    accept: Option<JoinHandle<()>>,
    pub error: Option<String>,
}

impl NodeEntry {
    pub fn info(&self) -> NodeInfo {
        NodeInfo {
            node_id: self.node_id,
            upstream: self.upstream.clone(),
            proxy_port: self.proxy_port,
            state: *self.state.borrow(),
            connections: self.conns.load(Ordering::Relaxed),
            error: self.error.clone(),
        }
    }
}

/// Everything a connection needs to know about its target.
pub(crate) struct TargetInfo {
    pub upstream: String,
    pub state: watch::Receiver<NodeState>,
    pub conns: Arc<AtomicU32>,
    pub requests: Option<Arc<AtomicU64>>,
}

pub(crate) fn host_port(host: &str, port: i32) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub(crate) fn bind_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(1024)
}

impl Shared {
    /// Make sure upstream node `node_id` has a proxy listener and return its
    /// port. Updates the upstream address if the cluster moved the node.
    pub fn ensure_node(self: &Arc<Self>, node_id: i32, host: &str, port: i32) -> u16 {
        let upstream = host_port(host, port);
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(entry) = nodes.get_mut(&node_id) {
            if entry.upstream == upstream {
                return entry.proxy_port;
            }
            info!(node_id, from = %entry.upstream, to = %upstream, "upstream node moved");
            entry.upstream = upstream;
            let port = entry.proxy_port;
            drop(nodes);
            self.publish_nodes();
            return port;
        }

        let wanted = match self.config.node_ports {
            NodePorts::Sequential(base) => base.saturating_add(nodes.len() as u16),
            NodePorts::Ephemeral => 0,
        };
        let ip = self.config.bind.ip();
        let listener = bind_listener(SocketAddr::new(ip, wanted)).or_else(|e| {
            warn!(node_id, port = wanted, error = %e, "port unavailable, using an ephemeral port");
            bind_listener(SocketAddr::new(ip, 0))
        });
        let (proxy_port, accept, error) = match listener {
            Ok(l) => {
                let port = l.local_addr().map(|a| a.port()).unwrap_or(wanted);
                (port, Some(self.spawn_accept(l, Target::Node(node_id))), None)
            }
            Err(e) => {
                warn!(node_id, error = %e, "failed to bind node listener");
                (wanted, None, Some(e.to_string()))
            }
        };
        info!(node_id, %upstream, proxy_port, "discovered upstream node");
        nodes.insert(
            node_id,
            NodeEntry {
                node_id,
                upstream,
                proxy_port,
                state: watch::Sender::new(NodeState::Live),
                conns: Arc::default(),
                requests: Arc::default(),
                accept,
                error,
            },
        );
        drop(nodes);
        self.publish_nodes();
        proxy_port
    }

    fn spawn_accept(self: &Arc<Self>, listener: TcpListener, target: Target) -> JoinHandle<()> {
        tokio::spawn(crate::accept_loop(self.clone(), listener, target))
    }

    pub fn node_infos(&self) -> Vec<NodeInfo> {
        self.nodes.lock().unwrap().values().map(NodeEntry::info).collect()
    }

    pub fn node_ticks(&self, before: &mut Vec<(i32, u64)>) -> Vec<NodeTick> {
        let nodes = self.nodes.lock().unwrap();
        let ticks = nodes
            .values()
            .map(|n| {
                let total = n.requests.load(Ordering::Relaxed);
                let prev = before.iter().find(|(id, _)| *id == n.node_id).map_or(0, |(_, r)| *r);
                NodeTick {
                    node_id: n.node_id,
                    connections: n.conns.load(Ordering::Relaxed),
                    requests: total - prev,
                }
            })
            .collect();
        *before = nodes.values().map(|n| (n.node_id, n.requests.load(Ordering::Relaxed))).collect();
        ticks
    }

    /// Switch a node between live, unresponsive and down.
    pub async fn set_node_state(self: &Arc<Self>, node_id: i32, state: NodeState) -> anyhow::Result<()> {
        // Stop accepting first, and wait until the listener is really gone,
        // so a quick Down → Live can rebind the same port.
        let stopped = {
            let mut nodes = self.nodes.lock().unwrap();
            let entry = nodes.get_mut(&node_id).ok_or_else(|| anyhow::anyhow!("unknown node {node_id}"))?;
            if state == NodeState::Down { entry.accept.take() } else { None }
        };
        if let Some(handle) = stopped {
            handle.abort();
            let _ = handle.await;
        }

        let result = {
            let mut nodes = self.nodes.lock().unwrap();
            let entry = nodes.get_mut(&node_id).ok_or_else(|| anyhow::anyhow!("unknown node {node_id}"))?;
            let mut result = Ok(());
            if state != NodeState::Down && entry.accept.is_none() {
                let addr = SocketAddr::new(self.config.bind.ip(), entry.proxy_port);
                match bind_listener(addr) {
                    Ok(listener) => {
                        entry.accept = Some(self.spawn_accept(listener, Target::Node(node_id)));
                        entry.error = None;
                    }
                    Err(e) => {
                        let msg = format!("cannot rebind {addr}: {e}");
                        entry.error = Some(msg.clone());
                        result = Err(anyhow::anyhow!(msg));
                    }
                }
            }
            if result.is_ok() {
                entry.state.send_replace(state);
                info!(node_id, ?state, "node state changed");
            }
            result
        };
        self.publish_nodes();
        result
    }

    pub fn target_info(&self, target: Target) -> Option<TargetInfo> {
        let nodes = self.nodes.lock().unwrap();
        match target {
            Target::Node(id) => nodes.get(&id).map(|n| TargetInfo {
                upstream: n.upstream.clone(),
                state: n.state.subscribe(),
                conns: n.conns.clone(),
                requests: Some(n.requests.clone()),
            }),
            Target::Bootstrap => {
                // The bootstrap address usually is one of the brokers. If so,
                // it goes down together with that broker.
                let node = nodes.values().find(|n| n.upstream == self.config.upstream);
                Some(TargetInfo {
                    upstream: self.config.upstream.clone(),
                    state: node.map_or_else(|| self.bootstrap_state.subscribe(), |n| n.state.subscribe()),
                    conns: self.bootstrap_conns.clone(),
                    requests: node.map(|n| n.requests.clone()),
                })
            }
        }
    }

    /// Brokers as clients should see them: every known node at its proxy
    /// listener.
    pub fn advertised_brokers(&self) -> Vec<MetadataResponseBroker> {
        let host = StrBytes::from_string(self.advertised_host.clone());
        self.nodes
            .lock()
            .unwrap()
            .values()
            .map(|n| {
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(n.node_id))
                    .with_host(host.clone())
                    .with_port(n.proxy_port as i32)
            })
            .collect()
    }

    /// Abort all node accept loops (on shutdown).
    pub fn stop_nodes(&self) {
        for entry in self.nodes.lock().unwrap().values_mut() {
            if let Some(handle) = entry.accept.take() {
                handle.abort();
            }
        }
    }
}

impl Advertise for Arc<Shared> {
    fn advertise(&self, node_id: i32, host: &str, port: i32) -> (StrBytes, i32) {
        let proxy_port = self.ensure_node(node_id, host, port);
        (StrBytes::from_string(self.advertised_host.clone()), proxy_port as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_port_brackets_ipv6() {
        assert_eq!(host_port("kafka", 9092), "kafka:9092");
        assert_eq!(host_port("::1", 9092), "[::1]:9092");
    }
}
