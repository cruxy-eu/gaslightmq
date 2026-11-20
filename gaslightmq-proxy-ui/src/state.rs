//! Client-side mirror of the proxy state, shared through context.

use std::collections::HashMap;

use dioxus::prelude::*;
use gaslightmq_proxy_api::{
    ApiCatalogEntry, ApiRow, BootstrapInfo, ClientMsg, NodeInfo, NodeState, ServerMsg, StatsTick,
};

/// Weight of the newest sample in the smoothed req/s readouts.
const RATE_SMOOTHING: f64 = 0.35;

/// State of the control-plane connection.
// Only the wasm socket loop constructs the connected states.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Conn {
    /// Socket opened, waiting for the snapshot.
    Connecting,
    Connected,
    /// Disconnected; the next attempt starts in `remaining_ms`.
    Retrying { remaining_ms: u32 },
    /// `?demo`: no socket, synthetic data.
    Demo,
}

impl Conn {
    pub fn is_live(self) -> bool {
        matches!(self, Self::Connected | Self::Demo)
    }
}

/// Everything the UI renders. Server messages are the source of truth; local
/// edits are applied optimistically through [`Ctl`].
#[derive(Clone, Copy)]
pub struct AppState {
    pub conn: Signal<Conn>,
    pub bootstrap: Signal<Option<BootstrapInfo>>,
    pub catalog: Signal<Vec<ApiCatalogEntry>>,
    /// One row per catalog entry, in catalog order.
    pub rows: Signal<Vec<ApiRow>>,
    pub nodes: Signal<Vec<NodeInfo>>,
    pub tick: Signal<Option<StatsTick>>,
    /// Smoothed requests per second, by API key.
    pub rates: Signal<HashMap<i16, f64>>,
}

impl AppState {
    /// Must be called inside a component so the signals have an owner.
    pub fn create() -> Self {
        Self {
            conn: Signal::new(Conn::Connecting),
            bootstrap: Signal::new(None),
            catalog: Signal::new(Vec::new()),
            rows: Signal::new(Vec::new()),
            nodes: Signal::new(Vec::new()),
            tick: Signal::new(None),
            rates: Signal::new(HashMap::new()),
        }
    }

    pub fn apply(mut self, msg: ServerMsg) {
        match msg {
            ServerMsg::Snapshot(snapshot) => {
                let rows = snapshot
                    .catalog
                    .iter()
                    .map(|entry| {
                        snapshot
                            .rows
                            .iter()
                            .find(|r| r.api_key == entry.api_key)
                            .cloned()
                            .unwrap_or_else(|| ApiRow::new(entry.api_key))
                    })
                    .collect();
                self.rows.set(rows);
                self.catalog.set(snapshot.catalog);
                self.nodes.set(snapshot.nodes);
                self.bootstrap.set(Some(snapshot.bootstrap));
                self.rates.write().clear();
            }
            ServerMsg::RowChanged(row) => {
                // Peek first: echoes of our own edits shouldn't re-render.
                let index = self
                    .rows
                    .peek()
                    .iter()
                    .position(|r| r.api_key == row.api_key && *r != row);
                if let Some(index) = index {
                    self.rows.write()[index] = row;
                }
            }
            ServerMsg::RowsCleared => {
                for row in self.rows.write().iter_mut() {
                    *row = ApiRow::new(row.api_key);
                }
            }
            ServerMsg::Nodes { nodes } => self.nodes.set(nodes),
            ServerMsg::Stats(tick) => {
                self.update_rates(&tick);
                self.tick.set(Some(tick));
            }
        }
    }

    fn update_rates(&mut self, tick: &StatsTick) {
        let interval = f64::from(tick.interval_ms.max(1));
        let catalog = self.catalog.peek();
        let mut rates = self.rates.write();
        for entry in catalog.iter() {
            let requests = tick
                .apis
                .iter()
                .find(|a| a.api_key == entry.api_key)
                .map_or(0, |a| a.requests);
            let instant = requests as f64 * 1000.0 / interval;
            let prev = rates.get(&entry.api_key).copied().unwrap_or(instant);
            let smoothed = prev + (instant - prev) * RATE_SMOOTHING;
            rates.insert(entry.api_key, if smoothed < 0.05 { 0.0 } else { smoothed });
        }
    }
}

/// Sends commands to the proxy, applying them locally first so the UI
/// reacts immediately. The server's broadcast confirms or corrects them.
#[derive(Clone, Copy)]
pub struct Ctl {
    pub state: AppState,
    pub tx: Coroutine<ClientMsg>,
}

impl Ctl {
    /// Edit the row for `api_key` in place and send the result.
    pub fn update_row(self, api_key: i16, edit: impl FnOnce(&mut ApiRow)) {
        let Some(mut row) = self
            .state
            .rows
            .peek()
            .iter()
            .find(|r| r.api_key == api_key)
            .cloned()
        else {
            return;
        };
        edit(&mut row);
        self.state.apply(ServerMsg::RowChanged(row.clone()));
        self.tx.send(ClientMsg::SetRow(row));
    }

    pub fn set_node_state(self, node_id: i32, state: NodeState) {
        let mut nodes = self.state.nodes;
        if let Some(node) = nodes.write().iter_mut().find(|n| n.node_id == node_id) {
            node.state = state;
        }
        self.tx.send(ClientMsg::SetNodeState { node_id, state });
    }

    pub fn clear_all(self) {
        self.state.apply(ServerMsg::RowsCleared);
        self.tx.send(ClientMsg::ClearAll);
    }
}
