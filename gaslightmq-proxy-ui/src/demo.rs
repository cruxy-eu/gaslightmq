//! `?demo`: a fake proxy for checking the look without a backend.
//!
//! Echoes client messages back as server messages and synthesizes a stats
//! tick every 100 ms.

use dioxus::prelude::*;
use futures::{FutureExt, StreamExt};
use gaslightmq_proxy_api::{
    ApiRow, ApiTick, BootstrapInfo, ClientMsg, NodeInfo, NodeState, NodeTick, ServerMsg, Snapshot,
    StatsTick, catalog,
    catalog::api,
};

use crate::{
    platform,
    state::{AppState, Conn},
};

const TICK_MS: u32 = 100;

pub async fn run(mut state: AppState, mut rx: UnboundedReceiver<ClientMsg>) {
    state.conn.set(Conn::Demo);
    state.apply(ServerMsg::Snapshot(snapshot()));
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut seq = 0;
    loop {
        while let Some(Some(msg)) = rx.next().now_or_never() {
            echo(state, msg);
        }
        platform::sleep_ms(TICK_MS).await;
        seq += 1;
        let tick = synth_tick(
            seq,
            &state.rows.peek(),
            &state.nodes.peek(),
            &mut rng,
        );
        state.apply(ServerMsg::Stats(tick));
    }
}

fn echo(state: AppState, msg: ClientMsg) {
    match msg {
        ClientMsg::SetRow(row) => state.apply(ServerMsg::RowChanged(row)),
        ClientMsg::ClearAll => state.apply(ServerMsg::RowsCleared),
        ClientMsg::SetNodeState { node_id, state: node_state } => {
            let mut nodes = state.nodes.peek().clone();
            if let Some(node) = nodes.iter_mut().find(|n| n.node_id == node_id) {
                node.state = node_state;
                node.error = (node_state == NodeState::Down && node_id == 3)
                    .then(|| "demo: address already in use".to_string());
                if node_state == NodeState::Down {
                    node.connections = 0;
                }
            }
            state.apply(ServerMsg::Nodes { nodes });
        }
    }
}

fn snapshot() -> Snapshot {
    let mut describe = ApiRow::new(api::DESCRIBE_CONFIGS);
    describe.delay.enabled = true;
    describe.delay.ms = 2_000;
    describe.delay.probability = 25;

    let mut produce = ApiRow::new(api::PRODUCE);
    produce.topic = Some("restore-events".to_string());
    produce.error.enabled = true;
    produce.error.code = 6;
    produce.error.probability = 10;

    let mut fetch = ApiRow::new(api::FETCH);
    fetch.blackhole.enabled = true;
    fetch.blackhole.probability = 5;

    let nodes = (1..=3)
        .map(|id| NodeInfo {
            node_id: id,
            upstream: format!("kafka-{id}.internal:9092"),
            proxy_port: 9092 + id as u16,
            state: NodeState::Live,
            connections: 2,
            error: None,
        })
        .collect();

    Snapshot {
        bootstrap: BootstrapInfo {
            upstream: "kafka-1.internal:9092".to_string(),
            proxy_port: 9092,
            connections: 1,
        },
        rows: vec![produce, fetch, describe],
        nodes,
        catalog: catalog(),
    }
}

/// Mean requests per second for an API under the fake workload.
fn base_rate(api_key: i16) -> f64 {
    match api_key {
        api::PRODUCE => 180.0,
        api::FETCH => 60.0,
        api::METADATA => 4.0,
        api::DESCRIBE_CONFIGS => 2.0,
        api::HEARTBEAT => 3.0,
        api::OFFSET_COMMIT => 1.0,
        api::LIST_OFFSETS => 0.5,
        api::API_VERSIONS => 0.3,
        _ => 0.0,
    }
}

fn synth_tick(seq: u64, rows: &[ApiRow], nodes: &[NodeInfo], rng: &mut Rng) -> StatsTick {
    let live = nodes.iter().filter(|n| n.state == NodeState::Live).count() as f64;
    let health = if nodes.is_empty() { 1.0 } else { live / nodes.len() as f64 };
    let apis: Vec<ApiTick> = rows
        .iter()
        .filter_map(|row| {
            let mean = base_rate(row.api_key) * f64::from(TICK_MS) / 1000.0 * health;
            let requests = rng.poisson(mean);
            if requests == 0 {
                return None;
            }
            let fires = |rng: &mut Rng, on: bool, p: u8| {
                if !on {
                    return 0;
                }
                (0..requests).filter(|_| rng.chance(f64::from(p) / 100.0)).count() as u64
            };
            Some(ApiTick {
                api_key: row.api_key,
                requests,
                errors: fires(rng, row.error.enabled, row.error.probability),
                delays: fires(rng, row.delay.enabled, row.delay.probability),
                blackholes: fires(rng, row.blackhole.enabled, row.blackhole.probability),
            })
        })
        .collect();
    let total: u64 = apis.iter().map(|a| a.requests).sum();
    let nodes = nodes
        .iter()
        .map(|n| {
            let live = n.state == NodeState::Live;
            NodeTick {
                node_id: n.node_id,
                connections: match n.state {
                    NodeState::Down => 0,
                    _ => n.connections,
                },
                requests: if live && rng.chance(0.7) {
                    (total as f64 / live_or_one(nodes)) as u64
                } else {
                    0
                },
            }
        })
        .collect();
    StatsTick { seq, interval_ms: TICK_MS, bootstrap_connections: 1, apis, nodes }
}

fn live_or_one(nodes: &[NodeInfo]) -> f64 {
    nodes.iter().filter(|n| n.state == NodeState::Live).count().max(1) as f64
}

/// xorshift64*: deterministic and plenty for fake traffic.
struct Rng(u64);

impl Rng {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, p: f64) -> bool {
        self.next_f64() < p
    }

    /// Knuth's method; fine for the small means used here.
    fn poisson(&mut self, mean: f64) -> u64 {
        if mean <= 0.0 {
            return 0;
        }
        let limit = (-mean).exp();
        let mut k = 0;
        let mut p = 1.0;
        loop {
            p *= self.next_f64();
            if p <= limit {
                return k;
            }
            k += 1;
        }
    }
}
