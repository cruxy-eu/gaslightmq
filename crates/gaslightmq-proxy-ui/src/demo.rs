//! `?demo`: a fake proxy for trying the UI without a backend. Client
//! messages are echoed back as server messages.

use dioxus::prelude::*;
use futures::StreamExt;
use gaslightmq_proxy_api::{
    ApiRow, BootstrapInfo, ClientMsg, NodeInfo, NodeState, ServerMsg, Snapshot, catalog,
    catalog::api,
};

use crate::state::{AppState, Conn};

pub async fn run(mut state: AppState, mut rx: UnboundedReceiver<ClientMsg>) {
    state.conn.set(Conn::Demo);
    state.apply(ServerMsg::Snapshot(snapshot()));
    while let Some(msg) = rx.next().await {
        echo(state, msg);
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
