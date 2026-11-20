//! One pad per proxied broker; clicking cycles live → unresponsive → down.

use dioxus::prelude::*;
use gaslightmq_proxy_api::{NodeInfo, NodeState, NodeTick};

use crate::state::Ctl;

/// Segments in the connection meter.
const METER_SEGMENTS: u32 = 8;

#[component]
pub fn BrokerStrip() -> Element {
    let state = use_context::<Ctl>().state;
    let tick = state.tick.read();
    let seq = tick.as_ref().map_or(0, |t| t.seq);

    rsx! {
        section { class: "brokers",
            for node in state.nodes.read().iter() {
                BrokerPad {
                    key: "{node.node_id}",
                    node: node.clone(),
                    tick: tick
                        .as_ref()
                        .and_then(|t| t.nodes.iter().find(|n| n.node_id == node.node_id))
                        .copied(),
                    seq,
                }
            }
        }
    }
}

#[component]
fn BrokerPad(node: NodeInfo, tick: Option<NodeTick>, seq: u64) -> Element {
    let ctl = use_context::<Ctl>();
    let (class, label) = match node.state {
        NodeState::Live => ("live", "LIVE"),
        NodeState::Unresponsive => ("unresponsive", "HANG"),
        NodeState::Down => ("down", "DOWN"),
    };
    let connections = tick.map_or(node.connections, |t| t.connections);
    let active = tick.is_some_and(|t| t.requests > 0);
    let lit = connections.min(METER_SEGMENTS);
    let NodeInfo { node_id, state, .. } = node;

    rsx! {
        button {
            class: "broker {class}",
            title: "node {node_id}: click to make it {state.next():?}",
            onclick: move |_| ctl.set_node_state(node_id, state.next()),
            div { class: "broker-top",
                span { class: "led",
                    for s in active.then_some(seq) {
                        span { key: "{s}", class: "led-flash" }
                    }
                }
                span { class: "broker-id", "NODE {node_id}" }
                span { class: "broker-state", "{label}" }
            }
            div { class: "broker-port", ":{node.proxy_port}" }
            div { class: "broker-upstream", "{node.upstream}" }
            div { class: "meter", title: "{connections} connections",
                for i in 0..METER_SEGMENTS {
                    span { class: if i < lit { "seg lit" } else { "seg" } }
                }
                span { class: "meter-count", "{connections}" }
            }
            if let Some(error) = &node.error {
                div { class: "broker-error", "{error}" }
            }
        }
    }
}
