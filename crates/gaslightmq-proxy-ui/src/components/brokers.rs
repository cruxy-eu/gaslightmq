//! One line per proxied broker, with buttons to set its state.

use dioxus::prelude::*;
use gaslightmq_proxy_api::{NodeInfo, NodeState};

use crate::state::Ctl;

const STATES: [(NodeState, &str); 3] = [
    (NodeState::Live, "live"),
    (NodeState::Unresponsive, "unresponsive"),
    (NodeState::Down, "down"),
];

#[component]
pub fn Brokers() -> Element {
    let state = use_context::<Ctl>().state;
    let nodes = state.nodes.read();

    rsx! {
        section {
            h2 { "Brokers" }
            if nodes.is_empty() {
                p { "No brokers known yet." }
            } else {
                table {
                    thead {
                        tr {
                            th { "Node" }
                            th { "Proxy port" }
                            th { "Upstream" }
                            th { "Connections" }
                            th { "State" }
                            th {}
                        }
                    }
                    tbody {
                        for node in nodes.iter() {
                            BrokerRow { key: "{node.node_id}", node: node.clone() }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn BrokerRow(node: NodeInfo) -> Element {
    let ctl = use_context::<Ctl>();
    let node_id = node.node_id;

    rsx! {
        tr {
            td { "{node_id}" }
            td { "{node.proxy_port}" }
            td { "{node.upstream}" }
            td { "{node.connections}" }
            td {
                {STATES.iter().find(|(s, _)| *s == node.state).map_or("?", |(_, l)| *l)}
                if let Some(error) = &node.error {
                    div { class: "error", "{error}" }
                }
            }
            td {
                for (target, label) in STATES {
                    button {
                        disabled: node.state == target,
                        onclick: move |_| ctl.set_node_state(node_id, target),
                        "{label}"
                    }
                }
            }
        }
    }
}
