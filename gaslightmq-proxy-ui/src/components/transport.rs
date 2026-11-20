//! Top bar: connection LED, title, bootstrap address and CLEAR ALL.

use dioxus::prelude::*;

use crate::state::{Conn, Ctl};

#[component]
pub fn TransportBar() -> Element {
    let ctl = use_context::<Ctl>();
    let state = ctl.state;
    let conn = *state.conn.read();
    let armed = state.rows.read().iter().filter(|r| r.is_active()).count();

    let (led, status) = match conn {
        Conn::Connected => ("green", "LIVE".to_string()),
        Conn::Demo => ("green", "DEMO".to_string()),
        Conn::Connecting => ("amber", "CONNECTING".to_string()),
        Conn::Retrying { remaining_ms } => (
            "red",
            format!("RETRY {:.1}s", f64::from(remaining_ms) / 1000.0),
        ),
    };

    rsx! {
        header { class: "transport",
            div { class: "conn",
                span { class: "conn-led {led}" }
                span { class: "conn-status", "{status}" }
            }
            h1 { class: "title", "GASLIGHTMQ PROXY" }
            if let Some(b) = state.bootstrap.read().as_ref() {
                div { class: "bootstrap", title: "bootstrap listener",
                    span { class: "port", ":{b.proxy_port}" }
                    span { class: "arrow", "→" }
                    span { class: "upstream", "{b.upstream}" }
                    span { class: "conns", "{b.connections} conn" }
                }
            }
            div { class: "spacer" }
            div { class: "armed", class: if armed > 0 { "hot" }, "{armed} ARMED" }
            button {
                class: "clear-all",
                disabled: !conn.is_live(),
                onclick: move |_| ctl.clear_all(),
                "CLEAR ALL"
            }
        }
    }
}
