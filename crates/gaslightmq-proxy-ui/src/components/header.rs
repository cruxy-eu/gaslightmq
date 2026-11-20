//! Connection status, bootstrap listener and Clear all.

use dioxus::prelude::*;

use crate::state::{Conn, Ctl};

#[component]
pub fn Header() -> Element {
    let ctl = use_context::<Ctl>();
    let state = ctl.state;
    let conn = *state.conn.read();
    let status = match conn {
        Conn::Connected => "connected".to_string(),
        Conn::Demo => "demo".to_string(),
        Conn::Connecting => "connecting…".to_string(),
        Conn::Retrying { remaining_ms } => {
            format!("disconnected, retrying in {:.1}s", f64::from(remaining_ms) / 1000.0)
        }
    };

    rsx! {
        header {
            h1 { "GaslightMQ proxy" }
            p { "Status: {status}" }
            if let Some(b) = state.bootstrap.read().as_ref() {
                p { "Bootstrap: :{b.proxy_port} → {b.upstream} ({b.connections} connections)" }
            }
            button { disabled: !conn.is_live(), onclick: move |_| ctl.clear_all(), "Clear all" }
        }
    }
}
