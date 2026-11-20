//! Live control surface for the GaslightMQ Kafka chaos proxy.
//!
//! Built for the browser with `dx build --platform web`. A plain
//! `cargo check` compiles it natively so type drift against
//! `gaslightmq-proxy-api` is caught by normal workspace checks.

mod components;
mod demo;
mod platform;
mod state;
mod ws;

use dioxus::prelude::*;

use components::{broker::BrokerStrip, grid::PadGrid, transport::TransportBar};
use state::{AppState, Ctl};

const MAIN_CSS: Asset = asset!("/assets/main.css");

#[cfg(target_arch = "wasm32")]
fn main() {
    dioxus::launch(App);
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    // Referencing the root keeps the component tree type-checked natively.
    let _app: fn() -> Element = App;
    eprintln!("gaslightmq-proxy-ui runs in the browser: dx build --platform web");
}

#[component]
fn App() -> Element {
    let state = use_hook(AppState::create);
    let tx = use_coroutine(move |rx| ws::run(state, rx));
    use_context_provider(|| Ctl { state, tx });

    let live = state.conn.read().is_live();
    rsx! {
        document::Stylesheet { href: MAIN_CSS }
        document::Title { "GaslightMQ Proxy" }
        div { class: "surface", class: if !live { "offline" },
            TransportBar {}
            BrokerStrip {}
            PadGrid {}
        }
    }
}
