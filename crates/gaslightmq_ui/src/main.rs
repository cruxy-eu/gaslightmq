//! The web UI, launched in the browser. `just ui` builds it for the control server to serve.

/// Draws the UI in the page.
#[cfg(target_arch = "wasm32")]
fn main() {
    dioxus::launch(gaslightmq_ui::App);
}

/// A native build only checks the UI and runs the tests of what it knows: the UI draws in the browser alone.
#[cfg(not(target_arch = "wasm32"))]
fn main() {}
