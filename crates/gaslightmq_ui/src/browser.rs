//! What the UI takes from the browser: where the control plane is, and a timer.
//!
//! The UI draws in the browser alone. A native build only checks the components and runs the tests of what the UI
//! knows: it never draws, so it never needs the address of a page, and never waits.

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_client::ControlPlaneAddress;

/// The control plane that serves the page: the control server serves the UI and `/api/…` on one origin.
#[cfg(target_arch = "wasm32")]
pub fn control_plane() -> Option<ControlPlaneAddress> {
    let origin = web_sys::window()?.location().origin().ok()?;
    origin.parse().ok()
}

/// The control plane on its default address, for a build that never draws.
#[cfg(not(target_arch = "wasm32"))]
pub fn control_plane() -> Option<ControlPlaneAddress> {
    "http://127.0.0.1:8080".parse().ok()
}

/// Waits for `span` on a timer of the browser.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(span: TimeSpan) {
    let millis = u32::try_from(span.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

/// Waits forever: a build that never draws has no timer.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(span: TimeSpan) {
    let _ = span;
    futures::future::pending::<()>().await;
}
