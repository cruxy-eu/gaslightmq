//! The few browser facilities the UI needs, with inert native stand-ins so
//! the crate also type-checks with a plain `cargo check`.

/// Sleep for `ms` milliseconds.
#[cfg(target_arch = "wasm32")]
pub async fn sleep_ms(ms: u32) {
    gloo_timers::future::TimeoutFuture::new(ms).await;
}

/// `true` when the page was opened with `?demo`.
#[cfg(target_arch = "wasm32")]
pub fn is_demo() -> bool {
    web_sys::window()
        .and_then(|w| w.location().search().ok())
        .is_some_and(|q| {
            q.trim_start_matches('?')
                .split('&')
                .any(|kv| kv == "demo" || kv.starts_with("demo="))
        })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn is_demo() -> bool {
    false
}

/// `ws://{host}/api/ws`, or `wss://` when the page itself is served over TLS.
#[cfg(target_arch = "wasm32")]
pub fn ws_url() -> String {
    let location = web_sys::window().map(|w| w.location());
    let host = location
        .as_ref()
        .and_then(|l| l.host().ok())
        .unwrap_or_else(|| "localhost:9000".to_string());
    let scheme = match location.and_then(|l| l.protocol().ok()).as_deref() {
        Some("https:") => "wss",
        _ => "ws",
    };
    format!("{scheme}://{host}/api/ws")
}
