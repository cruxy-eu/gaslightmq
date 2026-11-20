//! The coroutine that owns the control-plane WebSocket.

use dioxus::prelude::*;
use gaslightmq_proxy_api::ClientMsg;

use crate::{demo, platform, state::AppState};

/// Run the control connection: the real socket, or the demo feed with `?demo`.
/// Outgoing [`ClientMsg`]s arrive on `rx`.
pub async fn run(state: AppState, rx: UnboundedReceiver<ClientMsg>) {
    if platform::is_demo() {
        demo::run(state, rx).await;
    } else {
        connect_loop(state, rx).await;
    }
}

/// Native builds only exist for type checking; there is no socket to open.
#[cfg(not(target_arch = "wasm32"))]
async fn connect_loop(_state: AppState, _rx: UnboundedReceiver<ClientMsg>) {}

#[cfg(target_arch = "wasm32")]
async fn connect_loop(mut state: AppState, mut rx: UnboundedReceiver<ClientMsg>) {
    use dioxus::logger::tracing::warn;
    use futures::{FutureExt, SinkExt, StreamExt, select};
    use gaslightmq_proxy_api::ServerMsg;
    use gloo_net::websocket::{Message, futures::WebSocket};

    use crate::state::Conn;

    const MIN_BACKOFF_MS: u32 = 250;
    const MAX_BACKOFF_MS: u32 = 5_000;

    let url = platform::ws_url();
    let mut backoff_ms = MIN_BACKOFF_MS;
    loop {
        state.conn.set(Conn::Connecting);
        // Edits made while offline are superseded by the coming snapshot.
        while let Some(Some(_)) = rx.next().now_or_never() {}

        match WebSocket::open(&url) {
            Ok(ws) => {
                let (mut sink, stream) = ws.split();
                let mut stream = stream.fuse();
                let mut synced = false;
                loop {
                    select! {
                        incoming = stream.next() => match incoming {
                            Some(Ok(Message::Text(text))) => {
                                match serde_json::from_str::<ServerMsg>(&text) {
                                    Ok(msg) => {
                                        if matches!(msg, ServerMsg::Snapshot(_)) {
                                            synced = true;
                                            backoff_ms = MIN_BACKOFF_MS;
                                            state.conn.set(Conn::Connected);
                                        }
                                        state.apply(msg);
                                    }
                                    Err(e) => warn!("ignoring unparsable server message: {e}"),
                                }
                            }
                            Some(Ok(Message::Bytes(_))) => {}
                            Some(Err(e)) => {
                                warn!("control socket closed: {e}");
                                break;
                            }
                            None => break,
                        },
                        outgoing = rx.next() => match outgoing {
                            // Before the snapshot the edit would be overwritten anyway.
                            Some(_) if !synced => {}
                            Some(msg) => {
                                let json = serde_json::to_string(&msg)
                                    .expect("ClientMsg always serializes");
                                if let Err(e) = sink.send(Message::Text(json)).await {
                                    warn!("control socket send failed: {e}");
                                    break;
                                }
                            }
                            // The app is shutting down.
                            None => return,
                        },
                    }
                }
            }
            Err(e) => warn!("could not open {url}: {e}"),
        }

        let mut remaining_ms = backoff_ms;
        while remaining_ms > 0 {
            state.conn.set(Conn::Retrying { remaining_ms });
            let step = remaining_ms.min(100);
            platform::sleep_ms(step).await;
            remaining_ms -= step;
        }
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}
