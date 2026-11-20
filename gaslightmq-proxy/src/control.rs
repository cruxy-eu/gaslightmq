//! HTTP + WebSocket control plane, and the embedded web UI.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, put};
use axum::Json;
use gaslightmq_proxy_api::{ApiRow, ClientMsg, RuleSet, ServerMsg, SetNodeState, Snapshot};
use tokio::net::TcpListener;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use crate::shared::Shared;

type AppState = State<Arc<Shared>>;

pub(crate) async fn serve(shared: Arc<Shared>, listener: TcpListener) {
    let mut shutdown = shared.shutdown.clone();
    let app = router(shared);
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move { crate::until_shutdown(&mut shutdown).await })
        .await;
    if let Err(e) = result {
        warn!(error = %e, "control plane stopped");
    }
}

fn router(shared: Arc<Shared>) -> Router {
    Router::new()
        .route("/api/health", get(|| async { "ok" }))
        .route("/api/state", get(state))
        .route("/api/rows", delete(clear_rows))
        .route("/api/rows/{api_key}", put(set_row))
        .route("/api/nodes/{node_id}/state", put(set_node_state))
        .route("/api/ws", get(ws))
        .fallback(ui::serve)
        .with_state(shared)
}

fn bad_request(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, msg.into()).into_response()
}

fn validate(row: &ApiRow) -> Result<(), String> {
    for (pad, p) in [
        ("error", row.error.probability),
        ("delay", row.delay.probability),
        ("blackhole", row.blackhole.probability),
    ] {
        if p > 100 {
            return Err(format!("{pad} probability {p} is above 100"));
        }
    }
    Ok(())
}

async fn state(State(shared): AppState) -> Json<Snapshot> {
    Json(shared.snapshot())
}

async fn clear_rows(State(shared): AppState) -> StatusCode {
    shared.set_rules(RuleSet::default());
    StatusCode::NO_CONTENT
}

async fn set_row(State(shared): AppState, Path(api_key): Path<i16>, Json(mut row): Json<ApiRow>) -> Response {
    row.api_key = api_key;
    if let Err(e) = validate(&row) {
        return bad_request(e);
    }
    shared.set_row(row.clone());
    Json(row).into_response()
}

async fn set_node_state(
    State(shared): AppState,
    Path(node_id): Path<i32>,
    Json(body): Json<SetNodeState>,
) -> Response {
    match shared.set_node_state(node_id, body.state).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) if e.to_string().starts_with("unknown node") => {
            (StatusCode::NOT_FOUND, e.to_string()).into_response()
        }
        Err(e) => (StatusCode::CONFLICT, e.to_string()).into_response(),
    }
}

async fn ws(State(shared): AppState, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| ws_session(shared, socket))
}

async fn send(socket: &mut WebSocket, msg: &ServerMsg) -> bool {
    match serde_json::to_string(msg) {
        Ok(json) => socket.send(Message::Text(json.into())).await.is_ok(),
        Err(e) => {
            warn!(error = %e, "cannot encode server message");
            true
        }
    }
}

async fn ws_session(shared: Arc<Shared>, mut socket: WebSocket) {
    let mut events = shared.events.subscribe();
    if !send(&mut socket, &ServerMsg::Snapshot(shared.snapshot())).await {
        return;
    }
    loop {
        tokio::select! {
            event = events.recv() => {
                let msg = match event {
                    Ok(msg) => msg,
                    // Missed updates: start over from a full snapshot.
                    Err(RecvError::Lagged(_)) => ServerMsg::Snapshot(shared.snapshot()),
                    Err(RecvError::Closed) => break,
                };
                if !send(&mut socket, &msg).await {
                    break;
                }
            }
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<ClientMsg>(&text) {
                    Ok(msg) => handle(&shared, msg).await,
                    Err(e) => debug!(error = %e, "ignoring malformed client message"),
                },
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
}

async fn handle(shared: &Arc<Shared>, msg: ClientMsg) {
    match msg {
        ClientMsg::SetRow(row) => match validate(&row) {
            Ok(()) => shared.set_row(row),
            Err(e) => debug!(error = %e, "ignoring invalid row"),
        },
        ClientMsg::SetNodeState { node_id, state } => {
            // Failures are reported through the node's `error` field.
            let _ = shared.set_node_state(node_id, state).await;
        }
        ClientMsg::ClearAll => shared.set_rules(RuleSet::default()),
    }
}

fn not_found_if_api(uri: &Uri) -> Option<Response> {
    uri.path().starts_with("/api/").then(|| StatusCode::NOT_FOUND.into_response())
}

#[cfg(feature = "proxy-ui")]
mod ui {
    use super::*;
    use axum::http::header;

    #[derive(rust_embed::Embed)]
    #[folder = "ui-dist/"]
    struct Assets;

    pub(super) async fn serve(uri: Uri) -> Response {
        if let Some(resp) = not_found_if_api(&uri) {
            return resp;
        }
        let path = uri.path().trim_start_matches('/');
        // Unknown paths get the app shell, so client-side routes work.
        let (path, file) = match Assets::get(path) {
            Some(file) if !path.is_empty() => (path, file),
            _ => match Assets::get("index.html") {
                Some(file) => ("index.html", file),
                None => return StatusCode::NOT_FOUND.into_response(),
            },
        };
        let cache = if path == "index.html" { "no-cache" } else { "public, max-age=3600" };
        (
            [(header::CONTENT_TYPE, file.metadata.mimetype().to_string()), (header::CACHE_CONTROL, cache.to_string())],
            file.data,
        )
            .into_response()
    }
}

#[cfg(not(feature = "proxy-ui"))]
mod ui {
    use super::*;
    use axum::response::Html;

    pub(super) async fn serve(uri: Uri) -> Response {
        if let Some(resp) = not_found_if_api(&uri) {
            return resp;
        }
        Html(
            "<!doctype html><title>gaslightmq-proxy</title>\
             <body style=\"background:#111;color:#ddd;font-family:monospace;padding:2em\">\
             <h1>gaslightmq-proxy</h1>\
             <p>This build has no web UI. Rebuild with <code>--features proxy-ui</code> \
             (see <code>just proxy-ui</code>).</p>\
             <p>The control API is available: <a href=\"/api/state\" style=\"color:#8cf\">/api/state</a>, \
             <code>PUT /api/rows/{api_key}</code>, <code>DELETE /api/rows</code>, \
             <code>PUT /api/nodes/{id}/state</code>, <code>GET /api/ws</code>.</p></body>",
        )
        .into_response()
    }
}
