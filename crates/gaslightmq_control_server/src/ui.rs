//! The web UI: the server serves its files on every path that the control plane leaves free.
//!
//! `just ui` builds the UI into the folder `ui/` of this crate. A release build holds the files inside the binary,
//! so the UI ships with it. A debug build reads them from the folder while it runs, so a new build of the UI shows
//! on the next reload. A build without them serves a page that says how to build them.

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

/// The files of the UI, by their path below the folder.
#[derive(rust_embed::Embed)]
#[folder = "ui/"]
#[allow_missing = true]
struct UiFiles;

/// The page of the UI, which shows the view of whichever path the browser asked for.
const PAGE: &str = "index.html";

/// The paths of the control plane. Nothing below them is a file of the UI.
const API: &str = "/api";

/// What a build without the files of the UI serves in place of the page.
const NO_UI: &str = "<!doctype html><meta charset=\"utf-8\"><title>GaslightMQ</title>\
    <body style=\"background:#08090a;color:#d0d6e0;font:14px/1.5 -apple-system,sans-serif;padding:3em\">\
    <h1 style=\"font-size:18px;color:#f7f8f8\">This build of GaslightMQ holds no UI.</h1>\
    <p><code>just ui</code> builds it. The control plane answers at <code>/api/operations</code> \
    and <code>/api/stream</code>.</p></body>";

/// Answers a request for a path outside the operations and the stream: a file of the UI, or the page of the UI for a
/// path that names no file, so that a view opens at its own address.
///
/// A path below `/api` that the control plane does not serve is not found, and every method but `GET` and `HEAD` is
/// not allowed. A file is sent with a tag of its content, so a browser that holds it already gets `304`.
pub(crate) async fn serve(method: Method, uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path();
    if path == API || path.starts_with("/api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let wanted = path.trim_start_matches('/');
    let file = if wanted.is_empty() { None } else { UiFiles::get(wanted) };
    let Some(file) = file.or_else(|| UiFiles::get(PAGE)) else {
        let html = [(header::CONTENT_TYPE, "text/html; charset=utf-8")];
        return (StatusCode::NOT_FOUND, html, NO_UI).into_response();
    };
    let tag = format!("\"{}\"", hex(&file.metadata.sha256_hash()));
    let known = headers
        .get(header::IF_NONE_MATCH)
        .is_some_and(|held| held.as_bytes() == tag.as_bytes());
    let mut response = if known {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Response::new(Body::from(file.data.into_owned()))
    };
    let headers = response.headers_mut();
    if let Ok(tag) = HeaderValue::from_str(&tag) {
        headers.insert(header::ETAG, tag);
    }
    // The browser asks every time, and the tag spares it the file when nothing changed.
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if let Ok(kind) = HeaderValue::from_str(file.metadata.mimetype()) {
        headers.insert(header::CONTENT_TYPE, kind);
    }
    response
}

/// The bytes in lower-case hexadecimal.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| [DIGITS[usize::from(byte >> 4)], DIGITS[usize::from(byte & 0x0f)]])
        .map(char::from)
        .collect()
}
