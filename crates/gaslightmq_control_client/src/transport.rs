//! The IO of the client: HTTP, the WebSocket and a timer, from the transport of the target it builds for.
//!
//! Each transport has the same three: an `Http` that posts a request and reads the answer,
//! a `Socket` that sends and receives text, and a `sleep`.

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
mod native;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use browser::{Http, Socket, sleep};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub(crate) use native::{Http, Socket, sleep};
