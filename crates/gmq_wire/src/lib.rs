//! The Kafka codec: frames, requests, responses and record batches.
//!
//! | Type | Is |
//! |---|---|
//! | [`Frame`] | One length-prefixed message, as it travels on a connection. |
//! | [`FrameDecoder`] | The bytes of a connection, cut into frames. |
//! | [`Request`] | A frame with its request header decoded. The body decodes on demand. |
//! | [`Response`] | A frame with its response header decoded. The body decodes on demand. |
//! | [`Api`] | An API key with a version, which decides how headers and bodies encode. |
//! | [`RecordBatch`] | Records as they travel inside Produce and Fetch. |
//!
//! The crate is sans-IO.
//! It works on bytes in memory and never touches a socket:
//! whoever owns the socket hands its bytes to a [`FrameDecoder`],
//! and writes the bytes of a [`Frame`].
//!
//! ```
//! use gmq_wire::messages::{MetadataRequest, MetadataResponse};
//! use gmq_wire::{FrameDecoder, Request, Response};
//!
//! // A client builds a request and puts it on the connection.
//! let request = Request::new(12, 7, Some("orders"), &MetadataRequest::default())?;
//! let sent = request.frame().to_bytes();
//!
//! // The other side cuts the bytes into frames, and decodes what it needs.
//! let mut frames = FrameDecoder::new();
//! frames.extend(&sent);
//! let received = Request::decode(frames.next_frame()?.expect("a whole frame"))?;
//! assert_eq!(received.client_id(), Some("orders"));
//! let body: MetadataRequest = received.body()?;
//!
//! // It answers under the same correlation ID.
//! let response = received.response(&MetadataResponse::default())?;
//! assert_eq!(Response::peek_correlation_id(response.frame())?, 7);
//! # Ok::<(), gmq_wire::WireError>(())
//! ```
//!
//! The message types come from the `kafka-protocol` crate.
//! This crate re-exports them as [`messages`] and [`protocol`],
//! so the rest of the workspace depends on `gmq_wire` alone.

mod api;
mod error;
mod frame;
mod record;
mod request;
mod response;

pub use api::Api;
pub use error::WireError;
pub use frame::{Frame, FrameDecoder};
pub use record::{Offset, Partition, Record, RecordBatch, RecordHeader};
pub use request::Request;
pub use response::Response;

pub use kafka_protocol::error::ResponseError;
pub use kafka_protocol::messages::ApiKey;
pub use kafka_protocol::records::Compression;
pub use kafka_protocol::{messages, protocol};
