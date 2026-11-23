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
//! use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse};
//! use gaslightmq_wire::{FrameDecoder, Request, Response};
//!
//! // A client builds a request and puts it on the connection.
//! let request = Request::new(12, 7, Some("orders"), &MetadataRequest::default()).expect("a Metadata v12");
//! let sent = request.frame().to_bytes();
//!
//! // The other side cuts the bytes into frames, and decodes what it needs.
//! let mut frames = FrameDecoder::new();
//! frames.extend(&sent);
//! let frame = frames.next_frame().expect("a length within the limit").expect("a whole frame");
//! let received = Request::decode(frame)?;
//! assert_eq!(received.client_id(), Some("orders"));
//! let body: MetadataRequest = received.body()?;
//!
//! // It answers under the same correlation ID.
//! let response = received.response(&MetadataResponse::default()).expect("a Metadata v12");
//! assert_eq!(Response::peek_correlation_id(response.frame())?, 7);
//! # Ok::<(), gaslightmq_wire::DecodeError>(())
//! ```
//!
//! # Errors
//!
//! The three things the crate does fail in different ways, so each has an error of its own.
//! No caller meets a failure that cannot come from what it called.
//!
//! | What failed | Error | What is wrong |
//! |---|---|---|
//! | [`FrameDecoder::next_frame`] | [`FrameError`] | The length that a frame announces is zero, or over the limit of the decoder. |
//! | Reading a header, a body or a record batch | [`DecodeError`] | The bytes are not what they were read as. |
//! | Writing a request, a response or a record batch | [`EncodeError`] | The message cannot be written, for example at a version it does not exist in. |
//!
//! A [`DecodeError`] and an [`EncodeError`] have no variants.
//! Their message names what did not decode or encode, and then says what is wrong, once.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! The message types come from the `kafka-protocol` crate.
//! This crate re-exports them as [`messages`] and [`protocol`],
//! so the rest of the workspace depends on `gaslightmq_wire` alone.

mod api;
mod error;
mod frame;
mod record;
mod request;
mod response;

pub use api::Api;
pub use error::{DecodeError, EncodeError, FrameError};
pub use frame::{Frame, FrameDecoder};
pub use record::{Offset, Partition, Record, RecordBatch, RecordHeader};
pub use request::Request;
pub use response::Response;

pub use kafka_protocol::error::ResponseError;
pub use kafka_protocol::messages::ApiKey;
pub use kafka_protocol::records::Compression;
pub use kafka_protocol::{messages, protocol};
