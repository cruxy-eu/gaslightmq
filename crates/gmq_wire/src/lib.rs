//! The Kafka codec: frames, headers and record batches.
//!
//! Every Kafka frame on the wire is a 4-byte big-endian length followed by
//! a header and a body. This crate splits frames out of a byte buffer
//! ([`decode_frame`]), decodes them fully ([`split_request`]) or only as far
//! as needed ([`peek_request`]), and converts records to and from record
//! batches ([`batch`]).
//!
//! The crate is sans-IO: it works on bytes in memory and never touches a
//! socket. Whoever owns the socket reads into a buffer and calls in here.

pub mod batch;
pub mod record;

pub use record::{Offset, Partition, Record};

use anyhow::{Result, bail};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable};

/// Upper bound for a single frame. Larger frames are rejected instead of
/// allocated, which protects against garbage on the socket.
pub const MAX_FRAME_LEN: usize = 100 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Header version helpers
// ---------------------------------------------------------------------------

/// Header version used to decode a request for the given API key and version.
/// Unknown keys default to v1.
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match ApiKey::try_from(api_key) {
        Ok(key) => key.request_header_version(api_version),
        Err(_) => 1,
    }
}

/// Header version used to encode a response for the given API key and
/// version. Unknown keys default to v0.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match ApiKey::try_from(api_key) {
        Ok(key) => key.response_header_version(api_version),
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------------------
// Raw frames
// ---------------------------------------------------------------------------

/// Take one length-prefixed frame from the front of `buf` and return its
/// payload without the length prefix.
///
/// Returns `None` and leaves `buf` untouched while the frame is incomplete.
/// The caller appends more bytes and calls again. An invalid length is an
/// error as soon as the prefix is there, before the payload arrives.
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<Bytes>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        bail!("received zero-length frame");
    }
    if len > MAX_FRAME_LEN {
        bail!("frame of {len} bytes exceeds limit of {MAX_FRAME_LEN}");
    }
    if buf.len() < 4 + len {
        buf.reserve(4 + len - buf.len());
        return Ok(None);
    }
    buf.advance(4);
    Ok(Some(buf.split_to(len).freeze()))
}

/// Wrap `payload` in one length-prefixed frame.
pub fn encode_frame(payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.freeze()
}

/// The fixed-position fields at the start of every request header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPeek {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

/// Read api key, version and correlation id from a raw request payload
/// without decoding the rest of the header.
pub fn peek_request(payload: &[u8]) -> Result<RequestPeek> {
    if payload.len() < 8 {
        bail!("request frame too short: {} bytes", payload.len());
    }
    let mut buf = &payload[..8];
    Ok(RequestPeek {
        api_key: buf.get_i16(),
        api_version: buf.get_i16(),
        correlation_id: buf.get_i32(),
    })
}

/// Read the correlation id from a raw response payload. It is always the
/// first field of the response header, whatever the header version.
pub fn peek_response_correlation_id(payload: &[u8]) -> Result<i32> {
    if payload.len() < 4 {
        bail!("response frame too short: {} bytes", payload.len());
    }
    Ok((&payload[..4]).get_i32())
}

// ---------------------------------------------------------------------------
// Decoded requests
// ---------------------------------------------------------------------------

/// A request frame split into its decoded header and the remaining body.
pub struct ParsedRequest {
    pub header: RequestHeader,
    pub api_key: i16,
    pub api_version: i16,
    pub body: Bytes,
}

/// Split a request payload (without length prefix) into header and body.
pub fn split_request(mut payload: Bytes) -> Result<ParsedRequest> {
    let header = kafka_protocol::protocol::decode_request_header_from_buffer(&mut payload)?;
    Ok(ParsedRequest {
        api_key: header.request_api_key,
        api_version: header.request_api_version,
        header,
        body: payload,
    })
}

/// Parse a request payload (without length prefix) from a byte slice.
pub fn parse_request(data: &[u8]) -> Result<ParsedRequest> {
    split_request(Bytes::copy_from_slice(data))
}

/// Encode a request header and body into a payload (without length prefix).
pub fn encode_request<E: Encodable>(header: &RequestHeader, body: &E) -> Result<Bytes> {
    let header_version = request_header_version(header.request_api_key, header.request_api_version);
    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version)?;
    body.encode(&mut buf, header.request_api_version)?;
    Ok(buf.freeze())
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Encode a response message body at the given version.
pub fn encode_response_body<E: Encodable>(response: &E, api_version: i16) -> Result<Vec<u8>> {
    let size = response.compute_size(api_version)?;
    let mut buf = BytesMut::with_capacity(size);
    response.encode(&mut buf, api_version)?;
    Ok(buf.to_vec())
}

/// Encode a response header and body into a payload (without length prefix).
pub fn encode_response<E: Encodable>(
    header: &ResponseHeader,
    api_key: i16,
    api_version: i16,
    body: &E,
) -> Result<Bytes> {
    let header_version = response_header_version(api_key, api_version);
    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version)?;
    body.encode(&mut buf, api_version)?;
    Ok(buf.freeze())
}

/// Decode a response payload (without length prefix) into header and body.
pub fn decode_response<D: Decodable>(
    mut payload: Bytes,
    api_key: i16,
    api_version: i16,
) -> Result<(ResponseHeader, D)> {
    let header_version = response_header_version(api_key, api_version);
    let header = ResponseHeader::decode(&mut payload, header_version)?;
    let body = D::decode(&mut payload, api_version)?;
    Ok((header, body))
}

/// Build a length-prefixed response frame with a fresh header for
/// `correlation_id`, followed by an already-encoded `body`.
pub fn response_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Result<Bytes> {
    let header_version = response_header_version(api_key, api_version);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    let header_size = header.compute_size(header_version)?;
    let total = header_size + body.len();

    let mut frame = BytesMut::with_capacity(4 + total);
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    header.encode(&mut frame, header_version)?;
    frame.extend_from_slice(body);
    Ok(frame.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn request_header_version_api_versions_v3_returns_2() {
        assert_eq!(request_header_version(ApiKey::ApiVersions as i16, 3), 2);
    }

    #[test]
    fn request_header_version_api_versions_v2_returns_1() {
        assert_eq!(request_header_version(ApiKey::ApiVersions as i16, 2), 1);
    }

    #[test]
    fn request_header_version_metadata_v1_returns_1() {
        assert_eq!(request_header_version(ApiKey::Metadata as i16, 1), 1);
    }

    #[test]
    fn request_header_version_metadata_v9_returns_2() {
        assert_eq!(request_header_version(ApiKey::Metadata as i16, 9), 2);
    }

    #[test]
    fn response_header_version_api_versions_always_0() {
        assert_eq!(response_header_version(ApiKey::ApiVersions as i16, 0), 0);
        assert_eq!(response_header_version(ApiKey::ApiVersions as i16, 3), 0);
    }

    #[test]
    fn response_header_version_metadata_v9_returns_1() {
        assert_eq!(response_header_version(ApiKey::Metadata as i16, 9), 1);
    }

    #[test]
    fn response_header_version_metadata_v1_returns_0() {
        assert_eq!(response_header_version(ApiKey::Metadata as i16, 1), 0);
    }

    fn metadata_header(correlation_id: i32) -> RequestHeader {
        RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(1)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("test-client")))
    }

    #[test]
    fn parse_request_decodes_header() {
        let header = metadata_header(42);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, request_header_version(3, 1)).unwrap();
        buf.extend_from_slice(&[0xDE, 0xAD]);

        let parsed = parse_request(&buf).unwrap();
        assert_eq!(parsed.api_key, ApiKey::Metadata as i16);
        assert_eq!(parsed.api_version, 1);
        assert_eq!(parsed.header.correlation_id, 42);
        assert_eq!(parsed.header.client_id.as_deref(), Some("test-client"));
        assert_eq!(parsed.body.as_ref(), &[0xDE, 0xAD]);
    }

    #[test]
    fn peek_request_reads_fixed_fields() {
        let mut buf = BytesMut::new();
        metadata_header(99).encode(&mut buf, 1).unwrap();
        let peek = peek_request(&buf).unwrap();
        assert_eq!(
            peek,
            RequestPeek { api_key: ApiKey::Metadata as i16, api_version: 1, correlation_id: 99 }
        );
    }

    #[test]
    fn peek_request_rejects_short_frame() {
        assert!(peek_request(&[0, 3, 0]).is_err());
    }

    #[test]
    fn decode_frame_splits_a_framed_request() {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(7);
        let mut payload = BytesMut::new();
        header.encode(&mut payload, request_header_version(18, 0)).unwrap();
        let mut buf = BytesMut::from(&encode_frame(&payload)[..]);

        let parsed = split_request(decode_frame(&mut buf).unwrap().unwrap()).unwrap();
        assert_eq!(parsed.api_key, ApiKey::ApiVersions as i16);
        assert_eq!(parsed.api_version, 0);
        assert_eq!(parsed.header.correlation_id, 7);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_frame_waits_for_the_whole_frame() {
        let frame = encode_frame(b"abcdef");
        let mut buf = BytesMut::new();
        for byte in &frame[..frame.len() - 1] {
            buf.extend_from_slice(&[*byte]);
            assert!(decode_frame(&mut buf).unwrap().is_none());
        }
        buf.extend_from_slice(&frame[frame.len() - 1..]);
        assert_eq!(decode_frame(&mut buf).unwrap().unwrap().as_ref(), b"abcdef");
    }

    #[test]
    fn decode_frame_leaves_the_next_frame_in_the_buffer() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encode_frame(b"one"));
        buf.extend_from_slice(&encode_frame(b"two"));
        assert_eq!(decode_frame(&mut buf).unwrap().unwrap().as_ref(), b"one");
        assert_eq!(decode_frame(&mut buf).unwrap().unwrap().as_ref(), b"two");
        assert!(decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_frame_rejects_oversized_frame() {
        let mut buf = BytesMut::from(&((MAX_FRAME_LEN + 1) as u32).to_be_bytes()[..]);
        assert!(decode_frame(&mut buf).is_err());
    }

    #[test]
    fn decode_frame_rejects_zero_length_frame() {
        let mut buf = BytesMut::from(&0u32.to_be_bytes()[..]);
        assert!(decode_frame(&mut buf).is_err());
    }

    #[test]
    fn response_frame_prefixes_length_and_header() {
        let mut buf = BytesMut::from(&response_frame(ApiKey::Metadata as i16, 1, 5, &[0xAB]).unwrap()[..]);
        let payload = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(peek_response_correlation_id(&payload).unwrap(), 5);
        assert_eq!(payload.last(), Some(&0xAB));
    }

    #[test]
    fn response_round_trips() {
        use kafka_protocol::messages::ApiVersionsResponse;
        let header = ResponseHeader::default().with_correlation_id(5);
        let body = ApiVersionsResponse::default().with_error_code(35);
        let payload = encode_response(&header, ApiKey::ApiVersions as i16, 3, &body).unwrap();
        assert_eq!(peek_response_correlation_id(&payload).unwrap(), 5);
        let (h, b): (ResponseHeader, ApiVersionsResponse) =
            decode_response(payload, ApiKey::ApiVersions as i16, 3).unwrap();
        assert_eq!(h.correlation_id, 5);
        assert_eq!(b.error_code, 35);
    }
}
