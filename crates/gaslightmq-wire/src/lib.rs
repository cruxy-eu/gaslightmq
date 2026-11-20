//! Kafka wire framing shared by the fake broker and the proxy.
//!
//! Every Kafka frame on the wire is a 4-byte big-endian length followed by
//! a header and a body. These helpers read and write such frames, either
//! fully decoded ([`read_request`]) or as raw bytes ([`read_frame`]) for
//! code that only needs to peek at the header and forward the rest.

use anyhow::{Result, bail};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// Read one length-prefixed frame and return its payload without the length
/// prefix. Returns `None` on clean EOF.
pub async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Option<Bytes>> {
    let len = match reader.read_u32().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if len == 0 {
        bail!("received zero-length frame");
    }
    if len > MAX_FRAME_LEN {
        bail!("frame of {len} bytes exceeds limit of {MAX_FRAME_LEN}");
    }
    let mut frame = BytesMut::zeroed(len);
    reader.read_exact(&mut frame).await?;
    Ok(Some(frame.freeze()))
}

/// Write `payload` as one length-prefixed frame.
pub async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, payload: &[u8]) -> Result<()> {
    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    writer.write_all(&frame).await?;
    Ok(())
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

/// Read one length-prefixed request and split it into header and body.
/// Returns `None` on clean EOF.
pub async fn read_request<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Option<ParsedRequest>> {
    match read_frame(reader).await? {
        Some(payload) => split_request(payload).map(Some),
        None => Ok(None),
    }
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

/// Write a length-prefixed response frame with a fresh header for
/// `correlation_id`, followed by an already-encoded `body`.
pub async fn write_response<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<()> {
    let header_version = response_header_version(api_key, api_version);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    let header_size = header.compute_size(header_version)?;
    let total = header_size + body.len();

    let mut frame = BytesMut::with_capacity(4 + total);
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    header.encode(&mut frame, header_version)?;
    frame.extend_from_slice(body);

    writer.write_all(&frame).await?;
    Ok(())
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

    #[tokio::test]
    async fn read_request_parses_length_prefixed_frame() {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(7);
        let mut payload = BytesMut::new();
        header.encode(&mut payload, request_header_version(18, 0)).unwrap();

        let (mut client, mut server) = tokio::io::duplex(1024);
        tokio::spawn(async move { write_frame(&mut client, &payload).await.unwrap() });

        let parsed = read_request(&mut server).await.unwrap().unwrap();
        assert_eq!(parsed.api_key, ApiKey::ApiVersions as i16);
        assert_eq!(parsed.api_version, 0);
        assert_eq!(parsed.header.correlation_id, 7);
    }

    #[tokio::test]
    async fn read_request_returns_none_on_eof() {
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);
        assert!(read_request(&mut server).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_frame() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_u32((MAX_FRAME_LEN + 1) as u32).await.unwrap();
        assert!(read_frame(&mut server).await.is_err());
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
