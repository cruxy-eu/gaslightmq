use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::Encodable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Header version helpers
// ---------------------------------------------------------------------------

/// Returns the header version to use when decoding a request for the given
/// API key and API version. ApiVersions v3+ uses header v2, everything else v1.
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    if let Ok(key) = ApiKey::try_from(api_key) {
        key.request_header_version(api_version)
    } else {
        1 // default to v1 for unknown keys
    }
}

/// Returns the header version to use when encoding a response for the given
/// API key and API version. ApiVersions v3+ uses header v1, everything else v0.
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if let Ok(key) = ApiKey::try_from(api_key) {
        key.response_header_version(api_version)
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// ParsedRequest
// ---------------------------------------------------------------------------

/// A parsed Kafka request frame containing the decoded header, the API key
/// and version extracted from it, and the remaining body bytes.
pub struct ParsedRequest {
    pub header: RequestHeader,
    pub api_key: i16,
    pub api_version: i16,
    pub body: Bytes,
}

// ---------------------------------------------------------------------------
// Sync frame parsing (for easy unit testing)
// ---------------------------------------------------------------------------

/// Parse a single Kafka request frame from raw bytes (without the 4-byte
/// length prefix). Returns a `ParsedRequest`.
pub fn parse_request(data: &[u8]) -> Result<ParsedRequest> {
    let mut buf = Bytes::copy_from_slice(data);
    let header = kafka_protocol::protocol::decode_request_header_from_buffer(&mut buf)?;
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;
    let body = buf;
    Ok(ParsedRequest {
        header,
        api_key,
        api_version,
        body,
    })
}

// ---------------------------------------------------------------------------
// Async framing: read_request / write_response
// ---------------------------------------------------------------------------

/// Read a length-prefixed Kafka request frame from an async reader.
/// Returns `None` on clean EOF (connection closed).
pub async fn read_request<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Option<ParsedRequest>> {
    // Read 4-byte big-endian length prefix.
    let len = match reader.read_u32().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    if len == 0 {
        bail!("received zero-length frame");
    }

    let mut frame = vec![0u8; len];
    reader.read_exact(&mut frame).await?;

    parse_request(&frame).map(Some)
}

/// Write a length-prefixed Kafka response frame.
///
/// Encodes the `ResponseHeader` followed by the `body` (which should already
/// be the encoded response message), prepends a 4-byte length, and writes
/// everything to the writer.
pub async fn write_response<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<()> {
    let resp_header_version = response_header_version(api_key, api_version);

    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    let header_size = header.compute_size(resp_header_version)?;
    let total = header_size + body.len();

    let mut frame = BytesMut::with_capacity(4 + total);
    // Length prefix
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    // Header
    header.encode(&mut frame, resp_header_version)?;
    // Body
    frame.extend_from_slice(body);

    writer.write_all(&frame).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;

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
        // ApiVersionsResponse.header_version always returns 0
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

    #[test]
    fn parse_request_decodes_header() {
        // Build a raw frame: RequestHeader (v1) for Metadata v1
        let api_key: i16 = ApiKey::Metadata as i16; // 3
        let api_version: i16 = 1;
        let correlation_id: i32 = 42;
        let client_id = Some(StrBytes::from_static_str("test-client"));

        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(client_id);

        let header_version = request_header_version(api_key, api_version);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        // Add some body bytes
        buf.extend_from_slice(&[0xDE, 0xAD]);

        let parsed = parse_request(&buf).unwrap();
        assert_eq!(parsed.api_key, api_key);
        assert_eq!(parsed.api_version, api_version);
        assert_eq!(parsed.header.correlation_id, 42);
        assert_eq!(
            parsed.header.client_id.as_deref(),
            Some("test-client")
        );
        assert_eq!(parsed.body.as_ref(), &[0xDE, 0xAD]);
    }

    #[tokio::test]
    async fn read_request_parses_length_prefixed_frame() {
        // Build a complete frame with length prefix
        let api_key: i16 = ApiKey::ApiVersions as i16; // 18
        let api_version: i16 = 0;
        let correlation_id: i32 = 7;

        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(None);

        let header_version = request_header_version(api_key, api_version);
        let mut payload = BytesMut::new();
        header.encode(&mut payload, header_version).unwrap();

        // Build the length-prefixed wire format
        let len = payload.len() as u32;
        let mut wire = Vec::new();
        wire.extend_from_slice(&len.to_be_bytes());
        wire.extend_from_slice(&payload);

        // Use a tokio duplex channel as our async reader
        let (mut client, mut server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            client.write_all(&wire).await.unwrap();
            // Dropping client closes the write half
        });

        let parsed = read_request(&mut server).await.unwrap().unwrap();
        assert_eq!(parsed.api_key, api_key);
        assert_eq!(parsed.api_version, api_version);
        assert_eq!(parsed.header.correlation_id, 7);
    }

    #[tokio::test]
    async fn read_request_returns_none_on_eof() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        drop(client); // immediately close
        let result = read_request(&mut server).await.unwrap();
        assert!(result.is_none());
    }
}
