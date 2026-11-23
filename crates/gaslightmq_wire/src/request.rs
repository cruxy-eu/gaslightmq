use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::{Decodable, Encodable, Request as KafkaRequest, StrBytes};

use crate::{Api, DecodeError, EncodeError, Frame, Response};

/// A request: a [`Frame`] with its header decoded.
///
/// The body stays as bytes until someone asks for it with
/// [`body`](Self::body), so a request that is only looked at and passed on
/// costs a header. The frame is kept as it arrived, and
/// [`frame`](Self::frame) gives it back byte for byte.
#[derive(Debug, Clone)]
pub struct Request {
    header: RequestHeader,
    frame: Frame,
    body_at: usize,
}

impl Request {
    /// Encode a request for the API of `body`.
    ///
    /// # Errors
    ///
    /// An [`EncodeError`] when the header or the body cannot be written
    /// at `version`.
    pub fn new<B: KafkaRequest>(
        version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
        body: &B,
    ) -> Result<Self, EncodeError> {
        let api = Api::of::<B>(version);
        let header = RequestHeader::default()
            .with_request_api_key(api.key)
            .with_request_api_version(api.version)
            .with_correlation_id(correlation_id)
            .with_client_id(client_id.map(|id| StrBytes::from_string(id.to_string())));
        let mut payload = BytesMut::new();
        header
            .encode(&mut payload, api.request_header_version())
            .map_err(|e| EncodeError::new("request header", e))?;
        let body_at = payload.len();
        body.encode(&mut payload, api.version)
            .map_err(|e| EncodeError::new("request body", e))?;
        Ok(Self {
            header,
            frame: Frame::new(payload.freeze()),
            body_at,
        })
    }

    /// Decode the header of a frame that a client sent.
    ///
    /// A frame of an API this crate has no messages for still decodes:
    /// its API, correlation ID and client ID are there, and its body is not.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`] when the frame is too short to name its API,
    /// or the rest of the header is not valid.
    pub fn decode(frame: Frame) -> Result<Self, DecodeError> {
        let payload = frame.payload();
        let Some(&[k0, k1, v0, v1]) = payload.first_chunk::<4>() else {
            return Err(DecodeError::cut_short("request header", payload.len()));
        };
        let api = Api::from_wire(i16::from_be_bytes([k0, k1]), i16::from_be_bytes([v0, v1]));
        let mut rest = payload.clone();
        let header = RequestHeader::decode(&mut rest, api.request_header_version())
            .map_err(|e| DecodeError::new("request header", e))?;
        let body_at = payload.len() - rest.len();
        Ok(Self { header, frame, body_at })
    }

    pub fn api(&self) -> Api {
        Api::from_wire(self.header.request_api_key, self.header.request_api_version)
    }

    /// The number the response repeats, so the client can pair the two.
    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    pub fn client_id(&self) -> Option<&str> {
        self.header.client_id.as_deref()
    }

    pub fn header(&self) -> &RequestHeader {
        &self.header
    }

    /// Decode the body as message `B`.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`] when the request is of another API than `B`,
    /// or the bytes are not a valid `B`.
    pub fn body<B: KafkaRequest>(&self) -> Result<B, DecodeError> {
        let actual = self.header.request_api_key;
        if B::KEY != actual {
            let asked = B::KEY;
            return Err(DecodeError::new(
                "request body",
                format_args!("it is of api key {actual}, and it was read as api key {asked}"),
            ));
        }
        B::decode(&mut self.body_bytes(), self.header.request_api_version)
            .map_err(|e| DecodeError::new("request body", e))
    }

    /// The body as it arrived, after the header.
    pub fn body_bytes(&self) -> Bytes {
        self.frame.payload().slice(self.body_at..)
    }

    /// The frame as it arrived or was encoded.
    pub fn frame(&self) -> &Frame {
        &self.frame
    }

    pub fn into_frame(self) -> Frame {
        self.frame
    }

    /// Encode a response to this request: same API, same correlation ID.
    ///
    /// # Errors
    ///
    /// An [`EncodeError`] when `body` cannot be written at the version of
    /// this request.
    pub fn response<B: Encodable>(&self, body: &B) -> Result<Response, EncodeError> {
        Response::new(self.api(), self.correlation_id(), body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::{ApiKey, FetchRequest, MetadataRequest};

    #[test]
    fn decodes_what_new_encoded() {
        let sent = Request::new(
            12,
            42,
            Some("test-client"),
            &MetadataRequest::default().with_allow_auto_topic_creation(false),
        )
        .unwrap();

        let received = Request::decode(sent.frame().clone()).unwrap();

        assert_eq!(received.api(), Api::new(ApiKey::Metadata, 12));
        assert_eq!(received.correlation_id(), 42);
        assert_eq!(received.client_id(), Some("test-client"));
        assert!(!received.body::<MetadataRequest>().unwrap().allow_auto_topic_creation);
        assert_eq!(received.frame(), sent.frame());
    }

    #[test]
    fn body_is_only_decoded_on_demand() {
        let mut payload = BytesMut::new();
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(1)
            .with_correlation_id(5);
        header.encode(&mut payload, 1).unwrap();
        payload.extend_from_slice(&[0xDE, 0xAD]);

        let request = Request::decode(Frame::new(payload.freeze())).unwrap();

        assert_eq!(request.correlation_id(), 5);
        assert_eq!(request.body_bytes().as_ref(), &[0xDE, 0xAD]);
        let error = request.body::<MetadataRequest>().unwrap_err();
        assert!(error.to_string().starts_with("cannot decode request body: "), "{error}");
    }

    #[test]
    fn body_of_another_api_is_refused() {
        let request = Request::new(12, 1, None, &MetadataRequest::default()).unwrap();
        assert_eq!(
            request.body::<FetchRequest>().unwrap_err().to_string(),
            "cannot decode request body: it is of api key 3, and it was read as api key 1"
        );
    }

    #[test]
    fn unknown_api_keeps_its_header_and_its_bytes() {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(&9999i16.to_be_bytes());
        payload.extend_from_slice(&3i16.to_be_bytes());
        payload.extend_from_slice(&77i32.to_be_bytes());
        payload.extend_from_slice(&2i16.to_be_bytes());
        payload.extend_from_slice(b"me");
        payload.extend_from_slice(&[1, 2, 3]);
        let frame = Frame::new(payload.freeze());

        let request = Request::decode(frame.clone()).unwrap();

        assert_eq!(request.api(), Api::from_wire(9999, 3));
        assert_eq!(request.api().known(), None);
        assert_eq!(request.correlation_id(), 77);
        assert_eq!(request.client_id(), Some("me"));
        assert_eq!(request.into_frame(), frame);
    }

    #[test]
    fn a_frame_shorter_than_a_header_is_cut_short() {
        let frame = Frame::new(Bytes::from_static(&[0, 3, 0]));
        assert_eq!(
            Request::decode(frame).unwrap_err().to_string(),
            "cannot decode request header: it is cut short at 3 bytes"
        );
    }
}
