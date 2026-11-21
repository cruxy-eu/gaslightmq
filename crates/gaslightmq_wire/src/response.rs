use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::{Api, Frame, WireError};

/// A response: a [`Frame`] with its header decoded.
///
/// A response does not say which API it belongs to. It repeats the
/// correlation ID of its request, and whoever decodes it supplies the
/// [`Api`] of that request. Like a [`Request`](crate::Request), it keeps its
/// body as bytes until asked, and keeps the frame as it arrived.
#[derive(Debug, Clone)]
pub struct Response {
    api: Api,
    header: ResponseHeader,
    frame: Frame,
    body_at: usize,
}

impl Response {
    /// Encode a response to a request of `api` with `correlation_id`.
    /// [`Request::response`](crate::Request::response) does this for a request at hand.
    ///
    /// # Errors
    ///
    /// [`WireError::Encode`] when the header or the body cannot be written
    /// at the version of `api`.
    pub fn new<B: Encodable>(api: Api, correlation_id: i32, body: &B) -> Result<Self, WireError> {
        let header = ResponseHeader::default().with_correlation_id(correlation_id);
        let mut payload = BytesMut::new();
        header
            .encode(&mut payload, api.response_header_version())
            .map_err(|e| WireError::encode("response header", e))?;
        let body_at = payload.len();
        body.encode(&mut payload, api.version)
            .map_err(|e| WireError::encode("response body", e))?;
        Ok(Self {
            api,
            header,
            frame: Frame::new(payload.freeze()),
            body_at,
        })
    }

    /// The correlation ID of a response frame, without knowing its API.
    /// It is the first field of every response header.
    ///
    /// # Errors
    ///
    /// [`WireError::Truncated`] when the frame is shorter than a correlation ID.
    pub fn peek_correlation_id(frame: &Frame) -> Result<i32, WireError> {
        match frame.payload().first_chunk::<4>() {
            Some(id) => Ok(i32::from_be_bytes(*id)),
            None => Err(WireError::Truncated {
                what: "response header",
                len: frame.payload().len(),
            }),
        }
    }

    /// Decode the header of a frame that answers a request of `api`.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] when the header is not valid.
    pub fn decode(api: Api, frame: Frame) -> Result<Self, WireError> {
        let payload = frame.payload();
        let mut rest = payload.clone();
        let header = ResponseHeader::decode(&mut rest, api.response_header_version())
            .map_err(|e| WireError::decode("response header", e))?;
        let body_at = payload.len() - rest.len();
        Ok(Self {
            api,
            header,
            frame,
            body_at,
        })
    }

    /// The API of the request this response answers.
    pub fn api(&self) -> Api {
        self.api
    }

    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// Decode the body as message `B`.
    /// The caller picks the message that goes with [`api`](Self::api).
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] when the bytes are not a valid `B` at the
    /// version of [`api`](Self::api).
    pub fn body<B: Decodable>(&self) -> Result<B, WireError> {
        B::decode(&mut self.body_bytes(), self.api.version).map_err(|e| WireError::decode("response body", e))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Request;
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse,
    };

    #[test]
    fn decodes_what_a_request_answered() {
        let request = Request::new(3, 5, None, &ApiVersionsRequest::default()).unwrap();
        let sent = request
            .response(&ApiVersionsResponse::default().with_error_code(35))
            .unwrap();

        assert_eq!(Response::peek_correlation_id(sent.frame()).unwrap(), 5);
        let received = Response::decode(request.api(), sent.into_frame()).unwrap();

        assert_eq!(received.api(), Api::new(ApiKey::ApiVersions, 3));
        assert_eq!(received.correlation_id(), 5);
        assert_eq!(received.body::<ApiVersionsResponse>().unwrap().error_code, 35);
    }

    #[test]
    fn header_version_follows_the_api() {
        let old = Request::new(1, 9, None, &MetadataRequest::default()).unwrap();
        let new = Request::new(12, 9, None, &MetadataRequest::default()).unwrap();
        // Flexible versions add a tagged-fields byte to the header.
        assert_eq!(old.response(&MetadataResponse::default()).unwrap().body_at, 4);
        assert_eq!(new.response(&MetadataResponse::default()).unwrap().body_at, 5);
    }

    #[test]
    fn a_frame_shorter_than_a_correlation_id_is_truncated() {
        let frame = Frame::new(Bytes::from_static(&[0, 0, 1]));
        assert_eq!(
            Response::peek_correlation_id(&frame).unwrap_err(),
            WireError::Truncated {
                what: "response header",
                len: 3
            }
        );
    }
}
