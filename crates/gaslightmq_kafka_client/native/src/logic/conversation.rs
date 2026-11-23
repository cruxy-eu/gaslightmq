//! What one connection and its broker say to each other.

use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kernel::ClientId;
use gaslightmq_wire::messages::ApiVersionsRequest;
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Frame, Request, Response};

use crate::logic::versions::{BrokerVersions, Versions};

/// The version of `ApiVersions` a greeting is written in.
/// Every broker reads it, whatever else it speaks.
const GREETING_VERSION: i16 = 0;

/// The correlation ID of the greeting. The requests after it count on from here.
const FIRST_CORRELATION_ID: i32 = 1;

/// The first exchange on a new connection: the client asks which versions the broker speaks.
///
/// A [`Conversation`] only comes from an answered greeting, so no request can go before it.
pub(crate) struct Greeting {
    client_id: ClientId,
    request: Request,
}

impl Greeting {
    /// # Errors
    ///
    /// [`ClientError::UnwritableRequest`] when the request does not encode.
    pub fn new(client_id: ClientId) -> Result<Self, ClientError> {
        let request = Request::new(
            GREETING_VERSION,
            FIRST_CORRELATION_ID,
            Some(client_id.as_str()),
            &ApiVersionsRequest::default(),
        )
        .map_err(ClientError::unwritable_request)?;
        Ok(Self { client_id, request })
    }

    /// The request to send first.
    pub fn request(&self) -> &Request {
        &self.request
    }

    /// The conversation that follows, once the broker answered with `frame`.
    ///
    /// # Errors
    ///
    /// [`ClientError::UnreadableAnswer`] when the frame is not the answer to the greeting,
    /// and [`ClientError::Refused`] when the answer carries an error code.
    pub fn answered(self, frame: Frame) -> Result<Conversation, ClientError> {
        let response = answer(&self.request, frame)?;
        Ok(Conversation {
            client_id: self.client_id,
            broker: BrokerVersions::read(&response.body().map_err(ClientError::unreadable_answer)?)?,
            next_correlation_id: FIRST_CORRELATION_ID.wrapping_add(1),
        })
    }
}

/// What one connection has agreed with its broker, and the correlation IDs it has used.
pub(crate) struct Conversation {
    client_id: ClientId,
    broker: BrokerVersions,
    next_correlation_id: i32,
}

impl Conversation {
    /// Encode `body` as the next request of the conversation,
    /// at the newest version of its API that the broker and `ours` share.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoCommonVersion`] when the two sides share no version,
    /// and [`ClientError::UnwritableRequest`] when `body` does not encode.
    pub fn request<R: KafkaRequest>(&mut self, ours: Versions, body: &R) -> Result<Request, ClientError> {
        let version = self.broker.newest_common(R::KEY, ours)?;
        let request = Request::new(version, self.next_correlation_id, Some(self.client_id.as_str()), body)
            .map_err(ClientError::unwritable_request)?;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        Ok(request)
    }
}

/// The response in `frame`, which has to answer `request`.
///
/// # Errors
///
/// [`ClientError::UnreadableAnswer`] when the frame is not a response, or carries another correlation ID.
pub(crate) fn answer(request: &Request, frame: Frame) -> Result<Response, ClientError> {
    let response = Response::decode(request.api(), frame).map_err(ClientError::unreadable_answer)?;
    if response.correlation_id() != request.correlation_id() {
        return Err(ClientError::unreadable_answer(format_args!(
            "correlation id {}, expected {}",
            response.correlation_id(),
            request.correlation_id()
        )));
    }
    Ok(response)
}

#[cfg(test)]
impl Conversation {
    /// A conversation with a broker that answered the greeting with `broker`.
    pub fn with(broker: BrokerVersions) -> Self {
        Self {
            client_id: ClientId::new("test").unwrap(),
            broker,
            next_correlation_id: FIRST_CORRELATION_ID.wrapping_add(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::api_versions_response::ApiVersion;
    use gaslightmq_wire::messages::{ApiVersionsResponse, MetadataRequest, MetadataResponse};
    use gaslightmq_wire::{Api, ApiKey};

    use super::*;

    fn client_id() -> ClientId {
        ClientId::new("order-ingest").unwrap()
    }

    /// The broker's answer to `greeting`: it speaks Metadata from version 1 to 9.
    fn answer_to(greeting: &Greeting) -> Frame {
        let metadata = ApiVersion::default()
            .with_api_key(ApiKey::Metadata as i16)
            .with_min_version(1)
            .with_max_version(9);
        greeting
            .request()
            .response(&ApiVersionsResponse::default().with_api_keys(vec![metadata]))
            .unwrap()
            .into_frame()
    }

    #[test]
    fn a_connection_first_asks_which_versions_the_broker_speaks() {
        let greeting = Greeting::new(client_id()).unwrap();

        assert_eq!(greeting.request().api(), Api::new(ApiKey::ApiVersions, 0));
        assert_eq!(greeting.request().client_id(), Some("order-ingest"));
    }

    #[test]
    fn a_request_goes_at_the_newest_common_version_under_the_next_correlation_id() {
        let greeting = Greeting::new(client_id()).unwrap();
        let frame = answer_to(&greeting);
        let mut conversation = greeting.answered(frame).unwrap();

        let first = conversation
            .request(Versions::new(1, 12), &MetadataRequest::default())
            .unwrap();
        let second = conversation
            .request(Versions::new(1, 4), &MetadataRequest::default())
            .unwrap();

        assert_eq!(first.api(), Api::new(ApiKey::Metadata, 9));
        assert_eq!(second.api(), Api::new(ApiKey::Metadata, 4));
        assert_eq!(first.client_id(), Some("order-ingest"));
        assert_eq!(first.correlation_id(), greeting_correlation_id() + 1);
        assert_eq!(second.correlation_id(), greeting_correlation_id() + 2);
    }

    fn greeting_correlation_id() -> i32 {
        Greeting::new(client_id()).unwrap().request().correlation_id()
    }

    #[test]
    fn an_api_without_a_common_version_is_not_sent() {
        let greeting = Greeting::new(client_id()).unwrap();
        let frame = answer_to(&greeting);
        let mut conversation = greeting.answered(frame).unwrap();

        let refused = conversation.request(Versions::new(10, 12), &MetadataRequest::default());

        assert_eq!(
            refused.unwrap_err(),
            ClientError::NoCommonVersion {
                api_key: gaslightmq_kernel::ApiKey::METADATA
            }
        );
    }

    #[test]
    fn a_response_has_to_answer_its_request() {
        let greeting = Greeting::new(client_id()).unwrap();
        let frame = answer_to(&greeting);
        let mut conversation = greeting.answered(frame).unwrap();
        let first = conversation
            .request(Versions::new(1, 9), &MetadataRequest::default())
            .unwrap();
        let second = conversation
            .request(Versions::new(1, 9), &MetadataRequest::default())
            .unwrap();
        let answer_to_first = first.response(&MetadataResponse::default()).unwrap().into_frame();

        assert!(answer(&first, answer_to_first.clone()).is_ok());
        assert_eq!(
            answer(&second, answer_to_first).unwrap_err(),
            ClientError::unreadable_answer("correlation id 2, expected 3")
        );
    }

    #[test]
    fn a_greeting_only_takes_its_own_answer() {
        let greeting = Greeting::new(client_id()).unwrap();
        let other = Request::new(0, 99, None, &ApiVersionsRequest::default()).unwrap();
        let frame = other.response(&ApiVersionsResponse::default()).unwrap().into_frame();

        assert!(matches!(
            greeting.answered(frame),
            Err(ClientError::UnreadableAnswer { .. })
        ));
    }
}
