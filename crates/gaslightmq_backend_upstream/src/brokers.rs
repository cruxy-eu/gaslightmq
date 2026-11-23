//! The question a backend asks its cluster: which brokers do you have?

use std::fmt;

use gaslightmq_kernel::{ClientId, ErrorCode};
use gaslightmq_wire::messages::{ApiVersionsRequest, ApiVersionsResponse, MetadataRequest};
use gaslightmq_wire::{ApiKey, Frame, Request, Response};

use crate::error::UpstreamBackendError;

/// The question which brokers a cluster has, as a backend asks it on a connection of its own.
///
/// A connection starts with a greeting: the backend asks which versions the broker knows,
/// and the two [agree](Self::agreed) on a version of Metadata.
/// The backend then [asks](Self::brokers) for the brokers, on that connection, as often as it likes.
///
/// The query writes requests and reads frames.
/// It opens no connection, and whoever owns the socket sends what the query writes.
#[derive(Debug, Clone)]
pub(crate) struct BrokerQuery {
    client_id: ClientId,
    /// The correlation ID of the request that was written last.
    asked: i32,
}

impl BrokerQuery {
    /// The version of `ApiVersions` of the greeting: the oldest, which every broker reads.
    const GREETING: i16 = 0;
    /// The oldest version of Metadata the query asks with: the first in which an empty list asks for no topic.
    const OLDEST: i16 = 1;
    /// The newest version of Metadata the query asks with.
    const NEWEST: i16 = 12;

    /// A query that names itself `client_id` in every request.
    pub(crate) const fn new(client_id: ClientId) -> Self {
        Self { client_id, asked: 0 }
    }

    /// The greeting: a request for the versions the broker knows.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the request does not encode.
    pub(crate) fn greeting(&mut self) -> Result<Request, UpstreamBackendError> {
        self.request(Self::GREETING, &ApiVersionsRequest::default())
    }

    /// Reads `answer`, the frame that answers `greeting`, and agrees on a version of Metadata:
    /// the newest that the broker and the query both know.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the frame is no response to the greeting,
    /// when the broker answers with an error, and when the two know no version of Metadata in common.
    pub(crate) fn agreed(greeting: &Request, answer: Frame) -> Result<MetadataVersion, UpstreamBackendError> {
        let response = Self::response(greeting, answer)?;
        let versions: ApiVersionsResponse = response.body().map_err(unreadable)?;
        if let Some(code) = ErrorCode::from_wire(versions.error_code) {
            return Err(UpstreamBackendError::undiscovered(format_args!(
                "the broker does not say which versions it knows: {code}"
            )));
        }
        let metadata = ApiKey::Metadata as i16;
        let known = versions.api_keys.iter().find(|api| api.api_key == metadata);
        let (oldest, newest) = known.map_or((0, -1), |api| (api.min_version, api.max_version));
        let version = newest.min(Self::NEWEST);
        if version < oldest.max(Self::OLDEST) {
            // A broker without Metadata has the versions 0 to -1.
            return Err(UpstreamBackendError::undiscovered(format_args!(
                "the broker knows Metadata from version {oldest} to {newest}, and the backend from {} to {}",
                Self::OLDEST,
                Self::NEWEST
            )));
        }
        Ok(MetadataVersion(version))
    }

    /// The question itself: a Metadata request that asks for the brokers, and for no topic.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the request does not encode.
    pub(crate) fn brokers(&mut self, version: MetadataVersion) -> Result<Request, UpstreamBackendError> {
        let no_topic = MetadataRequest::default()
            .with_topics(Some(Vec::new()))
            .with_allow_auto_topic_creation(false);
        self.request(version.0, &no_topic)
    }

    /// Reads `answer`, the frame that answers `question`, as the response that names the brokers.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the frame is no response to the question.
    pub(crate) fn named(question: &Request, answer: Frame) -> Result<Response, UpstreamBackendError> {
        Self::response(question, answer)
    }

    fn request<B>(&mut self, version: i16, body: &B) -> Result<Request, UpstreamBackendError>
    where
        B: gaslightmq_wire::protocol::Request,
    {
        self.asked = self.asked.wrapping_add(1);
        Request::new(version, self.asked, Some(self.client_id.as_str()), body).map_err(|error| {
            UpstreamBackendError::undiscovered(format_args!("the question for the brokers cannot be written: {error}"))
        })
    }

    fn response(request: &Request, answer: Frame) -> Result<Response, UpstreamBackendError> {
        let response = Response::decode(request.api(), answer).map_err(unreadable)?;
        if response.correlation_id() != request.correlation_id() {
            return Err(unreadable(format_args!(
                "correlation ID {} does not answer the {} request {}",
                response.correlation_id(),
                request.api(),
                request.correlation_id()
            )));
        }
        Ok(response)
    }
}

/// The version of Metadata that a broker and a [`BrokerQuery`] agreed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataVersion(i16);

/// A frame of the broker is no response to the request it answers. `flaw` says what is wrong with it.
fn unreadable(flaw: impl fmt::Display) -> UpstreamBackendError {
    UpstreamBackendError::undiscovered(format_args!("the answer of the broker cannot be read: {flaw}"))
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::Api;
    use gaslightmq_wire::messages::MetadataResponse;
    use gaslightmq_wire::messages::api_versions_response::ApiVersion;

    use super::*;

    fn query() -> BrokerQuery {
        BrokerQuery::new(ClientId::new("gaslightmq").unwrap())
    }

    /// The answer of a broker that knows Metadata from `oldest` to `newest`.
    fn knowing(greeting: &Request, oldest: i16, newest: i16) -> Frame {
        let metadata = ApiVersion::default()
            .with_api_key(ApiKey::Metadata as i16)
            .with_min_version(oldest)
            .with_max_version(newest);
        let fetch = ApiVersion::default()
            .with_api_key(ApiKey::Fetch as i16)
            .with_min_version(4)
            .with_max_version(17);
        let body = ApiVersionsResponse::default().with_api_keys(vec![fetch, metadata]);
        greeting.response(&body).unwrap().into_frame()
    }

    #[test]
    fn the_greeting_asks_for_versions_in_the_oldest_version() {
        let greeting = query().greeting().unwrap();

        assert_eq!(greeting.api(), Api::new(ApiKey::ApiVersions, 0));
        assert_eq!(greeting.client_id(), Some("gaslightmq"));
        assert_eq!(greeting.correlation_id(), 1);
    }

    #[test]
    fn the_two_agree_on_the_newest_version_both_know() {
        let greeting = query().greeting().unwrap();

        for (oldest, newest, agreed) in [(0, 13, 12), (4, 13, 12), (0, 9, 9), (1, 1, 1), (12, 20, 12)] {
            assert_eq!(
                BrokerQuery::agreed(&greeting, knowing(&greeting, oldest, newest)),
                Ok(MetadataVersion(agreed)),
                "a broker with Metadata v{oldest} to v{newest}"
            );
        }
    }

    #[test]
    fn a_broker_with_no_version_in_common_has_no_answer() {
        let greeting = query().greeting().unwrap();

        for (oldest, newest) in [(13, 20), (0, 0)] {
            let error = BrokerQuery::agreed(&greeting, knowing(&greeting, oldest, newest)).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!(
                    "the cluster did not name its brokers: \
                     the broker knows Metadata from version {oldest} to {newest}, and the backend from 1 to 12"
                )
            );
        }
        let without_metadata = greeting.response(&ApiVersionsResponse::default()).unwrap();
        let error = BrokerQuery::agreed(&greeting, without_metadata.into_frame()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the cluster did not name its brokers: \
             the broker knows Metadata from version 0 to -1, and the backend from 1 to 12"
        );
    }

    #[test]
    fn a_broker_that_refuses_the_greeting_has_no_answer() {
        let greeting = query().greeting().unwrap();
        let refused = greeting
            .response(&ApiVersionsResponse::default().with_error_code(ErrorCode::UNSUPPORTED_VERSION.value()))
            .unwrap();

        let error = BrokerQuery::agreed(&greeting, refused.into_frame()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the cluster did not name its brokers: \
             the broker does not say which versions it knows: UNSUPPORTED_VERSION"
        );
    }

    #[test]
    fn the_question_asks_for_the_brokers_and_for_no_topic() {
        let mut query = query();
        let greeting = query.greeting().unwrap();
        let version = BrokerQuery::agreed(&greeting, knowing(&greeting, 0, 13)).unwrap();

        let question = query.brokers(version).unwrap();

        assert_eq!(question.api(), Api::new(ApiKey::Metadata, 12));
        assert_eq!(question.correlation_id(), 2);
        let body: MetadataRequest = question.body().unwrap();
        assert_eq!(body.topics, Some(Vec::new()));
        assert!(!body.allow_auto_topic_creation);
    }

    #[test]
    fn the_answer_to_another_request_is_no_answer() {
        let mut query = query();
        let first = query.brokers(MetadataVersion(12)).unwrap();
        let second = query.brokers(MetadataVersion(12)).unwrap();
        let of_the_first = first.response(&MetadataResponse::default()).unwrap().into_frame();

        assert!(BrokerQuery::named(&first, of_the_first.clone()).is_ok());
        let error = BrokerQuery::named(&second, of_the_first).unwrap_err();
        assert_eq!(
            error.to_string(),
            "the cluster did not name its brokers: the answer of the broker cannot be read: \
             correlation ID 1 does not answer the Metadata v12 request 2"
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}
