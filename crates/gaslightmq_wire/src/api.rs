use std::fmt;

use kafka_protocol::messages::ApiKey;
use kafka_protocol::protocol::Request as KafkaRequest;

/// An API key with a version.
///
/// The pair decides how a request and its response encode: which header
/// version they carry and which fields the body has. A response does not
/// name its API on the wire, so whoever decodes one needs the `Api` of the
/// request it answers.
///
/// The key is the number from the wire. It may be one this crate has no
/// messages for; such a call can still be forwarded as a [`Frame`](crate::Frame).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Api {
    pub key: i16,
    pub version: i16,
}

impl Api {
    pub const fn new(key: ApiKey, version: i16) -> Self {
        Self {
            key: key as i16,
            version,
        }
    }

    /// The API of request message `R`, at `version`.
    pub const fn of<R: KafkaRequest>(version: i16) -> Self {
        Self { key: R::KEY, version }
    }

    /// From the two numbers at the start of every request.
    pub const fn from_wire(key: i16, version: i16) -> Self {
        Self { key, version }
    }

    /// The key as a known API, if this crate has messages for it.
    pub fn known(&self) -> Option<ApiKey> {
        ApiKey::try_from(self.key).ok()
    }

    /// Version of the header in front of a request body.
    /// An unknown API gets version 1, which every client-facing API has at least.
    pub fn request_header_version(&self) -> i16 {
        self.known().map_or(1, |key| key.request_header_version(self.version))
    }

    /// Version of the header in front of a response body.
    /// An unknown API gets version 0, which carries the correlation ID only.
    pub fn response_header_version(&self) -> i16 {
        self.known().map_or(0, |key| key.response_header_version(self.version))
    }
}

impl fmt::Display for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.known() {
            Some(key) => write!(f, "{key:?} v{}", self.version),
            None => write!(f, "api {} v{}", self.key, self.version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::MetadataRequest;

    #[test]
    fn header_versions_follow_the_api_version() {
        assert_eq!(Api::new(ApiKey::Metadata, 1).request_header_version(), 1);
        assert_eq!(Api::new(ApiKey::Metadata, 9).request_header_version(), 2);
        assert_eq!(Api::new(ApiKey::Metadata, 1).response_header_version(), 0);
        assert_eq!(Api::new(ApiKey::Metadata, 9).response_header_version(), 1);
    }

    #[test]
    fn api_versions_response_header_is_always_version_0() {
        assert_eq!(Api::new(ApiKey::ApiVersions, 2).request_header_version(), 1);
        assert_eq!(Api::new(ApiKey::ApiVersions, 3).request_header_version(), 2);
        assert_eq!(Api::new(ApiKey::ApiVersions, 0).response_header_version(), 0);
        assert_eq!(Api::new(ApiKey::ApiVersions, 3).response_header_version(), 0);
    }

    #[test]
    fn unknown_api_falls_back_to_the_oldest_headers() {
        let api = Api::from_wire(9999, 4);
        assert_eq!(api.known(), None);
        assert_eq!(api.request_header_version(), 1);
        assert_eq!(api.response_header_version(), 0);
    }

    #[test]
    fn of_takes_the_key_from_the_message() {
        assert_eq!(Api::of::<MetadataRequest>(12), Api::new(ApiKey::Metadata, 12));
    }

    #[test]
    fn displays_the_name_when_known() {
        assert_eq!(Api::new(ApiKey::Produce, 11).to_string(), "Produce v11");
        assert_eq!(Api::from_wire(9999, 4).to_string(), "api 9999 v4");
    }
}
