//! Which version of an API a connection speaks.

use std::collections::HashMap;

use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kernel::ApiKey;
use gaslightmq_wire::messages::ApiVersionsResponse;

/// The versions of one API that one side speaks, from the oldest to the newest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Versions {
    pub oldest: i16,
    pub newest: i16,
}

impl Versions {
    pub const fn new(oldest: i16, newest: i16) -> Self {
        Self { oldest, newest }
    }
}

/// The versions of every API that the broker of one connection speaks.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct BrokerVersions(HashMap<i16, Versions>);

impl BrokerVersions {
    /// What the broker says it speaks.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the response carries an error code.
    pub fn read(response: &ApiVersionsResponse) -> Result<Self, ClientError> {
        if let Some(error) = ClientError::refusal(response.error_code) {
            return Err(error);
        }
        Ok(Self(
            response
                .api_keys
                .iter()
                .map(|api| (api.api_key, Versions::new(api.min_version, api.max_version)))
                .collect(),
        ))
    }

    /// The newest version of an API that the broker and the client both speak.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoCommonVersion`] when the broker does not know the API,
    /// or when no version of `ours` is one of the broker's.
    pub fn newest_common(&self, api_key: i16, ours: Versions) -> Result<i16, ClientError> {
        let unsupported = ClientError::NoCommonVersion {
            api_key: ApiKey::new(api_key),
        };
        let theirs = self.0.get(&api_key).ok_or_else(|| unsupported.clone())?;
        let newest = ours.newest.min(theirs.newest);
        if newest >= ours.oldest.max(theirs.oldest) {
            Ok(newest)
        } else {
            Err(unsupported)
        }
    }
}

#[cfg(test)]
impl BrokerVersions {
    /// A broker that speaks `versions` of each of `api_keys`.
    pub fn speaking(api_keys: &[i16], versions: Versions) -> Self {
        Self(api_keys.iter().map(|key| (*key, versions)).collect())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::api_versions_response::ApiVersion;

    use super::*;

    fn broker(versions: &[(i16, i16, i16)]) -> BrokerVersions {
        let api_keys = versions
            .iter()
            .map(|&(key, oldest, newest)| {
                ApiVersion::default()
                    .with_api_key(key)
                    .with_min_version(oldest)
                    .with_max_version(newest)
            })
            .collect();
        BrokerVersions::read(&ApiVersionsResponse::default().with_api_keys(api_keys)).unwrap()
    }

    #[test]
    fn the_newest_version_both_sides_speak_wins() {
        let broker = broker(&[(1, 4, 17), (0, 3, 7)]);

        assert_eq!(broker.newest_common(1, Versions::new(4, 12)), Ok(12));
        assert_eq!(broker.newest_common(0, Versions::new(3, 9)), Ok(7));
    }

    #[test]
    fn ranges_that_do_not_meet_have_no_version() {
        let broker = broker(&[(1, 13, 17), (0, 0, 2)]);

        assert_eq!(
            broker.newest_common(1, Versions::new(4, 12)),
            Err(ClientError::NoCommonVersion { api_key: ApiKey::FETCH })
        );
        assert_eq!(
            broker.newest_common(0, Versions::new(3, 9)),
            Err(ClientError::NoCommonVersion {
                api_key: ApiKey::PRODUCE
            })
        );
    }

    #[test]
    fn an_api_the_broker_does_not_know_has_no_version() {
        assert_eq!(
            broker(&[]).newest_common(32, Versions::new(1, 4)),
            Err(ClientError::NoCommonVersion {
                api_key: ApiKey::DESCRIBE_CONFIGS
            })
        );
    }

    #[test]
    fn an_error_code_is_the_brokers_answer() {
        let response = ApiVersionsResponse::default().with_error_code(ResponseError::UnsupportedVersion.code());

        assert_eq!(
            BrokerVersions::read(&response),
            Err(ClientError::Refused {
                error: ResponseError::UnsupportedVersion
            })
        );
    }
}
