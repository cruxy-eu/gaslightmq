//! The one error of a cluster and its handles.

use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::TopicNameError;
use gaslightmq_wire::{ApiKey, ResponseError};

/// Why a [`KafkaCluster`](crate::KafkaCluster) or one of its handles could not do what it was asked to do.
///
/// Every message is complete, so a test writes `?` or `expect` and reads one line.
/// What a test matches on is [`Refused`](Self::Refused): the error of Kafka that a broker answered with.
///
/// ```
/// use gaslightmq_test_kafka::KafkaClusterError;
/// use gaslightmq_wire::{ApiKey, ResponseError};
///
/// let error = KafkaClusterError::Refused {
///     api: ApiKey::CreateTopics,
///     error: ResponseError::TopicAlreadyExists,
///     about: "topic `orders`".to_owned(),
///     detail: Some("Topic 'orders' already exists.".to_owned()),
/// };
/// assert_eq!(
///     error.to_string(),
///     "CreateTopics for topic `orders`: the cluster answered TopicAlreadyExists: Topic 'orders' already exists."
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaClusterError {
    /// The cluster did not start.
    /// `reason` says why, and `log` holds the last lines the brokers wrote, when they wrote any.
    NotStarted { reason: String, log: String },
    /// The name of a new topic is no topic name.
    InvalidTopicName { error: TopicNameError },
    /// The records make no batch that a broker takes. `reason` says why.
    InvalidRecords { reason: String },
    /// A broker answered a request of `api` with an error of Kafka.
    /// `about` names the topic, the partition or the transaction the request was about,
    /// and `detail` is what the broker said on top of the error, when it said something.
    Refused {
        api: ApiKey,
        error: ResponseError,
        about: String,
        detail: Option<String>,
    },
    /// A broker did not answer a request, or its answer could not be read.
    /// `request` names the request, and `reason` says what went wrong.
    Unanswered { request: String, reason: String },
    /// The cluster did not get somewhere within the time the configuration gives it.
    /// `about` names what did not settle: a topic that not every broker knows yet, or a transaction that does not end.
    NotSettledInTime { about: String, waited: TimeSpan },
}

impl KafkaClusterError {
    /// The cluster did not start for `reason`, before a broker wrote anything.
    pub(crate) fn not_started(reason: impl Into<String>) -> Self {
        Self::NotStarted {
            reason: reason.into(),
            log: String::new(),
        }
    }

    /// The error in `error_code`, when it holds one, as the answer to a request of `api` about `about`.
    pub(crate) fn refused(api: ApiKey, about: &dyn fmt::Display, error_code: i16) -> Result<(), Self> {
        match ResponseError::try_from_code(error_code) {
            None => Ok(()),
            Some(error) => Err(Self::Refused {
                api,
                error,
                about: about.to_string(),
                detail: None,
            }),
        }
    }

    /// The same error, with what the broker said on top of it.
    #[must_use]
    pub(crate) fn with_detail(self, said: Option<impl fmt::Display>) -> Self {
        match self {
            Self::Refused { api, error, about, .. } => Self::Refused {
                api,
                error,
                about,
                detail: said.map(|said| said.to_string()),
            },
            other => other,
        }
    }

    /// The response to a request of `api` about `about` says nothing about it.
    pub(crate) fn not_named(api: ApiKey, about: &dyn fmt::Display) -> Self {
        Self::Unanswered {
            request: format!("{api:?} for {about}"),
            reason: "the response does not name it".to_owned(),
        }
    }

    pub(crate) fn not_settled(about: &dyn fmt::Display, waited: TimeSpan) -> Self {
        Self::NotSettledInTime {
            about: about.to_string(),
            waited,
        }
    }
}

impl fmt::Display for KafkaClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted { reason, log } => {
                write!(f, "the Kafka cluster did not start: {reason}")?;
                if log.is_empty() {
                    Ok(())
                } else {
                    write!(f, "\nthe end of the log of its brokers:\n{log}")
                }
            }
            Self::InvalidTopicName { error } => write!(f, "new topic: {error}"),
            Self::InvalidRecords { reason } => write!(f, "the records make no batch: {reason}"),
            Self::Refused {
                api,
                error,
                about,
                detail,
            } => {
                write!(f, "{api:?} for {about}: the cluster answered {error:?}")?;
                detail.iter().try_for_each(|detail| write!(f, ": {detail}"))
            }
            Self::Unanswered { request, reason } => write!(f, "{request}: {reason}"),
            Self::NotSettledInTime { about, waited } => {
                write!(
                    f,
                    "{about}: the cluster did not settle it within {} ms",
                    waited.as_millis()
                )
            }
        }
    }
}

/// The message names the cause, so the error has no source.
impl std::error::Error for KafkaClusterError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_cluster_that_did_not_start_shows_the_log_of_its_brokers_when_there_is_one() {
        let silent = KafkaClusterError::not_started("the configuration asks for no broker");
        let logged = KafkaClusterError::NotStarted {
            reason: "the brokers stopped".to_owned(),
            log: "ERROR Exiting Kafka".to_owned(),
        };

        assert_eq!(
            silent.to_string(),
            "the Kafka cluster did not start: the configuration asks for no broker"
        );
        assert_eq!(
            logged.to_string(),
            "the Kafka cluster did not start: the brokers stopped\nthe end of the log of its brokers:\nERROR Exiting Kafka"
        );
        assert!(logged.source().is_none());
    }

    #[test]
    fn an_error_code_of_zero_is_no_refusal() {
        let about = "partition 2 of topic `orders`";

        assert_eq!(KafkaClusterError::refused(ApiKey::Produce, &about, 0), Ok(()));
        let refused =
            KafkaClusterError::refused(ApiKey::Produce, &about, ResponseError::NotLeaderOrFollower.code()).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "Produce for partition 2 of topic `orders`: the cluster answered NotLeaderOrFollower"
        );
    }
}
