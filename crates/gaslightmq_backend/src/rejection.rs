//! Why a backend did not apply or emulate an incident.

use gaslightmq_incident::{IncidentKind, UnsupportedIncidents};

/// Why a backend rejects an incident.
///
/// A rejected incident changed nothing: neither the incident nor any of its consequences took effect.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_incident::prelude::*;
///
/// let rejection = IncidentRejection::Impossible {
///     incident: IncidentKind::BrokerGoesDown,
///     reason: "the cluster has no broker 9".to_owned(),
/// };
/// assert_eq!(rejection.to_string(), "broker-goes-down cannot happen: the cluster has no broker 9");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IncidentRejection {
    /// The backend can neither apply nor emulate the incident, or one of the incidents that follow from it.
    ///
    /// A scenario is checked against the [support](crate::Backend::incident_support) of its backend when it loads.
    /// This rejection is for an incident that reaches the backend without that check.
    // The error names the incidents itself, and which incident each one follows from.
    #[error("{error}")]
    Unsupported { error: UnsupportedIncidents },
    /// The incident cannot happen in the cluster as it is.
    /// `reason` says why: the cluster has no such broker, or the new leader is no replica of the partition.
    #[error("{incident} cannot happen: {reason}")]
    Impossible { incident: IncidentKind, reason: String },
}

impl From<UnsupportedIncidents> for IncidentRejection {
    fn from(error: UnsupportedIncidents) -> Self {
        Self::Unsupported { error }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_incident::{IncidentDefinition, IncidentSupport, Requirements};

    use super::*;

    #[test]
    fn a_rejection_says_everything_in_one_message() {
        let failover = IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        let unsupported = IncidentSupport::none()
            .emulates([IncidentKind::BrokerGoesDown])
            .check(&Requirements::from(&failover))
            .unwrap_err();
        let cases = [
            (
                IncidentRejection::from(unsupported),
                "the backend cannot apply or emulate: leader-moves (a consequence of broker-goes-down), \
                 coordinator-moves (a consequence of broker-goes-down)",
            ),
            (
                IncidentRejection::Impossible {
                    incident: IncidentKind::LeaderMoves,
                    reason: "broker 3 is no replica of orders.eu/3".to_owned(),
                },
                "leader-moves cannot happen: broker 3 is no replica of orders.eu/3",
            ),
        ];

        for (rejection, message) in cases {
            assert_eq!(rejection.to_string(), message);
            assert!(rejection.source().is_none(), "{rejection}");
        }
    }
}
