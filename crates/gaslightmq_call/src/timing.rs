//! When a call arrived, how long the backend took, and how long the client waited.

use gaslightmq_clock::{Duration, Instant};

/// The moments of one call, on the clock of its engine.
///
/// | Moment | Is | Missing when |
/// |---|---|---|
/// | [`arrived_at`](Self::arrived_at) | The request arrived. | Never. |
/// | [`forwarded_at`](Self::forwarded_at) | The request left for the backend. | A lie ended or refused the whole call before forwarding. |
/// | [`answered_at`](Self::answered_at) | The backend answered. | The request never left, or the backend never answered. |
/// | [`delivered_at`](Self::delivered_at) | The response left for the client. | The client received no response. |
///
/// The moments follow each other, and a later one never comes before an earlier one.
/// A delay before forwarding shows between arriving and forwarding,
/// and a delay after the response between the answer and the delivery.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::{Duration, Instant};
///
/// let at = |millis| Instant::ZERO + Duration::from_millis(millis);
/// let timing = CallTiming::arrived(at(250)).forwarded(at(251))?.answered(at(265))?.delivered(at(1015))?;
///
/// assert_eq!(timing.backend_took(), Some(Duration::from_millis(14)));
/// assert_eq!(timing.client_waited(), Some(Duration::from_millis(765)));
/// assert_eq!(CallTiming::arrived(at(250)).forwarded(at(249)), Err(CallTimingError::ForwardedBeforeArrival));
/// # Ok::<(), CallTimingError>(())
/// ```
///
/// With the `serde` feature, a timing serializes as a map of the moments it has, each an instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CallTiming {
    arrived: Instant,
    forwarded: Option<Instant>,
    answered: Option<Instant>,
    delivered: Option<Instant>,
}

impl CallTiming {
    /// The timing of a call whose request arrived at `at`, and that has got no further yet.
    pub const fn arrived(at: Instant) -> CallTiming {
        CallTiming {
            arrived: at,
            forwarded: None,
            answered: None,
            delivered: None,
        }
    }

    /// The request left for the backend at `at`.
    ///
    /// # Errors
    ///
    /// [`CallTimingError::ForwardedBeforeArrival`] when `at` is before the request arrived.
    pub fn forwarded(mut self, at: Instant) -> Result<CallTiming, CallTimingError> {
        if at < self.arrived {
            return Err(CallTimingError::ForwardedBeforeArrival);
        }
        self.forwarded = Some(at);
        Ok(self)
    }

    /// The backend answered at `at`.
    ///
    /// # Errors
    ///
    /// - [`CallTimingError::AnsweredWithoutForwarding`] when the request never left for the backend.
    /// - [`CallTimingError::AnsweredBeforeForwarding`] when `at` is before the request left.
    pub fn answered(mut self, at: Instant) -> Result<CallTiming, CallTimingError> {
        match self.forwarded {
            None => Err(CallTimingError::AnsweredWithoutForwarding),
            Some(forwarded) if at < forwarded => Err(CallTimingError::AnsweredBeforeForwarding),
            Some(_) => {
                self.answered = Some(at);
                Ok(self)
            }
        }
    }

    /// The response left for the client at `at`.
    ///
    /// # Errors
    ///
    /// [`CallTimingError::DeliveredTooEarly`] when `at` is before the request arrived, before it left for the backend,
    /// or before the backend answered.
    pub fn delivered(mut self, at: Instant) -> Result<CallTiming, CallTimingError> {
        let latest = self.answered.or(self.forwarded).unwrap_or(self.arrived);
        if at < latest {
            return Err(CallTimingError::DeliveredTooEarly);
        }
        self.delivered = Some(at);
        Ok(self)
    }

    /// When the request arrived.
    pub const fn arrived_at(self) -> Instant {
        self.arrived
    }

    /// When the request left for the backend, or `None` when it never did.
    pub const fn forwarded_at(self) -> Option<Instant> {
        self.forwarded
    }

    /// When the backend answered, or `None` when it never did.
    pub const fn answered_at(self) -> Option<Instant> {
        self.answered
    }

    /// When the response left for the client, or `None` when the client received none.
    pub const fn delivered_at(self) -> Option<Instant> {
        self.delivered
    }

    /// How long the backend took to answer, or `None` when it never answered.
    pub fn backend_took(self) -> Option<Duration> {
        Some(self.answered? - self.forwarded?)
    }

    /// How long the client waited for its response, or `None` when it received none.
    pub fn client_waited(self) -> Option<Duration> {
        Some(self.delivered? - self.arrived)
    }
}

/// Why a moment does not fit the [`CallTiming`] of a call.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallTimingError {
    /// The request left for the backend before it arrived.
    #[error("a request does not leave for the backend before it arrives")]
    ForwardedBeforeArrival,
    /// The backend answered a request that never left for it.
    #[error("a backend does not answer a request that never left for it")]
    AnsweredWithoutForwarding,
    /// The backend answered before the request left for it.
    #[error("a backend does not answer before the request leaves for it")]
    AnsweredBeforeForwarding,
    /// The response left for the client before the request arrived, before it was forwarded,
    /// or before the backend answered.
    #[error("a response does not leave for the client before the call got that far")]
    DeliveredTooEarly,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: u64) -> Instant {
        Instant::ZERO + Duration::from_millis(millis)
    }

    #[test]
    fn a_call_that_only_arrived_has_one_moment() {
        let timing = CallTiming::arrived(at(250));

        assert_eq!(timing.arrived_at(), at(250));
        assert_eq!(timing.forwarded_at(), None);
        assert_eq!(timing.answered_at(), None);
        assert_eq!(timing.delivered_at(), None);
        assert_eq!(timing.backend_took(), None);
        assert_eq!(timing.client_waited(), None);
    }

    #[test]
    fn a_call_that_was_refused_before_forwarding_has_no_backend_time() {
        let timing = CallTiming::arrived(at(250)).delivered(at(251)).unwrap();

        assert_eq!(timing.backend_took(), None);
        assert_eq!(timing.client_waited(), Some(Duration::from_millis(1)));
    }

    #[test]
    fn a_call_that_a_hang_up_ended_after_the_response_has_no_delivery() {
        let timing = CallTiming::arrived(at(250))
            .forwarded(at(250))
            .unwrap()
            .answered(at(264))
            .unwrap();

        assert_eq!(timing.forwarded_at(), Some(at(250)));
        assert_eq!(timing.answered_at(), Some(at(264)));
        assert_eq!(timing.backend_took(), Some(Duration::from_millis(14)));
        assert_eq!(timing.client_waited(), None);
    }

    #[test]
    fn moments_at_the_same_instant_follow_each_other() {
        let timing = CallTiming::arrived(at(5))
            .forwarded(at(5))
            .unwrap()
            .answered(at(5))
            .unwrap()
            .delivered(at(5))
            .unwrap();

        assert_eq!(timing.delivered_at(), Some(at(5)));
        assert_eq!(timing.client_waited(), Some(Duration::ZERO));
    }

    #[test]
    fn a_moment_that_comes_too_early_is_an_error() {
        let arrived = CallTiming::arrived(at(10));
        let forwarded = arrived.forwarded(at(20)).unwrap();
        let answered = forwarded.answered(at(30)).unwrap();

        assert_eq!(arrived.forwarded(at(9)), Err(CallTimingError::ForwardedBeforeArrival));
        assert_eq!(
            arrived.answered(at(30)),
            Err(CallTimingError::AnsweredWithoutForwarding)
        );
        assert_eq!(
            forwarded.answered(at(19)),
            Err(CallTimingError::AnsweredBeforeForwarding)
        );
        assert_eq!(arrived.delivered(at(9)), Err(CallTimingError::DeliveredTooEarly));
        assert_eq!(forwarded.delivered(at(19)), Err(CallTimingError::DeliveredTooEarly));
        assert_eq!(answered.delivered(at(29)), Err(CallTimingError::DeliveredTooEarly));
    }

    #[test]
    fn a_timing_error_says_what_is_wrong() {
        let cases = [
            (
                CallTimingError::ForwardedBeforeArrival,
                "a request does not leave for the backend before it arrives",
            ),
            (
                CallTimingError::AnsweredWithoutForwarding,
                "a backend does not answer a request that never left for it",
            ),
            (
                CallTimingError::AnsweredBeforeForwarding,
                "a backend does not answer before the request leaves for it",
            ),
            (
                CallTimingError::DeliveredTooEarly,
                "a response does not leave for the client before the call got that far",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
