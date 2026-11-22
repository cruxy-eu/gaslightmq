//! Whether a failed call tries again, and when.

use std::time::Duration;

use gaslightmq_clock::{Instant, TimeSpan};

use crate::configuration::ClientConfiguration;

/// The times and counts that bound one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    /// How long the call may take, with every retry.
    pub call_timeout: Duration,
    /// How long one request waits for its response.
    pub request_timeout: Duration,
    /// How long a socket may take to connect.
    pub socket_connection_setup_timeout: Duration,
    /// How long the call waits after its first retriable failure. The wait doubles with every further one.
    pub retry_backoff: Duration,
    /// The longest the call waits between two tries.
    pub retry_backoff_max: Duration,
    /// How often the call tries again. `None` when only its time ends the retries.
    pub retries: Option<u32>,
}

impl Limits {
    /// The limits of a call that may take `call_timeout` and try again `retries` times,
    /// for a role with `configuration`.
    pub fn new(configuration: &ClientConfiguration, call_timeout: TimeSpan, retries: Option<u32>) -> Self {
        Self {
            call_timeout: call_timeout.duration(),
            request_timeout: configuration.request_timeout.duration(),
            socket_connection_setup_timeout: configuration.socket_connection_setup_timeout.duration(),
            retry_backoff: configuration.retry_backoff.duration(),
            retry_backoff_max: configuration.retry_backoff_max.duration(),
            retries,
        }
    }
}

/// How much longer a call waits after each failure: twice as long.
/// Kafka's clients fix this number, and have no property for it.
const BACKOFF_GROWTH: u32 = 2;

/// Which request of a call failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failed {
    /// The request of the operation. Trying it again uses one of the retries, as `retries` means in Kafka.
    Operation,
    /// A question that comes before it: who leads the partition, or who coordinates the group.
    /// Asking again uses no retry. Only the time of the call ends it.
    Lookup,
}

/// What a call does after a retriable failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AfterFailure {
    /// Try again at this instant.
    RetryAt(Instant),
    /// Stop: the call has used every retry.
    OutOfRetries,
    /// Stop: the call ends before the next try could start.
    OutOfTime,
}

/// The tries of one call: when the call ends, how long it waits next, and how many retries it has left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Attempts {
    deadline: Instant,
    /// The wait after the next failure, before the longest wait cuts it off.
    backoff: Duration,
    backoff_max: Duration,
    retries_left: Option<u32>,
}

impl Attempts {
    /// The tries of a call that starts at `now`.
    pub fn new(limits: &Limits, now: Instant) -> Self {
        Self {
            deadline: now + limits.call_timeout,
            backoff: limits.retry_backoff,
            backoff_max: limits.retry_backoff_max,
            retries_left: limits.retries,
        }
    }

    /// The instant the call ends.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// How much time the call has left.
    pub fn remaining(&self, now: Instant) -> Duration {
        self.deadline.duration_since(now)
    }

    /// Decide what follows a retriable failure at `now`.
    ///
    /// The first wait is the retry backoff, and every wait after it is twice the one before,
    /// until the longest wait.
    pub fn after_failure(&mut self, failed: Failed, now: Instant) -> AfterFailure {
        if failed == Failed::Operation && self.retries_left == Some(0) {
            return AfterFailure::OutOfRetries;
        }
        let retry_at = now + self.backoff.min(self.backoff_max);
        if retry_at >= self.deadline {
            return AfterFailure::OutOfTime;
        }
        if failed == Failed::Operation {
            self.retries_left = self.retries_left.map(|retries| retries.saturating_sub(1));
        }
        self.backoff = self.backoff.saturating_mul(BACKOFF_GROWTH);
        AfterFailure::RetryAt(retry_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(retries: Option<u32>) -> Limits {
        Limits {
            call_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            socket_connection_setup_timeout: Duration::from_secs(1),
            retry_backoff: Duration::from_millis(100),
            retry_backoff_max: Duration::from_millis(1_000),
            retries,
        }
    }

    fn at(millis: u64) -> Instant {
        Instant::ZERO + Duration::from_millis(millis)
    }

    #[test]
    fn a_call_tries_again_after_the_backoff() {
        let mut attempts = Attempts::new(&limits(None), at(0));

        assert_eq!(
            attempts.after_failure(Failed::Operation, at(250)),
            AfterFailure::RetryAt(at(350))
        );
    }

    #[test]
    fn the_wait_doubles_with_every_failure_up_to_the_longest_wait() {
        let mut attempts = Attempts::new(&limits(None), at(0));

        let waits: Vec<_> = (0..6)
            .map(|_| match attempts.after_failure(Failed::Operation, at(0)) {
                AfterFailure::RetryAt(retry_at) => retry_at.duration_since(at(0)).as_millis(),
                other => panic!("the call has time and retries left: {other:?}"),
            })
            .collect();

        assert_eq!(waits, [100, 200, 400, 800, 1_000, 1_000]);
    }

    #[test]
    fn a_backoff_longer_than_the_longest_wait_is_the_longest_wait_every_time() {
        let mut limits = limits(None);
        limits.retry_backoff = Duration::from_secs(3);
        let mut attempts = Attempts::new(&limits, at(0));

        assert_eq!(
            attempts.after_failure(Failed::Operation, at(0)),
            AfterFailure::RetryAt(at(1_000))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(0)),
            AfterFailure::RetryAt(at(1_000))
        );
    }

    #[test]
    fn a_call_stops_when_the_next_try_would_start_at_its_end() {
        let mut attempts = Attempts::new(&limits(None), at(1_000));

        assert_eq!(attempts.remaining(at(1_000)), Duration::from_secs(10));
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(10_899)),
            AfterFailure::RetryAt(at(10_999))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(10_800)),
            AfterFailure::OutOfTime
        );
        assert_eq!(attempts.remaining(at(12_000)), Duration::ZERO);
    }

    #[test]
    fn a_call_stops_when_it_has_used_every_retry() {
        let mut attempts = Attempts::new(&limits(Some(2)), at(0));

        assert_eq!(
            attempts.after_failure(Failed::Operation, at(0)),
            AfterFailure::RetryAt(at(100))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(100)),
            AfterFailure::RetryAt(at(300))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(300)),
            AfterFailure::OutOfRetries
        );
    }

    #[test]
    fn asking_again_for_a_leader_or_a_coordinator_uses_no_retry() {
        let mut attempts = Attempts::new(&limits(Some(1)), at(0));

        assert_eq!(
            attempts.after_failure(Failed::Lookup, at(0)),
            AfterFailure::RetryAt(at(100))
        );
        assert_eq!(
            attempts.after_failure(Failed::Lookup, at(100)),
            AfterFailure::RetryAt(at(300))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(300)),
            AfterFailure::RetryAt(at(700))
        );
        assert_eq!(
            attempts.after_failure(Failed::Operation, at(700)),
            AfterFailure::OutOfRetries
        );
        assert_eq!(
            attempts.after_failure(Failed::Lookup, at(700)),
            AfterFailure::RetryAt(at(1_500))
        );
        assert_eq!(
            attempts.after_failure(Failed::Lookup, at(9_500)),
            AfterFailure::OutOfTime
        );
    }

    #[test]
    fn a_call_without_retries_stops_at_its_first_failure() {
        let mut attempts = Attempts::new(&limits(Some(0)), at(0));

        assert_eq!(
            attempts.after_failure(Failed::Operation, at(0)),
            AfterFailure::OutOfRetries
        );
    }

    #[test]
    fn limits_come_from_the_configuration() {
        let mut configuration =
            ClientConfiguration::new("localhost:9092".parse::<crate::address::BootstrapServers>().unwrap());
        configuration.retry_backoff = TimeSpan::from_millis(20);
        configuration.retry_backoff_max = TimeSpan::from_millis(80);

        let limits = Limits::new(&configuration, TimeSpan::from_secs(7), Some(3));

        assert_eq!(
            limits,
            Limits {
                call_timeout: Duration::from_secs(7),
                request_timeout: Duration::from_secs(30),
                socket_connection_setup_timeout: Duration::from_secs(10),
                retry_backoff: Duration::from_millis(20),
                retry_backoff_max: Duration::from_millis(80),
                retries: Some(3),
            }
        );
    }
}
