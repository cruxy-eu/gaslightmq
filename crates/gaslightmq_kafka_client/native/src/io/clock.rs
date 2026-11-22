//! The clock a role reads.

use gaslightmq_clock::{Clock, Duration, Instant};

/// The time since a role instance was built, as the runtime counts it.
pub(crate) struct MonotonicClock {
    started: tokio::time::Instant,
}

impl MonotonicClock {
    /// A clock that starts now.
    pub fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
        }
    }

    /// How long it takes until the clock reaches `instant`. No time at all when it already has.
    pub fn time_until(&self, instant: Instant) -> Duration {
        instant.duration_since(self.now())
    }

    /// Wait until the clock reaches `instant`. Returns at once when it already has.
    pub async fn wait_until(&self, instant: Instant) {
        tokio::time::sleep(self.time_until(instant)).await;
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Instant {
        Instant::from_nanos(u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
}
