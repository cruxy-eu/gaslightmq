use core::time::Duration;

use crate::{Clock, Instant, Speed};

/// A clock that moves only when its owner moves it.
///
/// The clock starts at [`Instant::ZERO`] and never reads the system's time.
/// Its owner moves it in two ways.
///
/// - **Step on demand.**
///   [`advance`](VirtualClock::advance) moves the clock by a duration,
///   and [`advance_to`](VirtualClock::advance_to) moves it to an instant.
///   No real time passes, so a test waits for nothing.
/// - **Run against real time.**
///   A driver waits in real time, and reports how long it waited with [`run_for`](VirtualClock::run_for).
///   The clock moves that much, times its [`Speed`].
///   [`real_time_until`](VirtualClock::real_time_until) tells the driver how long to wait for a deadline.
///
/// The clock never goes back.
/// It stops at [`Instant::MAX`].
///
/// ```
/// use gaslightmq_clock::{Clock, Duration, Instant, Speed, VirtualClock};
///
/// let mut clock = VirtualClock::new();
///
/// // A 30-second session timeout takes no real time.
/// let timeout_at = clock.now() + Duration::from_secs(30);
/// clock.advance_to(timeout_at);
/// assert_eq!(clock.now(), timeout_at);
///
/// // At 3600x, an hour on the clock takes a second of real time.
/// clock.set_speed(Speed::times(3600));
/// let before = clock.now();
/// clock.run_for(Duration::from_secs(1));
/// assert_eq!(clock.now() - before, Duration::from_secs(3600));
/// ```
#[derive(Debug, Default)]
pub struct VirtualClock {
    now: Instant,
    speed: Speed,
}

impl VirtualClock {
    /// Creates a clock at [`Instant::ZERO`] that runs at [`Speed::REAL_TIME`].
    pub const fn new() -> VirtualClock {
        VirtualClock {
            now: Instant::ZERO,
            speed: Speed::REAL_TIME,
        }
    }

    /// Returns how fast the clock runs against real time.
    pub const fn speed(&self) -> Speed {
        self.speed
    }

    /// Sets how fast the clock runs against real time.
    ///
    /// The speed applies to the real time that passes from now on.
    /// A driver reports the real time that passed so far with [`run_for`](VirtualClock::run_for),
    /// before it changes the speed.
    pub fn set_speed(&mut self, speed: Speed) {
        self.speed = speed;
    }

    /// Moves the clock by `by`, whatever its speed, and returns the new instant.
    pub fn advance(&mut self, by: Duration) -> Instant {
        self.now += by;
        self.now
    }

    /// Moves the clock to `at`, whatever its speed, and returns the new instant.
    ///
    /// The clock never goes back.
    /// It stays where it is when `at` has passed.
    pub fn advance_to(&mut self, at: Instant) -> Instant {
        self.now = self.now.max(at);
        self.now
    }

    /// Moves the clock by the `real` time that passed, times its speed,
    /// and returns the new instant.
    ///
    /// A paused clock stays where it is.
    pub fn run_for(&mut self, real: Duration) -> Instant {
        let nanos = real.as_nanos().saturating_mul(u128::from(self.speed.factor()));
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);
        self.now = Instant::from_nanos(self.now.as_nanos().saturating_add(nanos));
        self.now
    }

    /// Returns how much real time passes until the clock reaches `at`, at its current speed.
    ///
    /// The result is zero when the clock reached `at` already.
    /// It is `None` when the clock is paused and `at` lies ahead:
    /// the clock then reaches `at` only when someone steps it.
    ///
    /// The result rounds up to a whole nanosecond,
    /// so [`run_for`](VirtualClock::run_for) with it always reaches `at`.
    pub fn real_time_until(&self, at: Instant) -> Option<Duration> {
        let remaining = at.as_nanos().saturating_sub(self.now.as_nanos());
        if remaining == 0 {
            return Some(Duration::ZERO);
        }
        if self.speed.is_paused() {
            return None;
        }
        let real = remaining.div_ceil(u64::from(self.speed.factor()));
        Some(Duration::from_nanos(real))
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Instant {
        self.now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_clock_starts_at_zero_and_runs_at_real_time() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), Instant::ZERO);
        assert_eq!(clock.speed(), Speed::REAL_TIME);

        let clock = VirtualClock::default();
        assert_eq!(clock.now(), Instant::ZERO);
        assert_eq!(clock.speed(), Speed::REAL_TIME);
    }

    #[test]
    fn the_clock_stands_still_until_someone_moves_it() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), clock.now());
    }

    #[test]
    fn advance_steps_the_clock_by_a_duration() {
        let mut clock = VirtualClock::new();
        assert_eq!(
            clock.advance(Duration::from_secs(10)),
            Instant::ZERO + Duration::from_secs(10)
        );
        assert_eq!(
            clock.advance(Duration::from_millis(500)),
            Instant::ZERO + Duration::from_millis(10_500)
        );
        assert_eq!(clock.now(), Instant::ZERO + Duration::from_millis(10_500));
        assert_eq!(clock.advance(Duration::ZERO), clock.now());
    }

    #[test]
    fn advance_ignores_the_speed() {
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(60));
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), Instant::ZERO + Duration::from_secs(1));

        clock.set_speed(Speed::PAUSED);
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), Instant::ZERO + Duration::from_secs(2));
    }

    #[test]
    fn advance_to_jumps_to_a_deadline() {
        let mut clock = VirtualClock::new();
        let deadline = Instant::ZERO + Duration::from_secs(30);
        assert_eq!(clock.advance_to(deadline), deadline);
        assert_eq!(clock.now(), deadline);
    }

    #[test]
    fn advance_to_never_moves_the_clock_back() {
        let mut clock = VirtualClock::new();
        let now = clock.advance(Duration::from_secs(30));
        assert_eq!(clock.advance_to(Instant::ZERO + Duration::from_secs(5)), now);
        assert_eq!(clock.advance_to(now), now);
        assert_eq!(clock.now(), now);
    }

    #[test]
    fn the_clock_stops_at_the_last_instant() {
        let mut clock = VirtualClock::new();
        clock.advance_to(Instant::from_nanos(u64::MAX - 1));
        assert_eq!(clock.advance(Duration::from_secs(1)), Instant::MAX);
        assert_eq!(clock.advance(Duration::MAX), Instant::MAX);
        assert_eq!(clock.run_for(Duration::MAX), Instant::MAX);
        assert_eq!(clock.now(), Instant::MAX);
    }

    #[test]
    fn run_for_moves_the_clock_as_fast_as_real_time_by_default() {
        let mut clock = VirtualClock::new();
        assert_eq!(
            clock.run_for(Duration::from_millis(250)),
            Instant::ZERO + Duration::from_millis(250)
        );
        assert_eq!(clock.run_for(Duration::from_nanos(1)), Instant::from_nanos(250_000_001));
    }

    #[test]
    fn run_for_multiplies_real_time_by_the_speed() {
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(3600));
        assert_eq!(clock.speed(), Speed::times(3600));
        clock.run_for(Duration::from_secs(1));
        assert_eq!(clock.now(), Instant::ZERO + Duration::from_secs(3600));
        clock.run_for(Duration::from_nanos(1));
        assert_eq!(clock.now().as_nanos(), 3_600_000_003_600);
    }

    #[test]
    fn run_for_leaves_a_paused_clock_where_it_is() {
        let mut clock = VirtualClock::new();
        let now = clock.advance(Duration::from_secs(7));
        clock.set_speed(Speed::PAUSED);
        assert_eq!(clock.run_for(Duration::from_secs(60)), now);
        assert_eq!(clock.run_for(Duration::MAX), now);
    }

    #[test]
    fn a_new_speed_applies_to_the_time_that_passes_afterwards() {
        let mut clock = VirtualClock::new();
        clock.run_for(Duration::from_secs(2));
        clock.set_speed(Speed::times(10));
        clock.run_for(Duration::from_secs(2));
        clock.set_speed(Speed::PAUSED);
        clock.run_for(Duration::from_secs(2));
        clock.set_speed(Speed::REAL_TIME);
        clock.run_for(Duration::from_secs(2));
        assert_eq!(clock.now(), Instant::ZERO + Duration::from_secs(24));
    }

    #[test]
    fn run_for_stops_at_the_last_instant_when_the_product_overflows() {
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(u32::MAX));
        assert_eq!(clock.run_for(Duration::MAX), Instant::MAX);

        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(2));
        assert_eq!(clock.run_for(Duration::from_nanos(u64::MAX)), Instant::MAX);
    }

    #[test]
    fn real_time_until_is_zero_for_an_instant_the_clock_reached() {
        let mut clock = VirtualClock::new();
        let now = clock.advance(Duration::from_secs(10));
        assert_eq!(clock.real_time_until(now), Some(Duration::ZERO));
        assert_eq!(clock.real_time_until(Instant::ZERO), Some(Duration::ZERO));

        clock.set_speed(Speed::PAUSED);
        assert_eq!(clock.real_time_until(now), Some(Duration::ZERO));
        assert_eq!(clock.real_time_until(Instant::ZERO), Some(Duration::ZERO));
    }

    #[test]
    fn real_time_until_is_the_virtual_time_at_real_time_speed() {
        let mut clock = VirtualClock::new();
        let now = clock.advance(Duration::from_secs(10));
        assert_eq!(
            clock.real_time_until(now + Duration::from_secs(30)),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            clock.real_time_until(Instant::MAX),
            Some(Duration::from_nanos(u64::MAX - 10_000_000_000))
        );
    }

    #[test]
    fn real_time_until_divides_by_the_speed() {
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(60));
        assert_eq!(
            clock.real_time_until(Instant::ZERO + Duration::from_secs(30)),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn real_time_until_is_none_for_a_paused_clock() {
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::PAUSED);
        assert_eq!(clock.real_time_until(Instant::from_nanos(1)), None);
        assert_eq!(clock.real_time_until(Instant::MAX), None);
    }

    #[test]
    fn real_time_until_rounds_up_so_the_wait_reaches_the_deadline() {
        for (factor, remaining) in [(3, 10), (7, 1), (60, 61), (1000, 999), (u32::MAX, 5)] {
            let mut clock = VirtualClock::new();
            clock.set_speed(Speed::times(factor));
            let deadline = Instant::from_nanos(remaining);

            let wait = clock.real_time_until(deadline).expect("the clock runs");
            let exact = remaining.div_ceil(u64::from(factor));
            assert_eq!(wait, Duration::from_nanos(exact));

            // One nanosecond less does not reach the deadline.
            let mut early = VirtualClock::new();
            early.set_speed(Speed::times(factor));
            early.run_for(wait.saturating_sub(Duration::from_nanos(1)));
            assert!(early.now() < deadline, "{factor}x, {remaining} ns");

            clock.run_for(wait);
            assert!(clock.now() >= deadline, "{factor}x, {remaining} ns");
            assert_eq!(clock.real_time_until(deadline), Some(Duration::ZERO));
        }
    }

    #[test]
    fn a_driver_loop_fires_timers_in_order() {
        // A driver waits for the next deadline, reports the wait, and fires what is due.
        let mut clock = VirtualClock::new();
        clock.set_speed(Speed::times(100));
        let start = clock.now();
        let mut timers = vec![
            start + Duration::from_secs(1),
            start + Duration::from_secs(5),
            start + Duration::from_secs(60),
        ];
        let mut real = Duration::ZERO;
        let mut fired = Vec::new();

        while let Some(&next) = timers.first() {
            let wait = clock.real_time_until(next).expect("the clock runs");
            real += wait;
            let now = clock.run_for(wait);
            while timers.first().is_some_and(|&at| at <= now) {
                fired.push(timers.remove(0));
            }
        }

        assert_eq!(fired.len(), 3);
        assert_eq!(clock.now(), start + Duration::from_secs(60));
        assert_eq!(real, Duration::from_millis(600));
    }
}
