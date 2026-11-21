use std::rc::Rc;
use std::sync::Arc;

use crate::Instant;

/// A source of the current instant.
///
/// The IO edge holds a clock, and passes [`Clock::now`] to the state machines it drives.
/// A state machine never holds a clock itself.
///
/// [`VirtualClock`](crate::VirtualClock) implements it for tests and for the simulated broker.
/// The IO edge implements it for the wall clock.
///
/// ```
/// use gaslightmq_clock::{Clock, Duration, Instant, VirtualClock};
///
/// fn seconds_since_start(clock: &impl Clock) -> u64 {
///     (clock.now() - Instant::ZERO).as_secs()
/// }
///
/// let mut clock = VirtualClock::new();
/// clock.advance(Duration::from_secs(90));
/// assert_eq!(seconds_since_start(&clock), 90);
/// ```
pub trait Clock {
    /// Returns the current instant.
    ///
    /// A clock never goes back:
    /// the instant is never earlier than one this clock returned before.
    fn now(&self) -> Instant;
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<C: Clock + ?Sized> Clock for Box<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<C: Clock + ?Sized> Clock for Rc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Duration, VirtualClock};

    fn read(clock: impl Clock) -> Instant {
        clock.now()
    }

    #[test]
    fn a_reference_and_every_pointer_to_a_clock_is_a_clock() {
        let mut clock = VirtualClock::new();
        let now = clock.advance(Duration::from_secs(3));

        assert_eq!(read(&clock), now);

        let boxed: Box<dyn Clock> = Box::new(clock);
        assert_eq!(read(&boxed), now);
        assert_eq!(read(boxed), now);

        let shared: Rc<dyn Clock> = Rc::new(VirtualClock::new());
        assert_eq!(read(shared), Instant::ZERO);

        let shared: Arc<dyn Clock + Send + Sync> = Arc::new(VirtualClock::new());
        assert_eq!(read(Arc::clone(&shared)), Instant::ZERO);
    }
}
