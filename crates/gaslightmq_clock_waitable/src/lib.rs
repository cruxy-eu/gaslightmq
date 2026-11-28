//! Clocks that a task waits on: the [`Clock`] of `gaslightmq_clock`, with a wait until an instant.
//!
//! | Type | Is |
//! |---|---|
//! | [`WaitableClock`] | A clock that a task waits on: it tells the time, and it says when it reaches an instant. |
//! | [`WallClock`] | Real time. The clock of a proxy in front of a real cluster, and the [`UtcClock`] of a process: the instant in UTC that names a recording. |
//! | [`SteppedClock`] | A clock that moves when its owner moves it. The clock of a test, and of a proxy in front of a broker. |
//!
//! `gaslightmq_clock` is sans-IO, so its [`Clock`] tells the time and nothing waits on it:
//! a wait is a future, which the timer of a runtime or another task ends.
//! That part belongs to the IO edge, and it is all this crate adds.
//! Both clocks here are a [`Clock`], and the stepped one is the [`VirtualClock`] of that crate behind a handle.
//!
//! An engine waits on the clock it is handed.
//! Whoever starts an engine picks that clock: a host picks the wall clock for a proxy in front of a real cluster,
//! and a test picks a stepped one. So the clocks stand in a crate of their own, and not in the engine.
//!
//! # IO
//!
//! This crate is part of the IO edge.
//! The wall clock reads the time of the system, and its waits are waits on the timer of Tokio,
//! so a wait on it runs inside the runtime. A wait on the stepped clock needs no timer, and takes no real time.

use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use gaslightmq_clock::{Clock, Duration, Instant, UtcClock, UtcInstant, VirtualClock};
use tokio::sync::watch;

/// A clock that a task waits on: it tells the time, and it says when it reaches an instant.
///
/// An engine has one clock.
/// Every session reads it when something happens, and hands the instant to its pipeline.
/// A delay is a wait on it, and so are the lifetime of a rule and the time a client context stays.
///
/// | Clock | Is |
/// |---|---|
/// | [`WallClock`] | Real time. The clock of a proxy in front of a real cluster. |
/// | [`SteppedClock`] | A clock that moves when its owner moves it. The clock of a test, and of a proxy in front of a broker. |
///
/// The trait stands next to the [`Clock`] it extends, and not in its crate:
/// that crate is sans-IO, and a wait is a future that a runtime ends.
///
/// An implementation can write [`until`](Self::until) as an `async fn`.
pub trait WaitableClock: Clock + Send + Sync + 'static {
    /// Waits until the clock reaches `at`. Returns at once when it already has.
    ///
    /// The wait ends when the clock reaches the instant, however fast the clock runs.
    /// Giving up the wait loses nothing.
    fn until(&self, at: Instant) -> impl Future<Output = ()> + Send;
}

/// A shared clock is a clock, so an engine and whoever moves its clock hold the same one.
impl<C: WaitableClock> WaitableClock for Arc<C> {
    fn until(&self, at: Instant) -> impl Future<Output = ()> + Send {
        (**self).until(at)
    }
}

/// Real time, counted from the moment the clock was made.
///
/// It is the clock of a proxy in front of a real cluster.
/// A wait on it is a wait on the timer of the runtime.
///
/// ```
/// use gaslightmq_clock::{Clock, Duration, Instant};
/// use gaslightmq_clock_waitable::{WaitableClock, WallClock};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let clock = WallClock::new();
/// let started = clock.now();
///
/// clock.until(started + Duration::from_millis(5)).await;
/// assert!(clock.now() - started >= Duration::from_millis(5));
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct WallClock {
    /// The moment that is [`Instant::ZERO`] on this clock.
    started: tokio::time::Instant,
}

impl WallClock {
    /// A clock that is at [`Instant::ZERO`] now.
    pub fn new() -> Self {
        Self {
            started: tokio::time::Instant::now(),
        }
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    fn now(&self) -> Instant {
        let nanos = self.started.elapsed().as_nanos();
        // A clock that ran for more than five centuries stays at its last instant.
        Instant::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// The wall clock tells the instant in UTC too: the time of the system, to the millisecond.
///
/// A system whose time is before 1970 tells 1970, and one after the year 9999 tells the last instant of it.
impl UtcClock for WallClock {
    fn utc_now(&self) -> UtcInstant {
        let since_1970 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let millis = u64::try_from(since_1970.as_millis()).unwrap_or(u64::MAX);
        UtcInstant::new(millis).unwrap_or(UtcInstant::LAST)
    }
}

impl WaitableClock for WallClock {
    async fn until(&self, at: Instant) {
        if self.now() >= at {
            return;
        }
        match self.started.checked_add(Duration::from_nanos(at.as_nanos())) {
            Some(at) => tokio::time::sleep_until(at).await,
            // The runtime cannot name the moment, so it never comes.
            None => std::future::pending().await,
        }
    }
}

/// A clock that stands still until its owner moves it: a [`VirtualClock`] that tasks wait on.
///
/// It starts at [`Instant::ZERO`], and never reads the time of the system.
/// A clone is a handle to the same clock: an engine holds one, and whoever moves the clock holds another.
/// A wait ends when someone moves the clock to its instant or past it, and no real time has to pass.
///
/// A test steps it, so a delay of a minute takes no time.
/// The task that drives a broker moves it as fast as the clock of the broker runs.
///
/// ```
/// use gaslightmq_clock::{Clock, Duration, Instant};
/// use gaslightmq_clock_waitable::{SteppedClock, WaitableClock};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let clock = SteppedClock::new();
/// let of_the_engine = clock.clone();
/// let timeout_at = clock.now() + Duration::from_secs(30);
///
/// let waiting = tokio::spawn(async move { of_the_engine.until(timeout_at).await });
/// clock.advance(Duration::from_secs(30));
///
/// waiting.await.expect("the wait ends");
/// assert_eq!(clock.now(), timeout_at);
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct SteppedClock {
    /// The clock, and the word to every wait that it moved.
    clock: Arc<watch::Sender<VirtualClock>>,
}

impl SteppedClock {
    /// A clock at [`Instant::ZERO`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves the clock by `by`, and returns the new instant.
    pub fn advance(&self, by: Duration) -> Instant {
        self.clock.send_modify(|clock| {
            clock.advance(by);
        });
        self.now()
    }

    /// Moves the clock to `at`, and returns the new instant.
    ///
    /// The clock never goes back: it stays where it is when `at` has passed.
    pub fn advance_to(&self, at: Instant) -> Instant {
        self.clock.send_modify(|clock| {
            clock.advance_to(at);
        });
        self.now()
    }
}

impl Clock for SteppedClock {
    fn now(&self) -> Instant {
        self.clock.borrow().now()
    }
}

impl WaitableClock for SteppedClock {
    async fn until(&self, at: Instant) {
        let mut moved = self.clock.subscribe();
        // The clock lives as long as this handle, so the wait only ends when the clock reaches the instant.
        let _ = moved.wait_for(|clock| clock.now() >= at).await;
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use super::*;

    #[tokio::test]
    async fn the_wall_clock_starts_at_zero_and_never_goes_back() {
        let clock = WallClock::default();

        let first = clock.now();
        let second = clock.now();

        assert!(first - Instant::ZERO < Duration::from_secs(1));
        assert!(second >= first);
    }

    #[tokio::test]
    async fn a_wait_for_an_instant_that_passed_ends_at_once() {
        let wall = WallClock::new();
        let stepped = SteppedClock::new();
        stepped.advance(Duration::from_secs(5));

        assert_eq!(futures::poll!(Box::pin(wall.until(Instant::ZERO))), Poll::Ready(()));
        assert_eq!(futures::poll!(Box::pin(stepped.until(stepped.now()))), Poll::Ready(()));
    }

    #[tokio::test]
    async fn a_wait_on_a_stepped_clock_ends_when_the_clock_reaches_its_instant() {
        let clock = SteppedClock::new();
        let at = Instant::ZERO + Duration::from_secs(10);
        let mut wait = Box::pin(clock.until(at));

        assert_eq!(futures::poll!(wait.as_mut()), Poll::Pending);
        assert_eq!(
            clock.advance(Duration::from_secs(9)),
            Instant::ZERO + Duration::from_secs(9)
        );
        assert_eq!(futures::poll!(wait.as_mut()), Poll::Pending);

        // Every handle moves the same clock, and the clock may jump past the instant.
        Arc::new(clock.clone()).advance_to(at + Duration::from_secs(60));
        assert_eq!(futures::poll!(wait.as_mut()), Poll::Ready(()));
    }

    #[tokio::test]
    async fn a_stepped_clock_never_goes_back() {
        let clock = SteppedClock::new();
        let now = clock.advance(Duration::from_secs(30));

        assert_eq!(clock.advance_to(Instant::ZERO + Duration::from_secs(5)), now);
        assert_eq!(clock.now(), now);
    }
}
