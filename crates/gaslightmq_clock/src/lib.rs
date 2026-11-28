//! The clock: instants that do not come from the system, and a clock that moves on demand.
//!
//! | Type | Is |
//! |---|---|
//! | [`Clock`] | A source of the current instant. |
//! | [`Instant`] | A point on a clock's timeline, in nanoseconds since the clock started. |
//! | [`Duration`] | A length of time that someone measured or computed. It is the standard type. |
//! | [`TimeSpan`] | A length of time as a definition writes it, such as `10s`. |
//! | [`VirtualClock`] | A clock that moves only when its owner moves it. |
//! | [`Speed`] | How fast a virtual clock runs against real time. |
//! | [`UtcInstant`], [`UtcClock`] | An instant in UTC to the millisecond, such as when a recording started, and a source of the current one. |
//!
//! [`UtcInstant`] checks its value, and its error is [`UtcInstantError`].
//!
//! The crate is sans-IO.
//! It never reads the system's time.
//! A state machine takes the current [`Instant`] as an argument,
//! and names a deadline as an [`Instant`] too.
//! Whoever drives the state machine owns the clock.
//!
//! ```
//! use gaslightmq_clock::{Clock, Duration, Instant, Speed, TimeSpan, VirtualClock};
//!
//! // A test owns a virtual clock, which starts at zero.
//! let mut clock = VirtualClock::new();
//! let armed_at = clock.now();
//! assert_eq!(armed_at, Instant::ZERO);
//!
//! // A definition says `after: 10s`, so the window opens ten seconds after arming.
//! let after: TimeSpan = "10s".parse()?;
//! let opens_at = armed_at + after;
//!
//! // The test steps the clock, and no real time passes.
//! clock.advance(Duration::from_secs(4));
//! assert!(clock.now() < opens_at);
//! clock.advance_to(opens_at);
//! assert_eq!(clock.now() - armed_at, Duration::from_secs(10));
//!
//! // A driver runs the clock sixty times faster than real time.
//! // It asks how long to wait for a deadline, waits, and reports the wait.
//! clock.set_speed(Speed::times(60));
//! let deadline = clock.now() + Duration::from_secs(30);
//! let wait = clock.real_time_until(deadline).expect("the clock runs");
//! assert_eq!(wait, Duration::from_millis(500));
//! clock.run_for(wait);
//! assert_eq!(clock.now(), deadline);
//! # Ok::<(), gaslightmq_clock::TimeSpanError>(())
//! ```
//!
//! The wall clock is not in this crate.
//! It reads the system, so it stands at the IO edge, in `gaslightmq_clock_waitable`,
//! with the clocks that a task waits on.
//!
//! The `serde` feature maps a [`TimeSpan`] to its text, such as `10s`,
//! an [`Instant`] and a [`Speed`] to plain numbers, and a [`UtcInstant`] to its text, such as
//! `2026-11-21T14:02:11.527Z`.

mod clock;
mod instant;
mod speed;
mod time_span;
mod utc_instant;
mod virtual_clock;

pub use clock::Clock;
pub use instant::Instant;
pub use speed::Speed;
pub use time_span::{TimeSpan, TimeSpanError};
pub use utc_instant::{InAName, UtcClock, UtcInstant, UtcInstantError};
pub use virtual_clock::VirtualClock;

pub use core::time::Duration;
