//! The pipeline runs in tests: hand-written inputs go in, on a virtual clock, and the outputs are read back.
//!
//! [`driver`] plays the engine: it turns requests into bytes, answers what the pipeline forwards, and moves the clock.
//! The lies of every build and the refusal are registered, as an engine does it.
//! The pipeline itself names none of them.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod driver;

mod ending;
mod failing;
mod learning;
mod ordering;
mod passing;
mod recording;
mod refusing;
mod timing;
mod two_passes;
