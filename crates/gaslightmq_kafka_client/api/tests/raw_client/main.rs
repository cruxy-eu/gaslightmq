//! The raw client against a broker that the test plays.
//!
//! The broker runs inside the test, which reads every request and decides what goes back, and when.
//! The tests here need exactly that: a broker that answers late, answers the wrong request, sends what is no response,
//! or hangs up.

mod failures;
mod requests;
mod support;
