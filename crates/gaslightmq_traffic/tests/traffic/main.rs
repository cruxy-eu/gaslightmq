//! A traffic that takes the records of calls, as an engine hands them over.
//!
//! [`support`] holds what the tests share: the connection of a client, whose calls end with a record.

mod support;

mod live_window;
mod seen_names;
mod statistics;
mod subscriptions;
