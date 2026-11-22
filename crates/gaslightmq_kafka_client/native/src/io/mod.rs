//! The sockets and timers of a role.
//!
//! This module decides nothing.
//! It waits for a socket or a timer, tells the [`logic`](crate::logic) what happened,
//! and carries out what the logic answers.
//!
//! | Type | Owns |
//! |---|---|
//! | [`MonotonicClock`](clock::MonotonicClock) | The time that passes while a role lives. |
//! | [`Connections`](connection::Connections) | The sockets of one role instance, one per connection. |
//! | [`ClusterAccess`](access::ClusterAccess) | The connections, the clock and the cluster view of one role instance, and the loop that drives a call. |

pub(crate) mod access;
pub(crate) mod clock;
pub(crate) mod connection;
