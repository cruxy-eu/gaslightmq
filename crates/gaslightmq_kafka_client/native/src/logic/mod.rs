//! What a client decides, as plain code.
//!
//! Nothing in this module is async, opens a socket or reads a clock.
//! Every function takes what happened and the current [`Instant`](gaslightmq_clock::Instant),
//! and returns what should happen, so a test drives it with plain function calls.
//! The [`io`](crate::io) module waits for sockets and timers, and carries out what this module decides.
//!
//! | Type | Decides |
//! |---|---|
//! | [`ClusterView`](cluster::ClusterView) | Which broker leads a partition or coordinates a group, and when to ask again. |
//! | [`Greeting`](conversation::Greeting), [`Conversation`](conversation::Conversation) | What a new connection says first, and which version of an API it speaks. |
//! | [`Attempts`](attempts::Attempts) | Whether a failed call tries again, and when. |
//! | [`Call`](call::Call) | Each step of one call: which connection gets which request, when to wait, and when it is over. |
//! | [`Membership`](group::Membership) | What a consumer says to the coordinator of its group, and what each answer changes. |
//!
//! The other modules hold one [`Operation`](call::Operation) per kind of call:
//! the request it sends, where the request goes, and what the response means.

use std::time::Duration;

pub(crate) mod attempts;
pub(crate) mod call;
pub(crate) mod cluster;
pub(crate) mod configs;
pub(crate) mod conversation;
pub(crate) mod fetch;
pub(crate) mod group;
pub(crate) mod offsets;
pub(crate) mod produce;
pub(crate) mod topics;
pub(crate) mod versions;

/// A time as a request carries it: whole milliseconds, and at most the largest `i32`.
pub(crate) fn wire_millis(time: Duration) -> i32 {
    i32::try_from(time.as_millis()).unwrap_or(i32::MAX)
}

/// A size as a request carries it: bytes, and at most the largest `i32`.
pub(crate) fn wire_bytes(size: u32) -> i32 {
    i32::try_from(size).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_time_or_a_size_too_large_for_a_request_travels_as_the_largest() {
        assert_eq!(wire_millis(Duration::from_millis(1_500)), 1_500);
        assert_eq!(wire_millis(Duration::from_micros(1_999)), 1);
        assert_eq!(wire_millis(Duration::MAX), i32::MAX);
        assert_eq!(wire_bytes(1_048_576), 1_048_576);
        assert_eq!(wire_bytes(u32::MAX), i32::MAX);
    }
}
