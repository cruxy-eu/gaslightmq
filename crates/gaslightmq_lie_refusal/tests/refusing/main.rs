//! Refusals on calls of Produce, Fetch, Metadata and Heartbeat, through the contract and through both stages.
//!
//! | Module | Holds |
//! |---|---|
//! | [`calls`] | The calls, a backend that answers them truthfully, and the stages of one call. |
//! | [`whole_call`] | A refusal of the whole call, on APIs with and without parts. |
//! | [`some_parts`] | A refusal of some parts before forwarding, with its merge after the response. |
//! | [`every_part`] | Refusals that leave nothing to forward. |
//! | [`after_response`] | A refusal after the response, and what asking again does. |
//! | [`cannot_be_told`] | The refusals that fail, and change nothing. |

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod after_response;
mod calls;
mod cannot_be_told;
mod every_part;
mod some_parts;
mod whole_call;
