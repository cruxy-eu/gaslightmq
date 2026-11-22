//! What each Kafka API says for itself: one [`Editable`](crate::body::Editable) for every request message.
//!
//! The modules follow the families of the Kafka protocol.
//! An API with [parts](gaslightmq_field::Part) removes them from its request, and says which entries its response holds.
//! Every API says how its response fails.
//!
//! To give an API parts:
//!
//! 1. Remove them in `retain_parts`.
//! 2. Leave a part out of `failed` when the verdict has no error code for it.
//! 3. Say what the entries of the response are, with the macros of [`crate::entry`], and add `response_in_parts!()`.
//! 4. Add the API to the table in the crate documentation.
//! 5. Give `tests/common` a request of the API with the three parts of the examples, and check it in `tests/parts.rs`.

mod admin;
mod cluster;
mod groups;
mod log;
mod offsets;
mod security;
mod share_groups;
mod topics;
mod transactions;

/// The APIs whose response fails in one place: the error code of the response itself.
macro_rules! fails_as_a_whole {
    ($($request:ty),+ $(,)?) => {
        $(
            impl $crate::body::Editable for $request {
                fn failed(&self, _version: i16, verdict: &$crate::body::Verdict<'_>) -> Self::Response {
                    <Self::Response>::default().with_error_code(verdict.call())
                }
            }
        )+
    };
}

use fails_as_a_whole;
