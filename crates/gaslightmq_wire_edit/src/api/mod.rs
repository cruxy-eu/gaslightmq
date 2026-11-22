//! What each Kafka API says for itself: one [`Editable`](crate::body::Editable) for every request message.
//!
//! The modules follow the families of the Kafka protocol.
//! An API with [parts](gaslightmq_field::Part) removes them from its request, and says which entries its response holds.
//! Every API says how its response fails, and whether its response has an error code of its own.
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

/// Says that the response of an API has an error code of its own, in the `Editable` of its request.
///
/// The message holds the field at every version, and it is zero where a version does not write it.
macro_rules! response_has_error_code {
    () => {
        fn error_of_response(response: &Self::Response) -> i16 {
            response.error_code
        }
    };
}

/// The APIs whose response fails in one place: the error code of the response itself.
macro_rules! fails_as_a_whole {
    ($($request:ty),+ $(,)?) => {
        $(
            impl $crate::body::Editable for $request {
                fn failed(&self, _version: i16, verdict: &$crate::body::Verdict<'_>) -> Self::Response {
                    <Self::Response>::default().with_error_code(verdict.call())
                }

                $crate::api::response_has_error_code!();
            }
        )+
    };
}

use fails_as_a_whole;
pub(crate) use response_has_error_code;
