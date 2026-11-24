//! Selectors: conditions over fields, what includes and scope add, and matching a call.
//!
//! A selector describes a set of calls.
//! The same selector filters recorded traffic and targets the calls of a rule,
//! because it only speaks to a [`FieldSource`](gaslightmq_field::FieldSource).
//!
//! | Type | Is |
//! |---|---|
//! | [`Condition`] | One clause: a field, an [`Operator`], and a value, a [`ValueSet`] or a [`Glob`]. |
//! | [`Conditions`] | Conditions that combine with **and**, one per field: the `match` of a selector. |
//! | [`Scope`] | Conditions that only test connection fields: *who* something applies to. |
//! | [`Selector`] | A named set of conditions, which can include another selector by its [`SelectorName`]. |
//! | [`SelectorRef`] | How a rule says which calls it considers: a selector name, or inline conditions. |
//! | [`SelectorSet`] | The selectors by name. It resolves includes, and refuses a circle of them. |
//! | [`EffectiveSelector`] | Every condition that applies, after includes and scope. It matches calls. |
//! | [`Match`] | What matching gives: the [`Target`](gaslightmq_field::Target), no match, or pending. |
//! | [`Pass`] | The evaluation pass that can decide a selector: request or response. |
//! | [`ApiIndex`] | Selectors by API, so a call only tests the selectors that can match its API. |
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_selector::prelude::*;
//!
//! // Two selectors of a workspace. The second includes the first.
//! let order_producers = Selector::new(
//!     "order-producers".parse().expect("a selector name"),
//!     Conditions::new()
//!         .with(Condition::is(ApiKey::PRODUCE))
//!         .with(Condition::parse(Field::Topic, "orders.*").expect("a condition")),
//! );
//! let from_ingest = Selector::new(
//!     "order-producers-from-ingest".parse().expect("a selector name"),
//!     Condition::is(ClientId::new("order-ingest").expect("a client ID")).into(),
//! )
//! .including("order-producers".parse().expect("a selector name"));
//! let selectors = SelectorSet::new([order_producers, from_ingest])?;
//!
//! // A rule names a selector and a scope. Its effective selector is the selector and the scope.
//! let selector = SelectorRef::Named("order-producers-from-ingest".parse().expect("a selector name"));
//! let scope = Scope::endpoint(Endpoint::Broker(BrokerId::from(2)));
//! let effective = selectors.resolve(&selector)?.and(scope);
//! assert_eq!(effective.pass(), Pass::Request);
//!
//! // A Produce for two topics, as a test writes it down.
//! let produce = Fields::new()
//!     .with(Endpoint::Broker(BrokerId::from(2)))
//!     .with(ClientId::new("order-ingest").expect("a client ID"))
//!     .with(ApiKey::PRODUCE)
//!     .with_part("orders.eu/3".parse::<TopicPartition>().expect("a partition"))
//!     .with_part("payments.settled/0".parse::<TopicPartition>().expect("a partition"));
//!
//! // The selector matches the call, and the lie of the rule applies to the `orders.eu` partition only.
//! let Match::Matched(target) = effective.evaluate(&produce) else {
//!     panic!("the Produce names an orders topic");
//! };
//! assert_eq!(target.to_string(), "orders.eu/3");
//! # Ok::<(), SelectorSetError>(())
//! ```
//!
//! # A definition and what matches
//!
//! A [`Selector`] is a definition: it lives in a file, travels to the UI, and names what it includes.
//! [`Conditions`] have one condition per field, as the map in the file has.
//!
//! An [`EffectiveSelector`] is what matches.
//! It is a plain list of conditions, and it can test one field twice:
//! a selector on `topic matches orders.*` that includes one on `topic matches *.eu` tests both.
//!
//! # Many values for one field
//!
//! A request can carry many values for one field, and a Produce can name several topics and partitions.
//!
//! - `is`, `is any of` and `matches` hold when **any** value passes.
//! - `is not` holds when **no** value equals, and when the call does not have the field at all.
//! - Positive conditions on `topic`, `partition`, `outcome` and `error.code` apply to the same partition.
//!   The partitions that pass all of them are the target of the match.
//!
//! See [what a match yields](EffectiveSelector#what-a-match-yields).
//!
//! # Two passes
//!
//! A selector that tests a response field cannot match before the backend answers.
//! [`EffectiveSelector::pass`] says which pass decides a selector.
//! Evaluated too early, such a selector gives [`Match::Pending`], unless a condition on the request already fails.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every definition type has a `serde` form.
//! The forms need a format that describes itself, such as YAML or JSON.
//!
//! | Type | Form |
//! |---|---|
//! | [`SelectorName`], [`SelectorTitle`], [`Glob`] | The text. |
//! | [`Conditions`], [`Scope`] | A map with one entry per condition: the field, and the form of the condition. |
//! | [`Condition`] | The same map with one entry. |
//! | [`Selector`] | A map with `name`, and with `include` and `match` when it has them. |
//! | [`SelectorRef`] | The name as text, or the map of inline conditions. |
//!
//! Under the key of its field, a condition has one of these forms.
//!
//! | Condition | Form | Example |
//! |---|---|---|
//! | `is` | The value. | `api: Produce` |
//! | `is any of` | A list of values. | `api: [JoinGroup, SyncGroup]` |
//! | `matches` | The pattern: a text with a `*` or a `?`. | `topic: "orders.*"` |
//! | `is not` | A map with the key `is-not`, and a value or a list of values. | `client.id: { is-not: kafka-ui }` |
//!
//! Every operator also has a form that names it: `{ is: … }`, `{ is-any-of: […] }` and `{ matches: … }`.
//! Under `is`, `is-any-of` and `is-not`, a text with a wildcard is that text, and no pattern.
//! `client.id: { is: "batch-*" }` is the client whose ID really holds a `*`.
//! Writing takes the short form whenever it reads back as the same condition.
//!
//! A value is checked against the type of its field when it is read:
//! `api: Prodcue` is an error, and not a selector that never matches.
//! A pattern inside a list is an error too, because a list holds values.
//!
//! ```yaml
//! name: order-producers-from-ingest
//! include: order-producers
//! match:
//!   client.id: order-ingest
//!   topic: "orders.*"
//!   api: [Produce, InitProducerId]
//!   host: { is-not: 10.0.4.27 }
//! ```
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`Glob`] | [`GlobError`] | The pattern is empty. |
//! | [`SelectorName`] | [`SelectorNameError`] | The text breaks a part of the [rule for names](SelectorName#the-rule-for-names). |
//! | [`SelectorTitle`] | [`SelectorTitleError`] | The text breaks a part of the [rule for titles](SelectorTitle#the-rule-for-titles). |
//! | [`Operator`] | [`OperatorError`] | No operator has this name. |
//! | [`ValueSet`] | [`ValueSetError`] | There is no value, the values belong to different fields, or a list holds a pattern. |
//! | [`Condition`] | [`ConditionError`] | The text is not a value of the field. |
//! | [`Scope`] | [`ScopeError`] | A condition tests a field that does not describe the connection. |
//! | [`SelectorSet`] | [`SelectorSetError`] | Two selectors share a name, a name is unknown, or selectors include each other in a circle. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! [`ConditionError::InvalidValue`] writes the message of the [`FieldValueError`](gaslightmq_field::FieldValueError) it holds, once.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_field` for the field vocabulary,
//! and on `gaslightmq_kernel` for the API key that the index is built on.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.

mod condition;
mod conditions;
mod effective;
mod glob;
mod index;
mod name;
mod selector;
#[cfg(feature = "serde")]
mod serde;
mod set;
mod text;
mod title;

pub use condition::{Condition, ConditionError, Operator, OperatorError, ValueSet, ValueSetError};
pub use conditions::{Conditions, Scope, ScopeError};
pub use effective::{Apis, EffectiveSelector, Match, Pass};
pub use glob::{Glob, GlobError};
pub use index::ApiIndex;
pub use name::{SelectorName, SelectorNameError};
pub use selector::{Selector, SelectorRef};
pub use set::{SelectorSet, SelectorSetError};
pub use title::{SelectorTitle, SelectorTitleError};

/// Every item of this crate.
///
/// Import it whole wherever code speaks in selectors, next to the preludes of the kernel and of the fields.
/// No name in it is a name of the standard prelude or of those two, so the import shadows nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let condition: Result<Condition, ConditionError> = Condition::parse(Field::Api, "Produce");
/// assert_eq!(condition?, Condition::is(ApiKey::PRODUCE));
/// # Ok::<(), gaslightmq_selector::ConditionError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ApiIndex, Apis, Condition, ConditionError, Conditions, EffectiveSelector, Glob, GlobError, Match, Operator,
        OperatorError, Pass, Scope, ScopeError, Selector, SelectorName, SelectorNameError, SelectorRef, SelectorSet,
        SelectorSetError, SelectorTitle, SelectorTitleError, ValueSet, ValueSetError,
    };
}
