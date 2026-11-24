//! The index: the calls of a recording grouped into facets, each with counts.
//!
//! An index answers *what is left if I narrow down further?*
//! It groups the calls by the values of a field, and counts the calls of every value under the filter that is active.
//! A click on a value adds a condition to that filter, and the filter is a selector:
//! it can be saved under a name, or turned into a rule straight away.
//!
//! | Type | Is |
//! |---|---|
//! | [`CallIndex`] | The index: the calls it was given, and their facets under any filter. |
//! | [`IndexConfiguration`] | Which fields the index groups by, and when values suggest a pattern. |
//! | [`Facets`] | The facets under one filter, and how many calls that filter leaves. |
//! | [`Facet`] | The values of one field, each with the calls that have it, and the patterns they suggest. |
//! | [`FacetValue`] | One value with its calls, and the condition that a click on it adds: `topic is orders.eu`. |
//! | [`SuggestedPattern`] | A pattern that several values share, with its calls and its condition: `topic matches orders.*`. |
//!
//! The [`prelude`] holds all of them.
//!
//! # The facets
//!
//! Every field can have a facet. Unless the configuration says otherwise, an index groups by these:
//!
//! | Facet | Field | Values |
//! |---|---|---|
//! | Topics | `topic` | `orders.eu`, `orders.us`, and suggested patterns such as `orders.*` |
//! | APIs | `api` | `Produce`, `Fetch`, `Heartbeat` |
//! | Consumer groups | `group` | `payments`, `analytics-etl` |
//! | Transactional IDs | `transactional.id` | `payments-tx-1` |
//! | Clients | `client.id` | `order-ingest`, `payments-svc` |
//! | Brokers | `endpoint` | `bootstrap`, `broker 1`, `broker 2` |
//! | Outcomes | `outcome` | `ok`, `error`, `timeout` |
//!
//! An outcome is the truth of a call, as the field `outcome` has it.
//! What a lie did to a call is its verdict, and no field holds a verdict, so no facet groups by it.
//!
//! # One language
//!
//! A filter is an [`EffectiveSelector`](gaslightmq_selector::EffectiveSelector),
//! and the index matches it against every call as a rule would.
//! It holds no rule of matching itself.
//! The count of a value is therefore the count of a selector:
//! the conditions of the filter on the other fields, and `is` that value.
//! See [what a count means](CallIndex#what-a-count-means).
//!
//! ```
//! use gaslightmq_call::prelude::*;
//! use gaslightmq_clock::Instant;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_index::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_selector::prelude::*;
//!
//! // The record of a call that the backend answered and nothing lied to.
//! let record = |request: Fields| {
//!     let fields = request.with(ConnectionNumber::new(7)).with(Outcome::Ok);
//!     let arrived = CallTiming::arrived(Instant::ZERO);
//!     Call::new(CallNumber::FIRST, fields, Delivered::Nothing, Explanation::none(), arrived)
//! };
//! let partition = |text: &str| text.parse::<TopicPartition>().expect("a topic partition");
//! let etl = GroupId::new("analytics-etl").expect("a group ID");
//!
//! let mut index = CallIndex::new(IndexConfiguration::new());
//! index.add(&record(Fields::new().with(ApiKey::PRODUCE).with_part(partition("orders.eu/3")))?);
//! index.add(&record(Fields::new().with(ApiKey::FETCH).with_learned(etl.clone()).with_part(partition("orders.eu/3")))?);
//! index.add(&record(Fields::new().with(ApiKey::FETCH).with_learned(etl.clone()).with_part(partition("orders.us/0")))?);
//! index.add(&record(Fields::new().with(ApiKey::HEARTBEAT).with(etl))?);
//!
//! // Nobody filters yet.
//! let everything = index.facets(&EffectiveSelector::new());
//! let facet = |field| everything.get(field).map(Facet::to_string).expect("the index groups by the field");
//! assert_eq!(everything.calls(), 4);
//! assert_eq!(facet(Field::Api), "Fetch (2), Produce (1), Heartbeat (1)");
//! assert_eq!(facet(Field::Topic), "orders.eu (2), orders.us (1); orders.* (3)");
//! assert_eq!(facet(Field::Group), "analytics-etl (3)");
//!
//! // Someone clicks `analytics-etl`, and the condition of that value is the filter.
//! let groups = everything.get(Field::Group).expect("the index groups by group");
//! let filter = EffectiveSelector::from(groups.values()[0].condition());
//! assert_eq!(filter.to_string(), "group is analytics-etl");
//!
//! let of_the_group = index.facets(&filter);
//! let topics = of_the_group.get(Field::Topic).expect("the index groups by topic");
//! assert_eq!(of_the_group.calls(), 3);
//! assert_eq!(topics.to_string(), "orders.eu (1), orders.us (1); orders.* (2)");
//! # Ok::<(), CallError>(())
//! ```
//!
//! # Errors
//!
//! Nothing in this crate can fail, so it has no error type.
//! An index takes every call record, a filter is a selector that someone built already,
//! and every value of a configuration is one that the index can work with.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_call` for the call record, on `gaslightmq_selector` for the filter and its
//! matching, and on `gaslightmq_field` for the fields and values that a facet groups by.
//! It has no async code, no IO, no system clock and no randomness.
//! An index reads no recording itself: whoever reads one hands the calls over.

mod configuration;
mod facet;
mod index;
mod pattern;

pub use configuration::IndexConfiguration;
pub use facet::{Facet, FacetValue, Facets};
pub use index::CallIndex;
pub use pattern::SuggestedPattern;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in facets,
/// next to the preludes of `gaslightmq_kernel`, `gaslightmq_field`, `gaslightmq_selector` and `gaslightmq_call`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_index::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let index = CallIndex::new(IndexConfiguration::new());
/// let facets: Facets = index.facets(&EffectiveSelector::new());
/// assert_eq!(facets.calls(), 0);
/// assert!(facets.iter().all(Facet::is_empty));
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{CallIndex, Facet, FacetValue, Facets, IndexConfiguration, SuggestedPattern};
}
