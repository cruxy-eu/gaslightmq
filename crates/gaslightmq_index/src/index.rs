//! The index of the calls of a recording.

use std::borrow::Cow;

use gaslightmq_call::Call;
use gaslightmq_field::{Field, FieldSource, FieldValue, Fields, Target};
use gaslightmq_selector::{EffectiveSelector, Match};

use crate::facet::Tally;
use crate::{Facet, Facets, IndexConfiguration};

/// The index of the calls of a recording: it groups them into [facets](Facet), each with counts.
///
/// Whoever holds the calls feeds them in, one [record](Call) at a time or all at once.
/// The engine does so for every call of the recording that runs, and again for a recording that it opens,
/// because an index is not stored.
///
/// | To | Call |
/// |---|---|
/// | Index a call. | [`add`](Self::add), or [`extend`](Extend::extend) for many. |
/// | Group the calls that a filter leaves. | [`facets`](Self::facets), or [`facet`](Self::facet) for one field. |
/// | Count the calls that a selector matches. | [`count`](Self::count) |
///
/// # What a count means
///
/// A filter is a selector: conditions that combine with **and**.
/// The count of a value is what is left once that value narrows the filter down.
///
/// - **A facet ignores the conditions on its own field.**
///   Under `api is Produce` and `topic is orders.eu`, the topics count the produced calls of every topic,
///   so the facet shows what another topic would give.
///   The APIs count the calls for `orders.eu` of every API.
/// - **The count of a value is the count of a selector.**
///   It is how many calls match the other conditions and `is` that value.
///   The same holds for a [pattern](crate::SuggestedPattern) and `matches`.
///   A click on a value therefore leaves exactly as many calls as the facet said.
/// - **Conditions on a part apply to the same part.**
///   Under `topic is orders.eu`, a Produce for `orders.eu` and `payments.settled` counts for the outcome of its
///   `orders.eu` partitions only, as the selector `topic is orders.eu` with `outcome is error` matches it.
/// - **A value counts whatever its origin.**
///   A Fetch counts for the group that the client context knows, as a selector on `group` matches it.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_index::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// // The record of a call that nothing lied to, on the session of one client.
/// let record = |client: &str, request: Fields| {
///     let fields = request
///         .with(ConnectionNumber::new(7))
///         .with(ClientId::new(client).expect("a client ID"))
///         .with(Outcome::Ok);
///     let arrived = CallTiming::arrived(Instant::ZERO);
///     Call::new(CallId::FIRST, fields, Delivered::Nothing, Explanation::none(), arrived)
/// };
/// let produce = |topic: &str| Fields::new().with(ApiKey::PRODUCE).with_part(TopicName::new(topic).expect("a topic"));
/// let fetch = |topic: &str| Fields::new().with(ApiKey::FETCH).with_part(TopicName::new(topic).expect("a topic"));
///
/// let mut index = CallIndex::new(IndexConfiguration::new());
/// index.add(&record("order-ingest", produce("orders.eu"))?);
/// index.add(&record("order-ingest", produce("orders.us"))?);
/// index.add(&record("payments-svc", produce("payments.settled"))?);
/// index.add(&record("analytics", fetch("orders.eu"))?);
/// assert_eq!(index.calls(), 4);
///
/// // Someone clicked `Produce` in the APIs, and then `orders.eu` in the topics.
/// let produced = Condition::is(ApiKey::PRODUCE);
/// let to_eu = Condition::is(TopicName::new("orders.eu").expect("a topic"));
/// let filter: EffectiveSelector = [produced, to_eu].into_iter().collect();
/// let facets = index.facets(&filter);
/// assert_eq!(facets.calls(), 1);
///
/// // The topics show what is left of the produced calls, and the APIs what is left of the calls for `orders.eu`.
/// let topics = facets.get(Field::Topic).expect("the index groups by topic");
/// let apis = facets.get(Field::Api).expect("the index groups by API");
/// assert_eq!(topics.to_string(), "orders.eu (1), orders.us (1), payments.settled (1); orders.* (2)");
/// assert_eq!(apis.to_string(), "Produce (1), Fetch (1)");
///
/// // A click on the suggested pattern replaces the condition on the topic, and leaves as many calls as it said.
/// let orders = topics.patterns()[0].condition();
/// assert_eq!(orders.to_string(), "topic matches orders.*");
/// assert_eq!(index.count(&EffectiveSelector::from(Condition::is(ApiKey::PRODUCE)).and(orders)), 2);
/// # Ok::<(), CallError>(())
/// ```
///
/// # What an index holds
///
/// An index keeps the fields of every call it was given, and nothing else of the record:
/// no explanation, no timing and no payload.
/// It takes memory for every call, so an index is for a recording that someone starts and stops.
/// A filter is any selector, so every count is exact, whatever the filter tests.
#[derive(Debug, Clone)]
pub struct CallIndex {
    configuration: IndexConfiguration,
    calls: Vec<Fields>,
}

impl CallIndex {
    /// An index without a call.
    pub const fn new(configuration: IndexConfiguration) -> Self {
        Self {
            configuration,
            calls: Vec::new(),
        }
    }

    /// What the index is configured with.
    pub const fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Indexes one call.
    pub fn add(&mut self, call: &Call) {
        self.calls.push(call.fields().clone());
    }

    /// How many calls the index holds.
    pub fn calls(&self) -> u64 {
        u64::try_from(self.calls.len()).unwrap_or(u64::MAX)
    }

    /// Whether the index holds no call.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// How many calls `selector` matches, across everything the index holds.
    ///
    /// It is the count of a selector that nobody saved yet, while someone still edits it.
    pub fn count(&self, selector: &EffectiveSelector) -> u64 {
        self.calls
            .iter()
            .map(|call| u64::from(selector.evaluate(call).is_matched()))
            .sum()
    }

    /// The facet of one field under `filter`, whether the configuration names the field or not.
    ///
    /// See [what a count means](Self#what-a-count-means).
    pub fn facet(&self, field: Field, filter: &EffectiveSelector) -> Facet {
        let mut view = View::new(field, filter);
        for call in &self.calls {
            view.count(call, None);
        }
        view.tally.into_facet(&self.configuration)
    }

    /// The facets of the [fields of the configuration](IndexConfiguration::facets) under `filter`,
    /// and how many calls the filter leaves.
    ///
    /// A filter without a condition leaves every call.
    /// See [what a count means](Self#what-a-count-means).
    pub fn facets(&self, filter: &EffectiveSelector) -> Facets {
        let mut views: Vec<View> = self
            .configuration
            .facets
            .iter()
            .map(|field| View::new(*field, filter))
            .collect();
        let mut left = 0;
        for call in &self.calls {
            let whole = filter.evaluate(call);
            left += u64::from(whole.is_matched());
            for view in &mut views {
                view.count(call, Some(&whole));
            }
        }
        let facets = views
            .into_iter()
            .map(|view| view.tally.into_facet(&self.configuration))
            .collect();
        Facets::new(left, facets)
    }
}

/// Indexes every call, in order.
impl<'a> Extend<&'a Call> for CallIndex {
    fn extend<I: IntoIterator<Item = &'a Call>>(&mut self, calls: I) {
        self.calls.extend(calls.into_iter().map(|call| call.fields().clone()));
    }
}

/// One facet while the index goes through its calls: which calls it counts, and what it counted of them.
struct View<'a> {
    field: Field,
    /// The filter without the conditions on the field of the facet.
    /// It is the filter itself, borrowed, when the filter has no such condition.
    others: Cow<'a, EffectiveSelector>,
    tally: Tally,
}

impl<'a> View<'a> {
    fn new(field: Field, filter: &'a EffectiveSelector) -> Self {
        let others = if filter.fields().any(|tested| tested == field) {
            let others = filter
                .conditions()
                .iter()
                .filter(|condition| condition.field() != field);
            Cow::Owned(others.cloned().collect())
        } else {
            Cow::Borrowed(filter)
        };
        Self {
            field,
            others,
            tally: Tally::new(field),
        }
    }

    /// Counts `call` when the other conditions of the filter match it.
    ///
    /// `whole` is what the whole filter gives for the call, when the caller asked already.
    /// It is also what this facet goes by, unless the filter has a condition on its field.
    fn count(&mut self, call: &Fields, whole: Option<&Match>) {
        let own;
        let matched = if let (Some(whole), Cow::Borrowed(_)) = (whole, &self.others) {
            whole
        } else {
            own = self.others.evaluate(call);
            &own
        };
        if let Match::Matched(target) = matched {
            self.tally.count(values_in(call, self.field, target));
        }
    }
}

/// The values of `field` that a call has in what a selector matched of it.
///
/// A field that each part has its own value of is read in the matched parts only,
/// because a condition on such a field applies to the same part as the conditions that matched.
fn values_in(call: &Fields, field: Field, target: &Target) -> Vec<FieldValue> {
    if field.is_per_part() && !target.is_call() {
        call.parts()
            .iter()
            .filter(|part| target.covers(part.part()))
            .filter_map(|part| call.read_in(part, field).present())
            .flat_map(|values| values.iter().cloned().collect::<Vec<_>>())
            .collect()
    } else {
        call.read(field)
            .present()
            .map_or_else(Vec::new, |values| values.iter().cloned().collect())
    }
}
