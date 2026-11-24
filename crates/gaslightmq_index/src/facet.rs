//! A facet: the values of one field, each with the calls that have it.

use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_field::{Field, FieldValue};
use gaslightmq_selector::Condition;

use crate::pattern::Suggester;
use crate::{IndexConfiguration, SuggestedPattern};

/// One value of a facet, with the calls that have it: `orders.eu` on 14 calls.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_index::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// # let record = |fields: Fields| {
/// #     use gaslightmq_call::prelude::*;
/// #     use gaslightmq_clock::Instant;
/// #     let fields = fields.with(ConnectionNumber::new(7)).with(Outcome::Ok);
/// #     Call::new(CallId::FIRST, fields, Delivered::Nothing, Explanation::none(), CallTiming::arrived(Instant::ZERO))
/// #         .expect("a call that ended on a connection")
/// # };
/// let mut index = CallIndex::new(IndexConfiguration::new());
/// index.extend(&[
///     record(Fields::new().with(ApiKey::FETCH)),
///     record(Fields::new().with(ApiKey::FETCH)),
///     record(Fields::new().with(ApiKey::HEARTBEAT)),
/// ]);
///
/// let apis = index.facet(Field::Api, &EffectiveSelector::new());
/// let fetch = &apis.values()[0];
/// assert_eq!(fetch.value(), &FieldValue::Api(ApiKey::FETCH));
/// assert_eq!(fetch.calls(), 2);
/// assert_eq!(fetch.to_string(), "Fetch (2)");
///
/// // A click on the value adds this condition to the filter.
/// assert_eq!(fetch.condition(), Condition::is(ApiKey::FETCH));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FacetValue {
    value: FieldValue,
    calls: u64,
}

impl FacetValue {
    /// The value.
    pub const fn value(&self) -> &FieldValue {
        &self.value
    }

    /// How many calls are left once the value narrows the filter down: those that have it.
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// The condition that a click on the value adds to the filter: `topic is orders.eu`.
    pub fn condition(&self) -> Condition {
        Condition::is(self.value.clone())
    }
}

/// Writes the value and its calls: `orders.eu (14)`.
impl fmt::Display for FacetValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.value, self.calls)
    }
}

/// The values of one field across the calls of an index, each with the calls that have it.
///
/// A facet answers *what is left if I narrow down further?*
/// Its counts respect every condition of the filter, except those on its own field:
/// the topics show what each topic would leave of the produced calls,
/// whichever topic the filter names already.
/// See [what a count means](crate::CallIndex#what-a-count-means).
///
/// The values come with the most calls first, and in their own order among those with as many calls.
/// A call that has several values of the field counts for each of them,
/// and a call that does not have the field counts for none.
///
/// The text form is the values, and behind a semicolon the [patterns](SuggestedPattern) that they suggest:
/// `orders.eu (2), orders.us (1), payments.settled (1); orders.* (3)`.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_index::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// # let record = |fields: Fields| {
/// #     use gaslightmq_call::prelude::*;
/// #     use gaslightmq_clock::Instant;
/// #     let fields = fields.with(ConnectionNumber::new(7)).with(Outcome::Ok);
/// #     Call::new(CallId::FIRST, fields, Delivered::Nothing, Explanation::none(), CallTiming::arrived(Instant::ZERO))
/// #         .expect("a call that ended on a connection")
/// # };
/// let topic = |name: &str| TopicName::new(name).expect("a topic name");
/// let mut index = CallIndex::new(IndexConfiguration::new());
/// index.extend(&[
///     record(Fields::new().with(ApiKey::PRODUCE).with_part(topic("orders.eu")).with_part(topic("orders.us"))),
///     record(Fields::new().with(ApiKey::PRODUCE).with_part(topic("orders.eu"))),
///     record(Fields::new().with(ApiKey::FETCH).with_part(topic("payments.settled"))),
///     record(Fields::new().with(ApiKey::HEARTBEAT)),
/// ]);
///
/// let topics = index.facet(Field::Topic, &EffectiveSelector::new());
/// assert_eq!(topics.field(), Field::Topic);
/// assert_eq!(topics.to_string(), "orders.eu (2), orders.us (1), payments.settled (1); orders.* (2)");
/// assert_eq!(topics.calls_with(&topic("orders.eu").into()), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Facet {
    field: Field,
    values: Vec<FacetValue>,
    patterns: Vec<SuggestedPattern>,
}

impl Facet {
    /// The field whose values the facet groups the calls by.
    pub const fn field(&self) -> Field {
        self.field
    }

    /// The values, with the most calls first.
    pub fn values(&self) -> &[FacetValue] {
        &self.values
    }

    /// The patterns that the values suggest, with the most calls first.
    pub fn patterns(&self) -> &[SuggestedPattern] {
        &self.patterns
    }

    /// How many calls have `value`, which is none for a value that the facet does not hold.
    pub fn calls_with(&self, value: &FieldValue) -> u64 {
        self.values
            .iter()
            .find(|own| own.value() == value)
            .map_or(0, FacetValue::calls)
    }

    /// Whether no call that is left has the field.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Writes the values with a comma and a space between them, and the patterns behind a semicolon.
/// A facet without a value writes nothing.
impl fmt::Display for Facet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, value) in self.values.iter().enumerate() {
            f.write_str(if position > 0 { ", " } else { "" })?;
            fmt::Display::fmt(value, f)?;
        }
        for (position, pattern) in self.patterns.iter().enumerate() {
            f.write_str(if position > 0 { ", " } else { "; " })?;
            fmt::Display::fmt(pattern, f)?;
        }
        Ok(())
    }
}

/// The facets of an index under one filter, and how many calls that filter leaves.
///
/// [`CallIndex::facets`](crate::CallIndex::facets) gives one facet for every
/// [field of the configuration](IndexConfiguration::facets), in that order.
/// A facet that no call has a value for is there too, and it is [empty](Facet::is_empty).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_index::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// # let record = |fields: Fields| {
/// #     use gaslightmq_call::prelude::*;
/// #     use gaslightmq_clock::Instant;
/// #     let fields = fields.with(ConnectionNumber::new(7)).with(Outcome::Ok);
/// #     Call::new(CallId::FIRST, fields, Delivered::Nothing, Explanation::none(), CallTiming::arrived(Instant::ZERO))
/// #         .expect("a call that ended on a connection")
/// # };
/// let mut index = CallIndex::new(IndexConfiguration::new());
/// index.extend(&[
///     record(Fields::new().with(ApiKey::FETCH)),
///     record(Fields::new().with(ApiKey::FETCH)),
///     record(Fields::new().with(ApiKey::HEARTBEAT)),
/// ]);
///
/// let facets = index.facets(&Condition::is(ApiKey::FETCH).into());
/// assert_eq!(facets.calls(), 2);
/// assert_eq!(facets.len(), IndexConfiguration::FACETS.len());
///
/// let outcomes = facets.get(Field::Outcome).expect("the index groups by outcome");
/// assert_eq!(outcomes.to_string(), "ok (2)");
/// assert!(facets.get(Field::Topic).is_some_and(Facet::is_empty));
/// assert!(facets.get(Field::Latency).is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Facets {
    calls: u64,
    facets: Vec<Facet>,
}

impl Facets {
    pub(crate) fn new(calls: u64, facets: Vec<Facet>) -> Self {
        Self { calls, facets }
    }

    /// How many calls the whole filter leaves.
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// The facet of `field`, or `None` when the index does not group by that field.
    pub fn get(&self, field: Field) -> Option<&Facet> {
        self.facets.iter().find(|facet| facet.field() == field)
    }

    /// The facets, in the order of the configuration.
    pub fn iter(&self) -> std::slice::Iter<'_, Facet> {
        self.facets.iter()
    }

    /// How many facets there are.
    pub fn len(&self) -> usize {
        self.facets.len()
    }

    /// Whether the index groups by no field at all.
    pub fn is_empty(&self) -> bool {
        self.facets.is_empty()
    }
}

impl<'a> IntoIterator for &'a Facets {
    type Item = &'a Facet;
    type IntoIter = std::slice::Iter<'a, Facet>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for Facets {
    type Item = Facet;
    type IntoIter = std::vec::IntoIter<Facet>;

    fn into_iter(self) -> Self::IntoIter {
        self.facets.into_iter()
    }
}

/// The calls of one facet while the index counts them.
///
/// Calls with the same values of the field are one group, so the tally grows with the distinct sets of values,
/// and not with the calls.
pub(crate) struct Tally {
    field: Field,
    /// Every value that a call had, with its position in the order they arrived.
    positions: BTreeMap<FieldValue, usize>,
    /// Which values a call has, by their positions in order, and how many calls have exactly those.
    groups: BTreeMap<Vec<usize>, u64>,
}

impl Tally {
    pub(crate) fn new(field: Field) -> Self {
        Self {
            field,
            positions: BTreeMap::new(),
            groups: BTreeMap::new(),
        }
    }

    /// One more call, which has `values` of the field. A call without a value counts for nothing.
    pub(crate) fn count(&mut self, values: impl IntoIterator<Item = FieldValue>) {
        let mut of_call: Vec<usize> = values
            .into_iter()
            .map(|value| {
                let next = self.positions.len();
                *self.positions.entry(value).or_insert(next)
            })
            .collect();
        if of_call.is_empty() {
            return;
        }
        of_call.sort_unstable();
        of_call.dedup();
        *self.groups.entry(of_call).or_default() += 1;
    }

    /// The facet of what was counted.
    pub(crate) fn into_facet(self, configuration: &IndexConfiguration) -> Facet {
        let mut by_position: Vec<(usize, FieldValue)> = self
            .positions
            .into_iter()
            .map(|(value, position)| (position, value))
            .collect();
        by_position.sort_unstable_by_key(|(position, _)| *position);
        let values: Vec<FieldValue> = by_position.into_iter().map(|(_, value)| value).collect();

        let mut calls = vec![0_u64; values.len()];
        for (of_call, count) in &self.groups {
            for position in of_call {
                if let Some(calls) = calls.get_mut(*position) {
                    *calls += count;
                }
            }
        }

        let suggester = Suggester {
            separators: &configuration.pattern_separators,
            min_values: configuration.pattern_min_values,
        };
        let patterns = suggester.suggest(self.field, &values, &self.groups);

        let mut values: Vec<FacetValue> = values
            .into_iter()
            .zip(calls)
            .map(|(value, calls)| FacetValue { value, calls })
            .collect();
        values.sort_by(|a, b| b.calls.cmp(&a.calls).then_with(|| a.value.cmp(&b.value)));

        Facet {
            field: self.field,
            values,
            patterns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tally of topics, which a test counts calls into by the names of their topics.
    struct Topics(Tally);

    impl Topics {
        fn new() -> Topics {
            Topics(Tally::new(Field::Topic))
        }

        /// As many calls as `calls`, each for those topics.
        fn with(mut self, calls: u64, names: &[&str]) -> Topics {
            for _ in 0..calls {
                self.0
                    .count(names.iter().map(|name| Field::Topic.parse_value(name).unwrap()));
            }
            self
        }

        fn facet(self) -> Facet {
            self.0.into_facet(&IndexConfiguration::new())
        }
    }

    #[test]
    fn a_value_counts_the_calls_that_have_it() {
        let facet = Topics::new()
            .with(3, &["payments.settled"])
            .with(5, &["orders.eu"])
            .facet();

        assert_eq!(facet.to_string(), "orders.eu (5), payments.settled (3)");
    }

    #[test]
    fn a_call_with_several_values_counts_for_each_of_them() {
        let facet = Topics::new()
            .with(2, &["orders.eu", "payments.settled"])
            .with(1, &["orders.eu"])
            .facet();

        assert_eq!(facet.to_string(), "orders.eu (3), payments.settled (2)");
    }

    #[test]
    fn a_call_that_names_a_value_twice_counts_for_it_once() {
        let facet = Topics::new().with(1, &["orders.eu", "orders.eu"]).facet();

        assert_eq!(facet.to_string(), "orders.eu (1)");
    }

    #[test]
    fn a_call_without_a_value_counts_for_nothing() {
        let facet = Topics::new().with(4, &[]).facet();

        assert!(facet.is_empty());
        assert_eq!(facet.to_string(), "");
        assert_eq!(facet.values(), []);
        assert_eq!(facet.patterns(), []);
    }

    #[test]
    fn values_with_as_many_calls_come_in_their_own_order() {
        let facet = Topics::new()
            .with(1, &["payments.settled"])
            .with(1, &["audit"])
            .with(2, &["orders.eu"])
            .facet();

        assert_eq!(facet.to_string(), "orders.eu (2), audit (1), payments.settled (1)");
    }

    #[test]
    fn a_facet_knows_the_calls_of_each_value() {
        let facet = Topics::new().with(2, &["orders.eu"]).facet();
        let value = |name: &str| Field::Topic.parse_value(name).unwrap();

        assert_eq!(facet.calls_with(&value("orders.eu")), 2);
        assert_eq!(facet.calls_with(&value("orders.us")), 0);
        assert_eq!(facet.values()[0].value(), &value("orders.eu"));
        assert_eq!(facet.values()[0].condition().to_string(), "topic is orders.eu");
    }

    #[test]
    fn the_patterns_stand_behind_the_values() {
        let facet = Topics::new()
            .with(2, &["orders.eu", "orders.us"])
            .with(1, &["orders.us"])
            .with(1, &["audit-log"])
            .with(1, &["audit-trail"])
            .facet();

        assert_eq!(
            facet.to_string(),
            "orders.us (3), orders.eu (2), audit-log (1), audit-trail (1); orders.* (3), audit-* (2)"
        );
        assert_eq!(facet.patterns()[0].values(), 2);
        assert_eq!(facet.patterns()[0].field(), Field::Topic);
    }

    #[test]
    fn facets_keep_the_order_they_were_asked_in() {
        let facets = Facets::new(
            7,
            [Tally::new(Field::Topic), Tally::new(Field::Api)]
                .into_iter()
                .map(|tally| tally.into_facet(&IndexConfiguration::new()))
                .collect(),
        );

        let fields: Vec<Field> = facets.iter().map(Facet::field).collect();
        assert_eq!(fields, [Field::Topic, Field::Api]);
        assert_eq!((&facets).into_iter().count(), 2);
        assert_eq!((facets.calls(), facets.len(), facets.is_empty()), (7, 2, false));
        assert!(facets.get(Field::Api).is_some());
        assert!(facets.get(Field::Outcome).is_none());
        assert_eq!(facets.into_iter().count(), 2);
    }
}
