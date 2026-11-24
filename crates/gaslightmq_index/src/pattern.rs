//! The patterns that the values of a facet suggest.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;

use gaslightmq_field::{Field, FieldValue};
use gaslightmq_selector::{Condition, Glob};

/// A pattern that several values of a facet share, such as `orders.*` for `orders.eu` and `orders.us`.
///
/// A facet suggests a pattern so that one click generalises a filter:
/// [`condition`](Self::condition) is `topic matches orders.*`, which also matches the topic that arrives tomorrow.
///
/// # Which patterns a facet suggests
///
/// The text of every value is cut behind each of its
/// [separators](crate::IndexConfiguration::pattern_separators), and what stands before the cut is a prefix:
/// `orders.eu.v1` has the prefixes `orders.` and `orders.eu.`.
/// A prefix with a `*` behind it is a pattern.
///
/// - A pattern is suggested when at least [as many values](crate::IndexConfiguration::pattern_min_values) share it
///   as the configuration says.
/// - Of the patterns that stand for the same values, only the longest is suggested, because it says most about them:
///   `payments-tx-1` and `payments-tx-2` suggest `payments-tx-*`, and not `payments-*`.
/// - A prefix that holds a wildcard itself suggests nothing, because a pattern has no escape character.
///
/// The patterns come with the most calls first, and by their text among those with as many calls.
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
/// #     Call::new(CallNumber::FIRST, fields, Delivered::Nothing, Explanation::none(), CallTiming::arrived(Instant::ZERO))
/// #         .expect("a call that ended on a connection")
/// # };
/// let produce = |topic: &str| Fields::new().with(ApiKey::PRODUCE).with_part(TopicName::new(topic).expect("a topic"));
///
/// let mut index = CallIndex::new(IndexConfiguration::new());
/// index.extend(&[
///     record(produce("orders.eu")),
///     record(produce("orders.us")),
///     record(produce("orders.us")),
///     record(produce("payments.settled")),
/// ]);
///
/// let topics = index.facet(Field::Topic, &EffectiveSelector::new());
/// let [orders] = topics.patterns() else { panic!("the two orders topics share one pattern") };
/// assert_eq!(orders.to_string(), "orders.* (3)");
/// assert_eq!(orders.values(), 2);
/// assert_eq!(orders.condition().to_string(), "topic matches orders.*");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SuggestedPattern {
    field: Field,
    pattern: Glob,
    values: usize,
    calls: u64,
}

impl SuggestedPattern {
    /// The field whose values share the pattern.
    pub const fn field(&self) -> Field {
        self.field
    }

    /// The pattern, such as `orders.*`.
    pub const fn pattern(&self) -> &Glob {
        &self.pattern
    }

    /// How many values of the facet the pattern stands for.
    pub const fn values(&self) -> usize {
        self.values
    }

    /// How many calls are left once the pattern narrows the filter down: those with a value that it matches.
    ///
    /// A call with several such values counts once, so this is not the sum over the values.
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// The condition that a click on the pattern adds to the filter: `topic matches orders.*`.
    pub fn condition(&self) -> Condition {
        Condition::matches(self.field, self.pattern.clone())
    }
}

/// Writes the pattern and its calls: `orders.* (3)`.
impl fmt::Display for SuggestedPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.pattern, self.calls)
    }
}

/// How many values a pattern stands for at least, and where a value is cut.
pub(crate) struct Suggester<'a> {
    pub(crate) separators: &'a [char],
    pub(crate) min_values: usize,
}

impl Suggester<'_> {
    /// The patterns that `values` suggest.
    ///
    /// `groups` are the calls of the facet: which values each call has, by their position in `values`,
    /// and how many calls have exactly those.
    pub(crate) fn suggest(
        &self,
        field: Field,
        values: &[FieldValue],
        groups: &BTreeMap<Vec<usize>, u64>,
    ) -> Vec<SuggestedPattern> {
        let texts: Vec<String> = values.iter().map(ToString::to_string).collect();
        let shared = self.shared_prefixes(&texts);

        let mut patterns: Vec<SuggestedPattern> = shared
            .iter()
            .filter(|(prefix, values)| !says_what_a_longer_one_says(&shared, prefix, values.len()))
            .filter_map(|(prefix, values)| {
                let calls = groups
                    .iter()
                    .filter(|(of_call, _)| of_call.iter().any(|value| values.contains(value)))
                    .map(|(_, calls)| calls)
                    .sum();
                Some(SuggestedPattern {
                    field,
                    pattern: Glob::new(format!("{prefix}*")).ok()?,
                    values: values.len(),
                    calls,
                })
            })
            .collect();
        patterns.sort_by(|a, b| b.calls.cmp(&a.calls).then_with(|| a.pattern.cmp(&b.pattern)));
        patterns
    }

    /// Every prefix that enough texts share, with the positions of those texts.
    fn shared_prefixes<'t>(&self, texts: &'t [String]) -> BTreeMap<&'t str, BTreeSet<usize>> {
        let mut prefixes: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
        for (position, text) in texts.iter().enumerate() {
            let literal = text.find(Glob::WILDCARDS).unwrap_or(text.len());
            for (at, separator) in text[..literal].match_indices(self.separators) {
                prefixes
                    .entry(&text[..at + separator.len()])
                    .or_default()
                    .insert(position);
            }
        }
        prefixes.retain(|_, values| values.len() >= self.min_values);
        prefixes
    }
}

/// Whether a longer prefix stands for the same values as `prefix`.
///
/// The values of a longer prefix are among those of the shorter one, so as many values are the same values.
/// The prefixes that extend `prefix` follow it directly in the order of the map.
fn says_what_a_longer_one_says(shared: &BTreeMap<&str, BTreeSet<usize>>, prefix: &str, values: usize) -> bool {
    shared
        .range::<str, _>((Bound::Excluded(prefix), Bound::Unbounded))
        .take_while(|(longer, _)| longer.starts_with(prefix))
        .any(|(_, of_longer)| of_longer.len() == values)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Topics that each have one call of their own.
    struct Topics {
        values: Vec<FieldValue>,
        groups: BTreeMap<Vec<usize>, u64>,
    }

    impl Topics {
        fn named(names: &[&str]) -> Topics {
            let values: Vec<FieldValue> = names
                .iter()
                .map(|name| Field::Topic.parse_value(name).unwrap())
                .collect();
            let groups = (0..values.len()).map(|position| (vec![position], 1)).collect();
            Topics { values, groups }
        }

        /// The patterns that the topics suggest, each as its text.
        fn suggest(&self, suggester: &Suggester<'_>) -> Vec<String> {
            suggester
                .suggest(Field::Topic, &self.values, &self.groups)
                .iter()
                .map(ToString::to_string)
                .collect()
        }
    }

    const BY_DEFAULT: Suggester<'static> = Suggester {
        separators: &['.', '-', '_'],
        min_values: 2,
    };

    #[test]
    fn values_that_share_a_prefix_suggest_a_pattern() {
        let topics = Topics::named(&["orders.eu", "orders.us", "payments.settled"]);

        assert_eq!(topics.suggest(&BY_DEFAULT), ["orders.* (2)"]);
    }

    #[test]
    fn a_prefix_ends_behind_a_separator() {
        let topics = Topics::named(&["orders", "orders2", "ordersEu"]);

        assert_eq!(topics.suggest(&BY_DEFAULT), [] as [&str; 0]);
    }

    #[test]
    fn every_separator_cuts_a_value() {
        let topics = Topics::named(&["orders-eu", "orders-us", "audit_log", "audit_trail"]);

        assert_eq!(topics.suggest(&BY_DEFAULT), ["audit_* (2)", "orders-* (2)"]);
    }

    #[test]
    fn a_longer_prefix_that_fewer_values_share_is_a_pattern_of_its_own() {
        let topics = Topics::named(&["orders.eu.v1", "orders.eu.v2", "orders.us.v1"]);

        assert_eq!(topics.suggest(&BY_DEFAULT), ["orders.* (3)", "orders.eu.* (2)"]);
    }

    #[test]
    fn of_the_patterns_for_the_same_values_only_the_longest_is_suggested() {
        let topics = Topics::named(&[
            "prod.orders.eu",
            "prod.orders.us",
            "__consumer_offsets",
            "__transaction_state",
        ]);

        assert_eq!(topics.suggest(&BY_DEFAULT), ["__* (2)", "prod.orders.* (2)"]);
    }

    #[test]
    fn a_pattern_needs_as_many_values_as_the_configuration_says() {
        let topics = Topics::named(&[
            "orders.eu",
            "orders.us",
            "orders.apac",
            "payments.settled",
            "payments.failed",
        ]);
        let three = Suggester {
            min_values: 3,
            ..BY_DEFAULT
        };
        let one = Suggester {
            min_values: 1,
            ..BY_DEFAULT
        };

        assert_eq!(topics.suggest(&three), ["orders.* (3)"]);
        assert_eq!(
            Topics::named(&["orders.eu", "payments.settled"]).suggest(&one),
            ["orders.* (1)", "payments.* (1)"]
        );
    }

    #[test]
    fn a_value_is_only_cut_behind_a_separator_of_the_configuration() {
        let topics = Topics::named(&["orders.eu", "orders.us", "audit-log", "audit-trail"]);
        let dots = Suggester {
            separators: &['.'],
            ..BY_DEFAULT
        };
        let none = Suggester {
            separators: &[],
            ..BY_DEFAULT
        };

        assert_eq!(topics.suggest(&dots), ["orders.* (2)"]);
        assert_eq!(topics.suggest(&none), [] as [&str; 0]);
    }

    #[test]
    fn a_prefix_with_a_wildcard_suggests_nothing() {
        let clients: Vec<FieldValue> = ["batch-*-eu", "batch-*-us", "batch-1"]
            .iter()
            .map(|id| Field::ClientId.parse_value(id).unwrap())
            .collect();
        let groups = (0..clients.len()).map(|position| (vec![position], 1)).collect();

        let patterns = BY_DEFAULT.suggest(Field::ClientId, &clients, &groups);

        // `batch-*` stands for all three, and `batch-*-*` would not mean the client IDs that hold a `*`.
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].to_string(), "batch-* (3)");
        assert_eq!(patterns[0].condition().to_string(), "client.id matches batch-*");
    }

    #[test]
    fn a_call_with_two_values_of_a_pattern_counts_once() {
        let topics = Topics {
            groups: BTreeMap::from([(vec![0, 1], 4), (vec![1], 2), (vec![2], 8)]),
            ..Topics::named(&["orders.eu", "orders.us", "payments.settled"])
        };

        assert_eq!(topics.suggest(&BY_DEFAULT), ["orders.* (6)"]);
    }

    #[test]
    fn patterns_come_with_the_most_calls_first() {
        let topics = Topics {
            groups: BTreeMap::from([(vec![0], 1), (vec![1], 1), (vec![2], 5), (vec![3], 5)]),
            ..Topics::named(&["orders.eu", "orders.us", "payments.settled", "payments.failed"])
        };

        assert_eq!(topics.suggest(&BY_DEFAULT), ["payments.* (10)", "orders.* (2)"]);
    }
}
