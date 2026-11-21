use std::collections::BTreeMap;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::{Apis, EffectiveSelector};

/// Selectors by API, each with what the caller keeps next to it: a rule, a subscription.
///
/// Most selectors name an API, and a call has one.
/// The index tests a call against the selectors that can match its API only, so most calls test a few.
///
/// The entries keep their order.
/// A snapshot of rules in priority order gives its matches in priority order.
///
/// The index knows nothing of [passes](crate::Pass).
/// Whoever evaluates in two passes keeps an index for each, by [`EffectiveSelector::pass`].
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let index = ApiIndex::new([
///     ("slow produce", EffectiveSelector::from(Condition::is(ApiKey::PRODUCE))),
///     ("lost fetch", EffectiveSelector::from(Condition::is(ApiKey::FETCH))),
///     ("everything", EffectiveSelector::new()),
/// ]);
///
/// let heartbeat = Fields::new().with(ApiKey::HEARTBEAT);
/// let produce = Fields::new().with(ApiKey::PRODUCE);
///
/// let matched = |call: &Fields| index.matches(call).map(|(rule, _)| *rule).collect::<Vec<_>>();
/// assert_eq!(matched(&heartbeat), ["everything"]);
/// assert_eq!(matched(&produce), ["slow produce", "everything"]);
/// assert_eq!(index.candidates(ApiKey::HEARTBEAT).count(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiIndex<T> {
    entries: Vec<(T, EffectiveSelector)>,
    /// For every API that a selector names: the positions of the entries that can match it, in order.
    by_api: BTreeMap<ApiKey, Vec<usize>>,
    /// The positions of the entries that can match any API: what a call of an API that nobody names tests.
    any_api: Vec<usize>,
    /// The position of every entry: what a source without one API tests.
    all: Vec<usize>,
}

impl<T> ApiIndex<T> {
    /// The index of those entries, in their order.
    pub fn new(entries: impl IntoIterator<Item = (T, EffectiveSelector)>) -> Self {
        let entries: Vec<(T, EffectiveSelector)> = entries.into_iter().collect();
        let apis: Vec<Apis> = entries.iter().map(|(_, selector)| selector.apis()).collect();

        let positions = |can_match: &dyn Fn(&Apis) -> bool| -> Vec<usize> {
            let matching = apis.iter().enumerate().filter(|(_, apis)| can_match(apis));
            matching.map(|(position, _)| position).collect()
        };
        let named = apis.iter().filter_map(|apis| match apis {
            Apis::Any => None,
            Apis::Only(named) => Some(named),
        });
        let by_api = named
            .flatten()
            .map(|api| (*api, positions(&|apis| apis.contains(*api))))
            .collect();
        let any_api = positions(&|apis| *apis == Apis::Any);

        let all = (0..entries.len()).collect();

        Self {
            entries,
            by_api,
            any_api,
            all,
        }
    }

    /// How many entries there are.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there is no entry.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every entry, in order.
    pub fn iter(&self) -> impl Iterator<Item = (&T, &EffectiveSelector)> {
        self.entries.iter().map(|(entry, selector)| (entry, selector))
    }

    /// The entries whose selector can match a call of `api`, in order.
    ///
    /// The selector of a candidate is not tested yet, and it can still fail on the call.
    pub fn candidates(&self, api: ApiKey) -> impl Iterator<Item = (&T, &EffectiveSelector)> {
        self.at(self.positions(api))
    }

    /// The entries whose selector matches the call, each with the [`Target`] it matched, in order.
    ///
    /// An entry whose match is [pending](crate::Match::Pending) is left out: it does not match yet.
    pub fn matches<'a, S: FieldSource + ?Sized>(&'a self, source: &'a S) -> impl Iterator<Item = (&'a T, Target)> {
        // A call always has one API. For a source that says otherwise, every selector decides for itself.
        let positions = one_api(source).map_or(self.all.as_slice(), |api| self.positions(api));
        self.at(positions)
            .filter_map(|(entry, selector)| Some((entry, selector.evaluate(source).into_target()?)))
    }

    /// The positions of the entries that can match a call of `api`, in order.
    fn positions(&self, api: ApiKey) -> &[usize] {
        self.by_api.get(&api).unwrap_or(&self.any_api)
    }

    fn at<'a>(&'a self, positions: &'a [usize]) -> impl Iterator<Item = (&'a T, &'a EffectiveSelector)> {
        positions.iter().filter_map(|position| {
            let (entry, selector) = self.entries.get(*position)?;
            Some((entry, selector))
        })
    }
}

/// The API of the call, when it has exactly one.
fn one_api<S: FieldSource + ?Sized>(source: &S) -> Option<ApiKey> {
    let apis = source.read(Field::Api).present()?;
    let mut apis = apis.iter();
    match (apis.next(), apis.next()) {
        (Some(FieldValue::Api(api)), None) => Some(*api),
        _ => None,
    }
}

impl<T> Default for ApiIndex<T> {
    fn default() -> Self {
        Self::new([])
    }
}

impl<T> FromIterator<(T, EffectiveSelector)> for ApiIndex<T> {
    fn from_iter<I: IntoIterator<Item = (T, EffectiveSelector)>>(entries: I) -> Self {
        Self::new(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Condition;

    fn is(api: ApiKey) -> EffectiveSelector {
        Condition::is(api).into()
    }

    fn topic(pattern: &str) -> Condition {
        Condition::parse(Field::Topic, pattern).unwrap()
    }

    /// Rules in priority order, with the APIs their selectors name.
    fn index() -> ApiIndex<&'static str> {
        ApiIndex::new([
            ("produce orders", is(ApiKey::PRODUCE).and(topic("orders.*"))),
            ("everything", EffectiveSelector::new()),
            ("fetch", is(ApiKey::FETCH)),
            (
                "group",
                Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP])
                    .unwrap()
                    .into(),
            ),
            ("orders", topic("orders.*").into()),
            ("produce", is(ApiKey::PRODUCE)),
            ("never", is(ApiKey::PRODUCE).and(Condition::is(ApiKey::FETCH))),
            ("not produce", Condition::is_not(ApiKey::PRODUCE).into()),
        ])
    }

    fn candidates(index: &ApiIndex<&'static str>, api: ApiKey) -> Vec<&'static str> {
        index.candidates(api).map(|(rule, _)| *rule).collect()
    }

    fn matches(index: &ApiIndex<&'static str>, call: &Fields) -> Vec<(&'static str, String)> {
        index
            .matches(call)
            .map(|(rule, target)| (*rule, target.to_string()))
            .collect()
    }

    #[test]
    fn the_candidates_of_an_api_are_the_selectors_that_name_it_or_name_none() {
        let index = index();

        assert_eq!(
            candidates(&index, ApiKey::PRODUCE),
            ["produce orders", "everything", "orders", "produce", "not produce"]
        );
        assert_eq!(
            candidates(&index, ApiKey::FETCH),
            ["everything", "fetch", "orders", "not produce"]
        );
        assert_eq!(
            candidates(&index, ApiKey::SYNC_GROUP),
            ["everything", "group", "orders", "not produce"]
        );
    }

    #[test]
    fn an_api_that_no_selector_names_has_the_selectors_that_name_none() {
        let index = index();

        assert_eq!(
            candidates(&index, ApiKey::HEARTBEAT),
            ["everything", "orders", "not produce"]
        );
        assert_eq!(
            candidates(&index, ApiKey::new(9_999)),
            ["everything", "orders", "not produce"]
        );
    }

    #[test]
    fn the_matches_of_a_call_come_in_the_order_of_the_entries_with_their_targets() {
        let index = index();
        let produce = Fields::new()
            .with(ApiKey::PRODUCE)
            .with_part("orders.eu/3".parse::<TopicPartition>().unwrap())
            .with_part("payments.settled/0".parse::<TopicPartition>().unwrap());

        assert_eq!(
            matches(&index, &produce),
            [
                ("produce orders", "orders.eu/3".to_owned()),
                ("everything", "orders.eu/3, payments.settled/0".to_owned()),
                ("orders", "orders.eu/3".to_owned()),
                ("produce", "orders.eu/3, payments.settled/0".to_owned()),
            ]
        );
    }

    #[test]
    fn a_candidate_still_has_to_match() {
        let index = index();
        let heartbeat = Fields::new().with(ApiKey::HEARTBEAT);

        assert_eq!(
            matches(&index, &heartbeat),
            [("everything", "call".to_owned()), ("not produce", "call".to_owned())]
        );
    }

    #[test]
    fn the_index_matches_what_testing_every_selector_matches() {
        let index = index();
        let calls = [
            Fields::new().with(ApiKey::PRODUCE),
            Fields::new()
                .with(ApiKey::FETCH)
                .with_part("orders.eu/3".parse::<TopicPartition>().unwrap()),
            Fields::new().with(ApiKey::JOIN_GROUP),
            Fields::new().with(ApiKey::new(9_999)),
        ];

        for call in calls {
            let one_by_one: Vec<(&str, Target)> = index
                .iter()
                .filter_map(|(rule, selector)| Some((*rule, selector.evaluate(&call).into_target()?)))
                .collect();
            let indexed: Vec<(&str, Target)> = index.matches(&call).map(|(rule, target)| (*rule, target)).collect();
            assert_eq!(indexed, one_by_one);
        }
    }

    #[test]
    fn a_source_without_one_api_tests_every_selector() {
        let index = index();
        let no_api = Fields::new().with_part("orders.eu/3".parse::<TopicPartition>().unwrap());

        assert_eq!(
            matches(&index, &no_api),
            [
                ("everything", "orders.eu/3".to_owned()),
                ("orders", "orders.eu/3".to_owned()),
                ("not produce", "orders.eu/3".to_owned()),
            ]
        );
    }

    #[test]
    fn a_match_that_waits_for_the_truth_is_left_out() {
        let index = ApiIndex::new([
            ("failed produce", is(ApiKey::PRODUCE).and(Condition::is(Outcome::Error))),
            ("produce", is(ApiKey::PRODUCE)),
        ]);
        let asked = Fields::new().with(ApiKey::PRODUCE);

        assert_eq!(matches(&index, &asked), [("produce", "call".to_owned())]);
        assert_eq!(
            matches(&index, &asked.with(Outcome::Error)),
            [("failed produce", "call".to_owned()), ("produce", "call".to_owned())]
        );
    }

    #[test]
    fn an_index_knows_its_entries() {
        let index = index();
        let empty: ApiIndex<&str> = ApiIndex::default();

        assert_eq!(index.len(), 8);
        assert!(!index.is_empty());
        assert_eq!(index.iter().count(), 8);
        assert!(empty.is_empty());
        assert_eq!(empty.candidates(ApiKey::PRODUCE).count(), 0);
        assert_eq!(
            [("produce", is(ApiKey::PRODUCE))].into_iter().collect::<ApiIndex<_>>(),
            ApiIndex::new([("produce", is(ApiKey::PRODUCE))])
        );
    }
}
