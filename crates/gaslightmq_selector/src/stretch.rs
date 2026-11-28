//! Whether a selector can match a call of a stretch of calls, as far as the values of the stretch tell.

use std::collections::BTreeSet;

use gaslightmq_field::{Field, FieldValue};

use crate::{Condition, EffectiveSelector};

/// What the calls of a stretch have for one field, as a summary of the stretch says.
///
/// A recording sums up every stretch it keeps: the values that its calls have for each field, up to a number of
/// them. Whoever reads the recording hands those values over, and the selector says whether the stretch can hold a
/// call that it matches: [`EffectiveSelector::may_match_in`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StretchValues<'a> {
    /// No call of the stretch has the field.
    Absent,
    /// Every value that a call of the stretch has for the field, for the call as a whole or for one of its parts.
    Listed(&'a BTreeSet<FieldValue>),
    /// The stretch has more values than its summary lists, so it can hold any value of the field.
    Any,
}

impl EffectiveSelector {
    /// Whether a call of a stretch can match, as far as the values of the stretch tell.
    ///
    /// `values` gives what the calls of the stretch have for a field.
    /// `false` promises that no call of the stretch matches, so a reader skips the stretch.
    /// `true` promises nothing: the reader decides every call it reads with [`evaluate`](Self::evaluate).
    ///
    /// | Condition | The stretch cannot match when |
    /// |---|---|
    /// | `is`, `is any of` | No call has the field, or the values hold none of those of the condition. |
    /// | `matches` | No call has the field, or the pattern matches none of the values. |
    /// | `is not` | Never: it also matches a call without the field. |
    /// | Any, on a field whose values are [`Any`](StretchValues::Any) | Never. |
    ///
    /// Values are summed up per field, so a stretch with `orders.eu/1` and `payments.settled/3` may match
    /// `orders.eu/3`, although no part of it does.
    ///
    /// ```
    /// use std::collections::BTreeSet;
    ///
    /// use gaslightmq_field::prelude::*;
    /// use gaslightmq_kernel::prelude::*;
    /// use gaslightmq_selector::prelude::*;
    ///
    /// let orders = EffectiveSelector::from(Condition::parse(Field::Topic, "orders.*")?);
    /// let topics: BTreeSet<FieldValue> = BTreeSet::from([TopicName::new("payments.settled").expect("a topic").into()]);
    ///
    /// // A stretch of payments holds no call for an orders topic, and a stretch of heartbeats has no topic at all.
    /// assert!(!orders.may_match_in(|field| match field {
    ///     Field::Topic => StretchValues::Listed(&topics),
    ///     _ => StretchValues::Absent,
    /// }));
    /// assert!(!orders.may_match_in(|_| StretchValues::Absent));
    /// // A stretch with too many topics to list may hold one.
    /// assert!(orders.may_match_in(|_| StretchValues::Any));
    /// # Ok::<(), ConditionError>(())
    /// ```
    pub fn may_match_in<'a>(&self, values: impl Fn(Field) -> StretchValues<'a>) -> bool {
        self.conditions()
            .iter()
            .all(|condition| condition.may_hold_in(values(condition.field())))
    }
}

impl Condition {
    /// Whether a value of the stretch can make the condition hold.
    fn may_hold_in(&self, values: StretchValues<'_>) -> bool {
        match (self, values) {
            // `is not` also matches a call without the field.
            (Condition::IsNot(_), _) | (_, StretchValues::Any) => true,
            (_, StretchValues::Absent) => false,
            (Condition::Is(expected), StretchValues::Listed(listed)) => listed.contains(expected),
            (Condition::IsAnyOf(expected), StretchValues::Listed(listed)) => {
                expected.iter().any(|value| listed.contains(value))
            }
            (Condition::Matches(_, pattern), StretchValues::Listed(listed)) => {
                listed.iter().any(|value| pattern.matches(&value.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn topics(names: &[&str]) -> BTreeSet<FieldValue> {
        names
            .iter()
            .map(|name| FieldValue::from(TopicName::new(*name).unwrap()))
            .collect()
    }

    fn topic(name: &str) -> FieldValue {
        TopicName::new(name).unwrap().into()
    }

    #[test]
    fn is_and_is_any_of_need_one_of_their_values_in_the_stretch() {
        let listed = topics(&["orders.eu", "orders.us"]);
        let values = |field| match field {
            Field::Topic => StretchValues::Listed(&listed),
            _ => StretchValues::Absent,
        };

        assert!(EffectiveSelector::from(Condition::is(topic("orders.eu"))).may_match_in(values));
        assert!(!EffectiveSelector::from(Condition::is(topic("payments.settled"))).may_match_in(values));
        let either = Condition::is_any_of([topic("audit"), topic("orders.us")]).unwrap();
        assert!(EffectiveSelector::from(either).may_match_in(values));
        let neither = Condition::is_any_of([topic("audit"), topic("payments.settled")]).unwrap();
        assert!(!EffectiveSelector::from(neither).may_match_in(values));
    }

    #[test]
    fn a_pattern_needs_a_value_that_it_matches() {
        let listed = topics(&["orders.eu"]);
        let values = |_| StretchValues::Listed(&listed);

        assert!(EffectiveSelector::from(Condition::parse(Field::Topic, "orders.*").unwrap()).may_match_in(values));
        assert!(!EffectiveSelector::from(Condition::parse(Field::Topic, "audit-*").unwrap()).may_match_in(values));
    }

    #[test]
    fn is_not_never_rules_a_stretch_out() {
        let listed = topics(&["orders.eu"]);
        let not_orders = EffectiveSelector::from(Condition::is_not(topic("orders.eu")));

        assert!(not_orders.may_match_in(|_| StretchValues::Listed(&listed)));
        assert!(not_orders.may_match_in(|_| StretchValues::Absent));
    }

    #[test]
    fn a_field_that_no_call_has_rules_out_every_positive_condition() {
        let produce = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));

        assert!(!produce.may_match_in(|_| StretchValues::Absent));
        assert!(produce.may_match_in(|_| StretchValues::Any));
    }

    #[test]
    fn every_condition_must_be_able_to_hold() {
        let listed = topics(&["orders.eu"]);
        let values = |field| match field {
            Field::Topic => StretchValues::Listed(&listed),
            _ => StretchValues::Absent,
        };
        let produced_to_orders =
            EffectiveSelector::from(Condition::is(topic("orders.eu"))).and(Condition::is(ApiKey::PRODUCE));

        assert!(!produced_to_orders.may_match_in(values));
        assert!(EffectiveSelector::new().may_match_in(values));
    }
}
