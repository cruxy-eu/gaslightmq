use std::fmt;
use std::str::FromStr;

use gaslightmq_field::prelude::*;

use crate::{Glob, SelectorError};

/// How a condition tests its field.
///
/// | Operator | Meaning | Example |
/// |---|---|---|
/// | `is` | Equals one value. | `api is Produce` |
/// | `is any of` | Equals one of several values. | `api is any of JoinGroup, SyncGroup` |
/// | `matches` | Matches a [glob pattern](Glob). | `topic matches orders.*` |
/// | `is not` | Equals none of the values. | `client.id is not kafka-ui` |
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// assert_eq!(Operator::IsAnyOf.to_string(), "is any of");
/// assert_eq!(Operator::IsAnyOf.key(), "is-any-of");
/// assert_eq!("is not".parse(), Ok(Operator::IsNot));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operator {
    /// `is`: the field equals one value.
    Is,
    /// `is any of`: the field equals one of several values.
    IsAnyOf,
    /// `matches`: the text form of the field matches a pattern.
    Matches,
    /// `is not`: the field equals none of the values.
    IsNot,
}

impl Operator {
    /// Every operator.
    pub const ALL: [Operator; 4] = [Operator::Is, Operator::IsAnyOf, Operator::Matches, Operator::IsNot];

    /// The name of the operator in a sentence, such as `is any of`.
    pub const fn name(self) -> &'static str {
        match self {
            Operator::Is => "is",
            Operator::IsAnyOf => "is any of",
            Operator::Matches => "matches",
            Operator::IsNot => "is not",
        }
    }

    /// The key of the operator in a file, such as `is-any-of`.
    pub const fn key(self) -> &'static str {
        match self {
            Operator::Is => "is",
            Operator::IsAnyOf => "is-any-of",
            Operator::Matches => "matches",
            Operator::IsNot => "is-not",
        }
    }

    /// Whether the operator says what the field is.
    /// `is not` alone says what the field is not.
    pub const fn is_positive(self) -> bool {
        !matches!(self, Operator::IsNot)
    }
}

/// Reads the name of an operator in a sentence, as [`Display`](fmt::Display) writes it.
impl FromStr for Operator {
    type Err = SelectorError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Operator::ALL
            .into_iter()
            .find(|operator| operator.name() == text)
            .ok_or_else(|| SelectorError::UnknownOperator { name: text.to_owned() })
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// One or more values of one field, for a condition to test against.
///
/// There is at least one value, every value is of the same field, and no value is there twice.
/// The values are in order, so two sets with the same values are equal.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let group_apis = ValueSet::new([ApiKey::SYNC_GROUP, ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP])?;
/// assert_eq!(group_apis.field(), Field::Api);
/// assert_eq!(group_apis.to_string(), "JoinGroup, SyncGroup");
/// assert!(group_apis.contains(&ApiKey::JOIN_GROUP.into()));
///
/// assert_eq!(ValueSet::new(Vec::<ApiKey>::new()), Err(SelectorError::NoValues));
/// # Ok::<(), SelectorError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueSet(Vec<FieldValue>);

impl ValueSet {
    /// The set of those values.
    ///
    /// # Errors
    ///
    /// [`SelectorError::NoValues`] when there is no value,
    /// and [`SelectorError::MixedFields`] when the values belong to different fields.
    pub fn new<V: Into<FieldValue>>(values: impl IntoIterator<Item = V>) -> Result<Self, SelectorError> {
        let mut values: Vec<FieldValue> = values.into_iter().map(Into::into).collect();
        let first = values.first().ok_or(SelectorError::NoValues)?.field();
        if let Some(other) = values.iter().map(FieldValue::field).find(|field| *field != first) {
            return Err(SelectorError::MixedFields { first, other });
        }
        values.sort();
        values.dedup();
        Ok(Self(values))
    }

    /// The set of one value.
    pub fn one(value: impl Into<FieldValue>) -> Self {
        Self(vec![value.into()])
    }

    /// The field these are values of.
    pub fn field(&self) -> Field {
        self.first().field()
    }

    /// The first value in order. A set always has one.
    pub fn first(&self) -> &FieldValue {
        &self.0[0]
    }

    /// The values in order, each one once.
    pub fn iter(&self) -> std::slice::Iter<'_, FieldValue> {
        self.0.iter()
    }

    /// The values in order, each one once.
    pub fn as_slice(&self) -> &[FieldValue] {
        &self.0
    }

    /// Whether `value` is one of the values.
    pub fn contains(&self, value: &FieldValue) -> bool {
        self.0.binary_search(value).is_ok()
    }
}

impl From<FieldValue> for ValueSet {
    fn from(value: FieldValue) -> Self {
        Self::one(value)
    }
}

/// The values of a field on a call: the set that a condition built from that call tests against.
impl From<&FieldValues> for ValueSet {
    fn from(values: &FieldValues) -> Self {
        // The values of a call are of one field, in order and without duplicates.
        Self(values.iter().cloned().collect())
    }
}

impl<'a> IntoIterator for &'a ValueSet {
    type Item = &'a FieldValue;
    type IntoIter = std::slice::Iter<'a, FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Writes the values with a comma and a space between them.
impl fmt::Display for ValueSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, value) in self.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            fmt::Display::fmt(value, f)?;
        }
        Ok(())
    }
}

/// One clause of a selector: a field, an operator, and what the operator tests against.
///
/// A value is typed by its field, so a condition never tests a field against a value of another one.
/// A pattern tests the text form of a value, and it works on every field.
///
/// A request can carry many values for one field.
/// `is`, `is any of` and `matches` hold when **any** value passes.
/// `is not` holds when **no** value equals, and also when the call does not have the field.
///
/// The text form is a sentence, such as `topic matches orders.*`.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let produce = Condition::is(ApiKey::PRODUCE);
/// assert_eq!(produce.field(), Field::Api);
/// assert_eq!(produce.operator(), Operator::Is);
/// assert_eq!(produce.to_string(), "api is Produce");
///
/// let group_apis = Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP])?;
/// assert_eq!(group_apis.to_string(), "api is any of JoinGroup, SyncGroup");
///
/// let orders = Condition::matches(Field::Topic, "orders.*".parse()?);
/// assert_eq!(orders.to_string(), "topic matches orders.*");
///
/// let not_the_ui = Condition::is_not(ClientId::new("kafka-ui").expect("a client ID"));
/// assert_eq!(not_the_ui.to_string(), "client.id is not kafka-ui");
/// # Ok::<(), SelectorError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Condition {
    /// `is`: a value of the field equals this value.
    Is(FieldValue),
    /// `is any of`: a value of the field equals one of these values.
    IsAnyOf(ValueSet),
    /// `matches`: the text form of a value of the field matches the pattern.
    Matches(Field, Glob),
    /// `is not`: no value of the field equals one of these values.
    IsNot(ValueSet),
}

impl Condition {
    /// `is`: the field of `value` equals `value`.
    pub fn is(value: impl Into<FieldValue>) -> Self {
        Condition::Is(value.into())
    }

    /// `is any of`: the field of the values equals one of them.
    ///
    /// # Errors
    ///
    /// The error of [`ValueSet::new`], when there is no value or the values belong to different fields.
    pub fn is_any_of<V: Into<FieldValue>>(values: impl IntoIterator<Item = V>) -> Result<Self, SelectorError> {
        ValueSet::new(values).map(Condition::IsAnyOf)
    }

    /// `matches`: the text form of `field` matches `pattern`.
    pub fn matches(field: Field, pattern: Glob) -> Self {
        Condition::Matches(field, pattern)
    }

    /// `is not`: the field of `value` does not equal `value`.
    /// [`Condition::IsNot`] takes several values.
    pub fn is_not(value: impl Into<FieldValue>) -> Self {
        Condition::IsNot(ValueSet::one(value))
    }

    /// Reads the condition that a file or a person writes as one text for `field`.
    ///
    /// A text with a [wildcard](Glob::is_pattern) is a pattern, and the condition is `matches`.
    /// Any other text is a value of the field, and the condition is `is`.
    ///
    /// ```
    /// use gaslightmq_field::prelude::*;
    /// use gaslightmq_kernel::prelude::*;
    /// use gaslightmq_selector::prelude::*;
    ///
    /// assert_eq!(Condition::parse(Field::Api, "Produce")?, Condition::is(ApiKey::PRODUCE));
    /// assert_eq!(Condition::parse(Field::Topic, "orders.*")?.operator(), Operator::Matches);
    ///
    /// // `Prodcue` is an error, and not a selector that never matches.
    /// assert!(Condition::parse(Field::Api, "Prodcue").is_err());
    /// # Ok::<(), SelectorError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`SelectorError::Value`] when the text has no wildcard and is not a value of the field.
    pub fn parse(field: Field, text: &str) -> Result<Self, SelectorError> {
        if Glob::is_pattern(text) {
            Glob::new(text).map(|pattern| Condition::Matches(field, pattern))
        } else {
            field
                .parse_value(text)
                .map(Condition::Is)
                .map_err(|error| SelectorError::Value { field, error })
        }
    }

    /// The field the condition tests.
    pub fn field(&self) -> Field {
        match self {
            Condition::Is(value) => value.field(),
            Condition::IsAnyOf(values) | Condition::IsNot(values) => values.field(),
            Condition::Matches(field, _) => *field,
        }
    }

    /// How the condition tests its field.
    pub const fn operator(&self) -> Operator {
        match self {
            Condition::Is(_) => Operator::Is,
            Condition::IsAnyOf(_) => Operator::IsAnyOf,
            Condition::Matches(..) => Operator::Matches,
            Condition::IsNot(_) => Operator::IsNot,
        }
    }

    /// Whether the condition tests each [part](Part) of a call on its own.
    ///
    /// A positive condition on `topic`, `partition`, `outcome` or `error.code` does.
    /// The parts that pass every such condition are the [`Target`] of a match.
    /// `is not` never does: it tests every value across the call.
    pub fn is_per_part(&self) -> bool {
        self.operator().is_positive() && self.field().is_per_part()
    }

    /// Whether the condition holds for what a source reads for its field.
    pub(crate) fn test(&self, reading: Reading<FieldValues>) -> Holds {
        let values = match reading {
            Reading::Present(values) => values,
            // The call does not have the field, so nothing equals and nothing matches.
            Reading::Absent => return Holds::from(!self.operator().is_positive()),
            Reading::Pending => return Holds::Unknown,
        };
        Holds::from(match self {
            Condition::Is(expected) => values.contains(expected),
            Condition::IsAnyOf(expected) => values.iter().any(|value| expected.contains(value)),
            Condition::Matches(_, pattern) => values.iter().any(|value| pattern.matches(&value.to_string())),
            Condition::IsNot(excluded) => !values.iter().any(|value| excluded.contains(value)),
        })
    }
}

/// Writes the sentence: the field, the operator and what it tests against.
impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} ", self.field(), self.operator())?;
        match self {
            Condition::Is(value) => fmt::Display::fmt(value, f),
            Condition::IsAnyOf(values) | Condition::IsNot(values) => fmt::Display::fmt(values, f),
            Condition::Matches(_, pattern) => fmt::Display::fmt(pattern, f),
        }
    }
}

/// The condition that a call's own values make: `is` for one value, and `is any of` for several.
///
/// A selector built from a recorded call starts with these, one for every field that someone ticks.
impl From<&FieldValues> for Condition {
    fn from(values: &FieldValues) -> Self {
        let mut all = values.iter();
        match (all.next(), all.next()) {
            (Some(only), None) => Condition::Is(only.clone()),
            _ => Condition::IsAnyOf(values.into()),
        }
    }
}

/// Whether a condition holds on a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Holds {
    Yes,
    No,
    /// The condition tests a field that the call does not have yet.
    Unknown,
}

impl From<bool> for Holds {
    fn from(holds: bool) -> Self {
        if holds { Holds::Yes } else { Holds::No }
    }
}

impl Holds {
    /// Both hold: one that fails decides, and otherwise one that is unknown leaves it unknown.
    pub(crate) fn and(self, other: Holds) -> Holds {
        match (self, other) {
            (Holds::No, _) | (_, Holds::No) => Holds::No,
            (Holds::Unknown, _) | (_, Holds::Unknown) => Holds::Unknown,
            (Holds::Yes, Holds::Yes) => Holds::Yes,
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;

    fn topic(name: &str) -> FieldValue {
        TopicName::new(name).unwrap().into()
    }

    fn topics(names: &[&str]) -> Reading<FieldValues> {
        FieldValues::of(names.iter().map(|name| TopicName::new(name).unwrap())).into()
    }

    #[test]
    fn every_operator_has_the_name_the_spec_gives_it() {
        let names: Vec<&str> = Operator::ALL.into_iter().map(Operator::name).collect();
        let keys: Vec<&str> = Operator::ALL.into_iter().map(Operator::key).collect();

        assert_eq!(names, ["is", "is any of", "matches", "is not"]);
        assert_eq!(keys, ["is", "is-any-of", "matches", "is-not"]);
    }

    #[test]
    fn a_name_reads_back_as_its_operator() {
        for operator in Operator::ALL {
            assert_eq!(operator.to_string().parse(), Ok(operator));
        }
        for text in ["", "Is", "is-not", "isnot", "is  not"] {
            assert_eq!(
                text.parse::<Operator>(),
                Err(SelectorError::UnknownOperator { name: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn only_is_not_is_negative() {
        let positive: Vec<Operator> = Operator::ALL
            .into_iter()
            .filter(|operator| operator.is_positive())
            .collect();
        assert_eq!(positive, [Operator::Is, Operator::IsAnyOf, Operator::Matches]);
    }

    #[test]
    fn a_value_set_holds_each_value_once_and_in_order() {
        let set = ValueSet::new([topic("payments.settled"), topic("orders.eu"), topic("payments.settled")]).unwrap();

        assert_eq!(set.as_slice(), [topic("orders.eu"), topic("payments.settled")]);
        assert_eq!(set.first(), &topic("orders.eu"));
        assert_eq!(set.iter().count(), 2);
        assert_eq!((&set).into_iter().count(), 2);
        assert_eq!(set.field(), Field::Topic);
        assert_eq!(set.to_string(), "orders.eu, payments.settled");
        assert!(set.contains(&topic("orders.eu")));
        assert!(!set.contains(&topic("orders.us")));
        assert_eq!(
            set,
            ValueSet::new([topic("orders.eu"), topic("payments.settled")]).unwrap()
        );
    }

    #[test]
    fn a_value_set_needs_a_value() {
        assert_eq!(ValueSet::new(Vec::<FieldValue>::new()), Err(SelectorError::NoValues));
    }

    #[test]
    fn a_value_set_is_of_one_field() {
        assert_eq!(
            ValueSet::new([topic("orders.eu"), ApiKey::PRODUCE.into()]),
            Err(SelectorError::MixedFields {
                first: Field::Topic,
                other: Field::Api
            })
        );
    }

    #[test]
    fn a_value_set_of_one_value_converts_from_it() {
        assert_eq!(
            ValueSet::one(ApiKey::PRODUCE),
            ValueSet::new([ApiKey::PRODUCE]).unwrap()
        );
        assert_eq!(ValueSet::from(topic("orders.eu")), ValueSet::one(topic("orders.eu")));
    }

    #[test]
    fn the_values_of_a_call_are_a_value_set() {
        let on_call = topics(&["payments.settled", "orders.eu"]).present().unwrap();

        assert_eq!(
            ValueSet::from(&on_call),
            ValueSet::new([topic("orders.eu"), topic("payments.settled")]).unwrap()
        );
    }

    #[test]
    fn a_condition_knows_its_field_and_its_operator() {
        let cases = [
            (Condition::is(ApiKey::PRODUCE), Field::Api, Operator::Is),
            (
                Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP]).unwrap(),
                Field::Api,
                Operator::IsAnyOf,
            ),
            (
                Condition::matches(Field::Topic, "orders.*".parse().unwrap()),
                Field::Topic,
                Operator::Matches,
            ),
            (
                Condition::is_not(ClientId::new("kafka-ui").unwrap()),
                Field::ClientId,
                Operator::IsNot,
            ),
        ];

        for (condition, field, operator) in cases {
            assert_eq!(condition.field(), field, "{condition}");
            assert_eq!(condition.operator(), operator, "{condition}");
        }
    }

    #[test]
    fn a_condition_writes_the_sentence_of_the_spec() {
        let cases = [
            (Condition::is(ApiKey::PRODUCE), "api is Produce"),
            (
                Condition::is_any_of([ApiKey::SYNC_GROUP, ApiKey::JOIN_GROUP]).unwrap(),
                "api is any of JoinGroup, SyncGroup",
            ),
            (
                Condition::matches(Field::Topic, "orders.*".parse().unwrap()),
                "topic matches orders.*",
            ),
            (
                Condition::is_not(ClientId::new("kafka-ui").unwrap()),
                "client.id is not kafka-ui",
            ),
            (
                Condition::IsNot(ValueSet::new([topic("orders.eu"), topic("orders.us")]).unwrap()),
                "topic is not orders.eu, orders.us",
            ),
            (Condition::is(ConnectionNumber::new(7)), "connection is #7"),
        ];

        for (condition, sentence) in cases {
            assert_eq!(condition.to_string(), sentence);
        }
    }

    #[test]
    fn a_text_with_a_wildcard_reads_as_a_pattern_and_any_other_as_a_value() {
        assert_eq!(
            Condition::parse(Field::Topic, "orders.*"),
            Ok(Condition::matches(Field::Topic, "orders.*".parse().unwrap()))
        );
        assert_eq!(
            Condition::parse(Field::Topic, "orders.eu"),
            Ok(Condition::is(TopicName::new("orders.eu").unwrap()))
        );
        assert_eq!(
            Condition::parse(Field::Connection, "#7"),
            Ok(Condition::is(ConnectionNumber::new(7)))
        );
        assert_eq!(
            Condition::parse(Field::Api, "*Group*"),
            Ok(Condition::matches(Field::Api, "*Group*".parse().unwrap()))
        );
    }

    #[test]
    fn a_text_that_is_not_a_value_of_the_field_is_an_error() {
        for (field, text) in [(Field::Api, "Prodcue"), (Field::Partition, "three"), (Field::Topic, "")] {
            assert_eq!(
                Condition::parse(field, text),
                Err(SelectorError::Value {
                    field,
                    error: field.parse_value(text).unwrap_err()
                }),
                "{field}: {text:?}"
            );
        }
    }

    #[test]
    fn a_positive_condition_holds_when_any_value_passes() {
        let both = || topics(&["orders.eu", "payments.settled"]);
        let is = Condition::is(topic("payments.settled"));
        let any_of = Condition::is_any_of([topic("orders.us"), topic("orders.eu")]).unwrap();
        let matches = Condition::matches(Field::Topic, "pay*".parse().unwrap());

        for condition in [&is, &any_of, &matches] {
            assert_eq!(condition.test(both()), Holds::Yes, "{condition}");
            assert_eq!(condition.test(topics(&["orders.de"])), Holds::No, "{condition}");
            assert_eq!(condition.test(Reading::Absent), Holds::No, "{condition}");
            assert_eq!(condition.test(Reading::Pending), Holds::Unknown, "{condition}");
        }
    }

    #[test]
    fn is_not_holds_when_no_value_equals() {
        let not_eu = Condition::is_not(topic("orders.eu"));
        let neither = Condition::IsNot(ValueSet::new([topic("orders.eu"), topic("orders.us")]).unwrap());

        assert_eq!(not_eu.test(topics(&["orders.eu", "payments.settled"])), Holds::No);
        assert_eq!(not_eu.test(topics(&["payments.settled"])), Holds::Yes);
        assert_eq!(not_eu.test(Reading::Absent), Holds::Yes);
        assert_eq!(not_eu.test(Reading::Pending), Holds::Unknown);
        assert_eq!(neither.test(topics(&["payments.settled", "orders.us"])), Holds::No);
        assert_eq!(neither.test(topics(&["payments.settled"])), Holds::Yes);
    }

    #[test]
    fn a_pattern_tests_the_text_form_of_a_value() {
        let reading = |value: FieldValue| Reading::Present(FieldValues::one(value));
        let cases: [(Field, &str, FieldValue, bool); 6] = [
            (Field::Api, "*Group*", ApiKey::JOIN_GROUP.into(), true),
            (Field::Api, "*Group*", ApiKey::HEARTBEAT.into(), false),
            (Field::Connection, "#?", ConnectionNumber::new(7).into(), true),
            (Field::Connection, "#?", ConnectionNumber::new(17).into(), false),
            (
                Field::ErrorCode,
                "NOT_*",
                ErrorCode::NOT_LEADER_OR_FOLLOWER.into(),
                true,
            ),
            (Field::Partition, "1*", PartitionNumber::from(12).into(), true),
        ];

        for (field, pattern, value, expected) in cases {
            let condition = Condition::matches(field, pattern.parse().unwrap());
            assert_eq!(condition.test(reading(value)), Holds::from(expected), "{condition}");
        }
    }

    #[test]
    fn a_positive_condition_on_a_field_of_a_part_tests_each_part() {
        let per_part = [
            Condition::is(topic("orders.eu")),
            Condition::is_any_of([PartitionNumber::from(3)]).unwrap(),
            Condition::matches(Field::ErrorCode, "NOT_*".parse().unwrap()),
            Condition::is(Outcome::Ok),
        ];
        let whole_call = [
            Condition::is_not(topic("orders.eu")),
            Condition::is_not(Outcome::Ok),
            Condition::is(ApiKey::PRODUCE),
            Condition::matches(Field::ClientId, "order-*".parse().unwrap()),
            Condition::is(Latency::from_millis(14)),
        ];

        for condition in per_part {
            assert!(condition.is_per_part(), "{condition}");
        }
        for condition in whole_call {
            assert!(!condition.is_per_part(), "{condition}");
        }
    }

    #[test]
    fn the_values_of_a_call_make_is_or_is_any_of() {
        let one = topics(&["orders.eu"]).present().unwrap();
        let two = topics(&["orders.eu", "payments.settled"]).present().unwrap();

        assert_eq!(Condition::from(&one), Condition::is(topic("orders.eu")));
        assert_eq!(
            Condition::from(&two),
            Condition::is_any_of([topic("orders.eu"), topic("payments.settled")]).unwrap()
        );
    }

    #[test]
    fn a_condition_that_fails_decides_and_an_unknown_one_does_not() {
        use Holds::{No, Unknown, Yes};

        assert_eq!(Yes.and(Yes), Yes);
        assert_eq!(Yes.and(Unknown), Unknown);
        assert_eq!(Unknown.and(Yes), Unknown);
        assert_eq!(Unknown.and(No), No);
        assert_eq!(No.and(Unknown), No);
        assert_eq!(No.and(Yes), No);
    }
}
