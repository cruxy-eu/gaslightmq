//! The `serde` form of conditions, and of what holds them.
//!
//! A condition stands under the key of its field, and its form says which operator it is.
//! Reading looks at what is there, so these forms need a format that describes itself, such as YAML or JSON.

use std::fmt;

use gaslightmq_field::prelude::*;
use serde::de::value::MapAccessDeserializer;
use serde::de::{DeserializeSeed, Error, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    Condition, ConditionError, Conditions, Glob, Operator, Scope, SelectorName, SelectorRef, ValueSet, ValueSetError,
};

/// The keys of the operators, for the message about a key that is none of them.
const OPERATOR_KEYS: [&str; 4] = [
    Operator::Is.key(),
    Operator::IsAnyOf.key(),
    Operator::Matches.key(),
    Operator::IsNot.key(),
];

/// Writes a map with one entry per condition: the field, and the form of the condition.
impl Serialize for Conditions {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for condition in self {
            map.serialize_entry(&condition.field(), &Form(condition))?;
        }
        map.end()
    }
}

/// Reads a map with one entry per condition: the field, and the form of the condition.
impl<'de> Deserialize<'de> for Conditions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByField;

        impl<'de> Visitor<'de> for ByField {
            type Value = Conditions;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of fields and their conditions")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Conditions, A::Error> {
                let mut conditions = Conditions::new();
                while let Some(field) = map.next_key::<Field>()? {
                    if conditions.insert(map.next_value_seed(FormOf(field))?).is_some() {
                        return Err(A::Error::custom(format_args!("{field} has two conditions")));
                    }
                }
                Ok(conditions)
            }
        }

        deserializer.deserialize_map(ByField)
    }
}

/// Writes a map with one entry: the field, and the form of the condition.
impl Serialize for Condition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.field(), &Form(self))?;
        map.end()
    }
}

/// Reads a map with one entry: the field, and the form of the condition.
impl<'de> Deserialize<'de> for Condition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OneEntry;

        impl<'de> Visitor<'de> for OneEntry {
            type Value = Condition;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map with one field and its condition")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Condition, A::Error> {
                let field: Field = map.next_key()?.ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let condition = map.next_value_seed(FormOf(field))?;
                match map.next_key::<Field>()? {
                    Some(_) => Err(A::Error::invalid_length(2, &self)),
                    None => Ok(condition),
                }
            }
        }

        deserializer.deserialize_map(OneEntry)
    }
}

/// Writes the conditions of the scope.
impl Serialize for Scope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.conditions().serialize(serializer)
    }
}

/// Reads conditions, and checks that they only test connection fields.
impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Scope::new(Conditions::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Writes the name of a selector as text, and inline conditions as their map.
impl Serialize for SelectorRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            SelectorRef::Named(name) => name.serialize(serializer),
            SelectorRef::Inline(conditions) => conditions.serialize(serializer),
        }
    }
}

/// Reads text as the name of a selector, and a map as inline conditions.
impl<'de> Deserialize<'de> for SelectorRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NameOrConditions;

        impl<'de> Visitor<'de> for NameOrConditions {
            type Value = SelectorRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the name of a selector, or a map of fields and their conditions")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<SelectorRef, E> {
                SelectorName::new(text).map(SelectorRef::Named).map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<SelectorRef, A::Error> {
                Conditions::deserialize(MapAccessDeserializer::new(map)).map(SelectorRef::Inline)
            }
        }

        deserializer.deserialize_any(NameOrConditions)
    }
}

/// The form of a condition under the key of its field.
///
/// | Condition | Short form | Form that names the operator |
/// |---|---|---|
/// | `is` | The value. | `{ is: value }` |
/// | `is any of` | A list of values. | `{ is-any-of: [values] }` |
/// | `matches` | The pattern. | `{ matches: pattern }` |
/// | `is not` | None. | `{ is-not: value }` or `{ is-not: [values] }` |
///
/// Writing takes the short form whenever it reads back as the same condition.
/// It does not for a value whose text holds a wildcard, nor for a pattern without one.
struct Form<'a>(&'a Condition);

impl Serialize for Form<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let reads_as_a_pattern = |value: &FieldValue| Glob::is_pattern(&value.to_string());
        match self.0 {
            Condition::Is(value) if !reads_as_a_pattern(value) => value.bare().serialize(serializer),
            Condition::IsAnyOf(values) if !values.iter().any(reads_as_a_pattern) => Bare(values).serialize(serializer),
            Condition::Matches(_, pattern) if pattern.has_wildcard() => pattern.serialize(serializer),
            named => {
                let mut map = serializer.serialize_map(Some(1))?;
                let key = named.operator().key();
                match named {
                    Condition::Is(value) => map.serialize_entry(key, &value.bare())?,
                    Condition::IsAnyOf(values) => map.serialize_entry(key, &Bare(values))?,
                    Condition::Matches(_, pattern) => map.serialize_entry(key, pattern)?,
                    Condition::IsNot(values) => match values.as_slice() {
                        [only] => map.serialize_entry(key, &only.bare())?,
                        _ => map.serialize_entry(key, &Bare(values))?,
                    },
                }
                map.end()
            }
        }
    }
}

/// A list of values without their field, for a place that names the field itself.
struct Bare<'a>(&'a ValueSet);

impl Serialize for Bare<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(FieldValue::bare))
    }
}

/// Reads the [`Form`] of a condition on this field.
struct FormOf(Field);

impl<'de> DeserializeSeed<'de> for FormOf {
    type Value = Condition;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Condition, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for FormOf {
    type Value = Condition;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a value of {}, a pattern, a list of values, or a map with one operator",
            self.0
        )
    }

    fn visit_str<E: Error>(self, text: &str) -> Result<Condition, E> {
        Condition::parse(self.0, text).map_err(E::custom)
    }

    fn visit_i64<E: Error>(self, number: i64) -> Result<Condition, E> {
        ValueOf::short(self.0).visit_i64(number).map(Condition::Is)
    }

    fn visit_u64<E: Error>(self, number: u64) -> Result<Condition, E> {
        ValueOf::short(self.0).visit_u64(number).map(Condition::Is)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Condition, A::Error> {
        ValuesOf::short(self.0).visit_seq(seq).map(Condition::IsAnyOf)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Condition, A::Error> {
        let field = self.0;
        let OperatorKey(operator) = map
            .next_key()?
            .ok_or_else(|| A::Error::invalid_length(0, &ONE_OPERATOR))?;
        let condition = match operator {
            Operator::Is => Condition::Is(map.next_value_seed(ValueOf::named(field))?),
            Operator::IsAnyOf => Condition::IsAnyOf(map.next_value_seed(ValuesOf::named(field))?),
            Operator::Matches => Condition::Matches(field, map.next_value()?),
            Operator::IsNot => Condition::IsNot(map.next_value_seed(ValuesOf::named(field))?),
        };
        match map.next_key::<OperatorKey>()? {
            Some(_) => Err(A::Error::invalid_length(2, &ONE_OPERATOR)),
            None => Ok(condition),
        }
    }
}

const ONE_OPERATOR: &str = "a map with one operator";

/// The key of an operator in a file, such as `is-not`.
struct OperatorKey(Operator);

impl<'de> Deserialize<'de> for OperatorKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Key;

        impl Visitor<'_> for Key {
            type Value = OperatorKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the key of an operator")
            }

            fn visit_str<E: Error>(self, key: &str) -> Result<OperatorKey, E> {
                let operator = Operator::ALL.into_iter().find(|operator| operator.key() == key);
                operator
                    .map(OperatorKey)
                    .ok_or_else(|| E::unknown_variant(key, &OPERATOR_KEYS))
            }
        }

        deserializer.deserialize_str(Key)
    }
}

/// Reads one value of this field: text in the text form of the field, or a number.
#[derive(Clone, Copy)]
struct ValueOf {
    field: Field,
    /// Whether the value stands under the key of an operator.
    /// There, a text with a wildcard is that text. In the short form it is a pattern, and no value.
    named: bool,
}

impl ValueOf {
    fn short(field: Field) -> Self {
        Self { field, named: false }
    }

    fn named(field: Field) -> Self {
        Self { field, named: true }
    }

    /// A number is a value of a field that holds numbers, such as `partition: 3`.
    /// It is also the text of its digits, for a client that calls its group `42`.
    fn number<E: Error>(self, typed: Result<FieldValue, E>, digits: &str) -> Result<FieldValue, E> {
        typed
            .or_else(|error| self.field.parse_value(digits).map_err(|_| error))
            .map_err(|error| E::custom(format_args!("{}: {error}", self.field)))
    }
}

impl<'de> DeserializeSeed<'de> for ValueOf {
    type Value = FieldValue;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<FieldValue, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl Visitor<'_> for ValueOf {
    type Value = FieldValue;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a value of {}", self.field)
    }

    fn visit_str<E: Error>(self, text: &str) -> Result<FieldValue, E> {
        let field = self.field;
        if !self.named && Glob::is_pattern(text) {
            let pattern = text.to_owned();
            return Err(E::custom(ValueSetError::PatternAmongValues { field, pattern }));
        }
        field
            .parse_value(text)
            .map_err(|error| E::custom(ConditionError::InvalidValue { error }))
    }

    fn visit_i64<E: Error>(self, number: i64) -> Result<FieldValue, E> {
        self.number(self.field.deserialize(number.into_deserializer()), &number.to_string())
    }

    fn visit_u64<E: Error>(self, number: u64) -> Result<FieldValue, E> {
        self.number(self.field.deserialize(number.into_deserializer()), &number.to_string())
    }
}

/// Reads one value of this field, or a list of them.
struct ValuesOf(ValueOf);

impl ValuesOf {
    fn short(field: Field) -> Self {
        Self(ValueOf::short(field))
    }

    fn named(field: Field) -> Self {
        Self(ValueOf::named(field))
    }
}

impl<'de> DeserializeSeed<'de> for ValuesOf {
    type Value = ValueSet;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<ValueSet, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValuesOf {
    type Value = ValueSet;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a value of {}, or a list of them", self.0.field)
    }

    fn visit_str<E: Error>(self, text: &str) -> Result<ValueSet, E> {
        self.0.visit_str(text).map(ValueSet::one)
    }

    fn visit_i64<E: Error>(self, number: i64) -> Result<ValueSet, E> {
        self.0.visit_i64(number).map(ValueSet::one)
    }

    fn visit_u64<E: Error>(self, number: u64) -> Result<ValueSet, E> {
        self.0.visit_u64(number).map(ValueSet::one)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ValueSet, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(self.0)? {
            values.push(value);
        }
        ValueSet::new(values).map_err(A::Error::custom)
    }
}
