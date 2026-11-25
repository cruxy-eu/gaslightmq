//! The filter of the live traffic: which calls of the environment the UI follows.

use gaslightmq_control_api::field::{Field, FieldValue};
use gaslightmq_control_api::selector::{Condition, Conditions, SelectorRef, ValueSet};

/// The calls of the live traffic that someone wants to see: conditions that combine with **and**, one per field.
///
/// Someone clicks a value in the traffic, such as a client or a topic, and the filter toggles it.
/// The control plane follows the filter as the selector of the subscription,
/// so the UI only receives the calls it shows. A filter without conditions follows every call.
///
/// ```
/// use gaslightmq_control_api::field::FieldValue;
/// use gaslightmq_control_api::kernel::ClientId;
/// use gaslightmq_ui::TrafficFilter;
///
/// let ingest = FieldValue::from(ClientId::new("order-ingest").expect("a client ID"));
/// let mut filter = TrafficFilter::default();
///
/// filter.toggle(ingest.clone());
/// assert_eq!(filter.to_string(), "client.id is order-ingest");
///
/// filter.toggle(ingest);
/// assert!(filter.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrafficFilter {
    conditions: Conditions,
}

impl TrafficFilter {
    /// The conditions of the filter, one per field.
    pub fn conditions(&self) -> &Conditions {
        &self.conditions
    }

    /// Whether the filter follows every call.
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Whether the filter shows calls with `value`: its field has a condition that names the value.
    pub fn holds(&self, value: &FieldValue) -> bool {
        match self.conditions.get(value.field()) {
            Some(Condition::Is(held)) => held == value,
            Some(Condition::IsAnyOf(held)) => held.contains(value),
            _ => false,
        }
    }

    /// Adds `value` to the condition of its field, or takes it away when the condition names it already.
    ///
    /// A field with one value is `is` that value, and a field with several is `is any of` them.
    /// A condition that tests otherwise, such as a pattern, gives way to the value.
    pub fn toggle(&mut self, value: FieldValue) {
        let field = value.field();
        let mut values: Vec<FieldValue> = match self.conditions.remove(field) {
            Some(Condition::Is(held)) => vec![held],
            Some(Condition::IsAnyOf(held)) => held.iter().cloned().collect(),
            Some(Condition::Matches(..) | Condition::IsNot(_)) | None => Vec::new(),
        };
        if let Some(position) = values.iter().position(|held| held == &value) {
            values.remove(position);
        } else {
            values.push(value);
        }
        if let Some(condition) = condition_of(values) {
            self.conditions.insert(condition);
        }
    }

    /// Takes the condition of `field` away.
    pub fn clear(&mut self, field: Field) {
        self.conditions.remove(field);
    }

    /// The selector of a subscription that follows the calls of the filter.
    pub fn selector(&self) -> SelectorRef {
        SelectorRef::Inline(self.conditions.clone())
    }
}

/// The condition that holds `values`: none for no value, `is` for one, and `is any of` for several.
fn condition_of(values: Vec<FieldValue>) -> Option<Condition> {
    match <[FieldValue; 1]>::try_from(values) {
        Ok([only]) => Some(Condition::Is(only)),
        // Values of one field always make a set, and an empty list makes none.
        Err(values) => ValueSet::new(values).ok().map(Condition::IsAnyOf),
    }
}

/// Writes the conditions as a sentence: `client.id is order-ingest and api is any of Produce, Fetch`.
impl std::fmt::Display for TrafficFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (position, condition) in self.conditions.iter().enumerate() {
            if position > 0 {
                f.write_str(" and ")?;
            }
            write!(f, "{condition}")?;
        }
        Ok(())
    }
}
