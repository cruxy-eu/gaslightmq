//! The `serde` form of a [`FieldValue`]: alone with its field, or bare under a field that the context names.

use std::fmt;

use serde::de::{DeserializeSeed, Error, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Field, FieldValue};

/// Writes a map with one entry: the field and the bare value, such as `topic: orders.eu`.
impl Serialize for FieldValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.field(), &self.bare())?;
        map.end()
    }
}

/// Reads a map with one entry: the field and the bare value, such as `topic: orders.eu`.
impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct OneEntry;

        impl<'de> Visitor<'de> for OneEntry {
            type Value = FieldValue;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map with one field and its value")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<FieldValue, A::Error> {
                let field: Field = map.next_key()?.ok_or_else(|| A::Error::invalid_length(0, &self))?;
                let value = map.next_value_seed(field)?;
                match map.next_key::<Field>()? {
                    Some(_) => Err(A::Error::invalid_length(2, &self)),
                    None => Ok(value),
                }
            }
        }

        deserializer.deserialize_map(OneEntry)
    }
}

impl FieldValue {
    /// The value without its field, for a place that names the field itself.
    ///
    /// A selector writes `topic: orders.eu`: the key is the field, and the value is bare.
    /// The bare form is the `serde` form of the value's own type, so a partition writes a number.
    /// A [`Field`] reads it back as a [`DeserializeSeed`].
    pub fn bare(&self) -> impl Serialize + '_ {
        struct Bare<'a>(&'a FieldValue);

        impl Serialize for Bare<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize_bare(serializer)
            }
        }

        Bare(self)
    }
}

/// A field reads a bare value of its own type, as [`FieldValue::bare`] writes it.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use serde::de::DeserializeSeed;
///
/// let mut json = serde_json::Deserializer::from_str("3");
/// let value = Field::Partition.deserialize(&mut json).unwrap();
/// assert_eq!(value, FieldValue::Partition(PartitionNumber::from(3)));
/// assert_eq!(serde_json::to_string(&value.bare()).unwrap(), "3");
/// ```
impl<'de> DeserializeSeed<'de> for Field {
    type Value = FieldValue;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<FieldValue, D::Error> {
        FieldValue::deserialize_bare(self, deserializer)
    }
}
