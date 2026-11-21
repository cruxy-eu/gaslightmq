use std::fmt;

use gaslightmq_kernel::prelude::*;

use crate::{
    Acks, ApiVersion, ClientAddress, ClientHost, ClientSoftware, ConnectionNumber, Endpoint, Field, FieldError,
    Latency, Outcome,
};

/// Defines [`FieldValue`] from the type that each field holds.
///
/// A variant has the name of its field, so one list keeps the value, its field and its text form together.
macro_rules! field_values {
    ($($(#[$meta:meta])* $variant:ident($type:ty),)*) => {
        /// One value of one field, such as the topic `orders.eu`.
        ///
        /// Every field has its own variant with its own type, so a value always fits its field.
        /// Every type belongs to one field only, so a value converts into a `FieldValue` without naming the field.
        ///
        /// The text form of a value is that of its type.
        /// It is what a glob pattern tests, what the UI shows and what a selector in YAML writes.
        /// [`Field::parse_value`] reads it back.
        ///
        /// ```
        /// use gaslightmq_field::prelude::*;
        /// use gaslightmq_kernel::prelude::*;
        ///
        /// let value = FieldValue::from(TopicName::new("orders.eu")?);
        /// assert_eq!(value.field(), Field::Topic);
        /// assert_eq!(value.to_string(), "orders.eu");
        ///
        /// // A selector that says `api: Produce` reads the value through its field.
        /// assert_eq!(Field::Api.parse_value("Produce")?, FieldValue::Api(ApiKey::PRODUCE));
        /// assert!(Field::Api.parse_value("Prodcue").is_err());
        /// # Ok::<(), FieldError>(())
        /// ```
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum FieldValue {
            $($(#[$meta])* $variant($type),)*
        }

        impl FieldValue {
            /// The field this is a value of.
            pub const fn field(&self) -> Field {
                match self {
                    $(FieldValue::$variant(_) => Field::$variant,)*
                }
            }
        }

        impl fmt::Display for FieldValue {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(FieldValue::$variant(value) => fmt::Display::fmt(value, f),)*
                }
            }
        }

        $(
            impl From<$type> for FieldValue {
                fn from(value: $type) -> Self {
                    FieldValue::$variant(value)
                }
            }
        )*

        impl Field {
            /// Reads a value of this field from its text form.
            ///
            /// The text has to be a value of the field's type:
            /// `Produce` is a value of `api`, and `Prodcue` is an error instead of a selector that never matches.
            /// A pattern such as `orders.*` is not a value, and whoever reads a selector tells the two apart.
            ///
            /// # Errors
            ///
            /// The [`FieldError`] of the field's type, when `text` is not a value of it.
            pub fn parse_value(self, text: &str) -> Result<FieldValue, FieldError> {
                match self {
                    $(Field::$variant => Ok(FieldValue::$variant(text.parse::<$type>()?)),)*
                }
            }
        }

        #[cfg(feature = "serde")]
        impl FieldValue {
            pub(crate) fn serialize_bare<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $(FieldValue::$variant(value) => serde::Serialize::serialize(value, serializer),)*
                }
            }

            pub(crate) fn deserialize_bare<'de, D: serde::Deserializer<'de>>(
                field: Field,
                deserializer: D,
            ) -> Result<Self, D::Error> {
                match field {
                    $(
                        Field::$variant => {
                            <$type as serde::Deserialize>::deserialize(deserializer).map(FieldValue::$variant)
                        }
                    )*
                }
            }
        }
    };
}

field_values! {
    /// A value of `connection`.
    Connection(ConnectionNumber),
    /// A value of `address`.
    Address(ClientAddress),
    /// A value of `host`.
    Host(ClientHost),
    /// A value of `endpoint`.
    Endpoint(Endpoint),
    /// A value of `client.id`.
    ClientId(ClientId),
    /// A value of `client.software`.
    ClientSoftware(ClientSoftware),
    /// A value of `api`.
    Api(ApiKey),
    /// A value of `api.version`.
    ApiVersion(ApiVersion),
    /// A value of `topic`.
    Topic(TopicName),
    /// A value of `partition`.
    Partition(PartitionNumber),
    /// A value of `acks`.
    Acks(Acks),
    /// A value of `group`.
    Group(GroupId),
    /// A value of `member.id`.
    MemberId(MemberId),
    /// A value of `group.instance.id`.
    GroupInstanceId(GroupInstanceId),
    /// A value of `transactional.id`.
    TransactionalId(TransactionalId),
    /// A value of `outcome`.
    Outcome(Outcome),
    /// A value of `error.code`.
    ErrorCode(ErrorCode),
    /// A value of `latency`.
    Latency(Latency),
}

mod sealed {
    pub trait Sealed {}
}

/// A value of a field that a call has as a whole.
///
/// Every value type is one, except [`TopicName`] and [`PartitionNumber`]:
/// a topic and a partition belong to a [part](crate::Part) of the call.
/// The trait is sealed, because the fields are a closed list.
pub trait CallValue: Into<FieldValue> + sealed::Sealed {}

/// A value of a learned field, which the client context can remember:
/// [`GroupId`], [`MemberId`], [`GroupInstanceId`] and [`TransactionalId`].
///
/// The trait is sealed, because the fields are a closed list.
pub trait LearnedValue: CallValue {}

macro_rules! call_values {
    ($($type:ty),*) => {
        $(
            impl sealed::Sealed for $type {}
            impl CallValue for $type {}
        )*
    };
}

call_values!(
    ConnectionNumber,
    ClientAddress,
    ClientHost,
    Endpoint,
    ClientId,
    ClientSoftware,
    ApiKey,
    ApiVersion,
    Acks,
    GroupId,
    MemberId,
    GroupInstanceId,
    TransactionalId,
    Outcome,
    ErrorCode,
    Latency
);

impl LearnedValue for GroupId {}
impl LearnedValue for MemberId {}
impl LearnedValue for GroupInstanceId {}
impl LearnedValue for TransactionalId {}

/// Where the values of a field come from.
///
/// A selector matches a value whatever its origin.
/// An explanation and the UI show the origin,
/// because a match on a learned value depends on what the client did before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Origin {
    /// The call itself, or the session it runs on.
    Call,
    /// The client context, which remembers what another call of the same client carried.
    /// Only a learned field has this origin.
    ClientContext,
}

/// The values of one field on one call.
///
/// There is at least one value, every value is of the same field, and no value is there twice.
/// All of them have one [`Origin`]: a request that carries a learned field provides it alone,
/// and only a request that does not carry it gets the values of the client context.
///
/// A field that has no value is not an empty `FieldValues`.
/// It is [`Reading::Absent`](crate::Reading::Absent).
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// // A client that runs two consumers under one `client.id` has two groups.
/// let groups = [GroupId::new("payments")?, GroupId::new("analytics-etl")?];
/// let learned = FieldValues::learned(groups.clone()).expect("two values");
///
/// assert_eq!(learned.field(), Field::Group);
/// assert_eq!(learned.origin(), Origin::ClientContext);
/// assert!(learned.contains(&groups[0].clone().into()));
/// assert_eq!(learned.to_string(), "analytics-etl, payments");
///
/// assert_eq!(FieldValues::of(Vec::<GroupId>::new()), None);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldValues {
    field: Field,
    values: Values,
    origin: Origin,
}

/// One value needs no allocation, and most fields have one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Values {
    One(FieldValue),
    /// At least two values, in order and without duplicates.
    Many(Vec<FieldValue>),
}

impl FieldValues {
    /// The one value of a field.
    pub fn one(value: impl Into<FieldValue>) -> Self {
        let value = value.into();
        Self {
            field: value.field(),
            values: Values::One(value),
            origin: Origin::Call,
        }
    }

    /// The values of a field that the call or its session provides, or `None` when there is none.
    pub fn of<V: Into<FieldValue>>(values: impl IntoIterator<Item = V>) -> Option<Self> {
        Self::collect(values.into_iter().map(Into::into), Origin::Call)
    }

    /// The values of a learned field that the client context provides, or `None` when there is none.
    pub fn learned<V: LearnedValue>(values: impl IntoIterator<Item = V>) -> Option<Self> {
        Self::collect(values.into_iter().map(Into::into), Origin::ClientContext)
    }

    /// The caller passes values of one field.
    pub(crate) fn collect(values: impl Iterator<Item = FieldValue>, origin: Origin) -> Option<Self> {
        let mut values: Vec<FieldValue> = values.collect();
        values.sort();
        values.dedup();
        let field = values.first()?.field();
        debug_assert!(values.iter().all(|value| value.field() == field));
        let values = if values.len() > 1 {
            Values::Many(values)
        } else {
            Values::One(values.pop()?)
        };
        Some(Self { field, values, origin })
    }

    /// Adds a value of the same field.
    pub(crate) fn insert(&mut self, value: FieldValue) {
        debug_assert_eq!(value.field(), self.field);
        let values = self.iter().cloned().chain([value]);
        if let Some(Self { values, .. }) = Self::collect(values, self.origin) {
            self.values = values;
        }
    }

    /// The field these are values of.
    pub const fn field(&self) -> Field {
        self.field
    }

    /// Where the values come from.
    pub const fn origin(&self) -> Origin {
        self.origin
    }

    /// The values in order, each one once.
    pub fn iter(&self) -> std::slice::Iter<'_, FieldValue> {
        match &self.values {
            Values::One(value) => std::slice::from_ref(value).iter(),
            Values::Many(values) => values.iter(),
        }
    }

    /// Whether `value` is one of the values.
    pub fn contains(&self, value: &FieldValue) -> bool {
        self.iter().any(|own| own == value)
    }
}

impl<'a> IntoIterator for &'a FieldValues {
    type Item = &'a FieldValue;
    type IntoIter = std::slice::Iter<'a, FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Writes the values with a comma and a space between them.
impl fmt::Display for FieldValues {
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

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;

    use super::*;

    /// One value of every field, with its text form.
    fn examples() -> Vec<(FieldValue, &'static str)> {
        vec![
            (ConnectionNumber::new(7).into(), "#7"),
            (
                "10.0.4.27:51459".parse::<ClientAddress>().unwrap().into(),
                "10.0.4.27:51459",
            ),
            ("10.0.4.27".parse::<ClientHost>().unwrap().into(), "10.0.4.27"),
            (Endpoint::Broker(BrokerId::from(2)).into(), "broker 2"),
            (ClientId::new("order-ingest").unwrap().into(), "order-ingest"),
            (
                ClientSoftware::new("librdkafka", "2.6.1").unwrap().into(),
                "librdkafka 2.6.1",
            ),
            (ApiKey::PRODUCE.into(), "Produce"),
            (ApiVersion::from(11).into(), "11"),
            (TopicName::new("orders.eu").unwrap().into(), "orders.eu"),
            (PartitionNumber::from(3).into(), "3"),
            (Acks::ALL.into(), "-1"),
            (GroupId::new("analytics-etl").unwrap().into(), "analytics-etl"),
            (MemberId::new("consumer-1-4f2a").unwrap().into(), "consumer-1-4f2a"),
            (GroupInstanceId::new("etl-0").unwrap().into(), "etl-0"),
            (TransactionalId::new("payments-tx-1").unwrap().into(), "payments-tx-1"),
            (Outcome::Ok.into(), "ok"),
            (ErrorCode::NOT_LEADER_OR_FOLLOWER.into(), "NOT_LEADER_OR_FOLLOWER"),
            (Latency::new(TimeSpan::from_millis(14)).into(), "14ms"),
        ]
    }

    #[test]
    fn every_field_has_a_value_of_its_own_type() {
        let fields: Vec<Field> = examples().iter().map(|(value, _)| value.field()).collect();
        assert_eq!(fields, Field::ALL);
    }

    #[test]
    fn a_value_writes_the_text_form_of_its_type() {
        for (value, text) in examples() {
            assert_eq!(value.to_string(), text, "{}", value.field());
        }
    }

    #[test]
    fn a_field_reads_its_value_back_from_the_text_form() {
        for (value, text) in examples() {
            assert_eq!(value.field().parse_value(text), Ok(value.clone()), "{}", value.field());
            assert_eq!(value.field().parse_value(&value.to_string()), Ok(value));
        }
    }

    #[test]
    fn a_field_refuses_text_that_is_not_a_value_of_its_type() {
        let refused = [
            (Field::Connection, "seven"),
            (Field::Address, "10.0.4.27"),
            (Field::Host, "localhost"),
            (Field::Endpoint, "broker"),
            (Field::ClientSoftware, "librdkafka"),
            (Field::Api, "Prodcue"),
            (Field::ApiVersion, "-1"),
            (Field::Topic, "orders/eu"),
            (Field::Topic, "orders.*"),
            (Field::Partition, "-1"),
            (Field::Acks, "none"),
            (Field::Group, ""),
            (Field::MemberId, ""),
            (Field::GroupInstanceId, ""),
            (Field::TransactionalId, ""),
            (Field::Outcome, "delayed"),
            (Field::ErrorCode, "NONE"),
            (Field::ErrorCode, "NOT_LEADER"),
            (Field::Latency, "14"),
        ];

        for (field, text) in refused {
            assert!(field.parse_value(text).is_err(), "{field}: {text:?}");
        }
    }

    #[test]
    fn the_error_of_a_kernel_type_names_that_type() {
        assert_eq!(
            Field::Topic.parse_value("orders/eu"),
            Err(FieldError::Kernel(KernelError::InvalidCharacter {
                what: "topic name",
                character: '/',
                position: 6
            }))
        );
        assert_eq!(
            Field::Api.parse_value("Prodcue"),
            Err(FieldError::Kernel(KernelError::UnknownName {
                what: "API key",
                input: "Prodcue".to_owned()
            }))
        );
    }

    #[test]
    fn an_api_without_a_name_is_still_a_value() {
        assert_eq!(Field::Api.parse_value("4711"), Ok(FieldValue::Api(ApiKey::new(4711))));
        assert_eq!(FieldValue::from(ApiKey::new(4711)).to_string(), "4711");
        assert_eq!(Field::Api.parse_value("0"), Ok(FieldValue::Api(ApiKey::PRODUCE)));
    }

    #[test]
    fn values_of_the_same_text_are_values_of_different_fields() {
        let client = Field::ClientId.parse_value("payments").unwrap();
        let group = Field::Group.parse_value("payments").unwrap();

        assert_ne!(client, group);
        assert_eq!(client.to_string(), group.to_string());
    }

    #[test]
    fn one_value_comes_from_the_call() {
        let values = FieldValues::one(ApiKey::PRODUCE);

        assert_eq!(values.field(), Field::Api);
        assert_eq!(values.origin(), Origin::Call);
        assert_eq!(values.iter().collect::<Vec<_>>(), [&FieldValue::Api(ApiKey::PRODUCE)]);
        assert_eq!(values.to_string(), "Produce");
    }

    #[test]
    fn several_values_are_in_order_and_each_one_once() {
        let topic = |name: &str| TopicName::new(name).unwrap();
        let values =
            FieldValues::of([topic("payments.settled"), topic("orders.eu"), topic("payments.settled")]).unwrap();

        assert_eq!(values.field(), Field::Topic);
        assert_eq!(values.to_string(), "orders.eu, payments.settled");
        assert_eq!((&values).into_iter().count(), 2);
        assert!(values.contains(&topic("orders.eu").into()));
        assert!(!values.contains(&topic("orders.us").into()));
        assert_eq!(
            values,
            FieldValues::of([topic("orders.eu"), topic("payments.settled")]).unwrap()
        );
    }

    #[test]
    fn several_equal_values_are_one_value() {
        assert_eq!(
            FieldValues::of([ApiKey::FETCH, ApiKey::FETCH]),
            Some(FieldValues::one(ApiKey::FETCH))
        );
    }

    #[test]
    fn no_values_are_not_field_values() {
        assert_eq!(FieldValues::of(Vec::<TopicName>::new()), None);
        assert_eq!(FieldValues::learned(Vec::<GroupId>::new()), None);
    }

    #[test]
    fn learned_values_come_from_the_client_context() {
        let group = GroupId::new("analytics-etl").unwrap();
        let learned = FieldValues::learned([group.clone()]).unwrap();

        assert_eq!(learned.origin(), Origin::ClientContext);
        assert_eq!(learned.field(), Field::Group);
        assert_ne!(learned, FieldValues::one(group));
    }

    #[test]
    fn inserting_keeps_the_order_and_the_origin() {
        let mut values = FieldValues::learned([GroupId::new("payments").unwrap()]).unwrap();
        values.insert(GroupId::new("analytics-etl").unwrap().into());
        values.insert(GroupId::new("payments").unwrap().into());

        assert_eq!(values.to_string(), "analytics-etl, payments");
        assert_eq!(values.origin(), Origin::ClientContext);
    }
}
