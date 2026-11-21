//! The `serde` forms: a kind as its name, and an event as a map with an `event` key.
//!
//! An event, a definition, a consequence and an expanded event all write a flat map.
//! [`Fields`] holds every key such a map can have.
//! Reading takes the keys that belong to the value out of it, and whatever is left is an error.
//! A misspelt or misplaced key therefore never passes silently:
//! a definition without its `partition` key means the partition of the call, which is not what a typo intends.

use std::fmt;

use gaslightmq_kernel::prelude::*;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::{
    Capabilities, Consequence, Coordinator, Event, EventDefinition, EventError, EventKind,
    ExpandedEvent, PartitionCount, RecordCount, Support,
};

const EVENT: &str = "event";
const CONSEQUENCE: &str = "consequence";
const PARTITION: &str = "partition";
const TOPIC: &str = "topic";
const BROKER: &str = "broker";
const GROUP: &str = "group";
const TRANSACTIONAL_ID: &str = "transactional.id";
const TO: &str = "to";
const COUNT: &str = "count";
const FAILOVER: &str = "failover";
const CONSEQUENCES: &str = "consequences";

/// Every key, in the order a map writes them.
const KEYS: &[&str] = &[
    EVENT,
    CONSEQUENCE,
    PARTITION,
    TOPIC,
    BROKER,
    GROUP,
    TRANSACTIONAL_ID,
    TO,
    COUNT,
    FAILOVER,
    CONSEQUENCES,
];

const LEADERLESS: &str = "leaderless";
const COORDINATOR_UNAVAILABLE: &str = "coordinator-unavailable";

impl Serialize for EventKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for EventKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

impl Serialize for Support {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Support {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        const ALL: [Support; 3] = [Support::Applied, Support::Emulated, Support::Unsupported];
        const NAMES: &[&str] = &[ALL[0].name(), ALL[1].name(), ALL[2].name()];

        let name = String::deserialize(deserializer)?;
        ALL.into_iter()
            .find(|support| support.name() == name)
            .ok_or_else(|| de::Error::unknown_variant(&name, NAMES))
    }
}

/// A consequence that is not an event, as the `consequence` key names it.
#[derive(Clone, Copy)]
enum ConsequenceName {
    Leaderless,
    CoordinatorUnavailable,
}

impl ConsequenceName {
    const fn name(self) -> &'static str {
        match self {
            Self::Leaderless => LEADERLESS,
            Self::CoordinatorUnavailable => COORDINATOR_UNAVAILABLE,
        }
    }
}

impl Serialize for ConsequenceName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for ConsequenceName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        match name.as_str() {
            LEADERLESS => Ok(Self::Leaderless),
            COORDINATOR_UNAVAILABLE => Ok(Self::CoordinatorUnavailable),
            _ => Err(de::Error::unknown_variant(
                &name,
                &[LEADERLESS, COORDINATOR_UNAVAILABLE],
            )),
        }
    }
}

/// The keys of one map, each with its value when the map has it.
#[derive(Default)]
struct Fields {
    event: Option<EventKind>,
    consequence: Option<ConsequenceName>,
    partition: Option<TopicPartition>,
    topic: Option<TopicName>,
    broker: Option<BrokerId>,
    group: Option<GroupId>,
    transactional_id: Option<TransactionalId>,
    /// A broker ID or an offset, which only the kind of the event tells apart.
    to: Option<u64>,
    count: Option<u64>,
    failover: Option<bool>,
    consequences: Option<Vec<Consequence>>,
}

impl Fields {
    /// Every key with whether the map has it, in the order of [`KEYS`].
    fn present(&self) -> [(&'static str, bool); 11] {
        [
            (EVENT, self.event.is_some()),
            (CONSEQUENCE, self.consequence.is_some()),
            (PARTITION, self.partition.is_some()),
            (TOPIC, self.topic.is_some()),
            (BROKER, self.broker.is_some()),
            (GROUP, self.group.is_some()),
            (TRANSACTIONAL_ID, self.transactional_id.is_some()),
            (TO, self.to.is_some()),
            (COUNT, self.count.is_some()),
            (FAILOVER, self.failover.is_some()),
            (CONSEQUENCES, self.consequences.is_some()),
        ]
    }

    /// The fields of an event.
    fn of_event(event: &Event) -> Self {
        Self::from(&EventDefinition::from(event.clone()))
    }

    fn set_coordinator(&mut self, coordinator: Option<&Coordinator>) {
        match coordinator {
            Some(Coordinator::Group(group)) => self.group = Some(group.clone()),
            Some(Coordinator::Transaction(transactional_id)) => {
                self.transactional_id = Some(transactional_id.clone())
            }
            None => {}
        }
    }

    /// Take the coordinator that the `group` key or the `transactional.id` key names.
    fn take_coordinator<E: de::Error>(&mut self) -> Result<Option<Coordinator>, E> {
        match (self.group.take(), self.transactional_id.take()) {
            (Some(_), Some(_)) => Err(E::custom(format_args!(
                "a coordinator is the one of a `{GROUP}` or of a `{TRANSACTIONAL_ID}`, not of both"
            ))),
            (Some(group), None) => Ok(Some(Coordinator::Group(group))),
            (None, Some(transactional_id)) => Ok(Some(Coordinator::Transaction(transactional_id))),
            (None, None) => Ok(None),
        }
    }

    /// Take `to` as the broker something moves to.
    fn take_to_broker<E: de::Error>(&mut self) -> Result<Option<BrokerId>, E> {
        let Some(to) = self.to.take() else {
            return Ok(None);
        };
        let too_large = || KernelError::TooLarge {
            what: "broker ID",
            value: to,
            max: i32::MAX as u64,
        };
        let broker = u32::try_from(to)
            .map_err(|_| too_large())
            .and_then(BrokerId::new);
        broker.map(Some).map_err(E::custom)
    }

    /// Take `to` as the offset something moves to, which the event cannot do without.
    fn take_to_offset<E: de::Error>(&mut self) -> Result<Offset, E> {
        let to = self.to.take().ok_or_else(|| E::missing_field(TO))?;
        Offset::new(to).map_err(E::custom)
    }

    fn take_partition_count<E: de::Error>(&mut self) -> Result<PartitionCount, E> {
        let Some(count) = self.count.take() else {
            return Ok(PartitionCount::ONE);
        };
        let out_of_range = || EventError::CountOutOfRange {
            what: "partition count",
            value: count,
            max: PartitionCount::MAX.value().into(),
        };
        let count = u32::try_from(count)
            .map_err(|_| out_of_range())
            .and_then(PartitionCount::new);
        count.map_err(E::custom)
    }

    fn take_record_count<E: de::Error>(&mut self) -> Result<RecordCount, E> {
        let Some(count) = self.count.take() else {
            return Ok(RecordCount::ONE);
        };
        RecordCount::new(count).map_err(E::custom)
    }

    /// Fail when the map has a key that `owner` did not take.
    fn nothing_left<E: de::Error>(&self, owner: &str) -> Result<(), E> {
        match self.present().into_iter().find(|(_, present)| *present) {
            Some((key, _)) => Err(E::custom(format_args!(
                "`{key}` does not belong to {owner}"
            ))),
            None => Ok(()),
        }
    }

    /// Read the map as an event definition.
    fn into_definition<E: de::Error>(mut self) -> Result<EventDefinition, E> {
        let kind = self.event.take().ok_or_else(|| E::missing_field(EVENT))?;
        let definition = match kind {
            EventKind::LeaderMoves => EventDefinition::LeaderMoves {
                partition: self.partition.take(),
                to: self.take_to_broker()?,
            },
            EventKind::BrokerGoesDown => EventDefinition::BrokerGoesDown {
                broker: self.broker.take(),
                failover: self.failover.take().unwrap_or(false),
            },
            EventKind::BrokerGoesUnresponsive => EventDefinition::BrokerGoesUnresponsive {
                broker: self.broker.take(),
            },
            EventKind::BrokerComesBack => EventDefinition::BrokerComesBack {
                broker: self.broker.take(),
            },
            EventKind::Rebalance => EventDefinition::Rebalance {
                group: self.group.take(),
            },
            EventKind::CoordinatorMoves => EventDefinition::CoordinatorMoves {
                coordinator: self.take_coordinator()?,
                to: self.take_to_broker()?,
            },
            EventKind::PartitionAdded => EventDefinition::PartitionAdded {
                topic: self.topic.take(),
                count: self.take_partition_count()?,
            },
            EventKind::TopicDeleted => EventDefinition::TopicDeleted {
                topic: self.topic.take(),
            },
            EventKind::RecordsAppended => EventDefinition::RecordsAppended {
                partition: self.partition.take(),
                count: self.take_record_count()?,
            },
            EventKind::RecordsExpired => EventDefinition::RecordsExpired {
                partition: self.partition.take(),
                to: self.take_to_offset()?,
            },
            EventKind::LogTruncated => EventDefinition::LogTruncated {
                partition: self.partition.take(),
                to: self.take_to_offset()?,
            },
            EventKind::CommittedOffsetReset => EventDefinition::CommittedOffsetReset {
                group: self.group.take(),
                partition: self.partition.take(),
                to: self.take_to_offset()?,
            },
            EventKind::ProducerFenced => EventDefinition::ProducerFenced {
                transactional_id: self.transactional_id.take(),
            },
        };
        self.nothing_left(kind.name())?;
        Ok(definition)
    }

    /// Read the map as an event: a definition that names every subject.
    fn into_event<E: de::Error>(self) -> Result<Event, E> {
        Event::try_from(self.into_definition()?).map_err(E::custom)
    }
}

/// The fields a definition writes.
/// It leaves out what reading fills in: a subject it does not name, a count of one, and no failover.
impl From<&EventDefinition> for Fields {
    fn from(definition: &EventDefinition) -> Self {
        let mut fields = Self {
            event: Some(definition.kind()),
            ..Self::default()
        };
        let number = |broker: &Option<BrokerId>| broker.map(|broker| u64::from(broker.value()));
        match definition {
            EventDefinition::LeaderMoves { partition, to } => {
                fields.partition = partition.clone();
                fields.to = number(to);
            }
            EventDefinition::BrokerGoesDown { broker, failover } => {
                fields.broker = *broker;
                fields.failover = failover.then_some(true);
            }
            EventDefinition::BrokerGoesUnresponsive { broker }
            | EventDefinition::BrokerComesBack { broker } => {
                fields.broker = *broker;
            }
            EventDefinition::Rebalance { group } => fields.group = group.clone(),
            EventDefinition::CoordinatorMoves { coordinator, to } => {
                fields.set_coordinator(coordinator.as_ref());
                fields.to = number(to);
            }
            EventDefinition::PartitionAdded { topic, count } => {
                fields.topic = topic.clone();
                fields.count = (*count != PartitionCount::ONE).then(|| count.value().into());
            }
            EventDefinition::TopicDeleted { topic } => fields.topic = topic.clone(),
            EventDefinition::RecordsAppended { partition, count } => {
                fields.partition = partition.clone();
                fields.count = (*count != RecordCount::ONE).then(|| count.value());
            }
            EventDefinition::RecordsExpired { partition, to }
            | EventDefinition::LogTruncated { partition, to } => {
                fields.partition = partition.clone();
                fields.to = Some(to.value());
            }
            EventDefinition::CommittedOffsetReset {
                group,
                partition,
                to,
            } => {
                fields.group = group.clone();
                fields.partition = partition.clone();
                fields.to = Some(to.value());
            }
            EventDefinition::ProducerFenced { transactional_id } => {
                fields.transactional_id = transactional_id.clone();
            }
        }
        fields
    }
}

/// Writes a map with the keys that have a value.
impl Serialize for Fields {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn entry<M: SerializeMap, T: Serialize>(
            map: &mut M,
            key: &str,
            value: &Option<T>,
        ) -> Result<(), M::Error> {
            match value {
                Some(value) => map.serialize_entry(key, value),
                None => Ok(()),
            }
        }

        let len = self
            .present()
            .into_iter()
            .filter(|(_, present)| *present)
            .count();
        let mut map = serializer.serialize_map(Some(len))?;
        entry(&mut map, EVENT, &self.event)?;
        entry(&mut map, CONSEQUENCE, &self.consequence)?;
        entry(&mut map, PARTITION, &self.partition)?;
        entry(&mut map, TOPIC, &self.topic)?;
        entry(&mut map, BROKER, &self.broker)?;
        entry(&mut map, GROUP, &self.group)?;
        entry(&mut map, TRANSACTIONAL_ID, &self.transactional_id)?;
        entry(&mut map, TO, &self.to)?;
        entry(&mut map, COUNT, &self.count)?;
        entry(&mut map, FAILOVER, &self.failover)?;
        entry(&mut map, CONSEQUENCES, &self.consequences)?;
        map.end()
    }
}

/// Reads a map into its keys.
/// It rejects a key that no map of this crate has, and a key that comes twice.
struct FieldsVisitor;

impl<'de> Visitor<'de> for FieldsVisitor {
    type Value = Fields;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map with an `event` key")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
        fn set<'de, A: MapAccess<'de>, T: Deserialize<'de>>(
            map: &mut A,
            key: &'static str,
            slot: &mut Option<T>,
        ) -> Result<(), A::Error> {
            if slot.is_some() {
                return Err(de::Error::duplicate_field(key));
            }
            *slot = Some(map.next_value()?);
            Ok(())
        }

        let mut fields = Fields::default();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                EVENT => set(&mut map, EVENT, &mut fields.event)?,
                CONSEQUENCE => set(&mut map, CONSEQUENCE, &mut fields.consequence)?,
                PARTITION => set(&mut map, PARTITION, &mut fields.partition)?,
                TOPIC => set(&mut map, TOPIC, &mut fields.topic)?,
                BROKER => set(&mut map, BROKER, &mut fields.broker)?,
                GROUP => set(&mut map, GROUP, &mut fields.group)?,
                TRANSACTIONAL_ID => set(&mut map, TRANSACTIONAL_ID, &mut fields.transactional_id)?,
                TO => set(&mut map, TO, &mut fields.to)?,
                COUNT => set(&mut map, COUNT, &mut fields.count)?,
                FAILOVER => set(&mut map, FAILOVER, &mut fields.failover)?,
                CONSEQUENCES => set(&mut map, CONSEQUENCES, &mut fields.consequences)?,
                unknown => return Err(de::Error::unknown_field(unknown, KEYS)),
            }
        }
        Ok(fields)
    }
}

impl Serialize for EventDefinition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Fields::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EventDefinition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_map(FieldsVisitor)?
            .into_definition()
    }
}

/// Writes the form of a definition that names every subject.
impl Serialize for Event {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Fields::of_event(self).serialize(serializer)
    }
}

/// Reads the form of a definition, and rejects one that leaves a subject out.
impl<'de> Deserialize<'de> for Event {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(FieldsVisitor)?.into_event()
    }
}

/// Writes a map with a `consequence` key, or the map of the event that follows.
impl Serialize for Consequence {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = match self {
            Self::Leaderless { partition } => Fields {
                consequence: Some(ConsequenceName::Leaderless),
                partition: Some(partition.clone()),
                ..Fields::default()
            },
            Self::CoordinatorUnavailable { coordinator } => {
                let mut fields = Fields {
                    consequence: Some(ConsequenceName::CoordinatorUnavailable),
                    ..Fields::default()
                };
                fields.set_coordinator(Some(coordinator));
                fields
            }
            Self::Event(event) => Fields::of_event(event),
        };
        fields.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Consequence {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut fields = deserializer.deserialize_map(FieldsVisitor)?;
        let Some(consequence) = fields.consequence.take() else {
            return fields.into_event().map(Self::Event);
        };
        let consequence = match consequence {
            ConsequenceName::Leaderless => {
                let partition = fields
                    .partition
                    .take()
                    .ok_or_else(|| de::Error::missing_field(PARTITION))?;
                Self::Leaderless { partition }
            }
            ConsequenceName::CoordinatorUnavailable => {
                let coordinator = fields.take_coordinator()?.ok_or_else(|| {
                    de::Error::custom(format_args!(
                        "{COORDINATOR_UNAVAILABLE} needs a `{GROUP}` or a `{TRANSACTIONAL_ID}`"
                    ))
                })?;
                Self::CoordinatorUnavailable { coordinator }
            }
        };
        fields.nothing_left(consequence_name(&consequence))?;
        Ok(consequence)
    }
}

fn consequence_name(consequence: &Consequence) -> &'static str {
    match consequence {
        Consequence::Leaderless { .. } => LEADERLESS,
        Consequence::CoordinatorUnavailable { .. } => COORDINATOR_UNAVAILABLE,
        Consequence::Event(event) => event.kind().name(),
    }
}

/// Writes the map of the event, with a `consequences` key when the event has any.
impl Serialize for ExpandedEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut fields = Fields::of_event(&self.event);
        if !self.consequences.is_empty() {
            fields.consequences = Some(self.consequences.clone());
        }
        fields.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExpandedEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut fields = deserializer.deserialize_map(FieldsVisitor)?;
        let consequences = fields.consequences.take().unwrap_or_default();
        Ok(Self {
            event: fields.into_event()?,
            consequences,
        })
    }
}

/// Writes a map of every kind to what the backend does with it.
impl Serialize for Capabilities {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

/// Reads a map of kinds, in which a kind that is left out is unsupported.
impl<'de> Deserialize<'de> for Capabilities {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CapabilitiesVisitor;

        impl<'de> Visitor<'de> for CapabilitiesVisitor {
            type Value = Capabilities;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of event kinds to `applied`, `emulated` or `unsupported`")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Capabilities, A::Error> {
                let mut seen = Vec::new();
                let mut capabilities = Capabilities::none();
                while let Some((kind, support)) = map.next_entry::<EventKind, Support>()? {
                    if seen.contains(&kind) {
                        return Err(de::Error::custom(format_args!("duplicate kind `{kind}`")));
                    }
                    seen.push(kind);
                    capabilities = capabilities.declare([kind], support);
                }
                Ok(capabilities)
            }
        }

        deserializer.deserialize_map(CapabilitiesVisitor)
    }
}
