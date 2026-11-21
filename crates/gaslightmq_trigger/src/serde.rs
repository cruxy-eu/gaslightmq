//! The `serde` form of every definition type.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use gaslightmq_clock::TimeSpan;
use serde::de::value::MapAccessDeserializer;
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Chance, Count, DiceKey, Lifetime, MatchNumber, Per, Seed, TimeWindow, Trigger, TriggerParts};

/// Reads a type from its text form, and says what text it expects when it finds something else.
struct Text<T> {
    expected: &'static str,
    of: PhantomData<T>,
}

impl<T> Text<T> {
    fn expecting(expected: &'static str) -> Text<T> {
        Text {
            expected,
            of: PhantomData,
        }
    }
}

impl<T> Visitor<'_> for Text<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.expected)
    }

    fn visit_str<E: Error>(self, text: &str) -> Result<T, E> {
        text.parse().map_err(E::custom)
    }
}

/// Give a type the `serde` form of its text.
macro_rules! text_form {
    ($name:ident, $expected:literal) => {
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<$name, D::Error> {
                deserializer.deserialize_str(Text::expecting($expected))
            }
        }
    };
}

text_form!(Chance, "a percentage such as `40%`");
text_form!(Per, "`rule` or `session`");
text_form!(DiceKey, "a dice key");
text_form!(
    Lifetime,
    "`until disarmed`, a time span such as `30s`, or a count such as `100 matches` or `3 firings`"
);

/// Give a number that is at least one the `serde` form of a number.
macro_rules! number_form {
    ($name:ident) => {
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_u64(self.value())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<$name, D::Error> {
                $name::new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
            }
        }
    };
}

number_form!(Count);
number_form!(MatchNumber);

impl Serialize for Seed {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.value())
    }
}

impl<'de> Deserialize<'de> for Seed {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Seed, D::Error> {
        u64::deserialize(deserializer).map(Seed::new)
    }
}

/// The keys of the map that trigger parts write.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Key {
    Chance,
    Nth,
    Every,
    First,
    Skip,
    After,
    Between,
    Per,
}

impl Key {
    const NAMES: &'static [&'static str] = &["chance", "nth", "every", "first", "skip", "after", "between", "per"];

    fn name(self) -> &'static str {
        Key::NAMES[self as usize]
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Key, D::Error> {
        struct Name;

        impl Visitor<'_> for Name {
            type Value = Key;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a trigger part")
            }

            fn visit_str<E: Error>(self, name: &str) -> Result<Key, E> {
                match name {
                    "chance" => Ok(Key::Chance),
                    "nth" => Ok(Key::Nth),
                    "every" => Ok(Key::Every),
                    "first" => Ok(Key::First),
                    "skip" => Ok(Key::Skip),
                    "after" => Ok(Key::After),
                    "between" => Ok(Key::Between),
                    "per" => Ok(Key::Per),
                    _ => Err(E::unknown_field(name, Key::NAMES)),
                }
            }
        }

        deserializer.deserialize_identifier(Name)
    }
}

/// Writes a map with only the parts the trigger has, such as `{ chance: 40%, nth: 3, per: session }`.
///
/// A window writes `after: 10s` or `between: [10s, 40s]`.
/// The map leaves `per` out when the rule counts per rule.
impl Serialize for TriggerParts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let per = (self.per() != Per::Rule).then_some(self.per());
        let entries = [
            self.chance().is_some(),
            self.nth().is_some(),
            self.every().is_some(),
            self.first().is_some(),
            self.skip().is_some(),
            self.window().is_some(),
            per.is_some(),
        ];
        let mut map = serializer.serialize_map(Some(entries.into_iter().filter(|&present| present).count()))?;
        if let Some(chance) = self.chance() {
            map.serialize_entry(Key::Chance.name(), &chance)?;
        }
        if let Some(nth) = self.nth() {
            map.serialize_entry(Key::Nth.name(), &nth)?;
        }
        if let Some(every) = self.every() {
            map.serialize_entry(Key::Every.name(), &every)?;
        }
        if let Some(first) = self.first() {
            map.serialize_entry(Key::First.name(), &first)?;
        }
        if let Some(skip) = self.skip() {
            map.serialize_entry(Key::Skip.name(), &skip)?;
        }
        if let Some(window) = self.window() {
            match window.closes_after() {
                None => map.serialize_entry(Key::After.name(), &window.opens_after())?,
                Some(closes) => map.serialize_entry(Key::Between.name(), &[window.opens_after(), closes])?,
            }
        }
        if let Some(per) = per {
            map.serialize_entry(Key::Per.name(), &per)?;
        }
        map.end()
    }
}

/// Reads a map of trigger parts, and rejects an unknown part, a part named twice, and two time windows.
impl<'de> Deserialize<'de> for TriggerParts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<TriggerParts, D::Error> {
        struct Parts;

        impl<'de> Visitor<'de> for Parts {
            type Value = TriggerParts;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of trigger parts")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<TriggerParts, A::Error> {
                let mut parts = TriggerParts::new();
                let mut seen = Vec::new();
                while let Some(key) = map.next_key::<Key>()? {
                    if seen.contains(&key) {
                        return Err(A::Error::duplicate_field(key.name()));
                    }
                    let other_window = match key {
                        Key::After => Some(Key::Between),
                        Key::Between => Some(Key::After),
                        _ => None,
                    };
                    if other_window.is_some_and(|other| seen.contains(&other)) {
                        return Err(A::Error::custom(
                            "a trigger has one time window: `after` or `between`, not both",
                        ));
                    }
                    seen.push(key);

                    parts = match key {
                        Key::Chance => parts.with_chance(map.next_value()?),
                        Key::Nth => parts.with_nth(map.next_value()?),
                        Key::Every => parts.with_every(map.next_value()?),
                        Key::First => parts.with_first(map.next_value()?),
                        Key::Skip => parts.with_skip(map.next_value()?),
                        Key::After => parts.with_window(TimeWindow::after(map.next_value()?)),
                        Key::Between => {
                            let [from, until]: [TimeSpan; 2] = map.next_value()?;
                            parts.with_window(TimeWindow::between(from, until).map_err(A::Error::custom)?)
                        }
                        Key::Per => parts.with_per(map.next_value()?),
                    };
                }
                Ok(parts)
            }
        }

        deserializer.deserialize_map(Parts)
    }
}

/// Writes the text `manual`, or the map of the parts.
impl Serialize for Trigger {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Trigger::Parts(parts) => parts.serialize(serializer),
            Trigger::Manual => serializer.serialize_str(MANUAL),
        }
    }
}

/// The text form of a manual trigger.
const MANUAL: &str = "manual";

/// Reads the text `manual`, or the map of the parts.
///
/// The two forms have different shapes, so the format has to say which one it holds.
/// YAML and JSON do.
impl<'de> Deserialize<'de> for Trigger {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Trigger, D::Error> {
        struct ManualOrParts;

        impl<'de> Visitor<'de> for ManualOrParts {
            type Value = Trigger;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`manual` or a map of trigger parts")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<Trigger, E> {
                if text == MANUAL {
                    Ok(Trigger::Manual)
                } else {
                    Err(E::invalid_value(Unexpected::Str(text), &self))
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Trigger, A::Error> {
                TriggerParts::deserialize(MapAccessDeserializer::new(map)).map(Trigger::Parts)
            }
        }

        deserializer.deserialize_any(ManualOrParts)
    }
}
