//! The `serde` forms: a kind, a stage, a class and an aspect as their names, and a lie as a map with a `lie` key.
//!
//! A definition and a targeted lie both write a flat map.
//! [`Fields`] holds every key such a map can have.
//! Reading takes the keys that belong to the lie out of it, and whatever is left is an error.
//! A misspelt or misplaced key therefore never passes silently:
//! a refusal without its `stage` key acts before forwarding, which is not what a typo intends.
//!
//! Reading also rejects a value that no lie can hold: a jitter longer than the delay,
//! a throttle longer than Kafka can tell a client, and a target without a part.
//! The methods that build a lie in code stop such a value at the longest one that holds, and reading does not:
//! whoever wrote `jitter: 200ms` without a delay meant something else, and gets to hear it.
//!
//! Reading does not reject a lie that does nothing.
//! `{ lie: timing }` is what [`TimingLie::new`] writes, and every value that writes also reads back.
//! Such a lie harms no call, and a verdict still lists it, as `+0 s`.

use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::{Aspect, CombinationClass, HangUpLie, LieDefinition, LieKind, RefusalLie, Stage, TargetedLie, TimingLie};

const LIE: &str = "lie";
const ERROR: &str = "error";
const DELAY: &str = "delay";
const JITTER: &str = "jitter";
const THROTTLE: &str = "throttle";
const STAGE: &str = "stage";
const TARGET: &str = "target";

/// The keys of a definition, in the order a map writes them.
const DEFINITION_KEYS: &[&str] = &[LIE, ERROR, DELAY, JITTER, THROTTLE, STAGE];

/// The keys of a targeted lie, in the order a map writes them.
const TARGETED_KEYS: &[&str] = &[LIE, ERROR, DELAY, JITTER, THROTTLE, STAGE, TARGET];

/// Map a type with a `name` and a `FromStr` to that name.
macro_rules! name_serde {
    ($($type:ident)*) => {
        $(
            impl Serialize for $type {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serializer.serialize_str(self.name())
                }
            }

            impl<'de> Deserialize<'de> for $type {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    String::deserialize(deserializer)?.parse().map_err(de::Error::custom)
                }
            }
        )*
    };
}

name_serde!(LieKind Stage CombinationClass Aspect);

/// The keys of one map, each with its value when the map has it.
#[derive(Default)]
struct Fields {
    lie: Option<LieKind>,
    error: Option<ErrorCode>,
    delay: Option<TimeSpan>,
    jitter: Option<TimeSpan>,
    throttle: Option<TimeSpan>,
    stage: Option<Stage>,
    target: Option<Target>,
}

impl Fields {
    /// Every key with whether the map has it, in the order of [`TARGETED_KEYS`].
    fn present(&self) -> [(&'static str, bool); 7] {
        [
            (LIE, self.lie.is_some()),
            (ERROR, self.error.is_some()),
            (DELAY, self.delay.is_some()),
            (JITTER, self.jitter.is_some()),
            (THROTTLE, self.throttle.is_some()),
            (STAGE, self.stage.is_some()),
            (TARGET, self.target.is_some()),
        ]
    }

    /// Fail when the map has a key that `owner` did not take.
    fn nothing_left<E: de::Error>(&self, owner: LieKind) -> Result<(), E> {
        match self.present().into_iter().find(|(_, present)| *present) {
            Some((key, _)) => Err(E::custom(format_args!("`{key}` does not belong to {owner}"))),
            None => Ok(()),
        }
    }

    /// Take the stage of a lie of `kind`: the one the map names, or the default one of the kind.
    fn take_stage<E: de::Error>(&mut self, kind: LieKind) -> Result<Stage, E> {
        kind.stage_for(self.stage.take()).map_err(E::custom)
    }

    /// Read the map as a definition.
    fn into_definition<E: de::Error>(mut self) -> Result<LieDefinition, E> {
        let kind = self.lie.take().ok_or_else(|| E::missing_field(LIE))?;
        let definition = match kind {
            // A kind without a definition says so before anything else about the map is wrong.
            // Its keys are not known yet, so no other reason would be true.
            LieKind::Topology | LieKind::Data | LieKind::Progress | LieKind::Configuration => {
                return Err(E::custom(format_args!("{kind} has no definition yet")));
            }
            LieKind::Refusal => {
                let stage = self.take_stage(kind)?;
                let error = self.error.take().ok_or_else(|| E::missing_field(ERROR))?;
                LieDefinition::Refusal(RefusalLie { error, stage })
            }
            LieKind::Timing => {
                let stage = self.take_stage(kind)?;
                let delay = self.delay.take().unwrap_or_default();
                let jitter = self.jitter.take().unwrap_or_default();
                let throttle = self.throttle.take().unwrap_or_default();
                TimingLie::checked(delay, jitter, throttle, stage)
                    .map(LieDefinition::Timing)
                    .map_err(E::custom)?
            }
            LieKind::Silence => {
                // Silence names no stage, and a map may still name the only one it has.
                self.take_stage(kind)?;
                LieDefinition::Silence
            }
            LieKind::HangUp => LieDefinition::HangUp(HangUpLie {
                stage: self.take_stage(kind)?,
            }),
        };
        self.nothing_left(kind)?;
        Ok(definition)
    }

    /// Read the map as a lie with its target, which is the whole call when the map names none.
    ///
    /// The target has at least one part, because [`Target`] reads no other one.
    fn into_targeted<E: de::Error>(mut self) -> Result<TargetedLie, E> {
        let target = self.target.take().unwrap_or(Target::Call);
        let lie = self.into_definition()?;
        if lie.kind().targets_whole_call() && !target.is_call() {
            return Err(E::custom(format_args!("{} always targets the whole call", lie.kind())));
        }
        Ok(TargetedLie { lie, target })
    }
}

/// The fields a definition writes.
/// It leaves out what reading fills in: the default stage of the kind, and a time span of zero.
impl From<&LieDefinition> for Fields {
    fn from(definition: &LieDefinition) -> Self {
        let kind = definition.kind();
        let stage = definition.stage();
        let mut fields = Self {
            lie: Some(kind),
            stage: (stage != kind.default_stage()).then_some(stage),
            ..Self::default()
        };
        let unless_zero = |span: TimeSpan| (span != TimeSpan::ZERO).then_some(span);
        match definition {
            LieDefinition::Refusal(lie) => fields.error = Some(lie.error),
            LieDefinition::Timing(lie) => {
                fields.delay = unless_zero(lie.delay());
                fields.jitter = unless_zero(lie.jitter());
                fields.throttle = unless_zero(lie.throttle());
            }
            LieDefinition::Silence | LieDefinition::HangUp(_) => {}
        }
        fields
    }
}

/// The fields a targeted lie writes: those of its definition, and the target unless it is the whole call.
impl From<&TargetedLie> for Fields {
    fn from(targeted: &TargetedLie) -> Self {
        let mut fields = Self::from(targeted.lie());
        if !targeted.target().is_call() {
            fields.target = Some(targeted.target().clone());
        }
        fields
    }
}

/// Writes a map with the keys that have a value.
impl Serialize for Fields {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn entry<M: SerializeMap, T: Serialize>(map: &mut M, key: &str, value: Option<&T>) -> Result<(), M::Error> {
            match value {
                Some(value) => map.serialize_entry(key, value),
                None => Ok(()),
            }
        }

        let len = self.present().into_iter().filter(|(_, present)| *present).count();
        let mut map = serializer.serialize_map(Some(len))?;
        entry(&mut map, LIE, self.lie.as_ref())?;
        entry(&mut map, ERROR, self.error.as_ref())?;
        entry(&mut map, DELAY, self.delay.as_ref())?;
        entry(&mut map, JITTER, self.jitter.as_ref())?;
        entry(&mut map, THROTTLE, self.throttle.as_ref())?;
        entry(&mut map, STAGE, self.stage.as_ref())?;
        entry(&mut map, TARGET, self.target.as_ref())?;
        map.end()
    }
}

/// Reads a map into its keys.
/// It rejects a key that is not one of `keys`, and a key that comes twice.
/// Only the `target` key is not in every map.
struct FieldsVisitor {
    keys: &'static [&'static str],
}

impl<'de> Visitor<'de> for FieldsVisitor {
    type Value = Fields;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map with a `lie` key")
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
                LIE => set(&mut map, LIE, &mut fields.lie)?,
                ERROR => set(&mut map, ERROR, &mut fields.error)?,
                DELAY => set(&mut map, DELAY, &mut fields.delay)?,
                JITTER => set(&mut map, JITTER, &mut fields.jitter)?,
                THROTTLE => set(&mut map, THROTTLE, &mut fields.throttle)?,
                STAGE => set(&mut map, STAGE, &mut fields.stage)?,
                TARGET if self.keys.contains(&TARGET) => set(&mut map, TARGET, &mut fields.target)?,
                unknown => return Err(de::Error::unknown_field(unknown, self.keys)),
            }
        }
        Ok(fields)
    }
}

/// Writes a map with the `lie` key and the parameters.
impl Serialize for LieDefinition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Fields::from(self).serialize(serializer)
    }
}

/// Reads a map with the `lie` key and the parameters, and rejects every other key.
impl<'de> Deserialize<'de> for LieDefinition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let visitor = FieldsVisitor { keys: DEFINITION_KEYS };
        deserializer.deserialize_map(visitor)?.into_definition()
    }
}

/// Writes the map of the definition, with a `target` key unless the target is the whole call.
impl Serialize for TargetedLie {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Fields::from(self).serialize(serializer)
    }
}

/// Reads the map of a definition with an optional `target` key.
///
/// It rejects a target without a part.
/// It also rejects parts as the target of a lie that always targets the whole call.
/// [`TargetedLie::new`] gives such a lie the whole call instead, because it takes what a selector matched.
/// A map holds what the lie changed, and for such a lie that is never a part.
impl<'de> Deserialize<'de> for TargetedLie {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let visitor = FieldsVisitor { keys: TARGETED_KEYS };
        deserializer.deserialize_map(visitor)?.into_targeted()
    }
}
