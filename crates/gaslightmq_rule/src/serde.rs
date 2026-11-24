//! The `serde` form of an action, of a reference to one, and of a state.
//!
//! An action is a flat map, and its `lie` or `incident` key says which of the two it is.
//! That key can stand anywhere in the map, so reading keeps the entries until it has seen all keys,
//! and then hands them to the definition that the key names.
//! These forms therefore need a format that describes itself, such as YAML or JSON.
//!
//! [`Rule`](crate::Rule) and [`Preset`](crate::Preset) derive their forms from the ones here.

use std::fmt;
use std::marker::PhantomData;

use gaslightmq_incident::IncidentDefinition;
use gaslightmq_lie::LieDefinition;
use serde::de::value::{MapAccessDeserializer, MapDeserializer, SeqDeserializer};
use serde::de::{self, Deserialize, Deserializer, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, Serializer};

use crate::{Action, ActionRef, PresetName, RuleState};

const LIE: &str = "lie";
const INCIDENT: &str = "incident";

/// Writes the map of the lie or of the incident.
impl Serialize for Action {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Action::Lie(lie) => lie.serialize(serializer),
            Action::Incident(incident) => incident.serialize(serializer),
        }
    }
}

/// Reads a map with a `lie` key as a lie, and a map with an `incident` key as an incident.
impl<'de> Deserialize<'de> for Action {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ActionMap)
    }
}

/// Writes the name of a preset as text, and an inline action as its map.
impl Serialize for ActionRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            ActionRef::Preset(name) => name.serialize(serializer),
            ActionRef::Inline(action) => action.serialize(serializer),
        }
    }
}

/// Reads text as the name of a preset, and a map as an inline action.
impl<'de> Deserialize<'de> for ActionRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NameOrAction;

        impl<'de> Visitor<'de> for NameOrAction {
            type Value = ActionRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the name of a preset, or a map with a `lie` or an `incident` key")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<ActionRef, E> {
                PresetName::new(text).map(ActionRef::Preset).map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<ActionRef, A::Error> {
                ActionMap.visit_map(map).map(ActionRef::Inline)
            }
        }

        deserializer.deserialize_any(NameOrAction)
    }
}

/// Writes the name of the state.
impl Serialize for RuleState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

/// Reads the name of a state.
impl<'de> Deserialize<'de> for RuleState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(de::Error::custom)
    }
}

/// Reads the map of an action: it keeps every entry, and lets the key that names the kind decide who reads them.
struct ActionMap;

impl<'de> Visitor<'de> for ActionMap {
    type Value = Action;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map with a `lie` or an `incident` key")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Action, A::Error> {
        let mut entries: Vec<(String, Kept)> = Vec::new();
        while let Some(entry) = map.next_entry()? {
            entries.push(entry);
        }

        let has = |key: &str| entries.iter().any(|(name, _)| name == key);
        let (is_lie, is_incident) = (has(LIE), has(INCIDENT));
        let entries: MapDeserializer<'de, _, A::Error> = MapDeserializer::new(entries.into_iter());
        match (is_lie, is_incident) {
            (true, false) => LieDefinition::deserialize(entries).map(Action::Lie),
            (false, true) => IncidentDefinition::deserialize(entries).map(Action::Incident),
            (true, true) => Err(de::Error::custom(
                "an action is a lie or an incident, and this one has both a `lie` and an `incident` key",
            )),
            (false, false) => Err(de::Error::custom("an action needs a `lie` or an `incident` key")),
        }
    }
}

/// A value of an entry, kept as the format gave it until the definition that owns the entry reads it.
#[derive(Debug, Clone, PartialEq)]
enum Kept {
    Nothing,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Text(String),
    List(Vec<Kept>),
    Map(Vec<(Kept, Kept)>),
}

impl<'de> Deserialize<'de> for Kept {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Keep;

        impl<'de> Visitor<'de> for Keep {
            type Value = Kept;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a value")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Kept, E> {
                Ok(Kept::Bool(value))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Kept, E> {
                Ok(Kept::Signed(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Kept, E> {
                Ok(Kept::Unsigned(value))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Kept, E> {
                Ok(Kept::Float(value))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Kept, E> {
                Ok(Kept::Text(value.to_owned()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Kept, E> {
                Ok(Kept::Text(value))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Kept, E> {
                Ok(Kept::Nothing)
            }

            fn visit_none<E: de::Error>(self) -> Result<Kept, E> {
                Ok(Kept::Nothing)
            }

            fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Kept, D::Error> {
                Kept::deserialize(deserializer)
            }

            fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<Kept, D::Error> {
                Kept::deserialize(deserializer)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Kept, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(Kept::List(items))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Kept, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(Kept::Map(entries))
            }
        }

        deserializer.deserialize_any(Keep)
    }
}

impl<E: de::Error> IntoDeserializer<'_, E> for Kept {
    type Deserializer = KeptDeserializer<E>;

    fn into_deserializer(self) -> KeptDeserializer<E> {
        KeptDeserializer {
            value: self,
            error: PhantomData,
        }
    }
}

/// Hands a kept value to whoever reads it, as the format would have.
struct KeptDeserializer<E> {
    value: Kept,
    error: PhantomData<E>,
}

impl<'de, E: de::Error> Deserializer<'de> for KeptDeserializer<E> {
    type Error = E;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.value {
            Kept::Nothing => visitor.visit_unit(),
            Kept::Bool(value) => visitor.visit_bool(value),
            Kept::Signed(value) => visitor.visit_i64(value),
            Kept::Unsigned(value) => visitor.visit_u64(value),
            Kept::Float(value) => visitor.visit_f64(value),
            Kept::Text(value) => visitor.visit_string(value),
            Kept::List(items) => visitor.visit_seq(SeqDeserializer::new(items.into_iter())),
            Kept::Map(entries) => visitor.visit_map(MapDeserializer::new(entries.into_iter())),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.value {
            Kept::Nothing => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, E> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        match self.value {
            Kept::Text(variant) => visitor.visit_enum(variant.into_deserializer()),
            Kept::Map(entries) => {
                let entries: MapDeserializer<'de, _, E> = MapDeserializer::new(entries.into_iter());
                visitor.visit_enum(MapAccessDeserializer::new(entries))
            }
            _ => self.deserialize_any(visitor),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit unit_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use serde_json::{Value, json};

    use super::*;
    use crate::{Preset, Rule};

    fn read<T: serde::de::DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).unwrap()
    }

    fn error<T: serde::de::DeserializeOwned + fmt::Debug>(value: Value) -> String {
        serde_json::from_value::<T>(value).unwrap_err().to_string()
    }

    fn write<T: Serialize>(value: &T) -> Value {
        serde_json::to_value(value).unwrap()
    }

    fn delay_1s() -> TimingLie {
        TimingLie::new()
            .with_delay(TimeSpan::from_secs(1))
            .with_jitter(TimeSpan::from_millis(200))
    }

    fn leader_moves(partition: Option<&str>) -> IncidentDefinition {
        IncidentDefinition::LeaderMoves {
            partition: partition.map(|text| text.parse().unwrap()),
            to: None,
        }
    }

    #[test]
    fn the_presets_of_the_specs_read_and_write_back() {
        let cases = [
            (
                json!({ "name": "delay-1s", "title": "Delay 1 s", "lie": "timing", "delay": "1s", "jitter": "200ms" }),
                Preset::new("delay-1s".parse().unwrap(), delay_1s()).with_title("Delay 1 s".parse().unwrap()),
            ),
            (
                json!({ "name": "lost-acknowledgement", "lie": "refusal", "error": "REQUEST_TIMED_OUT", "stage": "after" }),
                Preset::new(
                    "lost-acknowledgement".parse().unwrap(),
                    RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse),
                ),
            ),
            (
                json!({ "name": "leader-flap", "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" }),
                Preset::new(
                    "leader-flap".parse().unwrap(),
                    RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
                ),
            ),
            (
                json!({ "name": "leader-election", "incident": "leader-moves" }),
                Preset::new("leader-election".parse().unwrap(), leader_moves(None)),
            ),
        ];

        for (form, preset) in cases {
            assert_eq!(read::<Preset>(form.clone()), preset);
            assert_eq!(write(&preset), form);
        }
    }

    #[test]
    fn a_preset_reads_its_keys_in_any_order() {
        let preset: Preset =
            serde_json::from_str(r#"{ "jitter": "200ms", "delay": "1s", "lie": "timing", "name": "delay-1s" }"#)
                .unwrap();

        assert_eq!(preset, Preset::new("delay-1s".parse().unwrap(), delay_1s()));
    }

    #[test]
    fn a_preset_needs_a_name_and_an_action() {
        assert_eq!(error::<Preset>(json!({ "lie": "silence" })), "missing field `name`");
        assert_eq!(
            error::<Preset>(json!({ "name": "nothing" })),
            "an action needs a `lie` or an `incident` key"
        );
        assert_eq!(
            error::<Preset>(json!({ "name": "-delay", "lie": "silence" })),
            "preset name \"-delay\" starts or ends with a hyphen"
        );
    }

    #[test]
    fn a_key_that_does_not_belong_to_the_action_is_an_error() {
        assert_eq!(
            error::<Preset>(
                json!({ "name": "flap", "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER", "delay": "1s" })
            ),
            "`delay` does not belong to refusal"
        );
        assert!(
            error::<Preset>(json!({ "name": "flap", "lie": "silence", "colour": "red" }))
                .starts_with("unknown field `colour`")
        );
        assert!(
            error::<Preset>(json!({ "name": "election", "incident": "leader-moves", "partitoin": "orders.eu/3" }))
                .starts_with("unknown field `partitoin`")
        );
    }

    #[test]
    fn an_action_is_a_lie_or_an_incident_and_not_both() {
        assert_eq!(
            error::<Action>(json!({ "lie": "silence", "incident": "leader-moves" })),
            "an action is a lie or an incident, and this one has both a `lie` and an `incident` key"
        );
        assert_eq!(
            error::<Action>(json!({})),
            "an action needs a `lie` or an `incident` key"
        );
        assert!(error::<Action>(json!("silence")).starts_with("invalid type: string"));
    }

    #[test]
    fn an_action_keeps_the_values_of_every_type_for_its_definition() {
        let cases: [(Value, Action); 4] = [
            (
                json!({ "incident": "broker-goes-down", "broker": 2, "failover": true }),
                IncidentDefinition::BrokerGoesDown {
                    broker: Some(BrokerId::from(2)),
                    failover: true,
                }
                .into(),
            ),
            (
                json!({ "incident": "records-expired", "partition": "orders.eu/3", "to": 5000 }),
                IncidentDefinition::RecordsExpired {
                    partition: Some("orders.eu/3".parse().unwrap()),
                    to: Offset::from(5000),
                }
                .into(),
            ),
            (
                json!({ "incident": "coordinator-moves", "group": "payments" }),
                IncidentDefinition::CoordinatorMoves {
                    coordinator: Some(GroupId::new("payments").unwrap().into()),
                    to: None,
                }
                .into(),
            ),
            (
                json!({ "lie": "hang-up", "stage": "after" }),
                HangUpLie::new().at(Stage::AfterResponse).into(),
            ),
        ];

        for (form, action) in cases {
            assert_eq!(read::<Action>(form.clone()), action);
            assert_eq!(write(&action), form);
        }
    }

    #[test]
    fn the_error_of_the_definition_comes_through() {
        assert_eq!(
            error::<Action>(json!({ "lie": "timing", "jitter": "200ms" })),
            "a jitter of 200ms is longer than the delay of 0s"
        );
        assert_eq!(
            error::<Action>(json!({ "lie": "topology", "leader": "previous" })),
            "unknown field `leader`, expected one of `lie`, `error`, `delay`, `jitter`, `throttle`, `stage`"
        );
        assert_eq!(error::<Action>(json!({ "lie": "refusal" })), "missing field `error`");
    }

    #[test]
    fn a_reference_is_the_name_of_a_preset_or_the_map_of_an_action() {
        let named = ActionRef::Preset("delay-1s".parse().unwrap());
        let inline = ActionRef::from(leader_moves(Some("orders.eu/3")));

        assert_eq!(read::<ActionRef>(json!("delay-1s")), named);
        assert_eq!(write(&named), json!("delay-1s"));

        let form = json!({ "incident": "leader-moves", "partition": "orders.eu/3" });
        assert_eq!(read::<ActionRef>(form.clone()), inline);
        assert_eq!(write(&inline), form);

        assert_eq!(error::<ActionRef>(json!("")), "preset name \"\" is empty");
        assert!(error::<ActionRef>(json!(3)).contains("the name of a preset, or a map"));
    }

    /// The rule of rules.md, with JSON for its YAML.
    fn slow_down_form() -> Value {
        json!({
            "name": "slow-order-producers",
            "title": "Slow down order producers",
            "selector": "order-producers",
            "scope": { "client.id": "order-ingest" },
            "action": "delay-1s",
            "trigger": { "chance": "40%" },
            "lifetime": "until disarmed",
        })
    }

    fn slow_down() -> Rule {
        Rule::new(
            "slow-order-producers".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            "delay-1s".parse::<PresetName>().unwrap(),
        )
        .with_title("Slow down order producers".parse().unwrap())
        .scoped(Scope::client(ClientId::new("order-ingest").unwrap()))
        .with_trigger(TriggerParts::new().with_chance("40%".parse().unwrap()))
    }

    #[test]
    fn the_rule_of_the_spec_reads() {
        assert_eq!(read::<Rule>(slow_down_form()), slow_down());
    }

    #[test]
    fn a_rule_writes_what_it_does_not_have_by_default() {
        let mut form = slow_down_form();
        form.as_object_mut().unwrap().remove("lifetime");

        assert_eq!(write(&slow_down()), form);
        assert_eq!(read::<Rule>(form), slow_down());
    }

    #[test]
    fn the_rules_of_the_leader_election_scenario_read_and_write_back() {
        let moves_away = Rule::new(
            "leader-moves-away".parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            leader_moves(Some("orders.eu/3")),
        )
        .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1).unwrap()));
        let moves_away_form = json!({
            "name": "leader-moves-away",
            "selector": "order-producers",
            "action": { "incident": "leader-moves", "partition": "orders.eu/3" },
            "trigger": { "nth": 1 },
        });

        let stale = Rule::new(
            "stale-metadata".parse().unwrap(),
            Conditions::from(Condition::is(ApiKey::METADATA)),
            LieDefinition::Silence,
        )
        .with_trigger(
            TriggerParts::new()
                .with_first(Count::new(2).unwrap())
                .with_per(Per::Session),
        );
        let stale_form = json!({
            "name": "stale-metadata",
            "selector": { "api": "Metadata" },
            "action": { "lie": "silence" },
            "trigger": { "first": 2, "per": "session" },
        });

        for (form, rule) in [(moves_away_form, moves_away), (stale_form, stale)] {
            assert_eq!(read::<Rule>(form.clone()), rule);
            assert_eq!(write(&rule), form);
        }
    }

    #[test]
    fn a_rule_writes_every_part_it_has() {
        let rule = Rule::new(
            "refuse-orders".parse().unwrap(),
            Conditions::from(Condition::parse(Field::Topic, "orders.*").unwrap()),
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        )
        .with_trigger(Trigger::Manual)
        .with_lifetime(Lifetime::Firings(Count::new(3).unwrap()))
        .with_dice(DiceKey::new("refuse-the-orders").unwrap());
        let form = json!({
            "name": "refuse-orders",
            "selector": { "topic": "orders.*" },
            "action": { "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" },
            "trigger": "manual",
            "lifetime": "3 firings",
            "dice": "refuse-the-orders",
        });

        assert_eq!(write(&rule), form);
        assert_eq!(read::<Rule>(form), rule);
    }

    #[test]
    fn a_rule_needs_a_name_a_selector_and_an_action() {
        let without = |key: &str| {
            let mut form = slow_down_form();
            form.as_object_mut().unwrap().remove(key);
            error::<Rule>(form)
        };

        assert_eq!(without("name"), "missing field `name`");
        assert_eq!(without("selector"), "missing field `selector`");
        assert_eq!(without("action"), "missing field `action`");
    }

    #[test]
    fn a_selector_without_conditions_is_every_call() {
        let rule: Rule = read(json!({ "name": "blackhole", "selector": {}, "action": { "lie": "silence" } }));

        assert_eq!(rule.selector(), &SelectorRef::Inline(Conditions::new()));
    }

    #[test]
    fn a_rule_rejects_what_it_does_not_know() {
        let with = |key: &str, value: Value| {
            let mut form = slow_down_form();
            form.as_object_mut().unwrap().insert(key.to_owned(), value);
            error::<Rule>(form)
        };

        assert!(with("selecter", json!("order-producers")).starts_with("unknown field `selecter`"));
        assert_eq!(
            with("scope", json!({ "topic": "orders.eu" })),
            "a scope only tests connection fields, and topic is a request field"
        );
        assert_eq!(
            with("trigger", json!({ "chance": 40 })),
            "invalid type: integer `40`, expected a percentage such as `40%`"
        );
        assert!(with("lifetime", json!("a while")).contains("a while"));
        assert_eq!(with("name", json!("")), "rule name \"\" is empty");
    }

    #[test]
    fn a_rule_writes_its_title_when_it_has_one() {
        let blackhole = Rule::new(
            "blackhole-metadata".parse().unwrap(),
            Conditions::from(Condition::is(ApiKey::METADATA)),
            LieDefinition::Silence,
        );
        let titled = blackhole.clone().with_title("No answer to Metadata".parse().unwrap());
        let form = json!({
            "name": "blackhole-metadata",
            "selector": { "api": "Metadata" },
            "action": { "lie": "silence" },
        });
        let titled_form = json!({
            "name": "blackhole-metadata",
            "title": "No answer to Metadata",
            "selector": { "api": "Metadata" },
            "action": { "lie": "silence" },
        });

        assert_eq!(write(&blackhole), form);
        assert_eq!(read::<Rule>(form), blackhole);
        assert_eq!(write(&titled), titled_form);
        assert_eq!(read::<Rule>(titled_form), titled);
    }

    #[test]
    fn a_preset_writes_its_title_when_it_has_one() {
        let blackhole = Preset::new("blackhole".parse().unwrap(), LieDefinition::Silence);
        let titled = blackhole.clone().with_title("No answer at all".parse().unwrap());
        let form = json!({ "name": "blackhole", "lie": "silence" });
        let titled_form = json!({ "name": "blackhole", "title": "No answer at all", "lie": "silence" });

        assert_eq!(write(&blackhole), form);
        assert_eq!(read::<Preset>(form), blackhole);
        assert_eq!(write(&titled), titled_form);
        assert_eq!(read::<Preset>(titled_form), titled);
    }

    #[test]
    fn a_title_that_breaks_the_rule_for_titles_does_not_read() {
        assert_eq!(
            error::<Rule>(json!({ "name": "blackhole", "title": "", "selector": {}, "action": { "lie": "silence" } })),
            "rule title \"\" is empty"
        );
        assert_eq!(
            error::<Preset>(json!({ "name": "blackhole", "title": "No answer ", "lie": "silence" })),
            "preset title \"No answer \" starts or ends with white space"
        );
        // A title is free text, and a name is not.
        assert_eq!(
            error::<Preset>(json!({ "name": "No answer at all", "lie": "silence" })),
            "preset name \"No answer at all\" holds 'N' at position 0, which is no lowercase letter, digit or hyphen"
        );
    }

    #[test]
    fn a_state_is_its_name() {
        for state in RuleState::ALL {
            assert_eq!(write(&state), json!(state.name()));
            assert_eq!(read::<RuleState>(json!(state.name())), state);
        }
        assert_eq!(error::<RuleState>(json!("Armed")), "no state is called \"Armed\"");
    }

    #[test]
    fn a_kept_value_reads_back_as_the_format_gave_it() {
        let form = json!({
            "nothing": null,
            "yes": true,
            "below": -3,
            "above": 3,
            "half": 0.5,
            "text": "orders.eu",
            "list": [1, "two", [3]],
            "map": { "inner": { "deep": 1 } },
        });

        let kept: Kept = read(form.clone());
        let back = Value::deserialize(IntoDeserializer::<serde_json::Error>::into_deserializer(kept)).unwrap();

        assert_eq!(back, form);
    }

    #[test]
    fn a_kept_value_reads_as_an_option_and_as_an_enum() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Shape {
            Call,
            Parts(Vec<String>),
        }

        fn from<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, serde_json::Error> {
            T::deserialize(read::<Kept>(value).into_deserializer())
        }

        assert_eq!(from::<Option<u64>>(json!(null)).unwrap(), None);
        assert_eq!(from::<Option<u64>>(json!(3)).unwrap(), Some(3));
        assert_eq!(from::<Shape>(json!("call")).unwrap(), Shape::Call);
        assert_eq!(
            from::<Shape>(json!({ "parts": ["orders.eu/3"] })).unwrap(),
            Shape::Parts(vec!["orders.eu/3".into()])
        );
        assert!(from::<Shape>(json!(3)).is_err());
    }
}
