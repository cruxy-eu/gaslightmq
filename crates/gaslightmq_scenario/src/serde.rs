//! The `serde` forms that are not derived where their type stands.
//!
//! A rule of a scenario is its name or a map, so reading needs a format that describes itself, such as YAML or JSON.

use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_rule::{Rule, RuleName};
use gaslightmq_selector::SelectorRef;
use gaslightmq_trigger::{Count, Seed};
use serde::de::value::MapAccessDeserializer;
use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::{Mode, Scenario, ScenarioName, ScenarioRule, ScenarioTitle, Step, Until};

/// A scenario as a file writes it, before anyone checked that its parts fit.
///
/// The mode is a word, and the steps of a Sequence stand next to it.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScenarioMap {
    name: ScenarioName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<ScenarioTitle>,
    mode: ModeWord,
    seed: Seed,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rules: Vec<ScenarioRule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    steps: Option<Vec<Step>>,
}

/// The word for a mode, as the key `mode` writes it.
#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ModeWord {
    Live,
    Chain,
    Sequence,
}

impl From<Scenario> for ScenarioMap {
    fn from(scenario: Scenario) -> Self {
        let (name, title, mode, seed, rules) = scenario.into_parts();
        let (mode, steps) = match mode {
            Mode::Live => (ModeWord::Live, None),
            Mode::Chain => (ModeWord::Chain, None),
            Mode::Sequence(steps) => (ModeWord::Sequence, Some(steps)),
        };
        Self {
            name,
            title,
            mode,
            seed,
            rules,
            steps,
        }
    }
}

/// Reads the map of a scenario, and checks that its parts fit.
///
/// Steps belong to a Sequence alone, so a Live or a Chain scenario with a key `steps` is an error,
/// even when it lists none.
impl<'de> Deserialize<'de> for Scenario {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = ScenarioMap::deserialize(deserializer)?;
        let mode = match (map.mode, map.steps) {
            (ModeWord::Sequence, steps) => Mode::Sequence(steps.unwrap_or_default()),
            (ModeWord::Live, None) => Mode::Live,
            (ModeWord::Chain, None) => Mode::Chain,
            (ModeWord::Live | ModeWord::Chain, Some(_)) => {
                return Err(de::Error::custom(format!(
                    "scenario {:?} has steps, which only a scenario of mode `sequence` has",
                    map.name.as_str()
                )));
            }
        };
        let scenario = Scenario::new(map.name, mode, map.seed, map.rules).map_err(de::Error::custom)?;
        Ok(match map.title {
            Some(title) => scenario.with_title(title),
            None => scenario,
        })
    }
}

/// Writes a named rule as its name, and a rule that the scenario owns as the map of the rule.
impl Serialize for ScenarioRule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            ScenarioRule::Named(name) => name.serialize(serializer),
            ScenarioRule::Owned(rule) => rule.serialize(serializer),
        }
    }
}

/// Reads text as the name of a rule of the workspace, and a map as a rule that the scenario owns.
impl<'de> Deserialize<'de> for ScenarioRule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NameOrRule;

        impl<'de> Visitor<'de> for NameOrRule {
            type Value = ScenarioRule;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the name of a rule, or the map of a rule that the scenario owns")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<ScenarioRule, E> {
                RuleName::new(text).map(ScenarioRule::Named).map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<ScenarioRule, A::Error> {
                Rule::deserialize(MapAccessDeserializer::new(map)).map(ScenarioRule::from)
            }
        }

        deserializer.deserialize_any(NameOrRule)
    }
}

/// The keys of the map of a condition that ends a step, before anyone checked that they make one condition.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UntilMap {
    calls: Option<Count>,
    selector: Option<SelectorRef>,
    firings: Option<Count>,
    rule: Option<RuleName>,
    after: Option<TimeSpan>,
}

/// Writes `{ calls, selector }`, `{ firings, rule }` or `{ after }`.
impl Serialize for Until {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Until::Calls { count, selector } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("calls", count)?;
                map.serialize_entry("selector", selector)?;
                map.end()
            }
            Until::Firings { count, rule } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("firings", count)?;
                map.serialize_entry("rule", rule)?;
                map.end()
            }
            Until::After(span) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("after", span)?;
                map.end()
            }
        }
    }
}

/// Reads `{ calls, selector }`, `{ firings, rule }` or `{ after }`, and nothing else.
impl<'de> Deserialize<'de> for Until {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = UntilMap::deserialize(deserializer)?;
        match map {
            UntilMap {
                calls: Some(count),
                selector: Some(selector),
                firings: None,
                rule: None,
                after: None,
            } => Ok(Until::Calls { count, selector }),
            UntilMap {
                calls: None,
                selector: None,
                firings: Some(count),
                rule: Some(rule),
                after: None,
            } => Ok(Until::Firings { count, rule }),
            UntilMap {
                calls: None,
                selector: None,
                firings: None,
                rule: None,
                after: Some(span),
            } => Ok(Until::After(span)),
            _ => Err(de::Error::custom(
                "a step ends on one condition: `calls` with a `selector`, `firings` with a `rule`, or `after` a time",
            )),
        }
    }
}
