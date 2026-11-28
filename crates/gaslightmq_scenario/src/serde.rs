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

use crate::{Mode, Scenario, ScenarioName, ScenarioRule, ScenarioTitle, Step, StepName, StepTitle, Until};

/// A scenario as a file writes it, before anyone checked that its parts fit.
///
/// The mode is a word, and the steps of a Sequence stand next to it.
/// A scenario writes its steps as [`Step`]s, and reads them as [`StepMap`]s, which may leave out what the scenario
/// gives them.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScenarioMap<S = Step> {
    name: ScenarioName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<ScenarioTitle>,
    mode: ModeWord,
    seed: Seed,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rules: Vec<ScenarioRule>,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    steps: Option<Vec<S>>,
}

/// A step as a file writes it: its name may be left out, and so may the rule of a condition that counts firings.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StepMap {
    #[serde(default)]
    name: Option<StepName>,
    #[serde(default)]
    title: Option<StepTitle>,
    #[serde(default)]
    arm: Vec<ScenarioRule>,
    #[serde(default)]
    until: Option<UntilMap>,
}

impl StepMap {
    /// The step at `place` among the steps, from 1: `step-` and its place when it has no name, ending on the one
    /// rule it arms when its condition counts firings and names no rule.
    fn into_step(self, place: usize) -> Result<Step, String> {
        let name = match self.name {
            Some(name) => name,
            None => StepName::new(format!("step-{place}")).map_err(|error| error.to_string())?,
        };
        let until = self.until.map(|until| until.into_until(&self.arm)).transpose()?;
        let step = Step::new(name).arming(self.arm);
        let step = match self.title {
            Some(title) => step.with_title(title),
            None => step,
        };
        Ok(match until {
            Some(until) => step.ending_on(until),
            None => step,
        })
    }
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

/// Reads the map of a scenario, names the steps that have no name, and checks that its parts fit.
///
/// Steps belong to a Sequence alone, so a Live or a Chain scenario with a key `steps` is an error,
/// even when it lists none.
impl<'de> Deserialize<'de> for Scenario {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = ScenarioMap::<StepMap>::deserialize(deserializer)?;
        let mode = match (map.mode, map.steps) {
            (ModeWord::Sequence, steps) => {
                let steps = steps
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .map(|(index, step)| step.into_step(index + 1))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(de::Error::custom)?;
                Mode::Sequence(steps)
            }
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
pub(crate) struct UntilMap {
    calls: Option<Count>,
    selector: Option<SelectorRef>,
    firings: Option<Count>,
    rule: Option<RuleName>,
    after: Option<TimeSpan>,
}

impl UntilMap {
    /// The condition of a step that arms `arm`: `firings` without a `rule` counts the one rule that the step arms.
    fn into_until(self, arm: &[ScenarioRule]) -> Result<Until, String> {
        match self {
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
                rule,
                after: None,
            } => match (rule, arm) {
                (Some(rule), _) => Ok(Until::Firings { count, rule }),
                (None, [only]) => Ok(Until::Firings {
                    count,
                    rule: only.name().clone(),
                }),
                (None, arm) => Err(format!(
                    "a step that ends on `firings` without a `rule` arms one rule, and this one arms {}",
                    arm.len()
                )),
            },
            UntilMap {
                calls: None,
                selector: None,
                firings: None,
                rule: None,
                after: Some(span),
            } => Ok(Until::After(span)),
            _ => Err(
                "a step ends on one condition: `calls` with a `selector`, `firings` with a `rule`, or `after` a time"
                    .to_owned(),
            ),
        }
    }
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
///
/// A condition alone names the rule whose firings it counts. Within a step, it may leave the rule out: see [`Step`].
impl<'de> Deserialize<'de> for Until {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        UntilMap::deserialize(deserializer)?
            .into_until(&[])
            .map_err(de::Error::custom)
    }
}
