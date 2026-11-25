//! The statistics of an environment: calls per API, and how far each rule got.

use std::collections::BTreeMap;

use gaslightmq_kernel::ApiKey;
use gaslightmq_rule::RuleName;
use serde::{Deserialize, Serialize};

/// What an environment counted since it started: the calls of each API, and what each rule did.
///
/// The numbers only grow. Whoever shows a rate keeps the statistics of a moment ago, and takes the difference.
///
/// With `serde`, it is a map with `calls`, `missed-events`, `apis` and `rules`.
/// The APIs are a map from the name of each API to its calls: `{ "Produce": 1200, "Fetch": 4400 }`,
/// and the rules a map from the name of each rule to what it did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Stats {
    /// How many calls the environment saw.
    pub calls: u64,
    /// How many events never reached the statistics, because whoever counted fell behind.
    /// While it is zero, every number is exact. Above zero, every number is the least it can be.
    pub missed_events: u64,
    /// How many calls of each API the environment saw.
    pub apis: BTreeMap<ApiKey, u64>,
    /// What each rule did, by the name of the rule.
    pub rules: BTreeMap<RuleName, RuleStats>,
}

/// How far one rule got: on how many calls it matched and fired, and what is left of its budget.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleStats {
    /// How many calls the selector of the rule matched.
    pub matches: u64,
    /// On how many calls the action of the rule took effect: the firings that count for a lifetime of firings.
    pub firings: u64,
    /// What is left of the budget of the rule while it is armed: matches for a lifetime of matches,
    /// and firings for a lifetime of firings. `None` for a rule without such a budget, or that is not armed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<u64>,
}
