//! Statistics: how many calls there were of each API, and how far each rule got on them.

use std::collections::BTreeMap;

use gaslightmq_call::{Call, ExplanationStatus, RecordedRuleName, RuleExplanation};
use gaslightmq_field::{Field, FieldSource, FieldValue};
use gaslightmq_kernel::ApiKey;
use gaslightmq_rule::RuleName;

/// What a [`Traffic`](crate::Traffic) counted since it began: the calls of each API, and what each rule did.
///
/// The numbers only grow, also when a call leaves the [live window](crate::LiveWindow).
/// Whoever shows a rate keeps the statistics of a moment ago, and takes the difference.
///
/// The statistics are plain numbers of a moment: a copy of them does not follow the traffic.
///
/// # What a traffic never saw
///
/// A traffic watches, and whoever watches can fall behind.
/// [`missed_events`](Self::missed_events) says how many events of the engine never reached the traffic.
/// While it is zero, every number is exact. Above zero, every number is the least it can be.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Statistics {
    calls: u64,
    missed_events: u64,
    apis: BTreeMap<ApiKey, u64>,
    rules: BTreeMap<RecordedRuleName, RuleStatistics>,
}

impl Statistics {
    /// Counts `call`: once for its API, and once for every rule that it touched.
    pub(crate) fn count(&mut self, call: &Call) {
        self.calls = self.calls.saturating_add(1);
        if let Some(api) = api_of(call) {
            increment(self.apis.entry(api).or_default());
        }
        for line in call.explanation() {
            // Most calls meet a rule that is there already, and its name is copied once, for the first of them.
            if let Some(rule) = self.rules.get_mut(line.rule().as_str()) {
                rule.count(line);
            } else {
                let mut rule = RuleStatistics::default();
                rule.count(line);
                self.rules.insert(line.rule().clone(), rule);
            }
        }
    }

    /// Counts `events` that never reached the traffic.
    pub(crate) const fn miss(&mut self, events: u64) {
        self.missed_events = self.missed_events.saturating_add(events);
    }

    /// How many calls the traffic recorded.
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// How many events of the engine never reached the traffic, because whoever fed it fell behind.
    ///
    /// An event is the record of a call, or an incident that a rule fired, and nobody knows which of them are gone.
    pub const fn missed_events(&self) -> u64 {
        self.missed_events
    }

    /// How many calls of `api` the traffic recorded.
    pub fn calls_of(&self, api: ApiKey) -> u64 {
        self.apis.get(&api).copied().unwrap_or(0)
    }

    /// Every API that a call had, with the number of its calls, in the order of the API keys.
    ///
    /// A call whose request nobody could read has no API. It counts in [`calls`](Self::calls) alone.
    pub fn apis(&self) -> impl Iterator<Item = (ApiKey, u64)> {
        self.apis.iter().map(|(api, calls)| (*api, *calls))
    }

    /// What the rule with that name did so far. A rule that no call touched did nothing.
    ///
    /// A call records the name of every rule it met, and the statistics count under that name.
    /// A rule keeps its name, so a rule under another name is a new rule, and it starts at zero.
    pub fn rule(&self, name: &RuleName) -> RuleStatistics {
        self.rules.get(name.as_str()).copied().unwrap_or_default()
    }

    /// Every rule that a call touched, under its name, with what it did so far, in the order of the names.
    ///
    /// A name stays after someone deleted its rule, with the numbers it had.
    pub fn rules(&self) -> impl Iterator<Item = (&RecordedRuleName, RuleStatistics)> {
        self.rules.iter().map(|(name, rule)| (name, *rule))
    }
}

/// How far one rule got, on how many calls.
///
/// A call counts once for every [status](ExplanationStatus) that the rule reached on it.
/// Two of the numbers have a name of their own, because a rule counts them too:
///
/// | Number | Counts | A rule uses it for |
/// |---|---|---|
/// | [`matches`](Self::matches) | The calls that the selector of the rule matched. | Its counter, and a lifetime of matches. |
/// | [`firings`](Self::firings) | The calls on which the action of the rule took effect. | A lifetime of firings. |
///
/// A firing is a lie that applied on at least one target, or incidents on their way.
/// A lie that was shadowed or pre-empted everywhere is no firing, and neither is a lie that GaslightMQ could not tell.
/// Those calls show under [`ExplanationStatus::Fired`], and not under [`ExplanationStatus::Applied`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RuleStatistics {
    matched: u64,
    rolled: u64,
    fired: u64,
    applied: u64,
    shadowed: u64,
    pre_empted: u64,
}

impl RuleStatistics {
    /// Counts one call, for every status that the rule reached on it.
    fn count(&mut self, line: &RuleExplanation) {
        for status in line.statuses() {
            increment(self.of(status));
        }
    }

    /// The number of calls with that status.
    const fn of(&mut self, status: ExplanationStatus) -> &mut u64 {
        match status {
            ExplanationStatus::Matched => &mut self.matched,
            ExplanationStatus::Rolled => &mut self.rolled,
            ExplanationStatus::Fired => &mut self.fired,
            ExplanationStatus::Applied => &mut self.applied,
            ExplanationStatus::Shadowed => &mut self.shadowed,
            ExplanationStatus::PreEmpted => &mut self.pre_empted,
        }
    }

    /// How many calls the selector of the rule matched.
    pub const fn matches(&self) -> u64 {
        self.matched
    }

    /// How many calls the action of the rule took effect on: the firings that count for a lifetime of firings.
    pub const fn firings(&self) -> u64 {
        self.applied
    }

    /// On how many calls the rule reached `status`.
    pub const fn calls(&self, status: ExplanationStatus) -> u64 {
        match status {
            ExplanationStatus::Matched => self.matched,
            ExplanationStatus::Rolled => self.rolled,
            ExplanationStatus::Fired => self.fired,
            ExplanationStatus::Applied => self.applied,
            ExplanationStatus::Shadowed => self.shadowed,
            ExplanationStatus::PreEmpted => self.pre_empted,
        }
    }
}

/// One more. A number that is full stays full: it never starts again at zero.
const fn increment(number: &mut u64) {
    *number = number.saturating_add(1);
}

/// The API of the call, when its request names exactly one.
fn api_of(call: &Call) -> Option<ApiKey> {
    let apis = call.read(Field::Api).present()?;
    let mut apis = apis.iter();
    match (apis.next(), apis.next()) {
        (Some(FieldValue::Api(api)), None) => Some(*api),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_call::{CallNumber, CallTiming, Delivered, Explanation};
    use gaslightmq_clock::Instant;
    use gaslightmq_field::{ConnectionNumber, Fields};

    use super::*;

    /// The record of a call with those fields on some connection, which got no answer and met no rule.
    fn record(fields: Fields) -> Call {
        let fields = fields.with(ConnectionNumber::new(7)).without_truth();
        Call::new(
            CallNumber::FIRST,
            fields,
            Delivered::Nothing,
            Explanation::none(),
            CallTiming::arrived(Instant::ZERO),
        )
        .unwrap()
    }

    #[test]
    fn statistics_start_at_zero() {
        let statistics = Statistics::default();

        assert_eq!(statistics.calls(), 0);
        assert_eq!(statistics.missed_events(), 0);
        assert_eq!(statistics.apis().count(), 0);
        assert_eq!(statistics.rules().count(), 0);
        let leader_flap: RuleName = "leader-flap".parse().unwrap();
        for status in ExplanationStatus::ALL {
            assert_eq!(statistics.rule(&leader_flap).calls(status), 0, "{status}");
        }
    }

    #[test]
    fn a_call_without_one_api_counts_in_the_total_alone() {
        let mut statistics = Statistics::default();

        statistics.count(&record(Fields::new()));
        statistics.count(&record(Fields::new().with(ApiKey::PRODUCE).with(ApiKey::FETCH)));
        statistics.count(&record(Fields::new().with(ApiKey::FETCH)));

        assert_eq!(statistics.calls(), 3);
        assert_eq!(statistics.apis().collect::<Vec<_>>(), [(ApiKey::FETCH, 1)]);
    }

    #[test]
    fn a_number_that_is_full_stays_full() {
        let mut statistics = Statistics::default();

        statistics.miss(u64::MAX);
        statistics.miss(1);

        assert_eq!(statistics.missed_events(), u64::MAX);
    }
}
