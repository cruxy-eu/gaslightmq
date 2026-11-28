//! What the host writes into the journal of an environment: the words of an entry for what the host changed.

use gaslightmq_engine::{Journal, ScenarioChange, StepsTaken};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::{Change, ChangeSource, ChangeSurface, Parameter, RecordedDefinition, RecordedName};
use gaslightmq_lie::LieDefinition;
use gaslightmq_rule::{PresetSet, Rule, RuleName};
use gaslightmq_surface::Encoder;
use gaslightmq_trigger::{Chance, Count};
use gaslightmq_workspace::{DefinitionName, Revision};

use crate::revisions::RuleRevisions;

/// The name of a rule, as an entry records it.
pub(crate) fn rule(name: &RuleName) -> RecordedName {
    recorded(name.as_str())
}

/// The name of an environment, as an entry and a recording record it.
pub(crate) fn environment(name: &EnvironmentName) -> RecordedName {
    recorded(name.as_str())
}

/// A definition of the workspace, as an entry records it: its kind, and its name.
pub(crate) fn definition(name: &DefinitionName) -> RecordedDefinition {
    match name {
        DefinitionName::Selector(name) => RecordedDefinition::Selector(recorded(name.as_str())),
        DefinitionName::Preset(name) => RecordedDefinition::Preset(recorded(name.as_str())),
        DefinitionName::Rule(name) => RecordedDefinition::Rule(recorded(name.as_str())),
        DefinitionName::Scenario(name) => RecordedDefinition::Scenario(recorded(name.as_str())),
        DefinitionName::Layout(name) => RecordedDefinition::Layout(recorded(name.as_str())),
        DefinitionName::Environment(name) => RecordedDefinition::Environment(recorded(name.as_str())),
    }
}

/// The name of a definition, of a scenario or of a step, as an entry records it.
pub(crate) fn recorded(name: &str) -> RecordedName {
    match RecordedName::new(name) {
        Ok(name) => name,
        // The name of every definition follows the one rule for names, and so does a recorded name.
        Err(error) => unreachable!("the name of a definition is a name that an entry records: {error}"),
    }
}

/// The revision that a change left, as an entry holds it.
pub(crate) const fn left(revision: Revision) -> gaslightmq_journal::Revision {
    gaslightmq_journal::Revision::new(revision.number())
}

/// What changed inside a proxy since the last look, without anyone asking: the rules that changed their state, and
/// whether the scenario advanced a step.
#[derive(Debug, Default)]
pub(crate) struct Uncounted {
    /// The rules that armed or disarmed.
    pub(crate) rules: Vec<RuleName>,
    /// Whether the scenario advanced a step by itself.
    pub(crate) scenario: bool,
}

/// Counts every step that the scenario of the environment took by itself, and every rule that changed its state
/// inside its proxy since the last look, and records each in `journal`.
///
/// Nobody asked for such a change: the condition of a step held, a lifetime ended, or a firing spent a budget.
/// Each step takes the next number, which the rules it disarmed and armed share, and its entries have the surface
/// `step`. Each rule that changed for another reason takes a number of its own, and its entry has the surface
/// `lifetime`. Returns what changed, for whoever tells the followers of the host.
pub(crate) fn catch_up(revisions: &mut RuleRevisions, taken: &StepsTaken, journal: &Journal) -> Uncounted {
    let mut stepped = Vec::new();
    for step in &taken.steps {
        let revision = revisions.count_scenario(&step.disarmed, &step.armed);
        let advanced = Change::StepAdvanced {
            scenario: recorded(step.position.scenario.as_str()),
            step: step.position.step.as_ref().map(|step| recorded(step.as_str())),
        };
        record_scenario_change(journal, step, advanced, ChangeSurface::Step.into(), revision);
        stepped.extend(step.disarmed.iter().chain(&step.armed).cloned());
    }
    let armed_now = &taken.snapshot;
    let uncounted = revisions.catch_up(armed_now);
    for (name, revision) in &uncounted {
        let rule = rule(name);
        let change = if armed_now.get(name).is_some() {
            Change::RuleArmed { rule }
        } else {
            Change::RuleDisarmed { rule }
        };
        journal.record(change, ChangeSurface::Lifetime, Some(left(*revision)));
    }
    let scenario = !taken.steps.is_empty();
    stepped.extend(uncounted.into_iter().map(|(name, _)| name));
    Uncounted {
        rules: stepped,
        scenario,
    }
}

/// Records a change of the scenario of an environment in `journal`, with one revision for all of its entries: the
/// rules that it disarmed, the entry of the scenario, and the rules that it armed.
pub(crate) fn record_scenario_change(
    journal: &Journal,
    change: &ScenarioChange,
    entry: Change,
    from: ChangeSource,
    revision: Revision,
) {
    for disarmed in &change.disarmed {
        journal.record(
            Change::RuleDisarmed { rule: rule(disarmed) },
            from,
            Some(left(revision)),
        );
    }
    journal.record(entry, from, Some(left(revision)));
    for armed in &change.armed {
        journal.record(Change::RuleArmed { rule: rule(armed) }, from, Some(left(revision)));
    }
}

/// The parameter of a rule that an encoder changes.
pub(crate) const fn parameter(encoder: Encoder) -> Parameter {
    match encoder {
        Encoder::Probability => Parameter::Chance,
        Encoder::Every => Parameter::Every,
        Encoder::Delay => Parameter::Delay,
        Encoder::Jitter => Parameter::Jitter,
        Encoder::Throttle => Parameter::Throttle,
        Encoder::ErrorCode => Parameter::Error,
        Encoder::Duration | Encoder::Firings => Parameter::Lifetime,
    }
}

/// The value that a parameter has in `rule`, as a definition writes it: `40%`, `1s`, `3 firings`.
///
/// A rule that leaves a chance or an `every` count out fires on every match, so its value is `100%` or `1`.
/// Returns `None` when the rule has no such parameter: its action is another one, its trigger is manual,
/// or it names a preset that `presets` does not hold.
pub(crate) fn value(parameter: Parameter, rule: &Rule, presets: &PresetSet) -> Option<String> {
    let lie = || presets.resolve(rule.action())?.lie();
    let timing = || match lie()? {
        LieDefinition::Timing(timing) => Some(*timing),
        _ => None,
    };
    match parameter {
        Parameter::Chance => {
            let chance = rule.trigger().parts()?.chance();
            Some(chance.unwrap_or(Chance::ALWAYS).to_string())
        }
        Parameter::Every => {
            let every = rule.trigger().parts()?.every();
            Some(every.unwrap_or(Count::ONE).to_string())
        }
        Parameter::Delay => Some(timing()?.delay().to_string()),
        Parameter::Jitter => Some(timing()?.jitter().to_string()),
        Parameter::Throttle => Some(timing()?.throttle().to_string()),
        Parameter::Error => match lie()? {
            LieDefinition::Refusal(refusal) => Some(refusal.error.to_string()),
            _ => None,
        },
        Parameter::Lifetime => Some(rule.lifetime().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_kernel::ErrorCode;
    use gaslightmq_lie::{RefusalLie, TimingLie};
    use gaslightmq_rule::Preset;
    use gaslightmq_selector::Conditions;
    use gaslightmq_trigger::{Lifetime, Trigger, TriggerParts};

    use super::*;

    #[test]
    fn every_encoder_changes_one_parameter_and_two_of_them_the_lifetime() {
        let parameters = Encoder::ALL.map(parameter);

        assert_eq!(
            parameters,
            [
                Parameter::Chance,
                Parameter::Delay,
                Parameter::Jitter,
                Parameter::Error,
                Parameter::Throttle,
                Parameter::Every,
                Parameter::Lifetime,
                Parameter::Lifetime,
            ]
        );
    }

    #[test]
    fn a_parameter_of_the_action_is_read_through_the_preset_that_the_rule_names() {
        let delay_1s = Preset::new(
            "delay-1s".parse().unwrap(),
            TimingLie::new().with_delay(TimeSpan::from_secs(1)),
        );
        let slow = Rule::new(
            "slow-order-producers".parse().unwrap(),
            Conditions::new(),
            delay_1s.name().clone(),
        );
        let presets = PresetSet::new([delay_1s]).unwrap();

        assert_eq!(value(Parameter::Delay, &slow, &presets), Some("1s".to_owned()));
        assert_eq!(value(Parameter::Jitter, &slow, &presets), Some("0s".to_owned()));
        assert_eq!(value(Parameter::Throttle, &slow, &presets), Some("0s".to_owned()));
        // A delay has no error to tell.
        assert_eq!(value(Parameter::Error, &slow, &presets), None);
        // Nobody holds the preset, so nobody knows the delay.
        assert_eq!(value(Parameter::Delay, &slow, &PresetSet::default()), None);

        let refusal = slow.with_action(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        assert_eq!(
            value(Parameter::Error, &refusal, &presets),
            Some("NOT_LEADER_OR_FOLLOWER".to_owned())
        );
        assert_eq!(value(Parameter::Delay, &refusal, &presets), None);
    }

    #[test]
    fn a_trigger_that_leaves_a_part_out_fires_on_every_match() {
        let silence = Rule::new("blackhole".parse().unwrap(), Conditions::new(), LieDefinition::Silence);
        let no_presets = PresetSet::default();

        assert_eq!(value(Parameter::Chance, &silence, &no_presets), Some("100%".to_owned()));
        assert_eq!(value(Parameter::Every, &silence, &no_presets), Some("1".to_owned()));

        let rarer = silence
            .clone()
            .with_trigger(TriggerParts::new().with_chance("40%".parse().unwrap()));
        assert_eq!(value(Parameter::Chance, &rarer, &no_presets), Some("40%".to_owned()));

        let by_hand = silence.with_trigger(Trigger::Manual);
        assert_eq!(value(Parameter::Chance, &by_hand, &no_presets), None);
        assert_eq!(value(Parameter::Every, &by_hand, &no_presets), None);
    }

    #[test]
    fn the_lifetime_is_written_as_a_definition_writes_it() {
        let silence = Rule::new("blackhole".parse().unwrap(), Conditions::new(), LieDefinition::Silence);
        let no_presets = PresetSet::default();

        assert_eq!(
            value(Parameter::Lifetime, &silence, &no_presets),
            Some("until disarmed".to_owned())
        );

        let short = silence.with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
        assert_eq!(value(Parameter::Lifetime, &short, &no_presets), Some("30s".to_owned()));
    }
}
