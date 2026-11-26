//! What the host writes into the journal of an environment: the words of an entry for what the host changed.

use gaslightmq_engine::Journal;
use gaslightmq_journal::{Change, ChangeSurface, Parameter, RecordedDefinition, RecordedName};
use gaslightmq_lie::LieDefinition;
use gaslightmq_rule::{PresetSet, Rule, RuleName, Snapshot};
use gaslightmq_surface::Encoder;
use gaslightmq_trigger::{Chance, Count};
use gaslightmq_workspace::{DefinitionName, Revision};

use crate::revisions::RuleRevisions;

/// The name of a rule, as an entry records it.
pub(crate) fn rule(name: &RuleName) -> RecordedName {
    recorded(name.as_str())
}

/// A definition of the workspace, as an entry records it: its kind, and its name.
pub(crate) fn definition(name: &DefinitionName) -> RecordedDefinition {
    match name {
        DefinitionName::Selector(name) => RecordedDefinition::Selector(recorded(name.as_str())),
        DefinitionName::Preset(name) => RecordedDefinition::Preset(recorded(name.as_str())),
        DefinitionName::Rule(name) => RecordedDefinition::Rule(recorded(name.as_str())),
        DefinitionName::Layout(name) => RecordedDefinition::Layout(recorded(name.as_str())),
        DefinitionName::Environment(name) => RecordedDefinition::Environment(recorded(name.as_str())),
    }
}

/// The name of a definition, as an entry records it.
fn recorded(name: &str) -> RecordedName {
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

/// Counts every rule that changed its state inside its proxy since the last look, and records each in `journal`.
///
/// Nobody asked for such a change: a lifetime ended, or a firing spent a budget.
/// Each rule takes the next number, and its entry has the surface `lifetime`.
/// Returns the names of the rules, for whoever tells the followers of the host.
pub(crate) fn catch_up(revisions: &mut RuleRevisions, armed_now: &Snapshot, journal: &Journal) -> Vec<RuleName> {
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
    uncounted.into_iter().map(|(name, _)| name).collect()
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

    fn delay_1s() -> Preset {
        Preset::new(
            "delay-1s".parse().unwrap(),
            TimingLie::new().with_delay(TimeSpan::from_secs(1)),
        )
    }

    fn presets() -> PresetSet {
        PresetSet::new([delay_1s()]).unwrap()
    }

    /// A rule that delays every call by the preset `delay-1s`.
    fn slow() -> Rule {
        Rule::new(
            "slow-order-producers".parse().unwrap(),
            Conditions::new(),
            delay_1s().name().clone(),
        )
    }

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
        assert_eq!(value(Parameter::Delay, &slow(), &presets()), Some("1s".to_owned()));
        assert_eq!(value(Parameter::Jitter, &slow(), &presets()), Some("0s".to_owned()));
        assert_eq!(value(Parameter::Throttle, &slow(), &presets()), Some("0s".to_owned()));
        // A delay has no error to tell.
        assert_eq!(value(Parameter::Error, &slow(), &presets()), None);
        // Nobody holds the preset, so nobody knows the delay.
        assert_eq!(value(Parameter::Delay, &slow(), &PresetSet::default()), None);

        let refusal = slow().with_action(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        assert_eq!(
            value(Parameter::Error, &refusal, &presets()),
            Some("NOT_LEADER_OR_FOLLOWER".to_owned())
        );
        assert_eq!(value(Parameter::Delay, &refusal, &presets()), None);
    }

    #[test]
    fn a_trigger_that_leaves_a_part_out_fires_on_every_match() {
        assert_eq!(value(Parameter::Chance, &slow(), &presets()), Some("100%".to_owned()));
        assert_eq!(value(Parameter::Every, &slow(), &presets()), Some("1".to_owned()));

        let rarer = slow().with_trigger(TriggerParts::new().with_chance("40%".parse().unwrap()));
        assert_eq!(value(Parameter::Chance, &rarer, &presets()), Some("40%".to_owned()));

        let by_hand = slow().with_trigger(Trigger::Manual);
        assert_eq!(value(Parameter::Chance, &by_hand, &presets()), None);
        assert_eq!(value(Parameter::Every, &by_hand, &presets()), None);
    }

    #[test]
    fn the_lifetime_is_written_as_a_definition_writes_it() {
        assert_eq!(
            value(Parameter::Lifetime, &slow(), &presets()),
            Some("until disarmed".to_owned())
        );

        let short = slow().with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
        assert_eq!(value(Parameter::Lifetime, &short, &presets()), Some("30s".to_owned()));
    }
}
