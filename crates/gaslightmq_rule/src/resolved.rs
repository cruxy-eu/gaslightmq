use gaslightmq_clock::Instant;
use gaslightmq_lie::Stage;
use gaslightmq_selector::{EffectiveSelector, Pass, SelectorSet};
use gaslightmq_trigger::{Arming, DiceKey, Die, Lifetime, Seed, Trigger};

use crate::{Action, ActionRef, ArmedRule, Preset, PresetSet, Rule, RuleError, RuleName};

/// A rule without names to look up: what [`Rule::resolve`] gives, and what can be armed.
///
/// | A [`Rule`] has | A resolved rule has |
/// |---|---|
/// | A selector name or inline conditions, and a scope. | The [`EffectiveSelector`]: the selector **and** the scope. |
/// | A preset name or an inline action. | The [`Action`]. |
/// | A dice key, or none. | The [`DiceKey`]: the one the rule sets, or else its name. |
///
/// A resolved rule is a copy.
/// It does not follow a later edit of the selector or the preset it came from:
/// resolve the rule again, and [retune](ArmedRule::retuned) what is armed.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let order_producers = Selector::new(
///     "Order producers".parse()?,
///     Conditions::new()
///         .with(Condition::is(ApiKey::PRODUCE))
///         .with(Condition::parse(Field::Topic, "orders.*")?),
/// );
/// let selectors = SelectorSet::new([order_producers])?;
/// let presets = PresetSet::new([Preset::new(
///     "Leader flap".parse()?,
///     RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
/// )])?;
///
/// let rule = Rule::new(
///     "Refuse order producers".parse()?,
///     "Order producers".parse::<SelectorName>()?,
///     "Leader flap".parse::<PresetName>()?,
/// )
/// .scoped(Scope::client(ClientId::new("order-ingest")?));
///
/// let resolved = rule.resolve(&selectors, &presets)?;
/// assert_eq!(
///     resolved.selector().to_string(),
///     "api is Produce and topic matches orders.* and client.id is order-ingest"
/// );
/// assert_eq!(resolved.action().to_string(), "NOT_LEADER_OR_FOLLOWER");
/// assert_eq!(resolved.pass(), Pass::Request);
///
/// // Arming gives the rule its live state: counters at zero, and a die of the seed.
/// let armed = resolved.arm(Seed::new(42), Instant::ZERO);
/// assert_eq!(armed.arming().counter().matches(), 0);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedRule {
    name: RuleName,
    selector: EffectiveSelector,
    pass: Pass,
    action: Action,
    trigger: Trigger,
    lifetime: Lifetime,
    dice: DiceKey,
}

impl Rule {
    /// Looks up the selector and the preset the rule names, and checks that the rule holds together.
    ///
    /// The check covers this rule alone.
    /// Whoever holds the whole workspace calls it for every rule,
    /// and checks what reaches across definitions itself: the rules a scenario or a pad names.
    ///
    /// # Errors
    ///
    /// - [`RuleError::UnknownSelector`] when the rule names a selector that `selectors` does not have.
    /// - [`RuleError::UnknownPreset`] when the rule names a preset that `presets` does not have.
    /// - [`RuleError::LieBeforeTruth`] when the rule tests a response field and its lie acts before forwarding.
    ///   The truth is not known yet at that stage, so the rule could never fire.
    ///   A rule with an incident can test a response field: the incident takes effect after the call.
    pub fn resolve(&self, selectors: &SelectorSet, presets: &PresetSet) -> Result<ResolvedRule, RuleError> {
        let rule = || self.name().clone();

        let selector = selectors
            .resolve(self.selector())
            .map_err(|error| RuleError::UnknownSelector { rule: rule(), error })?
            .and(self.scope().clone());
        let action = match self.action() {
            ActionRef::Inline(action) => action,
            ActionRef::Preset(preset) => {
                let found = presets.get(preset).map(Preset::action);
                found.ok_or_else(|| RuleError::UnknownPreset {
                    rule: rule(),
                    preset: preset.clone(),
                })?
            }
        };

        let pass = selector.pass();
        if let Action::Lie(lie) = action
            && pass == Pass::Response
            && lie.stage() == Stage::BeforeForwarding
        {
            return Err(RuleError::LieBeforeTruth {
                rule: rule(),
                lie: lie.kind(),
            });
        }

        Ok(ResolvedRule {
            name: rule(),
            selector,
            pass,
            action: action.clone(),
            trigger: self.trigger(),
            lifetime: self.lifetime(),
            dice: self.dice_key(),
        })
    }
}

impl ResolvedRule {
    /// Arms the rule at `now`, in a run with that seed.
    ///
    /// The seed is the one of the active scenario, or the default seed of the workspace when none is active.
    /// Arming starts the rule's live state: its counters at zero, its whole budget free,
    /// its time window starting at `now`, and the die that the seed and the dice key give.
    /// Arming the rule again makes a new arming, which is how arming resets all of that.
    pub fn arm(self, seed: Seed, now: Instant) -> ArmedRule {
        let die = Die::new(seed, &self.dice);
        let arming = Arming::new(self.trigger, self.lifetime, die, now);
        ArmedRule::new(self, arming)
    }

    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        &self.name
    }

    /// Every condition a call has to pass: those of the selector, of what it includes, and of the scope.
    pub fn selector(&self) -> &EffectiveSelector {
        &self.selector
    }

    /// The evaluation pass that decides the rule: the response pass when it tests a response field.
    pub fn pass(&self) -> Pass {
        self.pass
    }

    /// What happens when the rule fires.
    pub fn action(&self) -> &Action {
        &self.action
    }

    /// When the rule fires among the calls its selector matches.
    pub fn trigger(&self) -> Trigger {
        self.trigger
    }

    /// How long the rule stays armed.
    pub fn lifetime(&self) -> Lifetime {
        self.lifetime
    }

    /// What the die of the rule derives from, next to the seed.
    pub fn dice_key(&self) -> &DiceKey {
        &self.dice
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    use super::*;
    use crate::PresetName;

    fn name(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    fn selector_name(text: &str) -> SelectorName {
        text.parse().unwrap()
    }

    fn preset_name(text: &str) -> PresetName {
        text.parse().unwrap()
    }

    fn selectors() -> SelectorSet {
        let order_producers = Selector::new(
            selector_name("Order producers"),
            Conditions::new()
                .with(Condition::is(ApiKey::PRODUCE))
                .with(Condition::parse(Field::Topic, "orders.*").unwrap()),
        );
        SelectorSet::new([order_producers]).unwrap()
    }

    fn delay() -> TimingLie {
        TimingLie::new().with_delay(TimeSpan::from_secs(1))
    }

    fn presets() -> PresetSet {
        PresetSet::new([Preset::new(preset_name("Delay 1 s"), delay())]).unwrap()
    }

    fn slow_down() -> Rule {
        Rule::new(
            name("Slow down order producers"),
            selector_name("Order producers"),
            preset_name("Delay 1 s"),
        )
    }

    fn succeeded() -> Conditions {
        Conditions::new()
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::is(Outcome::Ok))
    }

    #[test]
    fn resolving_looks_up_the_selector_and_the_preset() {
        let trigger = Trigger::from(TriggerParts::new().with_chance("40%".parse().unwrap()));
        let lifetime = Lifetime::Firings(Count::new(3).unwrap());
        let rule = slow_down().with_trigger(trigger).with_lifetime(lifetime);

        let resolved = rule.resolve(&selectors(), &presets()).unwrap();

        assert_eq!(resolved.name(), "Slow down order producers");
        assert_eq!(
            resolved.selector().to_string(),
            "api is Produce and topic matches orders.*"
        );
        assert_eq!(resolved.action(), &Action::from(delay()));
        assert_eq!(resolved.trigger(), trigger);
        assert_eq!(resolved.lifetime(), lifetime);
        assert_eq!(resolved.pass(), Pass::Request);
    }

    #[test]
    fn the_effective_selector_is_the_selector_and_the_scope() {
        let rule = slow_down().scoped(Scope::client(ClientId::new("order-ingest").unwrap()));

        let resolved = rule.resolve(&selectors(), &presets()).unwrap();

        assert_eq!(
            resolved.selector().to_string(),
            "api is Produce and topic matches orders.* and client.id is order-ingest"
        );
    }

    #[test]
    fn inline_conditions_and_an_inline_action_need_no_set() {
        let metadata = Conditions::from(Condition::is(ApiKey::METADATA));
        let rule = Rule::new(name("Blackhole metadata"), metadata.clone(), LieDefinition::Silence);

        let resolved = rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();

        assert_eq!(resolved.selector(), &EffectiveSelector::from(metadata));
        assert_eq!(resolved.action(), &Action::Lie(LieDefinition::Silence));
    }

    #[test]
    fn the_dice_key_is_the_one_the_rule_sets_or_else_its_name() {
        let dice = DiceKey::new("an old name").unwrap();
        let resolve = |rule: Rule| rule.resolve(&selectors(), &presets()).unwrap();

        assert_eq!(resolve(slow_down()).dice_key().as_str(), "Slow down order producers");
        assert_eq!(resolve(slow_down().with_dice(dice.clone())).dice_key(), &dice);
    }

    #[test]
    fn a_selector_that_does_not_exist_is_an_error() {
        let rule = slow_down().with_selector(selector_name("Order consumers"));

        assert_eq!(
            rule.resolve(&selectors(), &presets()),
            Err(RuleError::UnknownSelector {
                rule: name("Slow down order producers"),
                error: SelectorError::UnknownSelector {
                    name: selector_name("Order consumers")
                },
            })
        );
    }

    #[test]
    fn a_preset_that_does_not_exist_is_an_error() {
        let rule = slow_down().with_action(preset_name("Blackhole"));

        assert_eq!(
            rule.resolve(&selectors(), &presets()),
            Err(RuleError::UnknownPreset {
                rule: name("Slow down order producers"),
                preset: preset_name("Blackhole"),
            })
        );
    }

    #[test]
    fn a_rule_that_tests_the_truth_is_decided_in_the_response_pass() {
        let lost = RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse);
        let rule = Rule::new(name("Lost acknowledgement"), succeeded(), lost);

        let resolved = rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();

        assert_eq!(resolved.pass(), Pass::Response);
    }

    #[test]
    fn a_lie_before_forwarding_cannot_test_the_truth() {
        let lies: [(LieDefinition, LieKind); 4] = [
            (RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).into(), LieKind::Refusal),
            (delay().at(Stage::BeforeForwarding).into(), LieKind::Timing),
            (LieDefinition::Silence, LieKind::Silence),
            (HangUpLie::new().into(), LieKind::HangUp),
        ];

        for (lie, kind) in lies {
            let rule = Rule::new(name("Too early"), succeeded(), lie);
            assert_eq!(
                rule.resolve(&SelectorSet::default(), &PresetSet::default()),
                Err(RuleError::LieBeforeTruth {
                    rule: name("Too early"),
                    lie: kind,
                })
            );
        }
    }

    #[test]
    fn a_lie_after_the_response_can_test_the_truth() {
        let lies: [LieDefinition; 2] = [delay().into(), HangUpLie::new().at(Stage::AfterResponse).into()];

        for lie in lies {
            let rule = Rule::new(name("Late enough"), succeeded(), lie);
            assert!(rule.resolve(&SelectorSet::default(), &PresetSet::default()).is_ok());
        }
    }

    #[test]
    fn an_incident_can_test_the_truth() {
        let election = IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        let rule = Rule::new(name("Election after a write"), succeeded(), election);

        let resolved = rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();

        assert_eq!(resolved.pass(), Pass::Response);
    }

    #[test]
    fn arming_starts_the_live_state_with_the_die_of_the_seed_and_the_dice_key() {
        let seed = Seed::new(42);
        let now = Instant::ZERO + TimeSpan::from_secs(5);
        let resolved = slow_down()
            .with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)))
            .resolve(&selectors(), &presets())
            .unwrap();

        let armed = resolved.clone().arm(seed, now);

        assert_eq!(armed.rule(), &resolved);
        assert_eq!(armed.arming().armed_at(), now);
        assert_eq!(armed.arming().die(), Die::new(seed, resolved.dice_key()));
        assert_eq!(armed.arming().ends_at(), Some(now + TimeSpan::from_secs(30)));
        assert_eq!(armed.arming().counter().matches(), 0);
    }
}
