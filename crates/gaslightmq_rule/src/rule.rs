use gaslightmq_selector::{Scope, SelectorRef};
use gaslightmq_trigger::{DiceKey, Lifetime, Trigger};

use crate::{ActionRef, RuleName};

/// A rule as a file, a scenario or the UI writes it: what happens to which calls, when, and for how long.
///
/// | Part | Meaning | When left out |
/// |---|---|---|
/// | Name | Unique among rules. Other things reference the rule by it. | |
/// | Selector | Which calls the rule considers: a selector name, or inline conditions. | |
/// | Scope | Extra conditions over connection fields: who the rule applies to. | All clients. |
/// | Action | What happens when the rule fires: a preset name, or an inline lie or incident. | |
/// | Trigger | When the rule fires among the calls its selector matches. | Every matching call. |
/// | Lifetime | How long the rule stays armed. | Until disarmed. |
/// | Dice key | What the rule's die derives from. | The name of the rule. |
///
/// ```yaml
/// name: Slow down order producers
/// selector: Order producers
/// scope:
///   client.id: order-ingest
/// action: Delay 1 s
/// trigger:
///   chance: 40%
/// lifetime: until disarmed
/// ```
///
/// A rule is a definition, and every value of it is a valid one.
/// It can still name a selector or a preset that does not exist.
/// [`resolve`](Self::resolve) looks the names up, and gives the [`ResolvedRule`](crate::ResolvedRule) that can be armed.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let rule = Rule::new(
///     "Slow down order producers".parse()?,
///     "Order producers".parse::<SelectorName>()?,
///     "Delay 1 s".parse::<PresetName>()?,
/// )
/// .scoped(Scope::client(ClientId::new("order-ingest")?))
/// .with_trigger(TriggerParts::new().with_chance("40%".parse()?));
///
/// assert_eq!(rule.name(), "Slow down order producers");
/// assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
/// assert_eq!(rule.dice_key().as_str(), "Slow down order producers");
///
/// // A rename does not change which calls the rule hits: the old name stays as the dice key.
/// let renamed = rule.renamed("Slow ingest".parse()?);
/// assert_eq!(renamed.dice_key().as_str(), "Slow down order producers");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Rule {
    name: RuleName,
    selector: SelectorRef,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Scope::is_all"))]
    scope: Scope,
    action: ActionRef,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Trigger::is_always"))]
    trigger: Trigger,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "until_disarmed"))]
    lifetime: Lifetime,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    dice: Option<DiceKey>,
}

/// Whether the lifetime is the one of a rule that names none, which a file leaves out.
#[cfg(feature = "serde")]
fn until_disarmed(lifetime: &Lifetime) -> bool {
    *lifetime == Lifetime::UntilDisarmed
}

impl Rule {
    /// A rule for all clients that fires on every matching call, and stays armed until disarmed.
    pub fn new(name: RuleName, selector: impl Into<SelectorRef>, action: impl Into<ActionRef>) -> Self {
        Self {
            name,
            selector: selector.into(),
            scope: Scope::all(),
            action: action.into(),
            trigger: Trigger::always(),
            lifetime: Lifetime::UntilDisarmed,
            dice: None,
        }
    }

    /// The same rule under another name, which keeps hitting the same calls.
    ///
    /// A rule that sets no dice key rolls a die of its name.
    /// The rename writes the old name as the dice key, so the die stays the same one.
    /// A rule that has a dice key already keeps it.
    ///
    /// What references the rule by its old name does not follow: whoever holds those references updates them.
    #[must_use]
    pub fn renamed(self, name: RuleName) -> Self {
        let dice = self.dice_key();
        Self {
            dice: (dice != name.dice_key()).then_some(dice),
            name,
            ..self
        }
    }

    /// The same rule for other calls.
    #[must_use]
    pub fn with_selector(self, selector: impl Into<SelectorRef>) -> Self {
        Self {
            selector: selector.into(),
            ..self
        }
    }

    /// The same rule for the clients of `scope` only.
    /// It replaces the scope the rule had before.
    #[must_use]
    pub fn scoped(self, scope: Scope) -> Self {
        Self { scope, ..self }
    }

    /// The same rule with another action.
    #[must_use]
    pub fn with_action(self, action: impl Into<ActionRef>) -> Self {
        Self {
            action: action.into(),
            ..self
        }
    }

    /// The same rule with another trigger.
    #[must_use]
    pub fn with_trigger(self, trigger: impl Into<Trigger>) -> Self {
        Self {
            trigger: trigger.into(),
            ..self
        }
    }

    /// The same rule with another lifetime.
    #[must_use]
    pub fn with_lifetime(self, lifetime: Lifetime) -> Self {
        Self { lifetime, ..self }
    }

    /// The same rule with a dice key of its own, instead of its name.
    #[must_use]
    pub fn with_dice(self, dice: DiceKey) -> Self {
        Self {
            dice: Some(dice),
            ..self
        }
    }

    /// The same rule without a dice key of its own, so its die derives from its name.
    #[must_use]
    pub fn without_dice(self) -> Self {
        Self { dice: None, ..self }
    }

    /// The name of the rule, unique among rules.
    pub fn name(&self) -> &RuleName {
        &self.name
    }

    /// Which calls the rule considers, before its scope.
    pub fn selector(&self) -> &SelectorRef {
        &self.selector
    }

    /// Who the rule applies to.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// What happens when the rule fires.
    pub fn action(&self) -> &ActionRef {
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

    /// The dice key the rule sets, or `None` when its die derives from its name.
    pub fn dice(&self) -> Option<&DiceKey> {
        self.dice.as_ref()
    }

    /// What the die of the rule derives from: the dice key it sets, or else its name.
    pub fn dice_key(&self) -> DiceKey {
        self.dice.clone().unwrap_or_else(|| self.name.dice_key())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    use super::*;
    use crate::PresetName;

    fn name(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    fn order_producers() -> SelectorName {
        "Order producers".parse().unwrap()
    }

    fn delay_1s() -> PresetName {
        "Delay 1 s".parse().unwrap()
    }

    fn slow_down() -> Rule {
        Rule::new(name("Slow down order producers"), order_producers(), delay_1s())
    }

    #[test]
    fn a_new_rule_is_for_all_clients_on_every_match_until_disarmed() {
        let rule = slow_down();

        assert_eq!(rule.name(), "Slow down order producers");
        assert_eq!(rule.selector(), &SelectorRef::Named(order_producers()));
        assert!(rule.scope().is_all());
        assert_eq!(rule.action(), &ActionRef::Preset(delay_1s()));
        assert!(rule.trigger().is_always());
        assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
        assert_eq!(rule.dice(), None);
    }

    #[test]
    fn a_rule_takes_inline_conditions_and_an_inline_action() {
        let metadata = Conditions::from(Condition::is(ApiKey::METADATA));
        let rule = Rule::new(name("Blackhole metadata"), metadata.clone(), LieDefinition::Silence);

        assert_eq!(rule.selector(), &SelectorRef::Inline(metadata));
        assert_eq!(rule.action(), &ActionRef::from(LieDefinition::Silence));
    }

    #[test]
    fn a_rule_changes_one_part_at_a_time() {
        let scope = Scope::client(ClientId::new("order-ingest").unwrap());
        let trigger = Trigger::from(TriggerParts::new().with_chance("40%".parse().unwrap()));
        let lifetime = Lifetime::Duration(TimeSpan::from_secs(30));
        let dice = DiceKey::new("an old name").unwrap();

        let rule = slow_down()
            .scoped(scope.clone())
            .with_trigger(trigger)
            .with_lifetime(lifetime)
            .with_dice(dice.clone());
        assert_eq!(rule.scope(), &scope);
        assert_eq!(rule.trigger(), trigger);
        assert_eq!(rule.lifetime(), lifetime);
        assert_eq!(rule.dice(), Some(&dice));
        assert_eq!(rule.selector(), slow_down().selector());
        assert_eq!(rule.action(), slow_down().action());

        let other = rule
            .clone()
            .with_selector(Conditions::new())
            .with_action(LieDefinition::Silence)
            .with_trigger(Trigger::Manual)
            .without_dice();
        assert_eq!(other.selector(), &SelectorRef::Inline(Conditions::new()));
        assert_eq!(other.action(), &ActionRef::from(LieDefinition::Silence));
        assert!(other.trigger().is_manual());
        assert_eq!(other.dice(), None);
        assert_eq!(other.scope(), rule.scope());
        assert_eq!(other.lifetime(), rule.lifetime());
    }

    #[test]
    fn the_die_derives_from_the_name_unless_the_rule_sets_a_dice_key() {
        let dice = DiceKey::new("an old name").unwrap();

        assert_eq!(slow_down().dice_key().as_str(), "Slow down order producers");
        assert_eq!(slow_down().with_dice(dice.clone()).dice_key(), dice);
    }

    #[test]
    fn a_rename_writes_the_old_name_as_the_dice_key() {
        let renamed = slow_down().renamed(name("Slow ingest"));

        assert_eq!(renamed.name(), "Slow ingest");
        assert_eq!(renamed.dice().map(DiceKey::as_str), Some("Slow down order producers"));
        assert_eq!(renamed.dice_key(), slow_down().dice_key());
    }

    #[test]
    fn a_second_rename_keeps_the_first_name_as_the_dice_key() {
        let renamed = slow_down().renamed(name("Slow ingest")).renamed(name("Slower ingest"));

        assert_eq!(renamed.name(), "Slower ingest");
        assert_eq!(renamed.dice_key(), slow_down().dice_key());
    }

    #[test]
    fn a_rename_keeps_a_dice_key_the_rule_set_itself() {
        let dice = DiceKey::new("shared die").unwrap();
        let renamed = slow_down().with_dice(dice.clone()).renamed(name("Slow ingest"));

        assert_eq!(renamed.dice(), Some(&dice));
    }

    #[test]
    fn a_rename_back_to_the_first_name_needs_no_dice_key() {
        let back = slow_down()
            .renamed(name("Slow ingest"))
            .renamed(name("Slow down order producers"));

        assert_eq!(back, slow_down());
        assert_eq!(slow_down().renamed(name("Slow down order producers")), slow_down());
    }
}
