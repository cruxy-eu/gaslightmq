use gaslightmq_selector::{Scope, SelectorRef};
use gaslightmq_trigger::{DiceKey, Lifetime, Trigger};

use crate::{ActionRef, RuleName, RuleTitle};

/// A rule as a file, a scenario or the UI writes it: what happens to which calls, when, and for how long.
///
/// | Part | Meaning | When left out |
/// |---|---|---|
/// | Name | The identity of the rule: a slug, unique among rules. Other things reference the rule by it. | |
/// | Title | What the rule is for, in free text. | No title. |
/// | Selector | Which calls the rule considers: a selector name, or inline conditions. | |
/// | Scope | Extra conditions over connection fields: who the rule applies to. | All clients. |
/// | Action | What happens when the rule fires: a preset name, or an inline lie or incident. | |
/// | Trigger | When the rule fires among the calls its selector matches. | Every matching call. |
/// | Lifetime | How long the rule stays armed. | Until disarmed. |
/// | Dice key | What the rule's die derives from. | The name of the rule. |
///
/// ```yaml
/// name: slow-order-producers
/// title: Slow down order producers
/// selector: order-producers
/// scope:
///   client.id: order-ingest
/// action: delay-1s
/// trigger:
///   chance: 40%
/// lifetime: until disarmed
/// ```
///
/// A rule holds what someone defined, and nothing that GaslightMQ assigns.
/// The [`RuleUuid`](crate::RuleUuid) of a rule is live state: the workspace keeps it in memory for a rule of the
/// workspace, and an environment for a live rule. No rule and no file holds it.
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
///     "slow-order-producers".parse()?,
///     "order-producers".parse::<SelectorName>().expect("a selector name"),
///     "delay-1s".parse::<PresetName>().expect("a preset name"),
/// )
/// .with_title("Slow down order producers".parse().expect("a rule title"))
/// .scoped(Scope::client(ClientId::new("order-ingest").expect("a client ID")))
/// .with_trigger(TriggerParts::new().with_chance("40%".parse().expect("a chance")));
///
/// // The name is the identity of the rule, and the title says what the rule is for.
/// assert_eq!(rule.name(), "slow-order-producers");
/// assert_eq!(rule.title().map(RuleTitle::as_str), Some("Slow down order producers"));
/// assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
/// assert_eq!(rule.dice_key().as_str(), "slow-order-producers");
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Rule {
    name: RuleName,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    title: Option<RuleTitle>,
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
    /// It has no title.
    pub fn new(name: RuleName, selector: impl Into<SelectorRef>, action: impl Into<ActionRef>) -> Self {
        Self {
            name,
            title: None,
            selector: selector.into(),
            scope: Scope::all(),
            action: action.into(),
            trigger: Trigger::always(),
            lifetime: Lifetime::UntilDisarmed,
            dice: None,
        }
    }

    /// The same rule with a title, which says what the rule is for.
    /// It replaces the title the rule had before.
    #[must_use]
    pub fn with_title(self, title: RuleTitle) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// The same rule without a title.
    #[must_use]
    pub fn without_title(self) -> Self {
        Self { title: None, ..self }
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

    /// The name of the rule, unique among rules. It is the identity of the rule.
    pub fn name(&self) -> &RuleName {
        &self.name
    }

    /// What the rule is for, in free text, when someone wrote it down.
    pub fn title(&self) -> Option<&RuleTitle> {
        self.title.as_ref()
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
        "order-producers".parse().unwrap()
    }

    fn delay_1s() -> PresetName {
        "delay-1s".parse().unwrap()
    }

    fn slow_down() -> Rule {
        Rule::new(name("slow-order-producers"), order_producers(), delay_1s())
    }

    #[test]
    fn a_new_rule_is_for_all_clients_on_every_match_until_disarmed() {
        let rule = slow_down();

        assert_eq!(rule.name(), "slow-order-producers");
        assert_eq!(rule.selector(), &SelectorRef::Named(order_producers()));
        assert!(rule.scope().is_all());
        assert_eq!(rule.action(), &ActionRef::Preset(delay_1s()));
        assert!(rule.trigger().is_always());
        assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
        assert_eq!(rule.dice(), None);
        assert_eq!(rule.title(), None);
    }

    #[test]
    fn a_rule_takes_inline_conditions_and_an_inline_action() {
        let metadata = Conditions::from(Condition::is(ApiKey::METADATA));
        let rule = Rule::new(name("blackhole-metadata"), metadata.clone(), LieDefinition::Silence);

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

        assert_eq!(slow_down().dice_key().as_str(), "slow-order-producers");
        assert_eq!(slow_down().with_dice(dice.clone()).dice_key(), dice);
    }

    #[test]
    fn a_rule_has_a_title_when_someone_gives_it_one() {
        let title = |text: &str| text.parse::<RuleTitle>().unwrap();
        let titled = slow_down().with_title(title("Slow down order producers"));

        assert_eq!(slow_down().title(), None);
        assert_eq!(titled.title(), Some(&title("Slow down order producers")));
        assert_eq!(
            titled.clone().with_title(title("Slow ingest")).title(),
            Some(&title("Slow ingest"))
        );
        assert_eq!(titled.without_title(), slow_down());
    }

    #[test]
    fn a_title_is_no_part_of_the_identity_of_a_rule() {
        let titled = slow_down().with_title("Slow down order producers".parse().unwrap());

        assert_eq!(titled.name(), slow_down().name());
        assert_eq!(titled.dice_key(), slow_down().dice_key());
    }
}
