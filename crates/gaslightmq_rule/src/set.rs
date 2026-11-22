use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fmt;

use crate::{Rule, RuleName};

/// Rules by name: what a scenario, a step or a pad references a rule in.
///
/// Every name is there once, and [`new`](Self::new) checks that.
/// Rule names are unique among all rules, whether a scenario owns the rule or the workspace does,
/// so whoever assembles the rules of a workspace puts all of them in one set.
///
/// A set does not change, apart from a [rename](Self::rename): build a new one from the rules after an edit.
/// It keeps no order of its own, and no state.
/// The order in which rules win a conflict comes from the live set and the scenario's mode,
/// and a [`Snapshot`](crate::Snapshot) takes its rules in that order.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let metadata = Conditions::from(Condition::is(ApiKey::METADATA));
/// let blackhole = Rule::new("Blackhole metadata".parse()?, metadata, LieDefinition::Silence);
/// let mut rules = RuleSet::new([blackhole])?;
///
/// rules.rename(&"Blackhole metadata".parse()?, "Silent metadata".parse()?)?;
/// let rule = rules.get(&"Silent metadata".parse()?).expect("the rule has its new name");
/// assert_eq!(rule.dice_key().as_str(), "Blackhole metadata");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleSet {
    by_name: BTreeMap<RuleName, Rule>,
}

impl RuleSet {
    /// The set of those rules.
    ///
    /// # Errors
    ///
    /// [`RuleSetError::DuplicateName`] when two rules have one name.
    pub fn new(rules: impl IntoIterator<Item = Rule>) -> Result<Self, RuleSetError> {
        let mut by_name = BTreeMap::new();
        for rule in rules {
            if let Some(twice) = by_name.insert(rule.name().clone(), rule) {
                return Err(RuleSetError::DuplicateName {
                    name: twice.name().clone(),
                });
            }
        }
        Ok(Self { by_name })
    }

    /// The rule of this name.
    pub fn get(&self, name: &RuleName) -> Option<&Rule> {
        self.by_name.get(name)
    }

    /// Whether a rule has this name.
    pub fn contains(&self, name: &RuleName) -> bool {
        self.by_name.contains_key(name)
    }

    /// The rules in the order of their names.
    pub fn iter(&self) -> btree_map::Values<'_, RuleName, Rule> {
        self.by_name.values()
    }

    /// How many rules there are.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether there is no rule.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Renames a rule, which [keeps its die](Rule::renamed).
    ///
    /// What references the rule from outside the set, such as a scenario or a pad, is for whoever holds it to update.
    ///
    /// # Errors
    ///
    /// [`RuleSetError::UnknownRule`] when no rule is named `from`,
    /// and [`RuleSetError::DuplicateName`] when another rule is named `to` already.
    pub fn rename(&mut self, from: &RuleName, to: RuleName) -> Result<(), RuleSetError> {
        if !self.contains(from) {
            return Err(RuleSetError::UnknownRule { name: from.clone() });
        }
        if *from == to {
            return Ok(());
        }
        if self.contains(&to) {
            return Err(RuleSetError::DuplicateName { name: to });
        }
        if let Some(rule) = self.by_name.remove(from) {
            self.by_name.insert(to.clone(), rule.renamed(to));
        }
        Ok(())
    }
}

impl<'a> IntoIterator for &'a RuleSet {
    type Item = &'a Rule;
    type IntoIter = btree_map::Values<'a, RuleName, Rule>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for RuleSet {
    type Item = Rule;
    type IntoIter = btree_map::IntoValues<RuleName, Rule>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_name.into_values()
    }
}

/// Why rules are not a [`RuleSet`], or why the set does not have what it is asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuleSetError {
    /// Two rules have the same name.
    DuplicateName { name: RuleName },
    /// No rule of the set has this name.
    UnknownRule { name: RuleName },
}

impl fmt::Display for RuleSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateName { name } => write!(f, "two rules are named {:?}", name.as_str()),
            Self::UnknownRule { name } => write!(f, "no rule is named {:?}", name.as_str()),
        }
    }
}

impl std::error::Error for RuleSetError {}

#[cfg(test)]
mod tests {
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;

    use super::*;

    fn name(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    fn rule(text: &str) -> Rule {
        Rule::new(name(text), Conditions::new(), LieDefinition::Silence)
    }

    #[test]
    fn a_set_holds_its_rules_by_name() {
        let set = RuleSet::new([rule("Stale metadata"), rule("Leader moves away")]).unwrap();

        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
        assert!(set.contains(&name("Stale metadata")));
        assert!(!set.contains(&name("stale metadata")));
        assert_eq!(set.get(&name("Leader moves away")), Some(&rule("Leader moves away")));
        assert_eq!(set.get(&name("Background jitter")), None);
        assert_eq!(
            set.iter().map(Rule::name).collect::<Vec<_>>(),
            [&name("Leader moves away"), &name("Stale metadata")]
        );
        assert_eq!((&set).into_iter().count(), 2);
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            [rule("Leader moves away"), rule("Stale metadata")]
        );
    }

    #[test]
    fn an_empty_set_is_a_set() {
        let set = RuleSet::new([]).unwrap();

        assert!(set.is_empty());
        assert_eq!(set, RuleSet::default());
    }

    #[test]
    fn two_rules_with_one_name_are_an_error() {
        assert_eq!(
            RuleSet::new([rule("A"), rule("B"), rule("A")]),
            Err(RuleSetError::DuplicateName { name: name("A") })
        );
    }

    #[test]
    fn renaming_a_rule_keeps_its_die() {
        let mut set = RuleSet::new([rule("A"), rule("B")]).unwrap();

        set.rename(&name("A"), name("C")).unwrap();

        assert!(!set.contains(&name("A")));
        assert_eq!(set.get(&name("C")), Some(&rule("A").renamed(name("C"))));
        assert_eq!(set.get(&name("C")).map(Rule::dice_key), Some(rule("A").dice_key()));
        assert_eq!(set.get(&name("B")), Some(&rule("B")));
    }

    #[test]
    fn renaming_needs_a_rule_and_a_free_name() {
        let mut set = RuleSet::new([rule("A"), rule("B")]).unwrap();
        let before = set.clone();

        assert_eq!(
            set.rename(&name("X"), name("Y")),
            Err(RuleSetError::UnknownRule { name: name("X") })
        );
        assert_eq!(
            set.rename(&name("X"), name("A")),
            Err(RuleSetError::UnknownRule { name: name("X") })
        );
        assert_eq!(
            set.rename(&name("A"), name("B")),
            Err(RuleSetError::DuplicateName { name: name("B") })
        );
        assert_eq!(set.rename(&name("A"), name("A")), Ok(()));
        assert_eq!(set, before);
    }
}
