use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::{Rule, RuleName};

/// Rules by name: what a scenario, a step or a pad references a rule in.
///
/// Every name is there once, and [`new`](Self::new) checks that.
/// Rule names are unique among all rules, whether a scenario owns the rule or the workspace does,
/// so whoever assembles the rules of a workspace puts all of them in one set.
///
/// A set does not change: build a new one from the rules after an edit.
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
/// let blackhole = Rule::new("blackhole-metadata".parse().expect("a rule name"), metadata, LieDefinition::Silence);
/// let rules = RuleSet::new([blackhole.clone()])?;
/// assert_eq!(rules.get(blackhole.name()), Some(&blackhole));
///
/// // A rule keeps its name, and two rules never share one.
/// assert_eq!(
///     RuleSet::new([blackhole.clone(), blackhole.clone()]),
///     Err(RuleSetError::DuplicateName { name: blackhole.name().clone() })
/// );
/// # Ok::<(), RuleSetError>(())
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

/// Why rules are not a [`RuleSet`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleSetError {
    /// Two rules have the same name.
    #[error("two rules are named {:?}", .name.as_str())]
    DuplicateName { name: RuleName },
}

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
        let set = RuleSet::new([rule("stale-metadata"), rule("leader-moves-away")]).unwrap();

        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
        assert!(set.contains(&name("stale-metadata")));
        assert_eq!(set.get(&name("leader-moves-away")), Some(&rule("leader-moves-away")));
        assert_eq!(set.get(&name("background-jitter")), None);
        assert_eq!(
            set.iter().map(Rule::name).collect::<Vec<_>>(),
            [&name("leader-moves-away"), &name("stale-metadata")]
        );
        assert_eq!((&set).into_iter().count(), 2);
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            [rule("leader-moves-away"), rule("stale-metadata")]
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
            RuleSet::new([rule("a"), rule("b"), rule("a")]),
            Err(RuleSetError::DuplicateName { name: name("a") })
        );
    }
}
