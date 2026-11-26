//! The holds of the rules of one environment: who holds the pad of a rule, and since which arming.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_rule::{ArmedRule, RuleName};
use gaslightmq_surface::Hold;

/// The holds of the rules of one environment.
///
/// A press in hold adds a hold to the rule of its pad, and its release takes that hold away.
/// The rule disarms when its last hold goes, so it stays armed while anyone still holds its pad.
///
/// A hold belongs to the arming of its rule.
/// A disarm ends that arming, whatever disarmed it: a press in latch, a command, a lifetime, or deleting the rule.
/// The holds of an earlier arming have ended with it, so a release of one changes nothing,
/// also when someone armed the rule again since.
#[derive(Debug, Default)]
pub(crate) struct Holds {
    of: BTreeMap<RuleName, HeldArming>,
}

/// The holds of one arming of a rule.
#[derive(Debug)]
struct HeldArming {
    arming: ArmedRule,
    holds: BTreeSet<Hold>,
}

impl Holds {
    /// Adds `hold` to the rule that is armed as `armed`.
    pub(crate) fn add(&mut self, armed: &ArmedRule, hold: Hold) {
        let fresh = || HeldArming {
            arming: armed.clone(),
            holds: BTreeSet::new(),
        };
        let of_the_rule = self.of.entry(armed.name().clone()).or_insert_with(fresh);
        if !of_the_rule.arming.is(armed) {
            // The holds of an earlier arming ended with it.
            *of_the_rule = fresh();
        }
        of_the_rule.holds.insert(hold);
    }

    /// Takes `hold` away from the rule `name`, which is armed as `armed` now, or not armed at all.
    ///
    /// Returns whether that was the last hold of the arming, so the rule disarms.
    /// A hold that the rule does not have, or that an earlier arming had, changes nothing.
    pub(crate) fn release(&mut self, name: &RuleName, hold: Hold, armed: Option<&ArmedRule>) -> bool {
        let Some(of_the_rule) = self.of.get_mut(name) else {
            return false;
        };
        if !armed.is_some_and(|armed| of_the_rule.arming.is(armed)) {
            // A disarm ended every hold of the rule.
            self.of.remove(name);
            return false;
        }
        if !of_the_rule.holds.remove(&hold) {
            return false;
        }
        let last = of_the_rule.holds.is_empty();
        if last {
            self.of.remove(name);
        }
        last
    }

    /// How many people hold the rule that is armed as `armed`.
    #[cfg(test)]
    fn count(&self, armed: &ArmedRule) -> usize {
        self.of
            .get(armed.name())
            .filter(|held| held.arming.is(armed))
            .map_or(0, |held| held.holds.len())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Instant;
    use gaslightmq_lie::LieDefinition;
    use gaslightmq_rule::{PresetSet, Rule};
    use gaslightmq_selector::{Conditions, SelectorSet};
    use gaslightmq_trigger::Seed;

    use super::*;

    /// The rule whose pad the tests hold: a silence on every call.
    fn silent_metadata() -> Rule {
        Rule::new(
            "blackhole-on-metadata-for-all-clients".parse().unwrap(),
            Conditions::new(),
            LieDefinition::Silence,
        )
    }

    /// One arming of the rule, as arming it again makes another one.
    fn armed() -> ArmedRule {
        silent_metadata()
            .resolve(&SelectorSet::default(), &PresetSet::default())
            .unwrap()
            .arm(Seed::new(42), Instant::ZERO)
    }

    #[test]
    fn a_rule_disarms_when_its_last_hold_goes() {
        let mut holds = Holds::default();
        let arming = armed();
        let (mine, theirs) = (Hold::new(1), Hold::new(2));
        holds.add(&arming, mine);
        holds.add(&arming, theirs);
        assert_eq!(holds.count(&arming), 2);

        assert!(!holds.release(arming.name(), mine, Some(&arming)));
        assert_eq!(holds.count(&arming), 1);
        assert!(holds.release(arming.name(), theirs, Some(&arming)));
        assert_eq!(holds.count(&arming), 0);
    }

    #[test]
    fn a_hold_is_released_once() {
        let mut holds = Holds::default();
        let arming = armed();
        let (mine, theirs) = (Hold::new(1), Hold::new(2));
        holds.add(&arming, mine);
        holds.add(&arming, theirs);

        assert!(!holds.release(arming.name(), mine, Some(&arming)));
        // The second release of the same hold does not take the other one's hold away.
        assert!(!holds.release(arming.name(), mine, Some(&arming)));
        assert_eq!(holds.count(&arming), 1);
    }

    #[test]
    fn a_disarm_ends_every_hold_also_when_someone_arms_the_rule_again() {
        let mut holds = Holds::default();
        let before = armed();
        let (mine, theirs) = (Hold::new(1), Hold::new(2));
        holds.add(&before, mine);

        // Someone disarmed the rule, and someone else holds it again, which arms it anew.
        let again = armed();
        holds.add(&again, theirs);
        assert_eq!(holds.count(&again), 1);

        // The release of the hold of the first arming changes nothing.
        assert!(!holds.release(again.name(), mine, Some(&again)));
        assert_eq!(holds.count(&again), 1);
        assert!(holds.release(again.name(), theirs, Some(&again)));
    }

    #[test]
    fn a_release_of_a_rule_that_is_not_armed_changes_nothing() {
        let mut holds = Holds::default();
        let arming = armed();
        let hold = Hold::new(1);
        holds.add(&arming, hold);

        // A lifetime ended the arming.
        assert!(!holds.release(arming.name(), hold, None));
        assert_eq!(holds.count(&arming), 0);
    }

    #[test]
    fn a_turn_keeps_the_arming_and_its_holds() {
        let mut holds = Holds::default();
        let arming = armed();
        let hold = Hold::new(1);
        holds.add(&arming, hold);

        let turned = arming.retuned(arming.rule().clone());

        assert_eq!(holds.count(&turned), 1);
        assert!(holds.release(turned.name(), hold, Some(&turned)));
    }
}
