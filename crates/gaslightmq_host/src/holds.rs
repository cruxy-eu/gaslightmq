//! The holds of the rules of one environment: who holds the pad of a rule, and since which arming.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_rule::{ArmedRule, RuleName};
use gaslightmq_surface::Hold;

/// The holds of the rules of one environment.
///
/// A press in hold adds a hold to the rule of its pad, and its release takes that hold away.
/// A rule that holds armed disarms when its last hold goes, so it stays armed while anyone still holds its pad.
/// A rule that something else armed, a press in latch, a command, a scenario step or a create armed, stays armed when
/// its holds go: a release never disarms what a hold did not arm.
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
    /// Whether a press in hold armed the arming, so it ends when its last hold goes.
    armed_by_holds: bool,
}

impl Holds {
    /// Adds `hold` to the rule that is armed as `armed`. `armed_it` says whether the press of the hold armed the rule.
    ///
    /// The arming disarms with its last hold once a press in hold armed it.
    pub(crate) fn add(&mut self, armed: &ArmedRule, hold: Hold, armed_it: bool) {
        let fresh = || HeldArming {
            arming: armed.clone(),
            holds: BTreeSet::new(),
            armed_by_holds: false,
        };
        let of_the_rule = self.of.entry(armed.name().clone()).or_insert_with(fresh);
        if !of_the_rule.arming.is(armed) {
            // The holds of an earlier arming ended with it.
            *of_the_rule = fresh();
        }
        of_the_rule.holds.insert(hold);
        of_the_rule.armed_by_holds |= armed_it;
    }

    /// Takes `hold` away from the rule `name`, which is armed as `armed` now, or not armed at all.
    ///
    /// Returns whether that was the last hold of an arming that holds armed, so the rule disarms.
    /// A hold that the rule does not have, or that an earlier arming had, changes nothing,
    /// and so does the last hold of a rule that something else armed.
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
        if !of_the_rule.holds.is_empty() {
            return false;
        }
        let armed_by_holds = of_the_rule.armed_by_holds;
        self.of.remove(name);
        armed_by_holds
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
        holds.add(&arming, mine, true);
        holds.add(&arming, theirs, false);
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
        holds.add(&arming, mine, true);
        holds.add(&arming, theirs, false);

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
        holds.add(&before, mine, true);

        // Someone disarmed the rule, and someone else holds it again, which arms it anew.
        let again = armed();
        holds.add(&again, theirs, true);
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
        holds.add(&arming, hold, true);

        // A lifetime ended the arming.
        assert!(!holds.release(arming.name(), hold, None));
        assert_eq!(holds.count(&arming), 0);
    }

    #[test]
    fn a_turn_keeps_the_arming_and_its_holds() {
        let mut holds = Holds::default();
        let arming = armed();
        let hold = Hold::new(1);
        holds.add(&arming, hold, true);

        let turned = arming.retuned(arming.rule().clone());

        assert_eq!(holds.count(&turned), 1);
        assert!(holds.release(turned.name(), hold, Some(&turned)));
    }

    #[test]
    fn the_last_hold_of_a_rule_that_something_else_armed_leaves_it_armed() {
        let mut holds = Holds::default();
        // Someone armed the rule in latch, and someone else holds its pad.
        let latched = armed();
        let hold = Hold::new(1);
        holds.add(&latched, hold, false);

        assert!(!holds.release(latched.name(), hold, Some(&latched)));
        assert_eq!(holds.count(&latched), 0);
    }

    #[test]
    fn a_rule_that_one_hold_armed_disarms_with_its_last_hold_whoever_held_it_first() {
        let mut holds = Holds::default();
        let arming = armed();
        let (joined, armed_it) = (Hold::new(1), Hold::new(2));
        // Two holds come at once: the one that armed the rule adds itself after the other one.
        holds.add(&arming, joined, false);
        holds.add(&arming, armed_it, true);

        assert!(!holds.release(arming.name(), armed_it, Some(&arming)));
        assert!(holds.release(arming.name(), joined, Some(&arming)));
    }
}
