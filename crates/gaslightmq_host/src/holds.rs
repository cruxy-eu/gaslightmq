//! The holds of the rules of one environment: who holds the pad of a rule, and since which arming.

use std::collections::BTreeMap;

use gaslightmq_rule::{ArmedRule, RuleName};
use gaslightmq_surface::{Hold, PresenceId};

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
///
/// A hold belongs to the presence that its press came from, when it came from one.
/// A presence that ended never sends a release, so [its holds end with it](Self::end_of).
/// A hold that came from no presence ends by its release or by a disarm alone.
#[derive(Debug, Default)]
pub(crate) struct Holds {
    of: BTreeMap<RuleName, HeldArming>,
}

/// The holds of one arming of a rule.
#[derive(Debug)]
struct HeldArming {
    arming: ArmedRule,
    /// The holds, each with the presence that its press came from, when it came from one.
    holds: BTreeMap<Hold, Option<PresenceId>>,
    /// Whether a press in hold armed the arming, so it ends when its last hold goes.
    armed_by_holds: bool,
}

impl Holds {
    /// Adds `hold` to the rule that is armed as `armed`. `armed_it` says whether the press of the hold armed the rule,
    /// and `of` names the presence that the press came from, when it came from one.
    ///
    /// The arming disarms with its last hold once a press in hold armed it.
    pub(crate) fn add(&mut self, armed: &ArmedRule, hold: Hold, armed_it: bool, of: Option<PresenceId>) {
        let fresh = || HeldArming {
            arming: armed.clone(),
            holds: BTreeMap::new(),
            armed_by_holds: false,
        };
        let of_the_rule = self.of.entry(armed.name().clone()).or_insert_with(fresh);
        if !of_the_rule.arming.is(armed) {
            // The holds of an earlier arming ended with it.
            *of_the_rule = fresh();
        }
        of_the_rule.holds.insert(hold, of);
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
        if of_the_rule.holds.remove(&hold).is_none() {
            return false;
        }
        if !of_the_rule.holds.is_empty() {
            return false;
        }
        let armed_by_holds = of_the_rule.armed_by_holds;
        self.of.remove(name);
        armed_by_holds
    }

    /// Takes away every hold of `presence`, which ended. `armed` gives the arming that a rule has now,
    /// or `None` for a rule that is not armed.
    ///
    /// Returns the rules that disarm: those that holds armed, and whose last hold this took away.
    /// A rule that someone else still holds stays armed, and so does a rule that something else armed.
    pub(crate) fn end_of(
        &mut self,
        presence: PresenceId,
        armed: impl Fn(&RuleName) -> Option<ArmedRule>,
    ) -> Vec<RuleName> {
        let mut disarms = Vec::new();
        self.of.retain(|name, of_the_rule| {
            if !armed(name).is_some_and(|armed| of_the_rule.arming.is(&armed)) {
                // A disarm ended every hold of the rule.
                return false;
            }
            of_the_rule.holds.retain(|_, of| *of != Some(presence));
            if !of_the_rule.holds.is_empty() {
                return true;
            }
            if of_the_rule.armed_by_holds {
                disarms.push(name.clone());
            }
            false
        });
        disarms
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
        silence("blackhole-on-metadata-for-all-clients")
    }

    /// A rule of that name: a silence on every call.
    fn silence(name: &str) -> Rule {
        Rule::new(name.parse().unwrap(), Conditions::new(), LieDefinition::Silence)
    }

    /// One arming of the rule, as arming it again makes another one.
    fn armed() -> ArmedRule {
        arming_of(&silent_metadata())
    }

    /// One arming of `rule`.
    fn arming_of(rule: &Rule) -> ArmedRule {
        rule.resolve(&SelectorSet::default(), &PresetSet::default())
            .unwrap()
            .arm(Seed::new(42), Instant::ZERO)
    }

    /// The presences of two tabs of the UI.
    const ANA: PresenceId = PresenceId::from_random_bits(0xa);
    const DAN: PresenceId = PresenceId::from_random_bits(0xd);

    #[test]
    fn a_rule_disarms_when_its_last_hold_goes() {
        let mut holds = Holds::default();
        let arming = armed();
        let (mine, theirs) = (Hold::new(1), Hold::new(2));
        holds.add(&arming, mine, true, None);
        holds.add(&arming, theirs, false, None);
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
        holds.add(&arming, mine, true, None);
        holds.add(&arming, theirs, false, None);

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
        holds.add(&before, mine, true, None);

        // Someone disarmed the rule, and someone else holds it again, which arms it anew.
        let again = armed();
        holds.add(&again, theirs, true, None);
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
        holds.add(&arming, hold, true, None);

        // A lifetime ended the arming.
        assert!(!holds.release(arming.name(), hold, None));
        assert_eq!(holds.count(&arming), 0);
    }

    #[test]
    fn a_turn_keeps_the_arming_and_its_holds() {
        let mut holds = Holds::default();
        let arming = armed();
        let hold = Hold::new(1);
        holds.add(&arming, hold, true, None);

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
        holds.add(&latched, hold, false, None);

        assert!(!holds.release(latched.name(), hold, Some(&latched)));
        assert_eq!(holds.count(&latched), 0);
    }

    #[test]
    fn a_rule_that_one_hold_armed_disarms_with_its_last_hold_whoever_held_it_first() {
        let mut holds = Holds::default();
        let arming = armed();
        let (joined, armed_it) = (Hold::new(1), Hold::new(2));
        // Two holds come at once: the one that armed the rule adds itself after the other one.
        holds.add(&arming, joined, false, None);
        holds.add(&arming, armed_it, true, None);

        assert!(!holds.release(arming.name(), armed_it, Some(&arming)));
        assert!(holds.release(arming.name(), joined, Some(&arming)));
    }

    #[test]
    fn a_presence_that_ends_takes_its_holds_away_and_the_rule_disarms_with_its_last_hold() {
        let mut holds = Holds::default();
        let arming = armed();
        let (anas, dans) = (Hold::new(1), Hold::new(2));
        holds.add(&arming, anas, true, Some(ANA));
        holds.add(&arming, dans, false, Some(DAN));
        let armed_now = |_: &RuleName| Some(arming.clone());

        // Ana's tab is gone, and Dan still holds the pad.
        assert_eq!(holds.end_of(ANA, armed_now), Vec::<RuleName>::new());
        assert_eq!(holds.count(&arming), 1);
        // Dan's tab is gone too, and nobody holds the pad any more.
        assert_eq!(holds.end_of(DAN, armed_now), [arming.name().clone()]);
        assert_eq!(holds.count(&arming), 0);
    }

    #[test]
    fn a_presence_that_ends_leaves_the_holds_of_no_presence() {
        let mut holds = Holds::default();
        let arming = armed();
        let (anas, of_the_cli) = (Hold::new(1), Hold::new(2));
        holds.add(&arming, anas, true, Some(ANA));
        // `gmq` holds the pad too, and it is no presence: its hold ends by its release.
        holds.add(&arming, of_the_cli, false, None);

        assert_eq!(holds.end_of(ANA, |_| Some(arming.clone())), Vec::<RuleName>::new());
        assert_eq!(holds.count(&arming), 1);
        assert!(holds.release(arming.name(), of_the_cli, Some(&arming)));
    }

    #[test]
    fn a_presence_that_ends_takes_its_holds_of_every_rule_away() {
        let mut holds = Holds::default();
        let (metadata, fetch) = (armed(), arming_of(&silence("blackhole-on-fetch-for-all-clients")));
        holds.add(&metadata, Hold::new(1), true, Some(ANA));
        holds.add(&fetch, Hold::new(2), true, Some(ANA));
        let armed_now = |name: &RuleName| {
            [&metadata, &fetch]
                .into_iter()
                .find(|armed| armed.name() == name)
                .cloned()
        };

        let disarms = holds.end_of(ANA, armed_now);

        assert_eq!(disarms, [fetch.name().clone(), metadata.name().clone()]);
    }

    #[test]
    fn a_presence_that_ends_never_disarms_a_rule_that_something_else_armed() {
        let mut holds = Holds::default();
        // Someone armed the rule in latch, and Ana holds its pad.
        let latched = armed();
        holds.add(&latched, Hold::new(1), false, Some(ANA));

        assert_eq!(holds.end_of(ANA, |_| Some(latched.clone())), Vec::<RuleName>::new());
        assert_eq!(holds.count(&latched), 0);
    }

    #[test]
    fn a_presence_that_ends_after_a_disarm_changes_nothing_also_when_someone_holds_the_rule_again() {
        let mut holds = Holds::default();
        let before = armed();
        holds.add(&before, Hold::new(1), true, Some(ANA));
        // Someone disarmed the rule, and Dan holds it again, which arms it anew.
        let again = armed();
        holds.add(&again, Hold::new(2), true, Some(DAN));

        assert_eq!(holds.end_of(ANA, |_| Some(again.clone())), Vec::<RuleName>::new());
        assert_eq!(holds.count(&again), 1);
    }
}
