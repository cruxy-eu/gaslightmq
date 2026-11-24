//! Dice are reproducible: fixed rolls for a fixed seed, and independent dice for each rule and each session.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_trigger::prelude::*;

const RULE: &str = "Slow down order producers";

fn die(seed: u64, key: &str) -> Die {
    Die::new(Seed::new(seed), &DiceKey::new(key).unwrap())
}

fn session(key: &str) -> CounterKey {
    CounterKey::new(key).unwrap()
}

/// What the die rolls for the first ten matches, in parts per million.
fn first_ten(die: Die) -> Vec<u32> {
    (1..=10)
        .map(|number| die.roll(MatchNumber::new(number).unwrap()).ppm())
        .collect()
}

/// These rolls are part of the format of a scenario.
/// A change to them changes which calls every recorded scenario hits, so they are written out here.
#[test]
fn a_fixed_seed_gives_fixed_rolls() {
    assert_eq!(
        first_ten(die(42, RULE)),
        [
            444_610, 116_519, 738_645, 485_064, 625_689, 346_021, 797_170, 120_951, 622_165, 735_343
        ]
    );
    assert_eq!(
        first_ten(die(7, RULE)),
        [
            753_565, 21_251, 952_322, 105_838, 48_225, 55_816, 843_911, 617_819, 594_674, 654_806
        ]
    );
    assert_eq!(
        first_ten(die(0, RULE)),
        [
            686_099, 136_496, 573_885, 616_034, 210_536, 482_267, 260_528, 963_829, 913_958, 546_414
        ]
    );
}

#[test]
fn the_same_seed_makes_the_same_match_numbers_fire() {
    let forty: Chance = "40%".parse().unwrap();
    let fires = |die: Die| -> Vec<u64> {
        (1..=10)
            .filter(|&number| die.roll(MatchNumber::new(number).unwrap()).is_under(forty))
            .collect()
    };
    assert_eq!(fires(die(42, RULE)), [2, 6, 8]);
    assert_eq!(fires(die(42, RULE)), [2, 6, 8]);
}

#[test]
fn another_seed_gives_other_rolls() {
    assert_eq!(
        first_ten(die(43, RULE)),
        [
            520_213, 175_420, 556_300, 66_907, 611_482, 221_892, 239_098, 573_584, 771_794, 818_710
        ]
    );
    assert_ne!(first_ten(die(42, RULE)), first_ten(die(43, RULE)));
}

#[test]
fn each_rule_rolls_its_own_die() {
    assert_eq!(
        first_ten(die(42, "Stale metadata")),
        [
            769_853, 870_319, 430_350, 554_613, 417_286, 300_844, 950_848, 804_341, 274_410, 148_549
        ]
    );
    assert_ne!(die(42, RULE), die(42, "Stale metadata"));
}

#[test]
fn a_rule_that_keeps_its_dice_key_keeps_its_rolls() {
    // A new rule that takes the place of another sets `dice` to the name of the other.
    let of_the_name = die(42, RULE);
    let of_the_dice_key = Die::new(Seed::new(42), &RULE.parse::<DiceKey>().unwrap());
    assert_eq!(first_ten(of_the_name), first_ten(of_the_dice_key));
}

#[test]
fn each_session_rolls_its_own_die_with_a_counter_key() {
    let rule = die(42, RULE);
    assert_eq!(
        first_ten(rule.for_counter(&session("10.0.4.27 broker 2 #1"))),
        [
            83_381, 155_059, 459_607, 805_339, 446_690, 530_089, 876_899, 541_323, 576_212, 51_968
        ]
    );
    assert_eq!(
        first_ten(rule.for_counter(&session("10.0.4.27 broker 2 #2"))),
        [
            679_795, 256_125, 635_918, 973_970, 345_809, 6_858, 80_516, 847_472, 587_636, 972_222
        ]
    );
}

#[test]
fn a_roll_does_not_depend_on_the_order_of_the_rolls() {
    let die = die(42, RULE);
    let number = |value| MatchNumber::new(value).unwrap();
    let tenth_first = die.roll(number(10));
    let first = die.roll(number(1));
    assert_eq!(first.ppm(), 444_610);
    assert_eq!(tenth_first.ppm(), 735_343);
    assert_eq!(die.roll(number(10)), tenth_first);
}

#[test]
fn a_die_rolls_every_chance_about_as_often_as_it_says() {
    let die = die(42, RULE);
    let matches = 200_000_u64;
    for percent in [1_u8, 10, 40, 50, 90] {
        let chance = Chance::from_percent(percent).unwrap();
        let fired = (1..=matches)
            .filter(|&number| die.roll(MatchNumber::new(number).unwrap()).is_under(chance))
            .count() as u64;
        let expected = matches * u64::from(percent) / 100;
        assert!(
            fired.abs_diff(expected) < matches / 200,
            "{percent}% fired {fired} times of {matches}"
        );
    }
}

#[test]
fn a_die_never_fires_at_zero_percent_and_always_at_one_hundred() {
    let die = die(42, RULE);
    for number in 1..=10_000 {
        let roll = die.roll(MatchNumber::new(number).unwrap());
        assert!(!roll.is_under(Chance::NEVER));
        assert!(roll.is_under(Chance::ALWAYS));
    }
}
