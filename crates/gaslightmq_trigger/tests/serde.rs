//! The `serde` form of every definition type, and what reading rejects.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::fmt::Debug;

use gaslightmq_clock::TimeSpan;
use gaslightmq_trigger::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// `json` reads as the value, though the value writes another form.
fn reads<T: DeserializeOwned + PartialEq + Debug>(json: &str, value: &T) {
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

fn chance(text: &str) -> Chance {
    text.parse().unwrap()
}

fn count(value: u64) -> Count {
    Count::new(value).unwrap()
}

fn number(value: u64) -> MatchNumber {
    MatchNumber::new(value).unwrap()
}

fn secs(secs: u64) -> TimeSpan {
    TimeSpan::from_secs(secs)
}

#[test]
fn a_chance_writes_its_text() {
    writes(&chance("40%"), r#""40%""#);
    writes(&chance("0.5%"), r#""0.5%""#);
    writes(&Chance::ALWAYS, r#""100%""#);
    reads(r#""40 %""#, &chance("40%"));
}

#[test]
fn a_bare_number_is_not_a_chance() {
    rejects::<Chance>("40", "expected a percentage such as `40%`");
    rejects::<Chance>("0.4", "expected a percentage such as `40%`");
    rejects::<Chance>(r#""40""#, "`40` is not a percentage such as `40%`");
    rejects::<Chance>(r#""140%""#, "chance `140%` is not between 0% and 100%");
    rejects::<Chance>(r#""0.00001%""#, "chance `0.00001%` has more than four decimals");
}

#[test]
fn counts_match_numbers_and_seeds_write_numbers() {
    writes(&count(10), "10");
    writes(&number(3), "3");
    writes(&Seed::new(42), "42");
    writes(&Seed::new(u64::MAX), "18446744073709551615");
    rejects::<Count>("0", "a count is at least one");
    rejects::<MatchNumber>("0", "a match number is at least one");
    rejects::<Count>("-1", "invalid value");
    rejects::<Count>(r#""10""#, "invalid type");
    rejects::<Seed>("-7", "invalid value");
}

#[test]
fn per_writes_its_name() {
    writes(&Per::Rule, r#""rule""#);
    writes(&Per::Session, r#""session""#);
    rejects::<Per>(
        r#""connection""#,
        "counters are per `rule` or per `session`, not per `connection`",
    );
}

#[test]
fn a_dice_key_writes_its_text() {
    writes(
        &DiceKey::new("slow-order-producers").unwrap(),
        r#""slow-order-producers""#,
    );
    rejects::<DiceKey>(r#""""#, "a dice key is not empty");
}

#[test]
fn a_lifetime_writes_its_text() {
    writes(&Lifetime::UntilDisarmed, r#""until disarmed""#);
    writes(&Lifetime::Duration(secs(30)), r#""30s""#);
    writes(&Lifetime::Matches(count(100)), r#""100 matches""#);
    writes(&Lifetime::Firings(count(3)), r#""3 firings""#);
    writes(&Lifetime::Firings(Count::ONE), r#""1 firing""#);
    reads(r#""30 s""#, &Lifetime::Duration(secs(30)));
    reads(r#""1 firings""#, &Lifetime::Firings(Count::ONE));
}

#[test]
fn a_text_that_is_no_lifetime_is_rejected() {
    rejects::<Lifetime>(r#""forever""#, "`forever` is not a lifetime");
    rejects::<Lifetime>(r#""0 firings""#, "lifetime `0 firings`: a count is at least one");
    rejects::<Lifetime>("30", "expected `until disarmed`, a time span such as `30s`");
    rejects::<Lifetime>(r#"{"firings":3}"#, "expected `until disarmed`");
}

#[test]
fn the_trigger_of_the_rule_in_the_spec_reads_as_a_chance() {
    // `trigger: { chance: 40% }`
    writes(
        &Trigger::from(TriggerParts::new().with_chance(chance("40%"))),
        r#"{"chance":"40%"}"#,
    );
}

#[test]
fn the_triggers_of_the_leader_election_scenario_read_as_parts() {
    // `trigger: { nth: 1 }`
    writes(
        &Trigger::from(TriggerParts::new().with_nth(MatchNumber::FIRST)),
        r#"{"nth":1}"#,
    );
    // `trigger: { first: 2, per: session }`
    writes(
        &Trigger::from(TriggerParts::new().with_first(count(2)).with_per(Per::Session)),
        r#"{"first":2,"per":"session"}"#,
    );
}

#[test]
fn a_combined_trigger_writes_every_part_it_has() {
    // `trigger: { chance: 40%, nth: 3, per: session }`
    writes(
        &Trigger::from(
            TriggerParts::new()
                .with_chance(chance("40%"))
                .with_nth(number(3))
                .with_per(Per::Session),
        ),
        r#"{"chance":"40%","nth":3,"per":"session"}"#,
    );
    writes(
        &TriggerParts::new()
            .with_chance(chance("12.5%"))
            .with_nth(number(3))
            .with_every(count(10))
            .with_first(count(5))
            .with_skip(count(100))
            .with_window(TimeWindow::after(secs(10)))
            .with_per(Per::Session),
        r#"{"chance":"12.5%","nth":3,"every":10,"first":5,"skip":100,"after":"10s","per":"session"}"#,
    );
}

#[test]
fn a_window_writes_after_or_between() {
    writes(
        &Trigger::from(TriggerParts::new().with_window(TimeWindow::after(secs(10)))),
        r#"{"after":"10s"}"#,
    );
    writes(
        &Trigger::from(TriggerParts::new().with_window(TimeWindow::between(secs(10), secs(40)).unwrap())),
        r#"{"between":["10s","40s"]}"#,
    );
}

#[test]
fn a_trigger_without_parts_writes_an_empty_map() {
    writes(&Trigger::always(), "{}");
    writes(&TriggerParts::new(), "{}");
}

#[test]
fn a_trigger_that_counts_per_rule_leaves_per_out() {
    reads(
        r#"{"nth":3,"per":"rule"}"#,
        &Trigger::from(TriggerParts::new().with_nth(number(3))),
    );
    writes(
        &Trigger::from(TriggerParts::new().with_per(Per::Session)),
        r#"{"per":"session"}"#,
    );
}

#[test]
fn a_manual_trigger_writes_the_text_manual() {
    writes(&Trigger::Manual, r#""manual""#);
    rejects::<Trigger>(r#""automatic""#, "expected `manual` or a map of trigger parts");
    rejects::<Trigger>("3", "expected `manual` or a map of trigger parts");
    rejects::<TriggerParts>(r#""manual""#, "expected a map of trigger parts");
}

#[test]
fn the_parts_read_in_any_order() {
    reads(
        r#"{"per":"session","nth":3,"chance":"40%"}"#,
        &Trigger::from(
            TriggerParts::new()
                .with_chance(chance("40%"))
                .with_nth(number(3))
                .with_per(Per::Session),
        ),
    );
}

#[test]
fn an_unknown_part_is_rejected() {
    rejects::<Trigger>(r#"{"chanse":"40%"}"#, "unknown field `chanse`");
    rejects::<Trigger>(r#"{"manual":true}"#, "unknown field `manual`");
    rejects::<Trigger>(
        r#"{"nht":3}"#,
        "expected one of `chance`, `nth`, `every`, `first`, `skip`, `after`, `between`, `per`",
    );
}

#[test]
fn a_part_named_twice_is_rejected() {
    rejects::<Trigger>(r#"{"nth":3,"nth":4}"#, "duplicate field `nth`");
    rejects::<Trigger>(r#"{"after":"10s","after":"20s"}"#, "duplicate field `after`");
}

#[test]
fn two_time_windows_are_rejected() {
    let reason = "a trigger has one time window: `after` or `between`, not both";
    rejects::<Trigger>(r#"{"after":"10s","between":["10s","40s"]}"#, reason);
    rejects::<Trigger>(r#"{"between":["10s","40s"],"after":"10s"}"#, reason);
}

#[test]
fn a_part_with_an_invalid_value_is_rejected() {
    rejects::<Trigger>(r#"{"chance":40}"#, "expected a percentage such as `40%`");
    rejects::<Trigger>(r#"{"nth":0}"#, "a match number is at least one");
    rejects::<Trigger>(r#"{"every":0}"#, "a count is at least one");
    rejects::<Trigger>(r#"{"first":"two"}"#, "invalid type");
    rejects::<Trigger>(r#"{"after":"10"}"#, "invalid time span `10`");
    rejects::<Trigger>(
        r#"{"between":["40s","10s"]}"#,
        "a window between 40s and 10s never opens",
    );
    rejects::<Trigger>(r#"{"between":["10s"]}"#, "invalid length 1");
    rejects::<Trigger>(r#"{"between":["10s","40s","50s"]}"#, "trailing characters");
    rejects::<Trigger>(r#"{"per":"connection"}"#, "not per `connection`");
}

#[test]
fn a_rule_keeps_its_trigger_its_lifetime_and_its_dice_key() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Rule {
        name: String,
        #[serde(default, skip_serializing_if = "Trigger::is_always")]
        trigger: Trigger,
        #[serde(default)]
        lifetime: Lifetime,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dice: Option<DiceKey>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Scenario {
        seed: Seed,
        rules: Vec<Rule>,
    }

    let json = concat!(
        r#"{"seed":42,"rules":["#,
        r#"{"name":"slow-order-producers","trigger":{"chance":"40%"},"lifetime":"until disarmed"},"#,
        r#"{"name":"stale-metadata-v2","trigger":{"first":2,"per":"session"},"lifetime":"3 firings","dice":"stale-metadata"},"#,
        r#"{"name":"leader-moves-away","trigger":"manual","lifetime":"30s"},"#,
        r#"{"name":"delay-everything","lifetime":"100 matches"}"#,
        r#"]}"#,
    );
    let scenario: Scenario = serde_json::from_str(json).unwrap();
    assert_eq!(
        scenario,
        Scenario {
            seed: Seed::new(42),
            rules: vec![
                Rule {
                    name: "slow-order-producers".into(),
                    trigger: TriggerParts::new().with_chance(chance("40%")).into(),
                    lifetime: Lifetime::UntilDisarmed,
                    dice: None,
                },
                Rule {
                    name: "stale-metadata-v2".into(),
                    trigger: TriggerParts::new().with_first(count(2)).with_per(Per::Session).into(),
                    lifetime: Lifetime::Firings(count(3)),
                    dice: Some(DiceKey::new("stale-metadata").unwrap()),
                },
                Rule {
                    name: "leader-moves-away".into(),
                    trigger: Trigger::Manual,
                    lifetime: Lifetime::Duration(secs(30)),
                    dice: None,
                },
                Rule {
                    name: "delay-everything".into(),
                    trigger: Trigger::always(),
                    lifetime: Lifetime::Matches(count(100)),
                    dice: None,
                },
            ],
        }
    );
    assert_eq!(serde_json::to_string(&scenario).unwrap(), json);

    // A rule that names no trigger and no lifetime fires on every matching call, until disarmed.
    let bare: Rule = serde_json::from_str(r#"{"name":"delay-everything"}"#).unwrap();
    assert!(bare.trigger.is_always());
    assert_eq!(bare.lifetime, Lifetime::UntilDisarmed);
}
