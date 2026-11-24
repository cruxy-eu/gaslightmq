//! The encoder mapping: what a turn of each of the eight encoders does to a rule.
//!
//! Every test applies turns to a rule as a pad creates it, and compares the rule that comes back
//! with the rule that someone would have written by hand.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::TimeSpan;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_surface::prelude::*;
use gaslightmq_trigger::prelude::*;

/// The presets that the columns of a layout name, and the rules that pads create from them.
struct Presets;

impl Presets {
    /// The delay and the jitter of `delay-1s`.
    const DELAY: TimeSpan = TimeSpan::from_secs(1);
    const JITTER: TimeSpan = TimeSpan::from_millis(200);
    /// The error of `leader-flap`.
    const FLAP: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
    const DELAY_1S: &'static str = "delay-1s";
    const LEADER_FLAP: &'static str = "leader-flap";
    const BLACKHOLE: &'static str = "blackhole";
    const LEADER_ELECTION: &'static str = "leader-election";
    /// A preset of every kind of action: a timing lie, a refusal, a lie without parameters and an incident.
    const NAMES: [&'static str; 4] = [
        Self::DELAY_1S,
        Self::LEADER_FLAP,
        Self::BLACKHOLE,
        Self::LEADER_ELECTION,
    ];

    fn delay_1s() -> TimingLie {
        TimingLie::new().with_delay(Self::DELAY).with_jitter(Self::JITTER)
    }

    fn leader_flap() -> RefusalLie {
        RefusalLie::new(Self::FLAP)
    }

    fn set() -> PresetSet {
        let [delay_1s, leader_flap, blackhole, leader_election] = Self::NAMES.map(|name| name.parse().unwrap());
        let election = IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        };
        PresetSet::new([
            Preset::new(delay_1s, Self::delay_1s()),
            Preset::new(leader_flap, Self::leader_flap()),
            Preset::new(blackhole, LieDefinition::Silence),
            Preset::new(leader_election, election),
        ])
        .unwrap()
    }

    /// The rule that a pad in the column of that preset creates, on the row of `order-producers`, for all clients.
    fn rule(preset: &str) -> Rule {
        Rule::new(
            format!("{preset}-on-order-producers-for-all-clients").parse().unwrap(),
            "order-producers".parse::<SelectorName>().unwrap(),
            preset.parse::<PresetName>().unwrap(),
        )
    }

    /// The rule of the pad of `delay-1s`.
    fn slow() -> Rule {
        Self::rule(Self::DELAY_1S)
    }

    /// The rule of the pad of `leader-flap`.
    fn flap() -> Rule {
        Self::rule(Self::LEADER_FLAP)
    }
}

// The properties

#[test]
fn every_step_has_a_default() {
    let mapping = EncoderMapping::new();

    assert_eq!(mapping, EncoderMapping::default());
    assert_eq!(mapping.probability_step(), "1%".parse().unwrap());
    assert_eq!(mapping.delay_step(), "50ms".parse().unwrap());
    assert_eq!(mapping.jitter_step(), "25ms".parse().unwrap());
    assert_eq!(mapping.throttle_step(), "100ms".parse().unwrap());
    assert_eq!(mapping.every_step(), Count::ONE);
    assert_eq!(mapping.duration_step(), "5s".parse().unwrap());
    assert_eq!(mapping.firings_step(), Count::ONE);
    assert_eq!(mapping.errors(), ErrorCode::known().collect::<Vec<_>>());
}

#[test]
fn every_step_takes_another_value() {
    let ten_percent: Chance = "10%".parse().unwrap();
    let [second, tenth, two_seconds, minute]: [TimeSpan; 4] =
        ["1s", "100ms", "2s", "1m"].map(|span| span.parse().unwrap());
    let (five, ten) = (Count::new(5).unwrap(), Count::new(10).unwrap());

    let coarse = EncoderMapping::new()
        .with_probability_step(ten_percent)
        .with_delay_step(second)
        .with_jitter_step(tenth)
        .with_throttle_step(two_seconds)
        .with_every_step(five)
        .with_duration_step(minute)
        .with_firings_step(ten)
        .with_errors([Presets::FLAP]);

    assert_eq!(coarse.probability_step(), ten_percent);
    assert_eq!(coarse.delay_step(), second);
    assert_eq!(coarse.jitter_step(), tenth);
    assert_eq!(coarse.throttle_step(), two_seconds);
    assert_eq!(coarse.every_step(), five);
    assert_eq!(coarse.duration_step(), minute);
    assert_eq!(coarse.firings_step(), ten);
    assert_eq!(coarse.errors(), [Presets::FLAP]);
}

// The trigger

#[test]
fn the_probability_encoder_moves_the_chance_between_none_and_every_call() {
    let mapping = EncoderMapping::new().with_probability_step("25%".parse().unwrap());
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Probability, Steps::new(steps));
    let with_chance =
        |chance: &str| Presets::slow().with_trigger(TriggerParts::new().with_chance(chance.parse().unwrap()));

    // A rule without a chance fires on every matching call, so the first step down takes it to 75%.
    let always = Presets::slow();
    let rarer = mapping.apply(turn(-1), &always, &presets).unwrap();
    assert_eq!(rarer, with_chance("75%"));
    assert_eq!(mapping.apply(turn(-2), &rarer, &presets).unwrap(), with_chance("25%"));

    // The chance stops at 0%, where the rule never fires by itself.
    let never = mapping.apply(turn(-4), &rarer, &presets).unwrap();
    assert_eq!(never, with_chance("0%"));
    assert_eq!(mapping.apply(turn(-1), &never, &presets).unwrap(), never);

    // At 100% the rule has no chance part, as before the first turn, and a step further up changes nothing.
    assert_eq!(mapping.apply(turn(1), &rarer, &presets).unwrap(), always);
    assert_eq!(mapping.apply(turn(4), &never, &presets).unwrap(), always);
    assert_eq!(mapping.apply(turn(1), &always, &presets).unwrap(), always);
}

#[test]
fn the_probability_encoder_keeps_the_other_parts_of_the_trigger() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Probability, Steps::new(steps));
    let first_five_per_session = TriggerParts::new()
        .with_first(Count::new(5).unwrap())
        .with_per(Per::Session);
    let rule = Presets::slow().with_trigger(first_five_per_session);

    // Ten steps of 1% down, and ten up again.
    let rarer = mapping.apply(turn(-10), &rule, &presets).unwrap();
    assert_eq!(
        rarer,
        rule.clone()
            .with_trigger(first_five_per_session.with_chance("90%".parse().unwrap()))
    );
    assert_eq!(mapping.apply(turn(10), &rarer, &presets).unwrap(), rule);
}

#[test]
fn the_every_encoder_moves_the_step_between_firings() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Every, Steps::new(steps));
    let every = |count| Presets::slow().with_trigger(TriggerParts::new().with_every(Count::new(count).unwrap()));

    // A rule without the part fires on each match, which is every 1st one: two steps up make it every 3rd.
    let each_match = Presets::slow();
    let every_third = mapping.apply(turn(2), &each_match, &presets).unwrap();
    assert_eq!(every_third, every(3));
    assert_eq!(mapping.apply(turn(7), &every_third, &presets).unwrap(), every(10));

    // At one the rule has no `every` part, and a step further down changes nothing.
    assert_eq!(mapping.apply(turn(-2), &every_third, &presets).unwrap(), each_match);
    assert_eq!(mapping.apply(turn(-5), &every_third, &presets).unwrap(), each_match);
    assert_eq!(mapping.apply(turn(-1), &each_match, &presets).unwrap(), each_match);
}

#[test]
fn a_manual_trigger_has_nothing_for_the_encoders_of_the_trigger() {
    let mapping = EncoderMapping::new();
    let by_hand = Presets::slow().with_trigger(Trigger::Manual);

    for encoder in [Encoder::Probability, Encoder::Every] {
        let turn = EncoderTurn::new(encoder, Steps::new(1));
        assert_eq!(
            mapping.apply(turn, &by_hand, &Presets::set()),
            Err(TurnError::Disabled {
                encoder,
                rule: by_hand.name().clone()
            })
        );
    }
}

// The timing lie

#[test]
fn the_delay_encoder_gives_the_rule_a_lie_of_its_own() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Delay, Steps::new(steps));
    let delayed = |delay: &str| Presets::slow().with_action(Presets::delay_1s().with_delay(delay.parse().unwrap()));

    // Two steps of 50 ms up from the 1 s of the preset. The jitter of the preset stays.
    let slower = mapping.apply(turn(2), &Presets::slow(), &presets).unwrap();
    assert_eq!(slower, delayed("1100ms"));

    // The next turn starts from the rule's own lie, and not from the preset again.
    assert_eq!(mapping.apply(turn(-5), &slower, &presets).unwrap(), delayed("850ms"));
}

#[test]
fn the_delay_stops_at_no_delay_and_takes_the_jitter_down_with_it() {
    let mapping = EncoderMapping::new().with_delay_step("450ms".parse().unwrap());
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Delay, Steps::new(steps));

    // 1 s less 900 ms leaves 100 ms, and a jitter is at most its delay.
    let short = mapping.apply(turn(-2), &Presets::slow(), &presets).unwrap();
    let tenth: TimeSpan = "100ms".parse().unwrap();
    assert_eq!(
        short,
        Presets::slow().with_action(TimingLie::new().with_delay(tenth).with_jitter(tenth))
    );

    let none = mapping.apply(turn(-1), &short, &presets).unwrap();
    assert_eq!(none, Presets::slow().with_action(TimingLie::new()));
    assert_eq!(mapping.apply(turn(-1), &none, &presets).unwrap(), none);
}

#[test]
fn the_jitter_encoder_moves_the_jitter_up_to_the_delay() {
    let mapping = EncoderMapping::new().with_jitter_step("300ms".parse().unwrap());
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Jitter, Steps::new(steps));
    let jittered = |jitter: TimeSpan| Presets::slow().with_action(Presets::delay_1s().with_jitter(jitter));

    // One step up from the 200 ms of the preset.
    let wider = mapping.apply(turn(1), &Presets::slow(), &presets).unwrap();
    assert_eq!(wider, jittered("500ms".parse().unwrap()));

    // Nine steps would be 2900 ms, and the jitter stops at the delay.
    let widest = mapping.apply(turn(9), &Presets::slow(), &presets).unwrap();
    assert_eq!(widest, jittered(Presets::DELAY));

    let none = mapping.apply(turn(-1), &Presets::slow(), &presets).unwrap();
    assert_eq!(none, jittered(TimeSpan::ZERO));
}

#[test]
fn the_throttle_encoder_moves_the_throttle_up_to_the_longest_one() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Throttle, Steps::new(steps));
    let throttled = |throttle: TimeSpan| Presets::slow().with_action(Presets::delay_1s().with_throttle(throttle));

    // Twenty steps of 100 ms.
    let backing_off = mapping.apply(turn(20), &Presets::slow(), &presets).unwrap();
    assert_eq!(backing_off, throttled("2s".parse().unwrap()));
    assert_eq!(
        mapping.apply(turn(-25), &backing_off, &presets).unwrap(),
        throttled(TimeSpan::ZERO)
    );

    let longest = mapping.apply(turn(i32::MAX), &Presets::slow(), &presets).unwrap();
    assert_eq!(longest, throttled(TimingLie::MAX_THROTTLE));
}

#[test]
fn a_turn_that_leaves_the_lie_as_it_is_leaves_the_preset_in_the_rule() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();

    // The preset has no throttle, so a step down has nothing to take.
    let down = EncoderTurn::new(Encoder::Throttle, Steps::new(-1));
    assert_eq!(
        mapping.apply(down, &Presets::slow(), &presets).unwrap(),
        Presets::slow()
    );
}

#[test]
fn the_encoders_of_a_timing_lie_are_disabled_for_every_other_action() {
    let mapping = EncoderMapping::new();
    let others = [Presets::LEADER_FLAP, Presets::BLACKHOLE, Presets::LEADER_ELECTION];

    for encoder in [Encoder::Delay, Encoder::Jitter, Encoder::Throttle] {
        for preset in others {
            let rule = Presets::rule(preset);
            assert_eq!(
                mapping.apply(EncoderTurn::new(encoder, Steps::new(1)), &rule, &Presets::set()),
                Err(TurnError::Disabled {
                    encoder,
                    rule: rule.name().clone()
                }),
                "{encoder} on {preset}"
            );
        }
    }
}

// The refusal

#[test]
fn the_error_encoder_steps_through_the_errors_of_the_mapping_and_stops_at_its_ends() {
    let errors = [
        ErrorCode::REQUEST_TIMED_OUT,
        Presets::FLAP,
        ErrorCode::NOT_ENOUGH_REPLICAS,
        ErrorCode::TOPIC_AUTHORIZATION_FAILED,
    ];
    let [first, _, third, last] = errors;
    let mapping = EncoderMapping::new().with_errors(errors);
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::ErrorCode, Steps::new(steps));
    let refusing = |error| Presets::flap().with_action(RefusalLie::new(error));

    // The error of `leader-flap` is the second of the four.
    let next = mapping.apply(turn(1), &Presets::flap(), &presets).unwrap();
    assert_eq!(next, refusing(third));
    assert_eq!(mapping.apply(turn(1), &next, &presets).unwrap(), refusing(last));
    assert_eq!(
        mapping.apply(turn(-1), &Presets::flap(), &presets).unwrap(),
        refusing(first)
    );

    // Nine steps lead past either end.
    assert_eq!(
        mapping.apply(turn(9), &Presets::flap(), &presets).unwrap(),
        refusing(last)
    );
    assert_eq!(
        mapping.apply(turn(-9), &Presets::flap(), &presets).unwrap(),
        refusing(first)
    );
}

#[test]
fn the_error_encoder_keeps_the_stage_of_the_refusal() {
    let mapping = EncoderMapping::new().with_errors([Presets::FLAP, ErrorCode::REQUEST_TIMED_OUT]);
    let lost_acknowledgement = Presets::flap().with_action(Presets::leader_flap().at(Stage::AfterResponse));

    let next = EncoderTurn::new(Encoder::ErrorCode, Steps::new(1));
    assert_eq!(
        mapping.apply(next, &lost_acknowledgement, &Presets::set()).unwrap(),
        Presets::flap().with_action(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse))
    );
}

#[test]
fn an_error_that_the_mapping_does_not_list_moves_to_the_first_of_the_list() {
    let errors = [ErrorCode::REQUEST_TIMED_OUT, ErrorCode::NOT_ENOUGH_REPLICAS];
    let [first, _] = errors;
    let mapping = EncoderMapping::new().with_errors(errors);

    for steps in [1, -1, 5] {
        let turn = EncoderTurn::new(Encoder::ErrorCode, Steps::new(steps));
        assert_eq!(
            mapping.apply(turn, &Presets::flap(), &Presets::set()).unwrap(),
            Presets::flap().with_action(RefusalLie::new(first)),
            "{turn}"
        );
    }
}

#[test]
fn a_mapping_lists_an_error_once_where_it_stands_first() {
    let mapping = EncoderMapping::new().with_errors([Presets::FLAP, ErrorCode::REQUEST_TIMED_OUT, Presets::FLAP]);

    assert_eq!(mapping.errors(), [Presets::FLAP, ErrorCode::REQUEST_TIMED_OUT]);
}

#[test]
fn a_mapping_without_errors_leaves_a_refusal_as_it_is() {
    let mapping = EncoderMapping::new().with_errors([]);

    let next = EncoderTurn::new(Encoder::ErrorCode, Steps::new(1));
    assert_eq!(
        mapping.apply(next, &Presets::flap(), &Presets::set()).unwrap(),
        Presets::flap()
    );
}

#[test]
fn the_error_encoder_is_disabled_for_every_action_but_a_refusal() {
    let mapping = EncoderMapping::new();
    let next = EncoderTurn::new(Encoder::ErrorCode, Steps::new(1));

    for preset in [Presets::DELAY_1S, Presets::BLACKHOLE, Presets::LEADER_ELECTION] {
        let rule = Presets::rule(preset);
        assert_eq!(
            mapping.apply(next, &rule, &Presets::set()),
            Err(TurnError::Disabled {
                encoder: Encoder::ErrorCode,
                rule: rule.name().clone()
            }),
            "{preset}"
        );
    }
}

// The lifetime

#[test]
fn the_duration_encoder_moves_a_duration_lifetime_and_ends_at_until_disarmed() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Duration, Steps::new(steps));
    let lasting = |lifetime: &str| Presets::slow().with_lifetime(lifetime.parse().unwrap());

    // Six steps of 5 s.
    let until_disarmed = Presets::slow();
    let limited = mapping.apply(turn(6), &until_disarmed, &presets).unwrap();
    assert_eq!(limited, lasting("30s"));
    assert_eq!(mapping.apply(turn(-2), &limited, &presets).unwrap(), lasting("20s"));

    // At no time the rule stays armed until disarmed, and a step further down changes nothing.
    assert_eq!(mapping.apply(turn(-6), &limited, &presets).unwrap(), until_disarmed);
    assert_eq!(mapping.apply(turn(-7), &limited, &presets).unwrap(), until_disarmed);
    assert_eq!(
        mapping.apply(turn(-1), &until_disarmed, &presets).unwrap(),
        until_disarmed
    );
}

#[test]
fn the_firings_encoder_moves_a_firings_lifetime_and_ends_at_until_disarmed() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let turn = |steps| EncoderTurn::new(Encoder::Firings, Steps::new(steps));
    let lasting = |lifetime: &str| Presets::flap().with_lifetime(lifetime.parse().unwrap());

    let until_disarmed = Presets::flap();
    let thrice = mapping.apply(turn(3), &until_disarmed, &presets).unwrap();
    assert_eq!(thrice, lasting("3 firings"));
    assert_eq!(
        mapping.apply(turn(-2), &thrice, &presets).unwrap(),
        lasting("1 firings")
    );

    // At no firing the rule stays armed until disarmed, and a step further down changes nothing.
    assert_eq!(mapping.apply(turn(-3), &thrice, &presets).unwrap(), until_disarmed);
    assert_eq!(mapping.apply(turn(-4), &thrice, &presets).unwrap(), until_disarmed);
    assert_eq!(
        mapping.apply(turn(-1), &until_disarmed, &presets).unwrap(),
        until_disarmed
    );
}

#[test]
fn a_lifetime_encoder_replaces_a_lifetime_of_another_kind() {
    let mapping = EncoderMapping::new();
    let presets = Presets::set();
    let one_up = |encoder| EncoderTurn::new(encoder, Steps::new(1));
    let lasting = |lifetime: &str| Presets::slow().with_lifetime(lifetime.parse().unwrap());

    // Another lifetime counts as no time and as no firings, so the first step up starts from nothing.
    assert_eq!(
        mapping
            .apply(one_up(Encoder::Duration), &lasting("3 firings"), &presets)
            .unwrap(),
        lasting("5s")
    );
    assert_eq!(
        mapping
            .apply(one_up(Encoder::Firings), &lasting("100 matches"), &presets)
            .unwrap(),
        lasting("1 firings")
    );
    assert_eq!(
        mapping
            .apply(one_up(Encoder::Firings), &lasting("30s"), &presets)
            .unwrap(),
        lasting("1 firings")
    );
}

// What holds for every encoder

#[test]
fn the_encoders_of_the_trigger_and_of_the_lifetime_tune_a_rule_of_every_action() {
    let mapping = EncoderMapping::new();
    let half = TriggerParts::new().with_chance("50%".parse().unwrap());

    for encoder in [
        Encoder::Probability,
        Encoder::Every,
        Encoder::Duration,
        Encoder::Firings,
    ] {
        for preset in Presets::NAMES {
            let rule = Presets::rule(preset).with_trigger(half);
            let turned = mapping
                .apply(EncoderTurn::new(encoder, Steps::new(1)), &rule, &Presets::set())
                .unwrap();
            assert_ne!(turned, rule, "{encoder} on {preset}");
            assert_eq!(turned.action(), rule.action(), "{encoder} on {preset}");
        }
    }
}

#[test]
fn a_turn_changes_one_part_of_the_rule_and_nothing_else() {
    let mapping = EncoderMapping::new();
    let half = TriggerParts::new().with_chance("50%".parse().unwrap());
    let rule = Presets::slow()
        .scoped(Scope::client(ClientId::new("order-ingest").unwrap()))
        .with_trigger(half)
        .with_dice(DiceKey::new("an older name").unwrap());

    for encoder in Encoder::ALL
        .into_iter()
        .filter(|encoder| encoder.applies_to(&Presets::delay_1s().into()))
    {
        let turned = mapping
            .apply(EncoderTurn::new(encoder, Steps::new(1)), &rule, &Presets::set())
            .unwrap();

        assert_eq!(turned.name(), rule.name(), "{encoder}");
        assert_eq!(turned.selector(), rule.selector(), "{encoder}");
        assert_eq!(turned.scope(), rule.scope(), "{encoder}");
        assert_eq!(turned.dice(), rule.dice(), "{encoder}");
        let changed = [
            turned.action() != rule.action(),
            turned.trigger() != rule.trigger(),
            turned.lifetime() != rule.lifetime(),
        ];
        assert_eq!(changed.into_iter().filter(|changed| *changed).count(), 1, "{encoder}");
    }
}

#[test]
fn a_turn_of_zero_steps_changes_nothing() {
    let mapping = EncoderMapping::new();
    // A chance of 100% is what a rule without a chance has, and no turn is not the moment to take the part away.
    let with_a_full_chance = Presets::slow().with_trigger(TriggerParts::new().with_chance(Chance::ALWAYS));

    for encoder in Encoder::ALL {
        let none = EncoderTurn::new(encoder, Steps::new(0));
        for rule in [Presets::slow(), with_a_full_chance.clone(), Presets::flap()] {
            let action = Presets::set().resolve(rule.action()).cloned().unwrap();
            if encoder.applies_to(&action) {
                assert_eq!(mapping.apply(none, &rule, &Presets::set()).unwrap(), rule, "{encoder}");
            }
        }
    }
}

#[test]
fn an_encoder_of_the_action_needs_the_preset_that_the_rule_names() {
    let mapping = EncoderMapping::new();
    let unknown: PresetName = "delay-30s".parse().unwrap();
    let rule = Presets::rule(unknown.as_str());

    for encoder in [Encoder::Delay, Encoder::Jitter, Encoder::Throttle, Encoder::ErrorCode] {
        assert_eq!(
            mapping.apply(EncoderTurn::new(encoder, Steps::new(1)), &rule, &Presets::set()),
            Err(TurnError::UnknownPreset {
                rule: rule.name().clone(),
                preset: unknown.clone(),
            }),
            "{encoder}"
        );
    }

    // The trigger and the lifetime are parts of the rule itself, so their encoders ask for no preset.
    for encoder in [
        Encoder::Probability,
        Encoder::Every,
        Encoder::Duration,
        Encoder::Firings,
    ] {
        let turn = EncoderTurn::new(encoder, Steps::new(1));
        assert!(mapping.apply(turn, &rule, &Presets::set()).is_ok(), "{encoder}");
    }
}

#[test]
fn an_encoder_tunes_an_action_that_the_rule_writes_itself() {
    let mapping = EncoderMapping::new();
    let inline = Presets::slow().with_action(Presets::delay_1s());

    // No preset is needed: one step of 50 ms up from the 1 s that the rule writes.
    let one_up = EncoderTurn::new(Encoder::Delay, Steps::new(1));
    assert_eq!(
        mapping.apply(one_up, &inline, &PresetSet::default()).unwrap(),
        Presets::slow().with_action(Presets::delay_1s().with_delay("1050ms".parse().unwrap()))
    );
}

#[test]
fn a_turn_error_says_which_encoder_found_nothing_to_change_in_which_rule() {
    let mapping = EncoderMapping::new();
    let one_up = |encoder| EncoderTurn::new(encoder, Steps::new(1));
    let message = |turn, rule: &Rule| mapping.apply(turn, rule, &Presets::set()).unwrap_err().to_string();

    assert_eq!(
        message(one_up(Encoder::ErrorCode), &Presets::slow()),
        "the error-code encoder changes the error of a refusal, \
         and rule \"delay-1s-on-order-producers-for-all-clients\" has none"
    );
    assert_eq!(
        message(one_up(Encoder::Delay), &Presets::flap()),
        "the delay encoder changes the delay of a timing lie, \
         and rule \"leader-flap-on-order-producers-for-all-clients\" has none"
    );
    assert_eq!(
        message(
            one_up(Encoder::Probability),
            &Presets::slow().with_trigger(Trigger::Manual)
        ),
        "the probability encoder changes the chance of a trigger, \
         and rule \"delay-1s-on-order-producers-for-all-clients\" has none"
    );
    assert_eq!(
        message(one_up(Encoder::Jitter), &Presets::rule("delay-30s")),
        "rule \"delay-30s-on-order-producers-for-all-clients\": no preset is named \"delay-30s\""
    );
}
