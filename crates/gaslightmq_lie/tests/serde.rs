//! The `serde` form of every type, and what reading rejects.

#![cfg(feature = "serde")]

use std::fmt::Debug;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const BEFORE: Stage = Stage::BeforeForwarding;
const AFTER: Stage = Stage::AfterResponse;

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// `json` reads as the value, though the value writes a shorter form.
fn reads<T: DeserializeOwned + PartialEq + Debug>(json: &str, value: &T) {
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

fn refusal(error: ErrorCode) -> LieDefinition {
    RefusalLie::new(error).into()
}

fn refusal_after_the_response(error: ErrorCode) -> LieDefinition {
    RefusalLie::new(error).at(AFTER).into()
}

fn timing(lie: TimingLie) -> LieDefinition {
    lie.into()
}

fn hang_up(stage: Stage) -> LieDefinition {
    HangUpLie::new().at(stage).into()
}

fn millis(millis: u64) -> TimeSpan {
    TimeSpan::from_millis(millis)
}

fn parts(texts: &[&str]) -> Target {
    Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
}

fn on(lie: LieDefinition, target: Target) -> TargetedLie {
    lie.on(target)
}

#[test]
fn a_kind_writes_its_name() {
    writes(&LieKind::HangUp, r#""hang-up""#);
    for kind in LieKind::ALL {
        writes(&kind, &format!("{:?}", kind.name()));
    }
    rejects::<LieKind>(r#""hangup""#, "no lie is called \"hangup\"");
    rejects::<LieKind>("3", "expected a string");
}

#[test]
fn a_stage_writes_before_or_after() {
    writes(&BEFORE, r#""before""#);
    writes(&AFTER, r#""after""#);
    rejects::<Stage>(r#""during""#, "no stage is called \"during\"");
}

#[test]
fn a_class_and_an_aspect_write_their_names() {
    for class in CombinationClass::ALL {
        writes(&class, &format!("{:?}", class.name()));
    }
    for aspect in Aspect::ALL {
        writes(&aspect, &format!("{:?}", aspect.name()));
    }
    writes(&Aspect::HighWatermark, r#""high-watermark""#);
    rejects::<CombinationClass>(r#""fatal""#, "no combination class is called \"fatal\"");
    rejects::<Aspect>(r#""watermark""#, "no aspect is called \"watermark\"");
}

#[test]
fn the_presets_of_the_specification_read_as_definitions() {
    // `lie: timing`, `delay: 1s`, `jitter: 200ms`
    writes(
        &timing(TimingLie::new().with_delay(millis(1000)).with_jitter(millis(200))),
        r#"{"lie":"timing","delay":"1s","jitter":"200ms"}"#,
    );
    // `lie: refusal`, `error: REQUEST_TIMED_OUT`, `stage: after`
    writes(
        &refusal_after_the_response(ErrorCode::REQUEST_TIMED_OUT),
        r#"{"lie":"refusal","error":"REQUEST_TIMED_OUT","stage":"after"}"#,
    );
    // `lie: refusal`, `error: NOT_LEADER_OR_FOLLOWER`
    writes(
        &refusal(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        r#"{"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER"}"#,
    );
    // `lie: silence`
    writes(&LieDefinition::Silence, r#"{"lie":"silence"}"#);
}

#[test]
fn every_definition_writes_a_map_with_its_parameters() {
    writes(
        &timing(
            TimingLie::new()
                .with_delay(millis(1000))
                .with_jitter(millis(200))
                .with_throttle(millis(500))
                .at(BEFORE),
        ),
        r#"{"lie":"timing","delay":"1s","jitter":"200ms","throttle":"500ms","stage":"before"}"#,
    );
    writes(
        &timing(TimingLie::new().with_throttle(millis(500))),
        r#"{"lie":"timing","throttle":"500ms"}"#,
    );
    writes(&hang_up(BEFORE), r#"{"lie":"hang-up"}"#);
    writes(&hang_up(AFTER), r#"{"lie":"hang-up","stage":"after"}"#);
}

#[test]
fn a_refusal_takes_a_code_that_kafka_gives_no_name() {
    writes(
        &refusal(ErrorCode::new(999).unwrap()),
        r#"{"lie":"refusal","error":999}"#,
    );
    reads(
        r#"{"lie":"refusal","error":6}"#,
        &refusal(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
}

#[test]
fn a_definition_leaves_out_the_default_stage_of_its_kind() {
    reads(
        r#"{"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER","stage":"before"}"#,
        &refusal(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    );
    reads(
        r#"{"lie":"timing","delay":"1s","stage":"after"}"#,
        &timing(TimingLie::new().with_delay(millis(1000))),
    );
    reads(r#"{"lie":"hang-up","stage":"before"}"#, &hang_up(BEFORE));
}

#[test]
fn a_timing_lie_leaves_out_what_is_zero() {
    // A timing lie that changes nothing is a valid lie: it is what `TimingLie::new` writes, so it reads back.
    writes(&timing(TimingLie::new()), r#"{"lie":"timing"}"#);
    reads(
        r#"{"lie":"timing","stage":"before"}"#,
        &timing(TimingLie::new().at(BEFORE)),
    );
    reads(
        r#"{"lie":"timing","delay":"0s","jitter":"0ms","throttle":"0s"}"#,
        &timing(TimingLie::new()),
    );
    reads(
        r#"{"lie":"timing","jitter":"200ms","delay":"1s"}"#,
        &timing(TimingLie::new().with_delay(millis(1000)).with_jitter(millis(200))),
    );
}

#[test]
fn the_keys_come_in_any_order() {
    reads(
        r#"{"stage":"after","error":"REQUEST_TIMED_OUT","lie":"refusal"}"#,
        &refusal_after_the_response(ErrorCode::REQUEST_TIMED_OUT),
    );
}

#[test]
fn silence_never_acts_after_the_response() {
    rejects::<LieDefinition>(
        r#"{"lie":"silence","stage":"after"}"#,
        "silence cannot act after the response",
    );
    // The stage silence has anyway is true, so it reads. Writing leaves it out.
    reads(r#"{"lie":"silence","stage":"before"}"#, &LieDefinition::Silence);
}

#[test]
fn a_kind_without_a_definition_does_not_read() {
    for kind in ["topology", "data", "progress", "configuration"] {
        rejects::<LieDefinition>(
            &format!(r#"{{"lie":"{kind}"}}"#),
            &format!("{kind} has no definition yet"),
        );
    }
    // `action: { lie: topology, leader: previous }` waits for the topology lie.
    rejects::<LieDefinition>(r#"{"lie":"topology","leader":"previous"}"#, "unknown field `leader`");
}

#[test]
fn a_kind_without_a_definition_says_so_before_any_other_reason() {
    for kind in ["topology", "data", "progress", "configuration"] {
        let reason = format!("{kind} has no definition yet");
        // A rewrite never acts before forwarding, and that is not why the map does not read.
        rejects::<LieDefinition>(&format!(r#"{{"lie":"{kind}","stage":"before"}}"#), &reason);
        rejects::<LieDefinition>(&format!(r#"{{"stage":"before","lie":"{kind}"}}"#), &reason);
        rejects::<LieDefinition>(&format!(r#"{{"lie":"{kind}","stage":"after"}}"#), &reason);
        rejects::<LieDefinition>(&format!(r#"{{"lie":"{kind}","delay":"1s"}}"#), &reason);
        rejects::<TargetedLie>(
            &format!(r#"{{"lie":"{kind}","stage":"before","target":"call"}}"#),
            &reason,
        );
    }
}

#[test]
fn a_jitter_longer_than_the_delay_does_not_read() {
    // The wait would be between -200 ms and 200 ms.
    rejects::<LieDefinition>(
        r#"{"lie":"timing","jitter":"200ms"}"#,
        "a jitter of 200ms is longer than the delay of 0s",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"timing","jitter":"2s","delay":"1s"}"#,
        "a jitter of 2s is longer than the delay of 1s",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"timing","delay":"100ms","jitter":"101ms","throttle":"1s"}"#,
        "a jitter of 101ms is longer than the delay of 100ms",
    );
    rejects::<TargetedLie>(
        r#"{"lie":"timing","jitter":"200ms","target":"call"}"#,
        "a jitter of 200ms is longer than the delay of 0s",
    );
    rejects::<Preset>(
        r#"{"name":"Jitter","lie":"timing","jitter":"200ms"}"#,
        "a jitter of 200ms is longer than the delay of 0s",
    );

    // A jitter as long as the delay waits between no time and twice the delay.
    writes(
        &timing(TimingLie::new().with_delay(millis(200)).with_jitter(millis(200))),
        r#"{"lie":"timing","delay":"200ms","jitter":"200ms"}"#,
    );
}

#[test]
fn a_throttle_longer_than_kafka_can_tell_does_not_read() {
    // `throttle_time_ms` is an `i32`.
    let longest = TimingLie::MAX_THROTTLE;
    assert_eq!(longest.to_string(), "2147483647ms");
    writes(
        &timing(TimingLie::new().with_throttle(longest)),
        r#"{"lie":"timing","throttle":"2147483647ms"}"#,
    );

    rejects::<LieDefinition>(
        r#"{"lie":"timing","throttle":"2147483648ms"}"#,
        "a throttle of 2147483648ms is longer than 2147483647ms, the longest that Kafka can tell a client",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"timing","delay":"1s","throttle":"600h"}"#,
        "a throttle of 600h is longer than 2147483647ms",
    );
    rejects::<TargetedLie>(
        r#"{"lie":"timing","throttle":"600h"}"#,
        "a throttle of 600h is longer than 2147483647ms",
    );
    // A delay has no such bound.
    reads(
        r#"{"lie":"timing","delay":"600h"}"#,
        &timing(TimingLie::new().with_delay(millis(600 * 3_600_000))),
    );
}

#[test]
fn every_timing_lie_that_code_builds_reads_back() {
    let spans = [0, 1, 200, 1000, u64::from(u32::MAX), u64::MAX].map(millis);

    for delay in spans {
        for jitter in spans {
            for throttle in spans {
                let lie = timing(
                    TimingLie::new()
                        .with_delay(delay)
                        .with_jitter(jitter)
                        .with_throttle(throttle),
                );
                let json = serde_json::to_string(&lie).unwrap();
                assert_eq!(serde_json::from_str::<LieDefinition>(&json).unwrap(), lie, "{json}");
            }
        }
    }
}

#[test]
fn a_definition_needs_its_kind_and_its_required_parameters() {
    rejects::<LieDefinition>("{}", "missing field `lie`");
    rejects::<LieDefinition>(r#"{"error":"NOT_LEADER_OR_FOLLOWER"}"#, "missing field `lie`");
    rejects::<LieDefinition>(r#"{"lie":"refusal"}"#, "missing field `error`");
    rejects::<LieDefinition>(r#"{"lie":"refusal","stage":"after"}"#, "missing field `error`");
}

#[test]
fn a_key_that_no_lie_has_does_not_read() {
    rejects::<LieDefinition>(r#"{"lie":"timing","dely":"1s"}"#, "unknown field `dely`");
    rejects::<LieDefinition>(
        r#"{"lie":"refusal","error":"REQUEST_TIMED_OUT","Stage":"after"}"#,
        "unknown field `Stage`",
    );
    rejects::<LieDefinition>(r#"{"lie":"silence","name":"Blackhole"}"#, "unknown field `name`");
    // A definition does not say what it lies about.
    rejects::<LieDefinition>(r#"{"lie":"silence","target":"call"}"#, "unknown field `target`");
}

#[test]
fn a_key_of_another_lie_does_not_read() {
    rejects::<LieDefinition>(
        r#"{"lie":"refusal","error":"REQUEST_TIMED_OUT","delay":"1s"}"#,
        "`delay` does not belong to refusal",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"timing","delay":"1s","error":"REQUEST_TIMED_OUT"}"#,
        "`error` does not belong to timing",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"silence","throttle":"1s"}"#,
        "`throttle` does not belong to silence",
    );
    rejects::<LieDefinition>(
        r#"{"lie":"hang-up","jitter":"1s"}"#,
        "`jitter` does not belong to hang-up",
    );
}

#[test]
fn a_key_that_comes_twice_does_not_read() {
    rejects::<LieDefinition>(r#"{"lie":"silence","lie":"hang-up"}"#, "duplicate field `lie`");
    rejects::<LieDefinition>(
        r#"{"lie":"timing","delay":"1s","delay":"2s"}"#,
        "duplicate field `delay`",
    );
}

#[test]
fn a_parameter_only_holds_a_valid_value() {
    rejects::<LieDefinition>(r#"{"lie":"hangup"}"#, "no lie is called \"hangup\"");
    rejects::<LieDefinition>(r#"{"lie":"hang-up","stage":"later"}"#, "no stage is called \"later\"");
    rejects::<LieDefinition>(r#"{"lie":"timing","delay":"1.5s"}"#, "invalid time span `1.5s`");
    rejects::<LieDefinition>(r#"{"lie":"timing","delay":1000}"#, "expected a string");
    rejects::<LieDefinition>(r#"{"lie":"refusal","error":"NOT_A_LEADER"}"#, "NOT_A_LEADER");
    // "No error" is not an error to refuse with.
    rejects::<LieDefinition>(r#"{"lie":"refusal","error":"NONE"}"#, "NONE");
    rejects::<LieDefinition>(r#"{"lie":"refusal","error":0}"#, "0");
}

#[test]
fn a_definition_is_a_map() {
    rejects::<LieDefinition>(r#""silence""#, "expected a map with a `lie` key");
    rejects::<LieDefinition>(r#"["silence"]"#, "expected a map with a `lie` key");
}

#[test]
fn a_targeted_lie_writes_its_definition_and_its_target() {
    // `NOT_LEADER_OR_FOLLOWER on orders.eu/3`
    writes(
        &on(refusal(ErrorCode::NOT_LEADER_OR_FOLLOWER), parts(&["orders.eu/3"])),
        r#"{"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER","target":{"parts":["orders.eu/3"]}}"#,
    );
    writes(
        &on(
            refusal_after_the_response(ErrorCode::REQUEST_TIMED_OUT),
            parts(&["orders.eu/4", "orders.eu/3"]),
        ),
        r#"{"lie":"refusal","error":"REQUEST_TIMED_OUT","stage":"after","target":{"parts":["orders.eu/3","orders.eu/4"]}}"#,
    );
}

#[test]
fn a_targeted_lie_leaves_out_the_whole_call() {
    // `+750 ms`
    writes(
        &on(timing(TimingLie::new().with_delay(millis(750))), Target::Call),
        r#"{"lie":"timing","delay":"750ms"}"#,
    );
    writes(
        &on(refusal(ErrorCode::COORDINATOR_NOT_AVAILABLE), Target::Call),
        r#"{"lie":"refusal","error":"COORDINATOR_NOT_AVAILABLE"}"#,
    );
    writes(&on(LieDefinition::Silence, Target::Call), r#"{"lie":"silence"}"#);
    reads(
        r#"{"lie":"hang-up","stage":"after","target":"call"}"#,
        &on(hang_up(AFTER), Target::Call),
    );
}

#[test]
fn a_lie_on_the_whole_call_does_not_read_with_parts() {
    for (json, kind) in [
        (
            r#"{"lie":"timing","delay":"750ms","target":{"parts":["orders.eu/3"]}}"#,
            "timing",
        ),
        (r#"{"lie":"silence","target":{"parts":["orders.eu/3"]}}"#, "silence"),
        (r#"{"lie":"hang-up","target":{"parts":["orders.eu/3"]}}"#, "hang-up"),
    ] {
        rejects::<TargetedLie>(json, &format!("{kind} always targets the whole call"));
    }
}

#[test]
fn a_targeted_lie_does_not_read_with_a_target_without_parts() {
    let reason = "invalid length 0, expected at least one part";

    // A refusal that covers no part would write `NOT_LEADER_OR_FOLLOWER on `.
    rejects::<TargetedLie>(
        r#"{"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER","target":{"parts":[]}}"#,
        reason,
    );
    rejects::<TargetedLie>(
        r#"{"target":{"parts":[]},"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER"}"#,
        reason,
    );
    // A lie on the whole call names the same reason, and not that it has parts.
    for json in [
        r#"{"lie":"timing","delay":"750ms","target":{"parts":[]}}"#,
        r#"{"lie":"silence","target":{"parts":[]}}"#,
        r#"{"lie":"hang-up","target":{"parts":[]}}"#,
    ] {
        rejects::<TargetedLie>(json, reason);
    }
}

#[test]
fn a_targeted_lie_rejects_what_its_definition_rejects() {
    rejects::<TargetedLie>(r#"{"target":"call"}"#, "missing field `lie`");
    rejects::<TargetedLie>(
        r#"{"lie":"silence","stage":"after"}"#,
        "silence cannot act after the response",
    );
    rejects::<TargetedLie>(
        r#"{"lie":"silence","delay":"1s"}"#,
        "`delay` does not belong to silence",
    );
    rejects::<TargetedLie>(r#"{"lie":"silence","on":"call"}"#, "unknown field `on`");
    rejects::<TargetedLie>(
        r#"{"lie":"silence","target":"call","target":"call"}"#,
        "duplicate field `target`",
    );
}

/// A preset as the `rule` crate will hold it: a name, and the keys of a lie next to it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Preset {
    name: String,
    #[serde(flatten)]
    lie: LieDefinition,
}

fn preset(name: &str, lie: LieDefinition) -> Preset {
    Preset {
        name: name.to_owned(),
        lie,
    }
}

#[test]
fn a_preset_holds_a_name_next_to_the_keys_of_its_lie() {
    writes(
        &preset(
            "Delay 1 s",
            timing(TimingLie::new().with_delay(millis(1000)).with_jitter(millis(200))),
        ),
        r#"{"name":"Delay 1 s","lie":"timing","delay":"1s","jitter":"200ms"}"#,
    );
    writes(
        &preset(
            "Lost acknowledgement",
            refusal_after_the_response(ErrorCode::REQUEST_TIMED_OUT),
        ),
        r#"{"name":"Lost acknowledgement","lie":"refusal","error":"REQUEST_TIMED_OUT","stage":"after"}"#,
    );
    writes(
        &preset("Leader flap", refusal(ErrorCode::NOT_LEADER_OR_FOLLOWER)),
        r#"{"name":"Leader flap","lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER"}"#,
    );
    writes(
        &preset("Blackhole", LieDefinition::Silence),
        r#"{"name":"Blackhole","lie":"silence"}"#,
    );
    reads(
        r#"{"lie":"hang-up","stage":"after","name":"Disconnect"}"#,
        &preset("Disconnect", hang_up(AFTER)),
    );
}

#[test]
fn a_preset_rejects_what_its_lie_rejects() {
    rejects::<Preset>(
        r#"{"name":"Delay 1 s","lie":"timing","dely":"1s"}"#,
        "unknown field `dely`",
    );
    rejects::<Preset>(
        r#"{"name":"Blackhole","lie":"silence","stage":"after"}"#,
        "silence cannot act after the response",
    );
    rejects::<Preset>(
        r#"{"name":"Leader flap","lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER","delay":"1s"}"#,
        "`delay` does not belong to refusal",
    );
    rejects::<Preset>(r#"{"name":"Leader flap","lie":"refusal"}"#, "missing field `error`");
    rejects::<Preset>(r#"{"name":"Nothing"}"#, "missing field `lie`");
    rejects::<Preset>(r#"{"lie":"silence"}"#, "missing field `name`");
}
