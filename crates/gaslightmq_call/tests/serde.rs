//! The `serde` form of every type, and what reading rejects.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::fmt::Debug;
use std::num::NonZeroU64;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

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

fn number(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn name(text: &str) -> RecordedRuleName {
    text.parse().unwrap()
}

fn parts(texts: &[&str]) -> Target {
    Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
}

fn secs(secs: u64) -> TimeSpan {
    TimeSpan::from_secs(secs)
}

fn at(millis: u64) -> Instant {
    Instant::ZERO + Duration::from_millis(millis)
}

fn refusal(target: Target) -> TargetedLie {
    LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(target)
}

fn trigger(decision: TriggerDecision) -> TriggerExplanation {
    TriggerExplanation::new(decision, Some(number(1)), TriggerChecks::new()).unwrap()
}

fn leader_moves() -> Incident {
    Incident::LeaderMoves {
        partition: "orders.eu/3".parse().unwrap(),
        to: None,
    }
}

const FIRED: &str = r#"{"decision":"fired","match-number":1}"#;
const REFUSAL: &str = r#"{"lie":"refusal","error":"NOT_LEADER_OR_FOLLOWER","target":{"parts":["orders.eu/3"]}}"#;
const THREE: &str = r#"{"parts":["orders.eu/3"]}"#;

#[test]
fn a_call_number_is_a_number() {
    writes(&CallNumber::new(3).unwrap(), "3");
    rejects::<CallNumber>("0", "a call number is at least one");
    rejects::<CallNumber>(r#""3""#, "invalid type");
}

#[test]
fn a_call_id_is_its_text() {
    writes(&"#7.3".parse::<CallId>().unwrap(), r##""#7.3""##);
    reads(r#""7.3""#, &"#7.3".parse::<CallId>().unwrap());
    rejects::<CallId>(r##""#7.0""##, "call ID `#7.0`: a call number is at least one");
    rejects::<CallId>(r##""#7""##, "is not a connection and a call number");
}

#[test]
fn a_recorded_rule_name_is_its_text() {
    writes(&name("Leader flap"), r#""Leader flap""#);
    rejects::<RecordedRuleName>(r#""""#, "recorded rule name \"\" is empty");
}

#[test]
fn a_trigger_decision_is_its_name() {
    for decision in TriggerDecision::ALL {
        writes(&decision, &format!("{:?}", decision.name()));
    }
    rejects::<TriggerDecision>(r#""rolled""#, "no trigger decision is called \"rolled\"");
}

#[test]
fn an_answer_is_ok_or_an_error_code() {
    writes(&Answer::Ok, r#""ok""#);
    writes(
        &Answer::Error(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        r#"{"error":"NOT_LEADER_OR_FOLLOWER"}"#,
    );
    rejects::<Answer>(r#"{"error":"NONE"}"#, "NONE");
}

#[test]
fn a_response_summary_leaves_out_parts_it_does_not_have() {
    let partition: Part = "orders.eu/3".parse().unwrap();

    writes(&ResponseSummary::new(Answer::Ok), r#"{"answer":"ok"}"#);
    writes(
        &ResponseSummary::new(Answer::Ok).with_part(partition, ErrorCode::NOT_LEADER_OR_FOLLOWER),
        r#"{"answer":"ok","parts":{"orders.eu/3":{"error":"NOT_LEADER_OR_FOLLOWER"}}}"#,
    );
}

#[test]
fn what_the_client_received_is_a_response_or_a_word() {
    writes(
        &Delivered::Response(ResponseSummary::new(Answer::Ok)),
        r#"{"response":{"answer":"ok"}}"#,
    );
    writes(&Delivered::UnreadResponse, r#""unread-response""#);
    writes(&Delivered::Nothing, r#""nothing""#);
    writes(&Delivered::ConnectionClosed, r#""connection-closed""#);
}

#[test]
fn a_verdict_is_the_list_of_its_lies() {
    writes(&Verdict::pass(), "[]");
    writes(
        &Verdict::lied([
            LieDefinition::Silence.on(Target::Call),
            refusal(parts(&["orders.eu/3"])),
        ]),
        &format!(r#"[{{"lie":"silence"}},{REFUSAL}]"#),
    );
}

#[test]
fn the_checks_of_a_trigger_are_plain_numbers_and_time_spans() {
    writes(
        &DieCheck::new(374_213, 400_000).unwrap(),
        r#"{"roll":374213,"chance":400000}"#,
    );
    writes(
        &WindowCheck::new(secs(10), Some(secs(40)), secs(12)).unwrap(),
        r#"{"opens-after":"10s","closes-after":"40s","since-armed":"12s"}"#,
    );
    writes(
        &WindowCheck::new(secs(10), None, secs(12)).unwrap(),
        r#"{"opens-after":"10s","since-armed":"12s"}"#,
    );
    writes(&BudgetCheck::new(2, number(3)).unwrap(), r#"{"left":2,"limit":3}"#);
}

#[test]
fn a_check_that_cannot_be_does_not_read() {
    rejects::<DieCheck>(
        r#"{"roll":1000000,"chance":400000}"#,
        "a roll of 1000000 parts per million is not below one million",
    );
    rejects::<DieCheck>(r#"{"roll":1,"chance":400000,"sides":6}"#, "unknown field `sides`");
    rejects::<WindowCheck>(
        r#"{"opens-after":"10s","closes-after":"5s","since-armed":"0ms"}"#,
        "a time window from 10s to 5s after arming is never open",
    );
    rejects::<BudgetCheck>(r#"{"left":4,"limit":3}"#, "a budget of 3 does not have 4 left");
    rejects::<BudgetCheck>(r#"{"left":0,"limit":0}"#, "invalid value");
}

#[test]
fn a_trigger_explanation_writes_the_parts_it_has() {
    let checks = TriggerChecks::new()
        .with_count(true)
        .with_die(DieCheck::new(374_213, 400_000).unwrap())
        .with_window(WindowCheck::new(secs(10), None, secs(12)).unwrap());
    let all = TriggerExplanation::new(TriggerDecision::Fired, Some(number(3)), checks)
        .unwrap()
        .with_budget(BudgetCheck::new(2, number(3)).unwrap());
    let nothing = trigger(TriggerDecision::NothingToActOn).with_reason("leader-moves needs a partition");

    writes(&trigger(TriggerDecision::Fired), FIRED);
    writes(&TriggerExplanation::lifetime_over(), r#"{"decision":"lifetime-over"}"#);
    writes(
        &all,
        concat!(
            r#"{"decision":"fired","match-number":3,"count-agreed":true,"#,
            r#""die":{"roll":374213,"chance":400000},"#,
            r#""window":{"opens-after":"10s","since-armed":"12s"},"#,
            r#""budget":{"left":2,"limit":3}}"#
        ),
    );
    writes(
        &nothing,
        r#"{"decision":"nothing-to-act-on","match-number":1,"reason":"leader-moves needs a partition"}"#,
    );
}

#[test]
fn a_trigger_explanation_that_contradicts_itself_does_not_read() {
    rejects::<TriggerExplanation>(
        r#"{"decision":"fired","match-number":4,"count-agreed":false}"#,
        "a trigger does not fire while one of its parts does not agree",
    );
    rejects::<TriggerExplanation>(
        r#"{"decision":"held"}"#,
        "a trigger that decided `held` counted the call, and the match number is missing",
    );
    rejects::<TriggerExplanation>(
        r#"{"decision":"lifetime-over","match-number":4}"#,
        "a rule whose lifetime is over neither counts the call nor checks its trigger",
    );
    rejects::<TriggerExplanation>(r#"{"decision":"held","match-number":0}"#, "invalid value");
    rejects::<TriggerExplanation>(
        r#"{"decision":"held","match-number":1,"roll":3}"#,
        "unknown field `roll`",
    );
}

#[test]
fn a_fate_writes_where_the_lie_went() {
    let three = parts(&["orders.eu/3"]);
    let lost = LostTarget::new(three.clone(), name("Leader flap"));

    writes(&lost, &format!(r#"{{"on":{THREE},"to":"Leader flap"}}"#));
    writes(
        &LieFate::all_applied(refusal(three.clone())),
        &format!(r#"{{"fired":{REFUSAL},"applied":{THREE}}}"#),
    );
    writes(
        &LieFate::new(refusal(three.clone()), None, vec![lost.clone()], vec![lost]).unwrap(),
        &format!(
            r#"{{"fired":{REFUSAL},"shadowed":[{{"on":{THREE},"to":"Leader flap"}}],"pre-empted":[{{"on":{THREE},"to":"Leader flap"}}]}}"#
        ),
    );
    writes(
        &LieFate::all_applied(LieDefinition::Silence.on(Target::Call)),
        r#"{"fired":{"lie":"silence"},"applied":"call"}"#,
    );
}

#[test]
fn a_fate_writes_why_the_lie_could_not_be_told() {
    let three = parts(&["orders.eu/3"]);
    let lost = LostTarget::new(three.clone(), name("Leader flap"));
    let failure = || Failure::new("no behaviour is registered for refusal");

    // The lie won its target and changed nothing there.
    writes(
        &LieFate::all_applied(refusal(three.clone())).untold(failure()),
        &format!(r#"{{"fired":{REFUSAL},"untold":"no behaviour is registered for refusal"}}"#),
    );
    // What it lost to another rule stays.
    writes(
        &LieFate::new(refusal(three.clone()), None, vec![lost], vec![])
            .unwrap()
            .untold(failure()),
        &format!(
            r#"{{"fired":{REFUSAL},"shadowed":[{{"on":{THREE},"to":"Leader flap"}}],"untold":"no behaviour is registered for refusal"}}"#
        ),
    );
    rejects::<LieFate>(
        &format!(r#"{{"fired":{REFUSAL},"applied":{THREE},"untold":"no behaviour is registered for refusal"}}"#),
        "a lie that could not be told does not apply on orders.eu/3",
    );
}

#[test]
fn a_fate_that_cannot_be_does_not_read() {
    rejects::<LieFate>(
        &format!(r#"{{"fired":{REFUSAL}}}"#),
        "a lie that fired applied or lost somewhere, and this one did neither",
    );
    rejects::<LieFate>(
        &format!(r#"{{"fired":{REFUSAL},"applied":"call"}}"#),
        "a lie on orders.eu/3 does not apply on call",
    );
}

#[test]
fn a_line_has_the_keys_of_how_far_its_rule_got() {
    let three = parts(&["orders.eu/3"]);
    let held = RuleExplanation::held(
        name("Slow"),
        Priority::new(2),
        Target::Call,
        trigger(TriggerDecision::Held),
    );
    let lied = RuleExplanation::lied(
        name("Leader flap"),
        Priority::HIGHEST,
        three.clone(),
        trigger(TriggerDecision::Fired),
        LieFate::all_applied(refusal(three.clone())),
    );
    let fired = RuleExplanation::fired_incidents(
        name("Move"),
        Priority::new(1),
        three,
        trigger(TriggerDecision::Fired),
        vec![leader_moves()],
    );
    let ended = RuleExplanation::ended_before_truth(name("Lost ack"), Priority::new(3), name("Blackhole"));

    writes(
        &held.unwrap(),
        r#"{"rule":"Slow","priority":2,"matched":"call","trigger":{"decision":"held","match-number":1}}"#,
    );
    writes(
        &lied.unwrap(),
        &format!(
            r#"{{"rule":"Leader flap","priority":0,"matched":{THREE},"trigger":{FIRED},"lie":{{"fired":{REFUSAL},"applied":{THREE}}}}}"#
        ),
    );
    writes(
        &fired.unwrap(),
        &format!(
            r#"{{"rule":"Move","priority":1,"matched":{THREE},"trigger":{FIRED},"incidents":[{{"incident":"leader-moves","partition":"orders.eu/3"}}]}}"#
        ),
    );
    writes(&ended, r#"{"rule":"Lost ack","priority":3,"ended-by":"Blackhole"}"#);
}

#[test]
fn a_line_with_keys_that_do_not_go_together_does_not_read() {
    let shape = "has `ended-by` alone, or `matched` and `trigger` with at most one of `lie` and `incidents`";

    rejects::<RuleExplanation>(r#"{"rule":"Slow","priority":0}"#, shape);
    rejects::<RuleExplanation>(r#"{"rule":"Slow","priority":0,"matched":"call"}"#, shape);
    rejects::<RuleExplanation>(
        &format!(r#"{{"rule":"Slow","priority":0,"matched":"call","trigger":{FIRED},"ended-by":"Blackhole"}}"#),
        shape,
    );
    rejects::<RuleExplanation>(
        &format!(r#"{{"rule":"Slow","priority":0,"matched":"call","trigger":{FIRED}}}"#),
        "rule \"Slow\" fired, and the explanation has no lie and no incident of it",
    );
    rejects::<RuleExplanation>(
        &format!(
            r#"{{"rule":"Slow","priority":0,"matched":"call","trigger":{{"decision":"held","match-number":1}},"lie":{{"fired":{REFUSAL},"applied":{THREE}}}}}"#
        ),
        "did not fire (`held`)",
    );
    rejects::<RuleExplanation>(r#"{"rule":"","priority":0,"ended-by":"Blackhole"}"#, "is empty");
    rejects::<RuleExplanation>(
        r#"{"rule":"Slow","priority":0,"ended-by":"Blackhole","pass":"request"}"#,
        "unknown field `pass`",
    );
}

#[test]
fn an_explanation_writes_its_rules_and_its_failure() {
    let ended = RuleExplanation::ended_before_truth(name("Lost ack"), Priority::new(1), name("Blackhole"));
    let blackhole = RuleExplanation::lied(
        name("Blackhole"),
        Priority::HIGHEST,
        Target::Call,
        trigger(TriggerDecision::Fired),
        LieFate::all_applied(LieDefinition::Silence.on(Target::Call)),
    )
    .unwrap();

    writes(&Explanation::none(), "{}");
    writes(
        &Explanation::none().with_failure(Failure::new("the frame ends early")),
        r#"{"failure":"the frame ends early"}"#,
    );
    writes(
        &Explanation::new([blackhole, ended]).unwrap(),
        &format!(
            r#"{{"rules":[{{"rule":"Blackhole","priority":0,"matched":"call","trigger":{FIRED},"lie":{{"fired":{{"lie":"silence"}},"applied":"call"}}}},{{"rule":"Lost ack","priority":1,"ended-by":"Blackhole"}}]}}"#
        ),
    );
}

#[test]
fn an_explanation_that_names_a_rule_it_does_not_list_does_not_read() {
    rejects::<Explanation>(
        r#"{"rules":[{"rule":"Lost ack","priority":1,"ended-by":"Blackhole"}]}"#,
        "rule \"Lost ack\" lost to rule \"Blackhole\", which has no line in the explanation",
    );
    rejects::<Explanation>(
        r#"{"rules":[{"rule":"A","priority":0,"ended-by":"A"},{"rule":"A","priority":1,"ended-by":"A"}]}"#,
        "rule \"A\" has two lines in one explanation",
    );
}

#[test]
fn a_timing_writes_the_moments_it_has() {
    let all = CallTiming::arrived(at(250))
        .forwarded(at(251))
        .unwrap()
        .answered(at(265))
        .unwrap()
        .delivered(at(1015))
        .unwrap();

    writes(&CallTiming::arrived(at(250)), r#"{"arrived":250000000}"#);
    writes(
        &all,
        r#"{"arrived":250000000,"forwarded":251000000,"answered":265000000,"delivered":1015000000}"#,
    );
    rejects::<CallTiming>(
        r#"{"arrived":250,"forwarded":249}"#,
        "a request does not leave for the backend before it arrives",
    );
    rejects::<CallTiming>(
        r#"{"arrived":250,"answered":251}"#,
        "a backend does not answer a request that never left for it",
    );
}

/// A Fetch for two partitions of a consumer whose group the client context knows.
fn fetch() -> Fields {
    let [three, four]: [TopicPartition; 2] = ["orders.eu/3".parse().unwrap(), "orders.eu/4".parse().unwrap()];
    Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ClientId::new("analytics").unwrap())
        .with(ApiKey::FETCH)
        .with_learned(GroupId::new("analytics-etl").unwrap())
        .with_learned(GroupId::new("payments").unwrap())
        .with_part(PartFields::new(three).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .with_part(PartFields::new(four).succeeded())
        .with(Outcome::Ok)
        .with(Latency::from_millis(14))
}

const FETCH: &str = concat!(
    r#"{"values":[{"connection":7},{"client.id":"analytics"},{"api":"Fetch"},{"outcome":"ok"},{"latency":"14ms"}],"#,
    r#""learned":[{"group":"analytics-etl"},{"group":"payments"}],"#,
    r#""parts":[{"part":"orders.eu/3","truth":{"error":"NOT_LEADER_OR_FOLLOWER"}},{"part":"orders.eu/4","truth":"ok"}]}"#
);

#[test]
fn a_call_writes_every_part_of_its_record() {
    let four = parts(&["orders.eu/4"]);
    let explanation = Explanation::new([RuleExplanation::lied(
        name("Refuse orders"),
        Priority::HIGHEST,
        four.clone(),
        trigger(TriggerDecision::Fired),
        LieFate::all_applied(
            LieDefinition::from(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse)).on(four),
        ),
    )
    .unwrap()])
    .unwrap();
    let timing = CallTiming::arrived(at(250))
        .forwarded(at(250))
        .unwrap()
        .answered(at(264))
        .unwrap()
        .delivered(at(264))
        .unwrap();
    let delivered = ResponseSummary::new(Answer::Ok)
        .with_part(
            "orders.eu/3".parse::<Part>().unwrap(),
            ErrorCode::NOT_LEADER_OR_FOLLOWER,
        )
        .with_part("orders.eu/4".parse::<Part>().unwrap(), ErrorCode::REQUEST_TIMED_OUT);
    let call = Call::new(
        CallNumber::new(3).unwrap(),
        fetch(),
        delivered.into(),
        explanation,
        timing,
    )
    .unwrap()
    .with_upstream(ResponseSummary::new(Answer::Ok));

    let lie = r#"{"lie":"refusal","error":"REQUEST_TIMED_OUT","stage":"after","target":{"parts":["orders.eu/4"]}}"#;
    let json = format!(
        concat!(
            r#"{{"number":3,"fields":{fields},"upstream":{{"answer":"ok"}},"#,
            r#""delivered":{{"response":{{"answer":"ok","parts":{{"orders.eu/3":{{"error":"NOT_LEADER_OR_FOLLOWER"}},"orders.eu/4":{{"error":"REQUEST_TIMED_OUT"}}}}}}}},"#,
            r#""verdict":[{lie}],"#,
            r#""explanation":{{"rules":[{{"rule":"Refuse orders","priority":0,"matched":{{"parts":["orders.eu/4"]}},"trigger":{fired},"lie":{{"fired":{lie},"applied":{{"parts":["orders.eu/4"]}}}}}}]}},"#,
            r#""timing":{{"arrived":250000000,"forwarded":250000000,"answered":264000000,"delivered":264000000}}}}"#
        ),
        fields = FETCH,
        lie = lie,
        fired = FIRED,
    );

    writes(&call, &json);
    assert_eq!(call.id().to_string(), "#7.3");
}

#[test]
fn a_call_that_never_reached_the_backend_reads_back_without_a_truth() {
    let partition: TopicPartition = "orders.eu/3".parse().unwrap();
    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition.clone()).without_truth())
        .with_part(TopicName::new("payments.settled").unwrap())
        .without_truth();
    let call = Call::new(
        CallNumber::FIRST,
        fields,
        Delivered::Nothing,
        Explanation::none(),
        CallTiming::arrived(at(0)),
    )
    .unwrap();

    writes(
        &call,
        concat!(
            r#"{"number":1,"fields":{"values":[{"connection":7},{"api":"Produce"}],"#,
            r#""parts":[{"part":"orders.eu/3","truth":"none"},{"part":"payments.settled"}]},"#,
            r#""delivered":"nothing","verdict":[],"explanation":{},"timing":{"arrived":0}}"#
        ),
    );
    assert_eq!(call.read(Field::Outcome), Reading::Absent);
}

#[test]
fn reading_a_call_takes_the_verdict_from_the_explanation() {
    let without = r#"{"number":1,"fields":{"values":[{"connection":7}]},"delivered":"nothing","explanation":{},"timing":{"arrived":0}}"#;
    let with_another = r#"{"number":1,"fields":{"values":[{"connection":7}]},"delivered":"nothing","verdict":[{"lie":"silence"}],"explanation":{},"timing":{"arrived":0}}"#;

    let call: Call = serde_json::from_str(without).unwrap();
    assert!(call.verdict().is_pass());
    reads(with_another, &call);
}

#[test]
fn a_call_whose_fields_cannot_be_does_not_read() {
    let call = |fields: &str| {
        format!(r#"{{"number":1,"fields":{fields},"delivered":"nothing","explanation":{{}},"timing":{{"arrived":0}}}}"#)
    };

    rejects::<Call>(
        &call(r#"{"values":[{"api":"Fetch"}]}"#),
        "a call record needs the one `connection` its call ran on",
    );
    rejects::<Call>(
        &call(r#"{"values":[{"connection":7},{"topic":"orders.eu"}]}"#),
        "`topic` belongs to a part of the call, and not to its values",
    );
    rejects::<Call>(
        &call(r#"{"values":[{"connection":7}],"learned":[{"api":"Fetch"}]}"#),
        "`api` is not a learned field",
    );
    rejects::<Call>(&call(r#"{"values":[{"connection":7},{"api":"Fecth"}]}"#), "Fecth");
    rejects::<Call>(
        &call(r#"{"values":[{"connection":7}],"parts":[{"part":"orders.eu/3","truth":"maybe"}]}"#),
        "unknown variant `maybe`",
    );
}

#[test]
fn a_failure_is_its_message() {
    writes(&Failure::new("the frame ends early"), r#""the frame ends early""#);
}
