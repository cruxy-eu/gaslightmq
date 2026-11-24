//! How the lies of the request pass and of the response pass combine:
//! priority holds within one pass, and between the passes the stage decides.

mod common;

use common::*;
use gaslightmq_combine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

type Lies = Vec<FiredLie<&'static str>>;

/// Combines the lies of both passes of one call.
fn combine(request: Lies, response: Lies) -> Combination<&'static str> {
    RequestPass::of(request).response_pass(response)
}

// A terminal lie before forwarding

#[test]
fn a_silence_that_won_the_request_pass_pre_empts_the_response_pass_whatever_the_priority() {
    // A live rule that waits for the truth does not stop a scenario rule from ending the call first.
    let request = RequestPass::of([lie("blackhole", 5, silence())]);
    assert_eq!(request.before_forwarding().terminal(), Some(&"blackhole"));

    let call = request.response_pass([
        lie(
            "lost-acknowledgement",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("slow-answer", 1, delay(1000).with_throttle(millis(500))),
        lie("stale-watermark", 2, stale_watermark(parts(&["orders.eu/3"]))),
        lie("late-disconnect", 3, late_hang_up()),
    ]);

    assert_eq!(fate(&call, "blackhole"), "applied on call");
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "pre-empted on orders.eu/3, orders.eu/4 by blackhole"
    );
    assert_eq!(fate(&call, "slow-answer"), "pre-empted on call by blackhole");
    assert_eq!(fate(&call, "stale-watermark"), "pre-empted on orders.eu/3 by blackhole");
    assert_eq!(fate(&call, "late-disconnect"), "pre-empted on call by blackhole");
    assert_eq!(call.fates().iter().filter(|fate| fate.counts()).count(), 1);

    // There is no answer, so nothing happens after the response.
    assert_eq!(call.before_forwarding().terminal(), Some(&"blackhole"));
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_hang_up_before_forwarding_pre_empts_the_response_pass_as_a_silence_does() {
    let call = combine(
        vec![lie("disconnect", 1, hang_up())],
        vec![lie("lost-acknowledgement", 0, late_refusal(Target::Call))],
    );

    assert_eq!(fate(&call, "lost-acknowledgement"), "pre-empted on call by disconnect");
}

#[test]
fn a_terminal_lie_that_lost_the_request_pass_ends_nothing() {
    let call = combine(
        vec![
            lie("leader-flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie("blackhole", 1, silence()),
        ],
        vec![lie("lost-acknowledgement", 2, late_refusal(parts(&["orders.eu/4"])))],
    );

    assert_eq!(fate(&call, "blackhole"), "shadowed on call by leader-flap");
    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/4");
    assert_eq!(call.before_forwarding().terminal(), None);
}

// A part refused before forwarding

#[test]
fn a_partition_that_was_refused_before_forwarding_has_no_truth_for_the_response_pass() {
    let request = RequestPass::of([lie("leader-flap", 5, refusal(parts(&["orders.eu/3"])))]);
    assert_eq!(
        held(request.before_forwarding().refusals()),
        ["orders.eu/3: leader-flap"]
    );

    // Both lies of the response pass have a higher priority, and the refused partition still skips them.
    let call = request.response_pass([
        lie(
            "lost-acknowledgement",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie(
            "stale-watermark",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/5"])),
        ),
    ]);

    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "applied on orders.eu/4; pre-empted on orders.eu/3 by leader-flap"
    );
    assert_eq!(
        fate(&call, "stale-watermark"),
        "applied on orders.eu/5; pre-empted on orders.eu/3 by leader-flap"
    );

    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: leader-flap"]);
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/4: lost-acknowledgement"]
    );
}

#[test]
fn a_response_pass_lie_on_refused_partitions_only_does_not_count() {
    let call = combine(
        vec![lie("leader-flap", 1, refusal(parts(&["orders.eu/3"])))],
        vec![lie("lost-acknowledgement", 0, late_refusal(parts(&["orders.eu/3"])))],
    );

    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "pre-empted on orders.eu/3 by leader-flap"
    );
    assert!(!call.fate_of(&"lost-acknowledgement").unwrap().counts());
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_whole_call_that_was_refused_before_forwarding_has_no_truth_at_all() {
    // The pipeline synthesizes the error response, and the response pass still runs.
    let call = combine(
        vec![lie("coordinator-gone", 3, refusal(Target::Call))],
        vec![
            lie("rebalance", 0, late_refusal(Target::Call)),
            lie("another-controller", 1, rewrite(Aspect::Controller, Target::Call)),
            lie("slow-answer", 2, delay(1000)),
        ],
    );

    assert_eq!(fate(&call, "coordinator-gone"), "applied on call");
    assert_eq!(fate(&call, "rebalance"), "pre-empted on call by coordinator-gone");
    assert_eq!(
        fate(&call, "another-controller"),
        "pre-empted on call by coordinator-gone"
    );
    // A refusal pre-empts no modifier: the error response waits.
    assert_eq!(fate(&call, "slow-answer"), "applied on call");
    assert_eq!(call.after_response().delay().length(), millis(1000));
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_refuses_what_a_refusal_before_forwarding_left() {
    let request = RequestPass::of([lie("leader-flap", 5, refusal(parts(&["orders.eu/3"])))]);
    let before = request.before_forwarding().clone();

    // `orders.eu/3` never reached the backend. Every other partition has a truth, and "rebalance" replaces it.
    let call = request.response_pass([
        lie("rebalance", 0, late_refusal(Target::Call)),
        lie(
            "lost-acknowledgement",
            1,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("stale-watermark", 2, stale_watermark(parts(&["orders.eu/4"]))),
        lie("another-controller", 3, rewrite(Aspect::Controller, Target::Call)),
    ]);

    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "rebalance"), "applied on call");
    assert!(call.fate_of(&"rebalance").unwrap().counts());

    // Below it, the refused partition has no truth, and the rest is refused.
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "shadowed on orders.eu/4 by rebalance; pre-empted on orders.eu/3 by leader-flap"
    );
    assert_eq!(fate(&call, "stale-watermark"), "pre-empted on orders.eu/4 by rebalance");
    assert_eq!(fate(&call, "another-controller"), "pre-empted on call by rebalance");

    // The refused partition keeps its refusal, and the refusal of the whole call covers the others.
    assert_eq!(call.before_forwarding(), &before);
    assert_eq!(held(before.refusals()), ["orders.eu/3: leader-flap"]);
    assert_eq!(held(call.after_response().refusals()), ["call: rebalance"]);
    assert_eq!(
        call.after_response().refusals().of(&part("orders.eu/4")),
        Some(&"rebalance")
    );
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_applies_whatever_the_priority_of_the_refusal_before_forwarding() {
    let call = combine(
        vec![lie("leader-flap", 0, refusal(parts(&["orders.eu/3"])))],
        vec![lie("rebalance", 1, late_refusal(Target::Call))],
    );

    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "rebalance"), "applied on call");
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_conflicts_with_a_refusal_after_the_response() {
    // The refusal before forwarding already happened. The refusal after the response is a conflict, by priority.
    let fired = |rebalance: usize| {
        combine(
            vec![
                lie("leader-flap", 1, refusal(parts(&["orders.eu/3"]))),
                lie("lost-acknowledgement", 2, late_refusal(parts(&["orders.eu/4"]))),
            ],
            vec![lie("rebalance", rebalance, late_refusal(Target::Call))],
        )
    };

    let below = fired(3);
    assert_eq!(fate(&below, "rebalance"), "shadowed on call by lost-acknowledgement");
    assert_eq!(fate(&below, "lost-acknowledgement"), "applied on orders.eu/4");
    assert_eq!(
        held(below.after_response().refusals()),
        ["orders.eu/4: lost-acknowledgement"]
    );

    let above = fired(0);
    assert_eq!(fate(&above, "rebalance"), "applied on call");
    assert_eq!(
        fate(&above, "lost-acknowledgement"),
        "shadowed on orders.eu/4 by rebalance"
    );
    assert_eq!(fate(&above, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(held(above.after_response().refusals()), ["call: rebalance"]);
}

#[test]
fn a_request_pass_refusal_of_the_whole_call_below_a_refusal_before_forwarding_stays_shadowed() {
    // Both fired in the request pass, where the whole call is one target and priority decides.
    let call = combine(
        vec![
            lie("leader-flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie("rebalance", 1, late_refusal(Target::Call)),
        ],
        vec![lie("slow-answer", 2, delay(1000))],
    );

    assert_eq!(fate(&call, "rebalance"), "shadowed on call by leader-flap");
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_refusal_before_forwarding_that_was_shadowed_refused_nothing() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("lost-acknowledgement", 1, late_refusal(partition.clone())),
            lie("leader-flap", 2, refusal(partition.clone())),
        ],
        vec![lie("stale-watermark", 0, stale_watermark(parts(&["orders.eu/4"])))],
    );

    // The partition reached the backend, and the refusal after the response replaces its truth.
    assert_eq!(
        fate(&call, "leader-flap"),
        "shadowed on orders.eu/3 by lost-acknowledgement"
    );
    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/3");
    assert!(call.before_forwarding().refusals().is_empty());
}

// Everything else meets by priority

#[test]
fn a_response_pass_refusal_with_a_higher_priority_shadows_a_request_pass_refusal_after_the_response() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("lost-acknowledgement", 1, late_refusal(partition.clone()))],
        vec![lie("fail-what-succeeded", 0, late_refusal(partition))],
    );

    assert_eq!(fate(&call, "fail-what-succeeded"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "shadowed on orders.eu/3 by fail-what-succeeded"
    );
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/3: fail-what-succeeded"]
    );
    assert_eq!(ids(&call), ["fail-what-succeeded", "lost-acknowledgement"]);
}

#[test]
fn a_response_pass_refusal_with_a_lower_priority_is_shadowed_by_a_request_pass_refusal_after_the_response() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("lost-acknowledgement", 0, late_refusal(partition.clone()))],
        vec![lie("fail-what-succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "fail-what-succeeded"),
        "shadowed on orders.eu/3 by lost-acknowledgement"
    );
}

#[test]
fn rewrites_of_both_passes_conflict_by_priority() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("stale-watermark", 1, stale_watermark(partition.clone())),
            lie("dropped-records", 3, rewrite(Aspect::Records, partition.clone())),
        ],
        vec![
            lie("watermark-at-zero", 0, stale_watermark(partition.clone())),
            lie("corrupt-records", 4, rewrite(Aspect::Records, partition)),
        ],
    );

    assert_eq!(fate(&call, "watermark-at-zero"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "stale-watermark"),
        "shadowed on orders.eu/3 by watermark-at-zero"
    );
    assert_eq!(fate(&call, "dropped-records"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "corrupt-records"),
        "shadowed on orders.eu/3 by dropped-records"
    );
}

#[test]
fn a_response_pass_refusal_pre_empts_a_request_pass_rewrite_of_its_partition() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("stale-watermark", 0, stale_watermark(partition.clone()))],
        vec![lie("fail-what-succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(
        fate(&call, "stale-watermark"),
        "pre-empted on orders.eu/3 by fail-what-succeeded"
    );
    assert_eq!(call.after_response().rewrites().count(), 0);
}

#[test]
fn a_hang_up_after_the_response_that_won_the_request_pass_can_lose_the_response_pass() {
    let call = combine(
        vec![
            lie("late-disconnect", 1, late_hang_up()),
            lie("leader-flap", 2, refusal(parts(&["orders.eu/3"]))),
            lie("lost-acknowledgement", 3, late_refusal(parts(&["orders.eu/4"]))),
            lie("slow-answer", 4, delay(1000)),
        ],
        vec![lie("fail-what-succeeded", 0, late_refusal(parts(&["orders.eu/5"])))],
    );

    // The refusal of the response pass has the higher priority, so the hang-up loses the conflict.
    assert_eq!(fate(&call, "fail-what-succeeded"), "applied on orders.eu/5");
    assert_eq!(
        fate(&call, "late-disconnect"),
        "shadowed on call by fail-what-succeeded"
    );

    // What the hang-up kept from acting after the response applies after all.
    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/4");
    assert_eq!(fate(&call, "slow-answer"), "applied on call");

    // What it kept from acting before forwarding lost its chance: priority held within the request pass.
    // The request was forwarded whole, so the loss stays, to a hang-up that ended nothing. Neither firing counts.
    assert_eq!(fate(&call, "leader-flap"), "shadowed on orders.eu/3 by late-disconnect");
    assert!(!call.fate_of(&"leader-flap").unwrap().counts());
    assert!(!call.fate_of(&"late-disconnect").unwrap().counts());
    assert!(call.before_forwarding().changes_nothing());

    let after = call.after_response();
    assert_eq!(after.terminal(), None);
    assert_eq!(
        held(after.refusals()),
        ["orders.eu/4: lost-acknowledgement", "orders.eu/5: fail-what-succeeded"]
    );
    assert_eq!(after.delay().length(), millis(1000));
}

#[test]
fn a_refusal_before_forwarding_keeps_its_loss_when_its_winner_loses_the_partition_in_the_response_pass() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie(
                "lost-acknowledgement",
                1,
                late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
            lie("leader-flap", 2, refusal(partition.clone())),
        ],
        vec![lie("fail-what-succeeded", 0, late_refusal(partition))],
    );

    // The partition reached the backend, so its refusal before forwarding cannot apply any more.
    assert_eq!(fate(&call, "fail-what-succeeded"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by fail-what-succeeded"
    );
    assert_eq!(
        fate(&call, "leader-flap"),
        "shadowed on orders.eu/3 by lost-acknowledgement"
    );
    assert!(!call.fate_of(&"leader-flap").unwrap().counts());
    assert!(call.before_forwarding().changes_nothing());
}

#[test]
fn a_hang_up_after_the_response_that_won_the_request_pass_wins_the_response_pass_from_above() {
    let call = combine(
        vec![
            lie("late-disconnect", 0, late_hang_up()),
            lie("slow-request", 1, delay(200).at(BEFORE)),
        ],
        vec![
            lie("fail-what-succeeded", 2, late_refusal(parts(&["orders.eu/5"]))),
            lie("slow-answer", 3, delay(1000)),
        ],
    );

    assert_eq!(fate(&call, "late-disconnect"), "applied on call");
    assert_eq!(
        fate(&call, "fail-what-succeeded"),
        "shadowed on orders.eu/5 by late-disconnect"
    );
    assert_eq!(fate(&call, "slow-answer"), "pre-empted on call by late-disconnect");
    assert_eq!(fate(&call, "slow-request"), "applied on call");
    assert_eq!(call.after_response().terminal(), Some(&"late-disconnect"));
}

#[test]
fn a_response_pass_hang_up_does_not_undo_a_refusal_before_forwarding() {
    // The partition never reached the backend, and the connection closes after the rest was answered.
    let call = combine(
        vec![
            lie("leader-flap", 3, refusal(parts(&["orders.eu/3"]))),
            lie("lost-acknowledgement", 4, late_refusal(parts(&["orders.eu/4"]))),
            lie("slow-request", 5, delay(200).at(BEFORE)),
            lie("slow-answer", 6, delay(1000)),
        ],
        vec![lie("disconnect-on-success", 0, late_hang_up())],
    );

    assert_eq!(fate(&call, "disconnect-on-success"), "applied on call");
    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "slow-request"), "applied on call");

    // The hang-up conflicts with the refusal after the response, and it pre-empts the delay of the answer.
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "shadowed on orders.eu/4 by disconnect-on-success"
    );
    assert_eq!(
        fate(&call, "slow-answer"),
        "pre-empted on call by disconnect-on-success"
    );

    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: leader-flap"]);
    assert_eq!(call.after_response().terminal(), Some(&"disconnect-on-success"));
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_response_pass_hang_up_leaves_no_answer_to_carry_the_throttle_of_the_request_pass() {
    let call = combine(
        vec![
            lie("slow-request", 1, delay(200).with_throttle(millis(700)).at(BEFORE)),
            lie("throttle", 2, TimingLie::new().with_throttle(millis(500))),
        ],
        vec![lie("disconnect-on-success", 0, late_hang_up())],
    );

    // The request waited before the hang-up fired, so that lie applied.
    assert_eq!(fate(&call, "slow-request"), "applied on call");
    assert_eq!(fate(&call, "throttle"), "pre-empted on call by disconnect-on-success");
    assert_eq!(call.before_forwarding().delay().length(), millis(200));

    assert_eq!(call.after_response().terminal(), Some(&"disconnect-on-success"));
    assert_eq!(call.after_response().throttle(), millis(0));
}

#[test]
fn a_response_pass_hang_up_below_a_refusal_after_the_response_is_shadowed() {
    let call = combine(
        vec![lie("lost-acknowledgement", 0, late_refusal(parts(&["orders.eu/4"])))],
        vec![lie("disconnect-on-success", 1, late_hang_up())],
    );

    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/4");
    assert_eq!(
        fate(&call, "disconnect-on-success"),
        "shadowed on call by lost-acknowledgement"
    );
}

#[test]
fn a_request_pass_hang_up_after_the_response_below_a_refusal_before_forwarding_stays_shadowed() {
    let call = combine(
        vec![
            lie("leader-flap", 1, refusal(parts(&["orders.eu/3"]))),
            lie("late-disconnect", 2, late_hang_up()),
        ],
        vec![lie("slow-answer", 0, delay(1000))],
    );

    assert_eq!(fate(&call, "late-disconnect"), "shadowed on call by leader-flap");
    assert_eq!(fate(&call, "slow-answer"), "applied on call");
}

#[test]
fn a_request_pass_refusal_after_the_response_below_a_refusal_before_forwarding_stays_shadowed() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("leader-flap", 1, refusal(partition.clone())),
            lie(
                "lost-acknowledgement",
                2,
                late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
        ],
        vec![lie("fail-what-succeeded", 0, late_refusal(partition))],
    );

    // The lie of its own pass lost a conflict, and the lie of the response pass found no truth.
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by leader-flap"
    );
    assert_eq!(
        fate(&call, "fail-what-succeeded"),
        "pre-empted on orders.eu/3 by leader-flap"
    );
}

// Modifiers of both passes

#[test]
fn delays_of_both_passes_add_up_at_their_stage_and_throttle_takes_the_maximum() {
    let call = combine(
        vec![
            lie("slow-request", 0, delay(200).with_throttle(millis(100)).at(BEFORE)),
            lie("slow-answer", 1, delay(1000).with_jitter(millis(200))),
        ],
        vec![
            lie("slow-success", 2, delay(500).with_jitter(millis(50))),
            lie("throttle-success", 3, TimingLie::new().with_throttle(millis(700))),
        ],
    );

    assert!(call.fates().iter().all(Fate::counts));

    let before = call.before_forwarding();
    assert_eq!(before.delay().length(), millis(200));
    assert_eq!(before.throttle(), millis(0));

    let after = call.after_response();
    assert_eq!(after.delay().length(), millis(1500));
    assert_eq!(after.delay().jitter(), millis(250));
    assert_eq!(after.throttle(), millis(700));
}

// The stage of a lie of the response pass

#[test]
fn a_response_pass_lie_acts_after_the_response_whatever_stage_it_names() {
    // The truth exists by the time the lie fires, so before forwarding has passed.
    let call = combine(
        vec![],
        vec![
            lie("refuse", 0, refusal(parts(&["orders.eu/3"]))),
            lie("slow", 1, delay(200).at(BEFORE)),
        ],
    );

    assert_eq!(fate(&call, "refuse"), "applied on orders.eu/3");
    assert_eq!(call.fate_of(&"refuse").unwrap().stage(), AFTER);
    assert_eq!(call.fate_of(&"refuse").unwrap().claim().stage(), BEFORE);
    assert_eq!(call.fate_of(&"slow").unwrap().stage(), AFTER);

    assert!(call.before_forwarding().changes_nothing());
    assert_eq!(held(call.after_response().refusals()), ["orders.eu/3: refuse"]);
    assert_eq!(call.after_response().delay().length(), millis(200));
}

#[test]
fn a_response_pass_silence_ends_the_call_after_the_response() {
    let call = combine(
        vec![lie("slow-answer", 0, delay(1000))],
        vec![lie("blackhole-on-success", 1, silence())],
    );

    assert_eq!(fate(&call, "blackhole-on-success"), "applied on call");
    assert_eq!(fate(&call, "slow-answer"), "pre-empted on call by blackhole-on-success");
    assert_eq!(call.before_forwarding().terminal(), None);
    assert_eq!(call.after_response().terminal(), Some(&"blackhole-on-success"));
}

// The passes as a whole

#[test]
fn the_effect_before_forwarding_is_final_after_the_request_pass() {
    let request = RequestPass::of([
        lie("leader-flap", 2, refusal(parts(&["orders.eu/3"]))),
        lie("slow-request", 3, delay(200).at(BEFORE)),
        lie("lost-acknowledgement", 4, late_refusal(parts(&["orders.eu/4"]))),
    ]);
    let before = request.before_forwarding().clone();

    let call = request.response_pass([
        lie(
            "fail-what-succeeded",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("disconnect-on-success", 1, late_hang_up()),
    ]);

    assert_eq!(call.before_forwarding(), &before);
    assert_eq!(held(before.refusals()), ["orders.eu/3: leader-flap"]);
    assert_eq!(before.delay().length(), millis(200));
}

#[test]
fn a_call_without_a_response_pass_is_a_call_whose_response_pass_fired_no_lie() {
    let fired = || {
        vec![
            lie("late-disconnect", 1, late_hang_up()),
            lie("leader-flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie(
                "stale-watermark",
                2,
                stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
            lie("slow-answer", 3, delay(1000)),
        ]
    };

    assert_eq!(
        RequestPass::of(fired()).without_response_pass(),
        RequestPass::of(fired()).response_pass([])
    );
}

#[test]
fn the_fates_of_both_passes_come_from_the_highest_priority_down() {
    let call = combine(
        vec![lie("third", 3, delay(1)), lie("first", 1, delay(1))],
        vec![
            lie("fourth", 4, delay(1)),
            lie("second", 2, delay(1)),
            lie("zeroth", 0, delay(1)),
        ],
    );

    assert_eq!(ids(&call), ["zeroth", "first", "second", "third", "fourth"]);
}

#[test]
fn a_request_pass_lie_comes_before_a_response_pass_lie_of_the_same_priority() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("lost-acknowledgement", 1, late_refusal(partition.clone()))],
        vec![lie("fail-what-succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(ids(&call), ["lost-acknowledgement", "fail-what-succeeded"]);
    assert_eq!(
        fate(&call, "fail-what-succeeded"),
        "shadowed on orders.eu/3 by lost-acknowledgement"
    );
}
