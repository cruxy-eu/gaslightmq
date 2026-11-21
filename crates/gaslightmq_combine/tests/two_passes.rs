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
    let request = RequestPass::of([lie("Blackhole", 5, silence())]);
    assert_eq!(request.before_forwarding().terminal(), Some(&"Blackhole"));

    let call = request.response_pass([
        lie(
            "Lost acknowledgement",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("Slow answer", 1, delay(1000).with_throttle(millis(500))),
        lie("Stale watermark", 2, stale_watermark(parts(&["orders.eu/3"]))),
        lie("Late disconnect", 3, late_hang_up()),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "applied on call");
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "pre-empted on orders.eu/3, orders.eu/4 by Blackhole"
    );
    assert_eq!(fate(&call, "Slow answer"), "pre-empted on call by Blackhole");
    assert_eq!(fate(&call, "Stale watermark"), "pre-empted on orders.eu/3 by Blackhole");
    assert_eq!(fate(&call, "Late disconnect"), "pre-empted on call by Blackhole");
    assert_eq!(call.fates().iter().filter(|fate| fate.counts()).count(), 1);

    // There is no answer, so nothing happens after the response.
    assert_eq!(call.before_forwarding().terminal(), Some(&"Blackhole"));
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_hang_up_before_forwarding_pre_empts_the_response_pass_as_a_silence_does() {
    let call = combine(
        vec![lie("Disconnect", 1, hang_up())],
        vec![lie("Lost acknowledgement", 0, late_refusal(Target::Call))],
    );

    assert_eq!(fate(&call, "Lost acknowledgement"), "pre-empted on call by Disconnect");
}

#[test]
fn a_terminal_lie_that_lost_the_request_pass_ends_nothing() {
    let call = combine(
        vec![
            lie("Leader flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie("Blackhole", 1, silence()),
        ],
        vec![lie("Lost acknowledgement", 2, late_refusal(parts(&["orders.eu/4"])))],
    );

    assert_eq!(fate(&call, "Blackhole"), "shadowed on call by Leader flap");
    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/4");
    assert_eq!(call.before_forwarding().terminal(), None);
}

// A part refused before forwarding

#[test]
fn a_partition_that_was_refused_before_forwarding_has_no_truth_for_the_response_pass() {
    let request = RequestPass::of([lie("Leader flap", 5, refusal(parts(&["orders.eu/3"])))]);
    assert_eq!(
        held(request.before_forwarding().refusals()),
        ["orders.eu/3: Leader flap"]
    );

    // Both lies of the response pass have a higher priority, and the refused partition still skips them.
    let call = request.response_pass([
        lie(
            "Lost acknowledgement",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie(
            "Stale watermark",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/5"])),
        ),
    ]);

    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "applied on orders.eu/4; pre-empted on orders.eu/3 by Leader flap"
    );
    assert_eq!(
        fate(&call, "Stale watermark"),
        "applied on orders.eu/5; pre-empted on orders.eu/3 by Leader flap"
    );

    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: Leader flap"]);
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/4: Lost acknowledgement"]
    );
}

#[test]
fn a_response_pass_lie_on_refused_partitions_only_does_not_count() {
    let call = combine(
        vec![lie("Leader flap", 1, refusal(parts(&["orders.eu/3"])))],
        vec![lie("Lost acknowledgement", 0, late_refusal(parts(&["orders.eu/3"])))],
    );

    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "pre-empted on orders.eu/3 by Leader flap"
    );
    assert!(!call.fate_of(&"Lost acknowledgement").unwrap().counts());
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_whole_call_that_was_refused_before_forwarding_has_no_truth_at_all() {
    // The pipeline synthesizes the error response, and the response pass still runs.
    let call = combine(
        vec![lie("Coordinator gone", 3, refusal(Target::Call))],
        vec![
            lie("Rebalance", 0, late_refusal(Target::Call)),
            lie("Another controller", 1, rewrite(Aspect::Controller, Target::Call)),
            lie("Slow answer", 2, delay(1000)),
        ],
    );

    assert_eq!(fate(&call, "Coordinator gone"), "applied on call");
    assert_eq!(fate(&call, "Rebalance"), "pre-empted on call by Coordinator gone");
    assert_eq!(
        fate(&call, "Another controller"),
        "pre-empted on call by Coordinator gone"
    );
    // A refusal pre-empts no modifier: the error response waits.
    assert_eq!(fate(&call, "Slow answer"), "applied on call");
    assert_eq!(call.after_response().delay().length(), millis(1000));
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_refuses_what_a_refusal_before_forwarding_left() {
    let request = RequestPass::of([lie("Leader flap", 5, refusal(parts(&["orders.eu/3"])))]);
    let before = request.before_forwarding().clone();

    // `orders.eu/3` never reached the backend. Every other partition has a truth, and "Rebalance" replaces it.
    let call = request.response_pass([
        lie("Rebalance", 0, late_refusal(Target::Call)),
        lie(
            "Lost acknowledgement",
            1,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("Stale watermark", 2, stale_watermark(parts(&["orders.eu/4"]))),
        lie("Another controller", 3, rewrite(Aspect::Controller, Target::Call)),
    ]);

    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Rebalance"), "applied on call");
    assert!(call.fate_of(&"Rebalance").unwrap().counts());

    // Below it, the refused partition has no truth, and the rest is refused.
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "shadowed on orders.eu/4 by Rebalance; pre-empted on orders.eu/3 by Leader flap"
    );
    assert_eq!(fate(&call, "Stale watermark"), "pre-empted on orders.eu/4 by Rebalance");
    assert_eq!(fate(&call, "Another controller"), "pre-empted on call by Rebalance");

    // The refused partition keeps its refusal, and the refusal of the whole call covers the others.
    assert_eq!(call.before_forwarding(), &before);
    assert_eq!(held(before.refusals()), ["orders.eu/3: Leader flap"]);
    assert_eq!(held(call.after_response().refusals()), ["call: Rebalance"]);
    assert_eq!(
        call.after_response().refusals().of(&part("orders.eu/4")),
        Some(&"Rebalance")
    );
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_applies_whatever_the_priority_of_the_refusal_before_forwarding() {
    let call = combine(
        vec![lie("Leader flap", 0, refusal(parts(&["orders.eu/3"])))],
        vec![lie("Rebalance", 1, late_refusal(Target::Call))],
    );

    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Rebalance"), "applied on call");
}

#[test]
fn a_response_pass_refusal_of_the_whole_call_conflicts_with_a_refusal_after_the_response() {
    // The refusal before forwarding already happened. The refusal after the response is a conflict, by priority.
    let fired = |rebalance: usize| {
        combine(
            vec![
                lie("Leader flap", 1, refusal(parts(&["orders.eu/3"]))),
                lie("Lost acknowledgement", 2, late_refusal(parts(&["orders.eu/4"]))),
            ],
            vec![lie("Rebalance", rebalance, late_refusal(Target::Call))],
        )
    };

    let below = fired(3);
    assert_eq!(fate(&below, "Rebalance"), "shadowed on call by Lost acknowledgement");
    assert_eq!(fate(&below, "Lost acknowledgement"), "applied on orders.eu/4");
    assert_eq!(
        held(below.after_response().refusals()),
        ["orders.eu/4: Lost acknowledgement"]
    );

    let above = fired(0);
    assert_eq!(fate(&above, "Rebalance"), "applied on call");
    assert_eq!(
        fate(&above, "Lost acknowledgement"),
        "shadowed on orders.eu/4 by Rebalance"
    );
    assert_eq!(fate(&above, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(held(above.after_response().refusals()), ["call: Rebalance"]);
}

#[test]
fn a_request_pass_refusal_of_the_whole_call_below_a_refusal_before_forwarding_stays_shadowed() {
    // Both fired in the request pass, where the whole call is one target and priority decides.
    let call = combine(
        vec![
            lie("Leader flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie("Rebalance", 1, late_refusal(Target::Call)),
        ],
        vec![lie("Slow answer", 2, delay(1000))],
    );

    assert_eq!(fate(&call, "Rebalance"), "shadowed on call by Leader flap");
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_refusal_before_forwarding_that_was_shadowed_refused_nothing() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("Lost acknowledgement", 1, late_refusal(partition.clone())),
            lie("Leader flap", 2, refusal(partition.clone())),
        ],
        vec![lie("Stale watermark", 0, stale_watermark(parts(&["orders.eu/4"])))],
    );

    // The partition reached the backend, and the refusal after the response replaces its truth.
    assert_eq!(
        fate(&call, "Leader flap"),
        "shadowed on orders.eu/3 by Lost acknowledgement"
    );
    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/3");
    assert!(call.before_forwarding().refusals().is_empty());
}

// Everything else meets by priority

#[test]
fn a_response_pass_refusal_with_a_higher_priority_shadows_a_request_pass_refusal_after_the_response() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("Lost acknowledgement", 1, late_refusal(partition.clone()))],
        vec![lie("Fail what succeeded", 0, late_refusal(partition))],
    );

    assert_eq!(fate(&call, "Fail what succeeded"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "shadowed on orders.eu/3 by Fail what succeeded"
    );
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/3: Fail what succeeded"]
    );
    assert_eq!(ids(&call), ["Fail what succeeded", "Lost acknowledgement"]);
}

#[test]
fn a_response_pass_refusal_with_a_lower_priority_is_shadowed_by_a_request_pass_refusal_after_the_response() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("Lost acknowledgement", 0, late_refusal(partition.clone()))],
        vec![lie("Fail what succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Fail what succeeded"),
        "shadowed on orders.eu/3 by Lost acknowledgement"
    );
}

#[test]
fn rewrites_of_both_passes_conflict_by_priority() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("Stale watermark", 1, stale_watermark(partition.clone())),
            lie("Dropped records", 3, rewrite(Aspect::Records, partition.clone())),
        ],
        vec![
            lie("Watermark at zero", 0, stale_watermark(partition.clone())),
            lie("Corrupt records", 4, rewrite(Aspect::Records, partition)),
        ],
    );

    assert_eq!(fate(&call, "Watermark at zero"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Stale watermark"),
        "shadowed on orders.eu/3 by Watermark at zero"
    );
    assert_eq!(fate(&call, "Dropped records"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Corrupt records"),
        "shadowed on orders.eu/3 by Dropped records"
    );
}

#[test]
fn a_response_pass_refusal_pre_empts_a_request_pass_rewrite_of_its_partition() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("Stale watermark", 0, stale_watermark(partition.clone()))],
        vec![lie("Fail what succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(
        fate(&call, "Stale watermark"),
        "pre-empted on orders.eu/3 by Fail what succeeded"
    );
    assert_eq!(call.after_response().rewrites().count(), 0);
}

#[test]
fn a_hang_up_after_the_response_that_won_the_request_pass_can_lose_the_response_pass() {
    let call = combine(
        vec![
            lie("Late disconnect", 1, late_hang_up()),
            lie("Leader flap", 2, refusal(parts(&["orders.eu/3"]))),
            lie("Lost acknowledgement", 3, late_refusal(parts(&["orders.eu/4"]))),
            lie("Slow answer", 4, delay(1000)),
        ],
        vec![lie("Fail what succeeded", 0, late_refusal(parts(&["orders.eu/5"])))],
    );

    // The refusal of the response pass has the higher priority, so the hang-up loses the conflict.
    assert_eq!(fate(&call, "Fail what succeeded"), "applied on orders.eu/5");
    assert_eq!(
        fate(&call, "Late disconnect"),
        "shadowed on call by Fail what succeeded"
    );

    // What the hang-up kept from acting after the response applies after all.
    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/4");
    assert_eq!(fate(&call, "Slow answer"), "applied on call");

    // What it kept from acting before forwarding lost its chance: priority held within the request pass.
    // The request was forwarded whole, so the loss stays, to a hang-up that ended nothing. Neither firing counts.
    assert_eq!(fate(&call, "Leader flap"), "shadowed on orders.eu/3 by Late disconnect");
    assert!(!call.fate_of(&"Leader flap").unwrap().counts());
    assert!(!call.fate_of(&"Late disconnect").unwrap().counts());
    assert!(call.before_forwarding().changes_nothing());

    let after = call.after_response();
    assert_eq!(after.terminal(), None);
    assert_eq!(
        held(after.refusals()),
        ["orders.eu/4: Lost acknowledgement", "orders.eu/5: Fail what succeeded"]
    );
    assert_eq!(after.delay().length(), millis(1000));
}

#[test]
fn a_refusal_before_forwarding_keeps_its_loss_when_its_winner_loses_the_partition_in_the_response_pass() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie(
                "Lost acknowledgement",
                1,
                late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
            lie("Leader flap", 2, refusal(partition.clone())),
        ],
        vec![lie("Fail what succeeded", 0, late_refusal(partition))],
    );

    // The partition reached the backend, so its refusal before forwarding cannot apply any more.
    assert_eq!(fate(&call, "Fail what succeeded"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by Fail what succeeded"
    );
    assert_eq!(
        fate(&call, "Leader flap"),
        "shadowed on orders.eu/3 by Lost acknowledgement"
    );
    assert!(!call.fate_of(&"Leader flap").unwrap().counts());
    assert!(call.before_forwarding().changes_nothing());
}

#[test]
fn a_hang_up_after_the_response_that_won_the_request_pass_wins_the_response_pass_from_above() {
    let call = combine(
        vec![
            lie("Late disconnect", 0, late_hang_up()),
            lie("Slow request", 1, delay(200).at(BEFORE)),
        ],
        vec![
            lie("Fail what succeeded", 2, late_refusal(parts(&["orders.eu/5"]))),
            lie("Slow answer", 3, delay(1000)),
        ],
    );

    assert_eq!(fate(&call, "Late disconnect"), "applied on call");
    assert_eq!(
        fate(&call, "Fail what succeeded"),
        "shadowed on orders.eu/5 by Late disconnect"
    );
    assert_eq!(fate(&call, "Slow answer"), "pre-empted on call by Late disconnect");
    assert_eq!(fate(&call, "Slow request"), "applied on call");
    assert_eq!(call.after_response().terminal(), Some(&"Late disconnect"));
}

#[test]
fn a_response_pass_hang_up_does_not_undo_a_refusal_before_forwarding() {
    // The partition never reached the backend, and the connection closes after the rest was answered.
    let call = combine(
        vec![
            lie("Leader flap", 3, refusal(parts(&["orders.eu/3"]))),
            lie("Lost acknowledgement", 4, late_refusal(parts(&["orders.eu/4"]))),
            lie("Slow request", 5, delay(200).at(BEFORE)),
            lie("Slow answer", 6, delay(1000)),
        ],
        vec![lie("Disconnect on success", 0, late_hang_up())],
    );

    assert_eq!(fate(&call, "Disconnect on success"), "applied on call");
    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Slow request"), "applied on call");

    // The hang-up conflicts with the refusal after the response, and it pre-empts the delay of the answer.
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "shadowed on orders.eu/4 by Disconnect on success"
    );
    assert_eq!(
        fate(&call, "Slow answer"),
        "pre-empted on call by Disconnect on success"
    );

    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: Leader flap"]);
    assert_eq!(call.after_response().terminal(), Some(&"Disconnect on success"));
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn a_response_pass_hang_up_leaves_no_answer_to_carry_the_throttle_of_the_request_pass() {
    let call = combine(
        vec![
            lie("Slow request", 1, delay(200).with_throttle(millis(700)).at(BEFORE)),
            lie("Throttle", 2, TimingLie::new().with_throttle(millis(500))),
        ],
        vec![lie("Disconnect on success", 0, late_hang_up())],
    );

    // The request waited before the hang-up fired, so that lie applied.
    assert_eq!(fate(&call, "Slow request"), "applied on call");
    assert_eq!(fate(&call, "Throttle"), "pre-empted on call by Disconnect on success");
    assert_eq!(call.before_forwarding().delay().length(), millis(200));

    assert_eq!(call.after_response().terminal(), Some(&"Disconnect on success"));
    assert_eq!(call.after_response().throttle(), millis(0));
}

#[test]
fn a_response_pass_hang_up_below_a_refusal_after_the_response_is_shadowed() {
    let call = combine(
        vec![lie("Lost acknowledgement", 0, late_refusal(parts(&["orders.eu/4"])))],
        vec![lie("Disconnect on success", 1, late_hang_up())],
    );

    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/4");
    assert_eq!(
        fate(&call, "Disconnect on success"),
        "shadowed on call by Lost acknowledgement"
    );
}

#[test]
fn a_request_pass_hang_up_after_the_response_below_a_refusal_before_forwarding_stays_shadowed() {
    let call = combine(
        vec![
            lie("Leader flap", 1, refusal(parts(&["orders.eu/3"]))),
            lie("Late disconnect", 2, late_hang_up()),
        ],
        vec![lie("Slow answer", 0, delay(1000))],
    );

    assert_eq!(fate(&call, "Late disconnect"), "shadowed on call by Leader flap");
    assert_eq!(fate(&call, "Slow answer"), "applied on call");
}

#[test]
fn a_request_pass_refusal_after_the_response_below_a_refusal_before_forwarding_stays_shadowed() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![
            lie("Leader flap", 1, refusal(partition.clone())),
            lie(
                "Lost acknowledgement",
                2,
                late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
        ],
        vec![lie("Fail what succeeded", 0, late_refusal(partition))],
    );

    // The lie of its own pass lost a conflict, and the lie of the response pass found no truth.
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by Leader flap"
    );
    assert_eq!(
        fate(&call, "Fail what succeeded"),
        "pre-empted on orders.eu/3 by Leader flap"
    );
}

// Modifiers of both passes

#[test]
fn delays_of_both_passes_add_up_at_their_stage_and_throttle_takes_the_maximum() {
    let call = combine(
        vec![
            lie("Slow request", 0, delay(200).with_throttle(millis(100)).at(BEFORE)),
            lie("Slow answer", 1, delay(1000).with_jitter(millis(200))),
        ],
        vec![
            lie("Slow success", 2, delay(500).with_jitter(millis(50))),
            lie("Throttle success", 3, TimingLie::new().with_throttle(millis(700))),
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
            lie("Refuse", 0, refusal(parts(&["orders.eu/3"]))),
            lie("Slow", 1, delay(200).at(BEFORE)),
        ],
    );

    assert_eq!(fate(&call, "Refuse"), "applied on orders.eu/3");
    assert_eq!(call.fate_of(&"Refuse").unwrap().stage(), AFTER);
    assert_eq!(call.fate_of(&"Refuse").unwrap().claim().stage(), BEFORE);
    assert_eq!(call.fate_of(&"Slow").unwrap().stage(), AFTER);

    assert!(call.before_forwarding().changes_nothing());
    assert_eq!(held(call.after_response().refusals()), ["orders.eu/3: Refuse"]);
    assert_eq!(call.after_response().delay().length(), millis(200));
}

#[test]
fn a_response_pass_silence_ends_the_call_after_the_response() {
    let call = combine(
        vec![lie("Slow answer", 0, delay(1000))],
        vec![lie("Blackhole on success", 1, silence())],
    );

    assert_eq!(fate(&call, "Blackhole on success"), "applied on call");
    assert_eq!(fate(&call, "Slow answer"), "pre-empted on call by Blackhole on success");
    assert_eq!(call.before_forwarding().terminal(), None);
    assert_eq!(call.after_response().terminal(), Some(&"Blackhole on success"));
}

// The passes as a whole

#[test]
fn the_effect_before_forwarding_is_final_after_the_request_pass() {
    let request = RequestPass::of([
        lie("Leader flap", 2, refusal(parts(&["orders.eu/3"]))),
        lie("Slow request", 3, delay(200).at(BEFORE)),
        lie("Lost acknowledgement", 4, late_refusal(parts(&["orders.eu/4"]))),
    ]);
    let before = request.before_forwarding().clone();

    let call = request.response_pass([
        lie(
            "Fail what succeeded",
            0,
            late_refusal(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("Disconnect on success", 1, late_hang_up()),
    ]);

    assert_eq!(call.before_forwarding(), &before);
    assert_eq!(held(before.refusals()), ["orders.eu/3: Leader flap"]);
    assert_eq!(before.delay().length(), millis(200));
}

#[test]
fn a_call_without_a_response_pass_is_a_call_whose_response_pass_fired_no_lie() {
    let fired = || {
        vec![
            lie("Late disconnect", 1, late_hang_up()),
            lie("Leader flap", 0, refusal(parts(&["orders.eu/3"]))),
            lie(
                "Stale watermark",
                2,
                stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
            ),
            lie("Slow answer", 3, delay(1000)),
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
        vec![lie("Third", 3, delay(1)), lie("First", 1, delay(1))],
        vec![
            lie("Fourth", 4, delay(1)),
            lie("Second", 2, delay(1)),
            lie("Zeroth", 0, delay(1)),
        ],
    );

    assert_eq!(ids(&call), ["Zeroth", "First", "Second", "Third", "Fourth"]);
}

#[test]
fn a_request_pass_lie_comes_before_a_response_pass_lie_of_the_same_priority() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine(
        vec![lie("Lost acknowledgement", 1, late_refusal(partition.clone()))],
        vec![lie("Fail what succeeded", 1, late_refusal(partition))],
    );

    assert_eq!(ids(&call), ["Lost acknowledgement", "Fail what succeeded"]);
    assert_eq!(
        fate(&call, "Fail what succeeded"),
        "shadowed on orders.eu/3 by Lost acknowledgement"
    );
}
