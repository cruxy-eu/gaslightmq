//! How the lies of one pass combine: the examples of the specification, and the rules around them.

mod common;

use common::*;
use gaslightmq_combine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

/// Combines the lies of a call on which no rule waited for the truth.
fn combine(fired: impl IntoIterator<Item = FiredLie<&'static str>>) -> Combination<&'static str> {
    RequestPass::of(fired).without_response_pass()
}

// Lies that do not conflict

#[test]
fn a_call_without_lies_changes_nothing() {
    let call = combine([]);

    assert_eq!(call.fates(), []);
    assert!(call.before_forwarding().changes_nothing());
    assert!(call.after_response().changes_nothing());
    assert_eq!(call.before_forwarding().stage(), BEFORE);
    assert_eq!(call.after_response().stage(), AFTER);
    assert_eq!(call.fate_of(&"blackhole"), None);
}

#[test]
fn a_chain_of_a_delay_above_a_refusal_delays_and_refuses() {
    // "delay-1s" above "refuse-the-3rd-produce": a delay and a refusal do not conflict.
    let produce = parts(&["orders.eu/3"]);
    let call = combine([
        lie("delay-1s", 0, claim(delay(1000), produce.clone())),
        lie("refuse-the-3rd-produce", 1, refusal(produce)),
    ]);

    assert_eq!(fate(&call, "delay-1s"), "applied on call");
    assert_eq!(fate(&call, "refuse-the-3rd-produce"), "applied on orders.eu/3");
    assert!(call.fates().iter().all(Fate::counts));

    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: refuse-the-3rd-produce"]
    );
    assert_eq!(call.after_response().delay().length(), millis(1000));
}

#[test]
fn a_delay_a_refusal_and_a_stale_watermark_combine_on_one_fetch() {
    let call = combine([
        lie("slow-fetch", 0, delay(750)),
        lie("leader-flap", 1, refusal(parts(&["orders.eu/3"]))),
        lie("stale-watermark", 2, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "slow-fetch"), "applied on call");
    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "stale-watermark"), "applied on orders.eu/4");

    let before = call.before_forwarding();
    assert_eq!(before.refusals().of(&part("orders.eu/3")), Some(&"leader-flap"));
    assert_eq!(before.refusals().of(&part("orders.eu/4")), None);
    assert!(before.delay().is_zero());
    assert_eq!(before.rewrites().count(), 0);

    let after = call.after_response();
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/4")),
        Some(&"stale-watermark")
    );
    assert_eq!(after.rewrite(Aspect::HighWatermark, &part("orders.eu/3")), None);
    assert_eq!(after.rewrite(Aspect::Records, &part("orders.eu/4")), None);
    assert_eq!(after.delay().length(), millis(750));
    assert!(after.refusals().is_empty());
    assert_eq!(after.terminal(), None);
}

#[test]
fn refusals_of_different_partitions_all_apply() {
    let call = combine([
        lie("refuse-3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("refuse-4", 1, refusal(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "refuse-3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "refuse-4"), "applied on orders.eu/4");
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: refuse-3", "orders.eu/4: refuse-4"]
    );
}

#[test]
fn a_stale_watermark_and_dropped_records_on_the_same_partition_both_apply() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("stale-watermark", 0, stale_watermark(partition.clone())),
        lie("dropped-records", 1, rewrite(Aspect::Records, partition)),
    ]);

    assert_eq!(fate(&call, "stale-watermark"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "dropped-records"), "applied on orders.eu/3");

    let after = call.after_response();
    let aspects: Vec<Aspect> = after.rewrites().map(|(aspect, _)| aspect).collect();
    assert_eq!(aspects, [Aspect::Records, Aspect::HighWatermark]);
    assert_eq!(
        after.rewrite(Aspect::Records, &part("orders.eu/3")),
        Some(&"dropped-records")
    );
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/3")),
        Some(&"stale-watermark")
    );
}

#[test]
fn rewrites_of_the_same_aspect_of_different_partitions_both_apply() {
    let call = combine([
        lie("stale-3", 0, stale_watermark(parts(&["orders.eu/3"]))),
        lie("stale-4", 1, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "stale-3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "stale-4"), "applied on orders.eu/4");
}

// Conflicts

#[test]
fn a_blackhole_above_a_refusal_shadows_the_refusal() {
    // Both are ways to fail the call, so the refusal only applies when the blackhole did not fire.
    let call = combine([
        lie("blackhole", 0, silence()),
        lie("refuse", 1, refusal(parts(&["orders.eu/3"]))),
    ]);

    assert_eq!(fate(&call, "blackhole"), "applied on call");
    assert_eq!(fate(&call, "refuse"), "shadowed on orders.eu/3 by blackhole");
    assert!(!call.fate_of(&"refuse").unwrap().counts());

    assert_eq!(call.before_forwarding().terminal(), Some(&"blackhole"));
    assert!(call.before_forwarding().refusals().is_empty());
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_refusal_above_a_blackhole_shadows_the_blackhole() {
    let call = combine([
        lie("refuse", 0, refusal(parts(&["orders.eu/3"]))),
        lie("blackhole", 1, silence()),
    ]);

    assert_eq!(fate(&call, "refuse"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "blackhole"), "shadowed on call by refuse");
    assert_eq!(call.before_forwarding().terminal(), None);
}

#[test]
fn the_refusal_alone_applies_when_the_blackhole_did_not_fire() {
    let call = combine([lie("refuse", 1, refusal(parts(&["orders.eu/3"])))]);

    assert_eq!(fate(&call, "refuse"), "applied on orders.eu/3");
}

#[test]
fn two_refusals_of_the_same_partition_conflict_and_the_higher_one_wins() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("leader-flap", 0, refusal(partition.clone())),
        lie("lost-acknowledgement", 1, late_refusal(partition.clone())),
    ]);

    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "lost-acknowledgement"),
        "shadowed on orders.eu/3 by leader-flap"
    );

    // The explanation names the winner and the target.
    let lost = call.fate_of(&"lost-acknowledgement").unwrap();
    assert_eq!(lost.applied(), None);
    assert_eq!(lost.shadowed().len(), 1);
    assert_eq!(lost.shadowed()[0].on(), &partition);
    assert_eq!(lost.shadowed()[0].to(), &"leader-flap");
    assert_eq!(lost.pre_empted(), []);
    assert!(!lost.counts());

    // The partition leaves the request, and nothing refuses it a second time after the response.
    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: leader-flap"]);
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn the_stage_plays_no_part_in_a_conflict_of_one_pass() {
    // The refusal after the response has the higher priority, so the partition reaches the backend.
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("lost-acknowledgement", 0, late_refusal(partition.clone())),
        lie("leader-flap", 1, refusal(partition)),
    ]);

    assert_eq!(fate(&call, "lost-acknowledgement"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "leader-flap"),
        "shadowed on orders.eu/3 by lost-acknowledgement"
    );
    assert!(call.before_forwarding().refusals().is_empty());
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/3: lost-acknowledgement"]
    );
}

#[test]
fn a_refusal_that_is_shadowed_on_one_partition_and_applies_on_another_counts_once() {
    let call = combine([
        lie("leader-flap", 0, refusal(parts(&["orders.eu/3"]))),
        lie("refuse-orders", 1, refusal(parts(&["orders.eu/3", "orders.eu/4"]))),
    ]);

    assert_eq!(
        fate(&call, "refuse-orders"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by leader-flap"
    );
    assert!(call.fate_of(&"refuse-orders").unwrap().counts());
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: leader-flap", "orders.eu/4: refuse-orders"]
    );
}

#[test]
fn a_refusal_names_every_winner_with_the_parts_it_lost_to_it() {
    let call = combine([
        lie("refuse-4-and-5", 1, refusal(parts(&["orders.eu/4", "orders.eu/5"]))),
        lie("refuse-3", 0, refusal(parts(&["orders.eu/3"]))),
        lie(
            "refuse-orders",
            2,
            refusal(parts(&["orders.eu/3", "orders.eu/4", "orders.eu/5", "orders.eu/6"])),
        ),
    ]);

    assert_eq!(
        fate(&call, "refuse-orders"),
        "applied on orders.eu/6; shadowed on orders.eu/3 by refuse-3; shadowed on orders.eu/4, orders.eu/5 by refuse-4-and-5"
    );
}

#[test]
fn two_rewrites_of_the_same_aspect_of_the_same_target_conflict() {
    let call = combine([
        lie("stale-watermark", 0, stale_watermark(parts(&["orders.eu/3"]))),
        lie(
            "watermark-at-zero",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
    ]);

    assert_eq!(fate(&call, "stale-watermark"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "watermark-at-zero"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by stale-watermark"
    );

    let after = call.after_response();
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/3")),
        Some(&"stale-watermark")
    );
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/4")),
        Some(&"watermark-at-zero")
    );
}

#[test]
fn only_one_terminal_lie_applies() {
    let call = combine([
        lie("disconnect", 1, hang_up()),
        lie("blackhole", 0, silence()),
        lie("late-disconnect", 2, late_hang_up()),
    ]);

    assert_eq!(fate(&call, "blackhole"), "applied on call");
    assert_eq!(fate(&call, "disconnect"), "shadowed on call by blackhole");
    assert_eq!(fate(&call, "late-disconnect"), "shadowed on call by blackhole");
    assert_eq!(call.before_forwarding().terminal(), Some(&"blackhole"));
    assert_eq!(call.after_response().terminal(), None);
}

#[test]
fn a_terminal_lie_conflicts_with_every_refusal_on_the_call() {
    let call = combine([
        lie("disconnect", 0, hang_up()),
        lie("refuse-3", 1, refusal(parts(&["orders.eu/3"]))),
        lie("lose-4-and-5", 2, late_refusal(parts(&["orders.eu/4", "orders.eu/5"]))),
    ]);

    assert_eq!(fate(&call, "disconnect"), "applied on call");
    assert_eq!(fate(&call, "refuse-3"), "shadowed on orders.eu/3 by disconnect");
    assert_eq!(
        fate(&call, "lose-4-and-5"),
        "shadowed on orders.eu/4, orders.eu/5 by disconnect"
    );
}

#[test]
fn a_terminal_lie_below_several_refusals_is_shadowed_by_the_highest() {
    let call = combine([
        lie("refuse-4", 1, refusal(parts(&["orders.eu/4"]))),
        lie("refuse-3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("blackhole", 2, silence()),
    ]);

    assert_eq!(fate(&call, "blackhole"), "shadowed on call by refuse-3");
    assert_eq!(fate(&call, "refuse-3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "refuse-4"), "applied on orders.eu/4");
}

#[test]
fn a_lie_that_lost_a_conflict_shadows_no_lie_below_it() {
    // The blackhole loses to the refusal above it, so it holds nothing, and the refusal below it applies.
    let call = combine([
        lie("refuse-3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("blackhole", 1, silence()),
        lie("refuse-4", 2, refusal(parts(&["orders.eu/4"]))),
        lie("slow", 3, delay(100)),
    ]);

    assert_eq!(fate(&call, "blackhole"), "shadowed on call by refuse-3");
    assert_eq!(fate(&call, "refuse-4"), "applied on orders.eu/4");
    assert_eq!(fate(&call, "slow"), "applied on call");
}

#[test]
fn a_refusal_that_lost_a_partition_shadows_no_refusal_below_it_there() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("first", 0, refusal(partition.clone())),
        lie("second", 1, refusal(partition.clone())),
        lie("third", 2, refusal(partition)),
    ]);

    assert_eq!(fate(&call, "second"), "shadowed on orders.eu/3 by first");
    assert_eq!(fate(&call, "third"), "shadowed on orders.eu/3 by first");
}

// Pre-emption

#[test]
fn a_refusal_pre_empts_a_rewrite_of_its_partition() {
    let call = combine([
        lie("leader-flap", 1, refusal(parts(&["orders.eu/3"]))),
        lie(
            "dropped-records",
            0,
            rewrite(Aspect::Records, parts(&["orders.eu/3", "orders.eu/4"])),
        ),
    ]);

    // The rewrite has the higher priority, and the refusal still replaces everything about its partition.
    assert_eq!(fate(&call, "leader-flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "dropped-records"),
        "applied on orders.eu/4; pre-empted on orders.eu/3 by leader-flap"
    );

    // The explanation names the lie that got there first.
    let dropped = call.fate_of(&"dropped-records").unwrap();
    assert_eq!(dropped.shadowed(), []);
    assert_eq!(dropped.pre_empted()[0].on(), &parts(&["orders.eu/3"]));
    assert_eq!(dropped.pre_empted()[0].to(), &"leader-flap");
    assert!(dropped.counts());

    let after = call.after_response();
    assert_eq!(after.rewrite(Aspect::Records, &part("orders.eu/3")), None);
    assert_eq!(
        after.rewrite(Aspect::Records, &part("orders.eu/4")),
        Some(&"dropped-records")
    );
}

#[test]
fn a_rewrite_of_a_refused_partition_only_does_not_count() {
    for claim in [refusal(parts(&["orders.eu/3"])), late_refusal(parts(&["orders.eu/3"]))] {
        let call = combine([
            lie("stale-watermark", 0, stale_watermark(parts(&["orders.eu/3"]))),
            lie("refuse", 1, claim),
        ]);

        assert_eq!(fate(&call, "stale-watermark"), "pre-empted on orders.eu/3 by refuse");
        assert!(!call.fate_of(&"stale-watermark").unwrap().counts());
        assert_eq!(call.after_response().rewrites().count(), 0);
    }
}

#[test]
fn a_rewrite_of_a_refused_partition_is_pre_empted_rather_than_shadowed() {
    // A rewrite above it changes the same aspect, yet neither has anything left to change.
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("stale-watermark", 0, stale_watermark(partition.clone())),
        lie("watermark-at-zero", 1, stale_watermark(partition.clone())),
        lie("leader-flap", 2, refusal(partition)),
    ]);

    assert_eq!(
        fate(&call, "stale-watermark"),
        "pre-empted on orders.eu/3 by leader-flap"
    );
    assert_eq!(
        fate(&call, "watermark-at-zero"),
        "pre-empted on orders.eu/3 by leader-flap"
    );
}

#[test]
fn a_refusal_that_was_shadowed_pre_empts_no_rewrite() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("late-disconnect", 0, late_hang_up()),
        lie("leader-flap", 1, refusal(partition.clone())),
        lie("stale-watermark", 2, stale_watermark(partition)),
    ]);

    assert_eq!(fate(&call, "leader-flap"), "shadowed on orders.eu/3 by late-disconnect");
    assert_eq!(
        fate(&call, "stale-watermark"),
        "pre-empted on orders.eu/3 by late-disconnect"
    );
}

#[test]
fn a_terminal_lie_pre_empts_modifiers_and_rewrites_whatever_their_priority() {
    let call = combine([
        lie("slow", 0, delay(1000).with_throttle(millis(500))),
        lie(
            "stale-watermark",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("blackhole", 2, silence()),
        lie("slow-request", 3, delay(200).at(BEFORE)),
    ]);

    assert_eq!(fate(&call, "blackhole"), "applied on call");
    assert_eq!(fate(&call, "slow"), "pre-empted on call by blackhole");
    assert_eq!(
        fate(&call, "stale-watermark"),
        "pre-empted on orders.eu/3, orders.eu/4 by blackhole"
    );
    assert_eq!(fate(&call, "slow-request"), "pre-empted on call by blackhole");
    assert_eq!(call.fates().iter().filter(|fate| fate.counts()).count(), 1);

    // A delay on a call that a silence ended adds nothing.
    assert!(call.before_forwarding().delay().is_zero());
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_terminal_lie_that_was_shadowed_pre_empts_nothing() {
    let call = combine([
        lie("refuse", 0, refusal(parts(&["orders.eu/3"]))),
        lie("blackhole", 1, silence()),
        lie("slow", 2, delay(1000)),
        lie("stale-watermark", 3, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "slow"), "applied on call");
    assert_eq!(fate(&call, "stale-watermark"), "applied on orders.eu/4");
}

#[test]
fn a_hang_up_after_the_response_leaves_a_delay_before_forwarding_alone() {
    // The request waits and the backend sees it late, which really happens in the cluster.
    let call = combine([
        lie("late-disconnect", 0, late_hang_up()),
        lie("slow-request", 1, delay(200).at(BEFORE)),
        lie("slow-answer", 2, delay(1000)),
    ]);

    assert_eq!(fate(&call, "late-disconnect"), "applied on call");
    assert_eq!(fate(&call, "slow-request"), "applied on call");
    assert_eq!(fate(&call, "slow-answer"), "pre-empted on call by late-disconnect");

    assert_eq!(call.before_forwarding().terminal(), None);
    assert_eq!(call.before_forwarding().delay().length(), millis(200));
    assert_eq!(call.after_response().terminal(), Some(&"late-disconnect"));
    assert!(call.after_response().delay().is_zero());
}

// Modifiers

#[test]
fn delays_add_up_and_throttle_takes_the_maximum() {
    let call = combine([
        lie(
            "delay-1s",
            0,
            delay(1000).with_jitter(millis(200)).with_throttle(millis(300)),
        ),
        lie("delay-100ms", 1, delay(100).with_jitter(millis(100))),
        lie("throttle", 2, TimingLie::new().with_throttle(millis(500))),
        lie("throttle-less", 3, TimingLie::new().with_throttle(millis(50))),
    ]);

    assert!(call.fates().iter().all(Fate::counts));

    let after = call.after_response();
    assert_eq!(after.delay().length(), millis(1100));
    assert_eq!(after.delay().jitter(), millis(300));
    assert_eq!(after.delay().shortest(), millis(800));
    assert_eq!(after.delay().longest(), millis(1400));
    assert_eq!(after.throttle(), millis(500));
    assert!(!after.changes_nothing());

    assert!(call.before_forwarding().changes_nothing());
}

#[test]
fn delays_add_up_at_their_own_stage() {
    let call = combine([
        lie("slow-request", 0, delay(200).at(BEFORE)),
        lie("slower-request", 1, delay(300).with_jitter(millis(30)).at(BEFORE)),
        lie("slow-answer", 2, delay(1000)),
    ]);

    let before = call.before_forwarding();
    assert_eq!(before.delay().length(), millis(500));
    assert_eq!(before.delay().jitter(), millis(30));
    assert_eq!(call.after_response().delay().length(), millis(1000));
}

#[test]
fn the_throttle_of_either_stage_goes_into_the_answer() {
    // The throttle is a field of the answer, wherever the delay of its lie acts.
    let call = combine([
        lie("slow-request", 0, delay(200).with_throttle(millis(700)).at(BEFORE)),
        lie("throttle", 1, TimingLie::new().with_throttle(millis(500))),
    ]);

    assert_eq!(call.before_forwarding().throttle(), millis(0));
    assert_eq!(call.after_response().throttle(), millis(700));
}

#[test]
fn a_call_that_ends_after_the_response_has_no_answer_to_carry_a_throttle() {
    let call = combine([
        lie("late-disconnect", 0, late_hang_up()),
        lie("slow-request", 1, delay(200).with_throttle(millis(700)).at(BEFORE)),
    ]);

    // The delay held the request back, so the lie applies. Its throttle reaches no client.
    assert_eq!(fate(&call, "slow-request"), "applied on call");
    assert_eq!(call.before_forwarding().delay().length(), millis(200));

    let after = call.after_response();
    assert_eq!(after.terminal(), Some(&"late-disconnect"));
    assert_eq!(after.throttle(), millis(0));
}

#[test]
fn a_timing_lie_that_changes_nothing_still_applies() {
    let call = combine([lie("nothing", 0, TimingLie::new())]);

    assert_eq!(fate(&call, "nothing"), "applied on call");
    assert!(call.after_response().changes_nothing());
}

// The whole call as a target

#[test]
fn two_refusals_of_a_call_without_parts_conflict() {
    // A Heartbeat has no parts, so a refusal targets the whole response.
    let call = combine([
        lie("coordinator-gone", 0, refusal(Target::Call)),
        lie("rebalance", 1, late_refusal(Target::Call)),
        lie("slow", 2, delay(100)),
    ]);

    assert_eq!(fate(&call, "coordinator-gone"), "applied on call");
    assert_eq!(fate(&call, "rebalance"), "shadowed on call by coordinator-gone");
    assert_eq!(fate(&call, "slow"), "applied on call");

    let refusals = call.before_forwarding().refusals();
    assert_eq!(refusals.of_call(), Some(&"coordinator-gone"));
    assert_eq!(refusals.of(&part("orders.eu/3")), Some(&"coordinator-gone"));
    assert_eq!(held(refusals), ["call: coordinator-gone"]);
}

#[test]
fn a_refusal_of_the_whole_call_shadows_the_refusals_of_parts_below_it() {
    let call = combine([
        lie("refuse-all", 0, refusal(Target::Call)),
        lie("refuse-3-and-4", 1, refusal(parts(&["orders.eu/3", "orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "refuse-all"), "applied on call");
    assert_eq!(
        fate(&call, "refuse-3-and-4"),
        "shadowed on orders.eu/3, orders.eu/4 by refuse-all"
    );
}

#[test]
fn a_refusal_of_the_whole_call_loses_the_whole_call_to_a_refusal_of_a_part_above_it() {
    // The whole call is one target: the refusal cannot apply around a part.
    let call = combine([
        lie("refuse-4", 1, refusal(parts(&["orders.eu/4"]))),
        lie("refuse-3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("refuse-all", 2, refusal(Target::Call)),
    ]);

    assert_eq!(fate(&call, "refuse-all"), "shadowed on call by refuse-3");
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: refuse-3", "orders.eu/4: refuse-4"]
    );
}

#[test]
fn rewrites_of_the_whole_call_conflict_per_aspect() {
    let call = combine([
        lie("another-controller", 0, rewrite(Aspect::Controller, Target::Call)),
        lie("no-controller", 1, rewrite(Aspect::Controller, Target::Call)),
        lie("missing-broker", 2, rewrite(Aspect::Brokers, Target::Call)),
        lie(
            "controller-per-topic",
            3,
            rewrite(Aspect::Controller, parts(&["orders.eu"])),
        ),
    ]);

    assert_eq!(fate(&call, "another-controller"), "applied on call");
    assert_eq!(fate(&call, "no-controller"), "shadowed on call by another-controller");
    assert_eq!(fate(&call, "missing-broker"), "applied on call");
    assert_eq!(
        fate(&call, "controller-per-topic"),
        "shadowed on orders.eu by another-controller"
    );

    let after = call.after_response();
    assert_eq!(after.rewrite_of_call(Aspect::Controller), Some(&"another-controller"));
    assert_eq!(after.rewrite_of_call(Aspect::Brokers), Some(&"missing-broker"));
    assert_eq!(after.rewrite_of_call(Aspect::Leader), None);
    assert_eq!(
        after.rewrite(Aspect::Controller, &part("orders.eu")),
        Some(&"another-controller")
    );
}

#[test]
fn a_refusal_of_the_whole_call_pre_empts_every_rewrite() {
    let call = combine([
        lie("stale-leader", 0, rewrite(Aspect::Leader, parts(&["orders.eu"]))),
        lie("another-controller", 1, rewrite(Aspect::Controller, Target::Call)),
        lie("refuse-all", 2, late_refusal(Target::Call)),
    ]);

    assert_eq!(fate(&call, "stale-leader"), "pre-empted on orders.eu by refuse-all");
    assert_eq!(fate(&call, "another-controller"), "pre-empted on call by refuse-all");
    assert_eq!(call.after_response().rewrites().count(), 0);
}

#[test]
fn a_rewrite_of_the_whole_call_keeps_something_to_change_when_a_part_is_refused() {
    let call = combine([
        lie("unknown-topic", 0, late_refusal(parts(&["orders.eu"]))),
        lie("another-controller", 1, rewrite(Aspect::Controller, Target::Call)),
    ]);

    assert_eq!(fate(&call, "unknown-topic"), "applied on orders.eu");
    assert_eq!(fate(&call, "another-controller"), "applied on call");
}

// Priority

#[test]
fn the_priority_orders_the_lies_and_not_the_order_they_come_in() {
    let partition = parts(&["orders.eu/3"]);
    let one = [
        lie("low", 5, refusal(partition.clone())),
        lie("high", 2, refusal(partition.clone())),
        lie("middle", 3, silence()),
    ];
    let mut other = one.clone();
    other.reverse();

    let call = combine(one);
    assert_eq!(call, combine(other));

    assert_eq!(ids(&call), ["high", "middle", "low"]);
    assert_eq!(fate(&call, "high"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "middle"), "shadowed on call by high");
    assert_eq!(fate(&call, "low"), "shadowed on orders.eu/3 by high");

    let priorities: Vec<usize> = call.fates().iter().map(|fate| fate.priority().position()).collect();
    assert_eq!(priorities, [2, 3, 5]);
}

#[test]
fn lies_of_the_same_priority_keep_the_order_they_came_in() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("first", 1, refusal(partition.clone())),
        lie("second", 1, refusal(partition)),
    ]);

    assert_eq!(ids(&call), ["first", "second"]);
    assert_eq!(fate(&call, "second"), "shadowed on orders.eu/3 by first");
}

#[test]
fn a_fate_holds_the_lie_it_is_the_fate_of() {
    let fired = lie("lost-acknowledgement", 4, late_refusal(parts(&["orders.eu/3"])));
    let call = combine([fired.clone()]);
    let fate = &call.fates()[0];

    assert_eq!(fate.lie(), &fired);
    assert_eq!(fate.id(), &"lost-acknowledgement");
    assert_eq!(fate.priority(), Priority::new(4));
    assert_eq!(fate.claim(), fired.claim());
    assert_eq!(fate.stage(), AFTER);
}

#[test]
fn an_identity_can_be_anything_the_caller_tells_its_lies_apart_with() {
    #[derive(Debug, Clone, PartialEq)]
    struct RuleId(u32);

    let call = RequestPass::of([
        FiredLie::new(RuleId(7), Priority::new(1), refusal(Target::Call)),
        FiredLie::new(RuleId(9), Priority::new(0), silence()),
    ])
    .without_response_pass();

    assert_eq!(call.before_forwarding().terminal(), Some(&RuleId(9)));
    assert_eq!(call.fate_of(&RuleId(7)).unwrap().shadowed()[0].to(), &RuleId(9));
}
