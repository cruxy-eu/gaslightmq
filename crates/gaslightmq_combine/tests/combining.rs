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
    assert_eq!(call.fate_of(&"Blackhole"), None);
}

#[test]
fn a_chain_of_a_delay_above_a_refusal_delays_and_refuses() {
    // "Delay 1 s" above "Refuse the 3rd Produce": a delay and a refusal do not conflict.
    let produce = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Delay 1 s", 0, claim(delay(1000), produce.clone())),
        lie("Refuse the 3rd Produce", 1, refusal(produce)),
    ]);

    assert_eq!(fate(&call, "Delay 1 s"), "applied on call");
    assert_eq!(fate(&call, "Refuse the 3rd Produce"), "applied on orders.eu/3");
    assert!(call.fates().iter().all(Fate::counts));

    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: Refuse the 3rd Produce"]
    );
    assert_eq!(call.after_response().delay().length(), millis(1000));
}

#[test]
fn a_delay_a_refusal_and_a_stale_watermark_combine_on_one_fetch() {
    let call = combine([
        lie("Slow fetch", 0, delay(750)),
        lie("Leader flap", 1, refusal(parts(&["orders.eu/3"]))),
        lie("Stale watermark", 2, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "Slow fetch"), "applied on call");
    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Stale watermark"), "applied on orders.eu/4");

    let before = call.before_forwarding();
    assert_eq!(before.refusals().of(&part("orders.eu/3")), Some(&"Leader flap"));
    assert_eq!(before.refusals().of(&part("orders.eu/4")), None);
    assert!(before.delay().is_zero());
    assert_eq!(before.rewrites().count(), 0);

    let after = call.after_response();
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/4")),
        Some(&"Stale watermark")
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
        lie("Refuse 3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Refuse 4", 1, refusal(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "Refuse 3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Refuse 4"), "applied on orders.eu/4");
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: Refuse 3", "orders.eu/4: Refuse 4"]
    );
}

#[test]
fn a_stale_watermark_and_dropped_records_on_the_same_partition_both_apply() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Stale watermark", 0, stale_watermark(partition.clone())),
        lie("Dropped records", 1, rewrite(Aspect::Records, partition)),
    ]);

    assert_eq!(fate(&call, "Stale watermark"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Dropped records"), "applied on orders.eu/3");

    let after = call.after_response();
    let aspects: Vec<Aspect> = after.rewrites().map(|(aspect, _)| aspect).collect();
    assert_eq!(aspects, [Aspect::Records, Aspect::HighWatermark]);
    assert_eq!(
        after.rewrite(Aspect::Records, &part("orders.eu/3")),
        Some(&"Dropped records")
    );
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/3")),
        Some(&"Stale watermark")
    );
}

#[test]
fn rewrites_of_the_same_aspect_of_different_partitions_both_apply() {
    let call = combine([
        lie("Stale 3", 0, stale_watermark(parts(&["orders.eu/3"]))),
        lie("Stale 4", 1, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "Stale 3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Stale 4"), "applied on orders.eu/4");
}

// Conflicts

#[test]
fn a_blackhole_above_a_refusal_shadows_the_refusal() {
    // Both are ways to fail the call, so the refusal only applies when the blackhole did not fire.
    let call = combine([
        lie("Blackhole", 0, silence()),
        lie("Refuse", 1, refusal(parts(&["orders.eu/3"]))),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "applied on call");
    assert_eq!(fate(&call, "Refuse"), "shadowed on orders.eu/3 by Blackhole");
    assert!(!call.fate_of(&"Refuse").unwrap().counts());

    assert_eq!(call.before_forwarding().terminal(), Some(&"Blackhole"));
    assert!(call.before_forwarding().refusals().is_empty());
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_refusal_above_a_blackhole_shadows_the_blackhole() {
    let call = combine([
        lie("Refuse", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Blackhole", 1, silence()),
    ]);

    assert_eq!(fate(&call, "Refuse"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Blackhole"), "shadowed on call by Refuse");
    assert_eq!(call.before_forwarding().terminal(), None);
}

#[test]
fn the_refusal_alone_applies_when_the_blackhole_did_not_fire() {
    let call = combine([lie("Refuse", 1, refusal(parts(&["orders.eu/3"])))]);

    assert_eq!(fate(&call, "Refuse"), "applied on orders.eu/3");
}

#[test]
fn two_refusals_of_the_same_partition_conflict_and_the_higher_one_wins() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Leader flap", 0, refusal(partition.clone())),
        lie("Lost acknowledgement", 1, late_refusal(partition.clone())),
    ]);

    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Lost acknowledgement"),
        "shadowed on orders.eu/3 by Leader flap"
    );

    // The explanation names the winner and the target.
    let lost = call.fate_of(&"Lost acknowledgement").unwrap();
    assert_eq!(lost.applied(), None);
    assert_eq!(lost.shadowed().len(), 1);
    assert_eq!(lost.shadowed()[0].on(), &partition);
    assert_eq!(lost.shadowed()[0].to(), &"Leader flap");
    assert_eq!(lost.pre_empted(), []);
    assert!(!lost.counts());

    // The partition leaves the request, and nothing refuses it a second time after the response.
    assert_eq!(held(call.before_forwarding().refusals()), ["orders.eu/3: Leader flap"]);
    assert!(call.after_response().refusals().is_empty());
}

#[test]
fn the_stage_plays_no_part_in_a_conflict_of_one_pass() {
    // The refusal after the response has the higher priority, so the partition reaches the backend.
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Lost acknowledgement", 0, late_refusal(partition.clone())),
        lie("Leader flap", 1, refusal(partition)),
    ]);

    assert_eq!(fate(&call, "Lost acknowledgement"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Leader flap"),
        "shadowed on orders.eu/3 by Lost acknowledgement"
    );
    assert!(call.before_forwarding().refusals().is_empty());
    assert_eq!(
        held(call.after_response().refusals()),
        ["orders.eu/3: Lost acknowledgement"]
    );
}

#[test]
fn a_refusal_that_is_shadowed_on_one_partition_and_applies_on_another_counts_once() {
    let call = combine([
        lie("Leader flap", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Refuse orders", 1, refusal(parts(&["orders.eu/3", "orders.eu/4"]))),
    ]);

    assert_eq!(
        fate(&call, "Refuse orders"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by Leader flap"
    );
    assert!(call.fate_of(&"Refuse orders").unwrap().counts());
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: Leader flap", "orders.eu/4: Refuse orders"]
    );
}

#[test]
fn a_refusal_names_every_winner_with_the_parts_it_lost_to_it() {
    let call = combine([
        lie("Refuse 4 and 5", 1, refusal(parts(&["orders.eu/4", "orders.eu/5"]))),
        lie("Refuse 3", 0, refusal(parts(&["orders.eu/3"]))),
        lie(
            "Refuse orders",
            2,
            refusal(parts(&["orders.eu/3", "orders.eu/4", "orders.eu/5", "orders.eu/6"])),
        ),
    ]);

    assert_eq!(
        fate(&call, "Refuse orders"),
        "applied on orders.eu/6; shadowed on orders.eu/3 by Refuse 3; shadowed on orders.eu/4, orders.eu/5 by Refuse 4 and 5"
    );
}

#[test]
fn two_rewrites_of_the_same_aspect_of_the_same_target_conflict() {
    let call = combine([
        lie("Stale watermark", 0, stale_watermark(parts(&["orders.eu/3"]))),
        lie(
            "Watermark at zero",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
    ]);

    assert_eq!(fate(&call, "Stale watermark"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Watermark at zero"),
        "applied on orders.eu/4; shadowed on orders.eu/3 by Stale watermark"
    );

    let after = call.after_response();
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/3")),
        Some(&"Stale watermark")
    );
    assert_eq!(
        after.rewrite(Aspect::HighWatermark, &part("orders.eu/4")),
        Some(&"Watermark at zero")
    );
}

#[test]
fn only_one_terminal_lie_applies() {
    let call = combine([
        lie("Disconnect", 1, hang_up()),
        lie("Blackhole", 0, silence()),
        lie("Late disconnect", 2, late_hang_up()),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "applied on call");
    assert_eq!(fate(&call, "Disconnect"), "shadowed on call by Blackhole");
    assert_eq!(fate(&call, "Late disconnect"), "shadowed on call by Blackhole");
    assert_eq!(call.before_forwarding().terminal(), Some(&"Blackhole"));
    assert_eq!(call.after_response().terminal(), None);
}

#[test]
fn a_terminal_lie_conflicts_with_every_refusal_on_the_call() {
    let call = combine([
        lie("Disconnect", 0, hang_up()),
        lie("Refuse 3", 1, refusal(parts(&["orders.eu/3"]))),
        lie("Lose 4 and 5", 2, late_refusal(parts(&["orders.eu/4", "orders.eu/5"]))),
    ]);

    assert_eq!(fate(&call, "Disconnect"), "applied on call");
    assert_eq!(fate(&call, "Refuse 3"), "shadowed on orders.eu/3 by Disconnect");
    assert_eq!(
        fate(&call, "Lose 4 and 5"),
        "shadowed on orders.eu/4, orders.eu/5 by Disconnect"
    );
}

#[test]
fn a_terminal_lie_below_several_refusals_is_shadowed_by_the_highest() {
    let call = combine([
        lie("Refuse 4", 1, refusal(parts(&["orders.eu/4"]))),
        lie("Refuse 3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Blackhole", 2, silence()),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "shadowed on call by Refuse 3");
    assert_eq!(fate(&call, "Refuse 3"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Refuse 4"), "applied on orders.eu/4");
}

#[test]
fn a_lie_that_lost_a_conflict_shadows_no_lie_below_it() {
    // The blackhole loses to the refusal above it, so it holds nothing, and the refusal below it applies.
    let call = combine([
        lie("Refuse 3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Blackhole", 1, silence()),
        lie("Refuse 4", 2, refusal(parts(&["orders.eu/4"]))),
        lie("Slow", 3, delay(100)),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "shadowed on call by Refuse 3");
    assert_eq!(fate(&call, "Refuse 4"), "applied on orders.eu/4");
    assert_eq!(fate(&call, "Slow"), "applied on call");
}

#[test]
fn a_refusal_that_lost_a_partition_shadows_no_refusal_below_it_there() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("First", 0, refusal(partition.clone())),
        lie("Second", 1, refusal(partition.clone())),
        lie("Third", 2, refusal(partition)),
    ]);

    assert_eq!(fate(&call, "Second"), "shadowed on orders.eu/3 by First");
    assert_eq!(fate(&call, "Third"), "shadowed on orders.eu/3 by First");
}

// Pre-emption

#[test]
fn a_refusal_pre_empts_a_rewrite_of_its_partition() {
    let call = combine([
        lie("Leader flap", 1, refusal(parts(&["orders.eu/3"]))),
        lie(
            "Dropped records",
            0,
            rewrite(Aspect::Records, parts(&["orders.eu/3", "orders.eu/4"])),
        ),
    ]);

    // The rewrite has the higher priority, and the refusal still replaces everything about its partition.
    assert_eq!(fate(&call, "Leader flap"), "applied on orders.eu/3");
    assert_eq!(
        fate(&call, "Dropped records"),
        "applied on orders.eu/4; pre-empted on orders.eu/3 by Leader flap"
    );

    // The explanation names the lie that got there first.
    let dropped = call.fate_of(&"Dropped records").unwrap();
    assert_eq!(dropped.shadowed(), []);
    assert_eq!(dropped.pre_empted()[0].on(), &parts(&["orders.eu/3"]));
    assert_eq!(dropped.pre_empted()[0].to(), &"Leader flap");
    assert!(dropped.counts());

    let after = call.after_response();
    assert_eq!(after.rewrite(Aspect::Records, &part("orders.eu/3")), None);
    assert_eq!(
        after.rewrite(Aspect::Records, &part("orders.eu/4")),
        Some(&"Dropped records")
    );
}

#[test]
fn a_rewrite_of_a_refused_partition_only_does_not_count() {
    for claim in [refusal(parts(&["orders.eu/3"])), late_refusal(parts(&["orders.eu/3"]))] {
        let call = combine([
            lie("Stale watermark", 0, stale_watermark(parts(&["orders.eu/3"]))),
            lie("Refuse", 1, claim),
        ]);

        assert_eq!(fate(&call, "Stale watermark"), "pre-empted on orders.eu/3 by Refuse");
        assert!(!call.fate_of(&"Stale watermark").unwrap().counts());
        assert_eq!(call.after_response().rewrites().count(), 0);
    }
}

#[test]
fn a_rewrite_of_a_refused_partition_is_pre_empted_rather_than_shadowed() {
    // A rewrite above it changes the same aspect, yet neither has anything left to change.
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Stale watermark", 0, stale_watermark(partition.clone())),
        lie("Watermark at zero", 1, stale_watermark(partition.clone())),
        lie("Leader flap", 2, refusal(partition)),
    ]);

    assert_eq!(
        fate(&call, "Stale watermark"),
        "pre-empted on orders.eu/3 by Leader flap"
    );
    assert_eq!(
        fate(&call, "Watermark at zero"),
        "pre-empted on orders.eu/3 by Leader flap"
    );
}

#[test]
fn a_refusal_that_was_shadowed_pre_empts_no_rewrite() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("Late disconnect", 0, late_hang_up()),
        lie("Leader flap", 1, refusal(partition.clone())),
        lie("Stale watermark", 2, stale_watermark(partition)),
    ]);

    assert_eq!(fate(&call, "Leader flap"), "shadowed on orders.eu/3 by Late disconnect");
    assert_eq!(
        fate(&call, "Stale watermark"),
        "pre-empted on orders.eu/3 by Late disconnect"
    );
}

#[test]
fn a_terminal_lie_pre_empts_modifiers_and_rewrites_whatever_their_priority() {
    let call = combine([
        lie("Slow", 0, delay(1000).with_throttle(millis(500))),
        lie(
            "Stale watermark",
            1,
            stale_watermark(parts(&["orders.eu/3", "orders.eu/4"])),
        ),
        lie("Blackhole", 2, silence()),
        lie("Slow request", 3, delay(200).at(BEFORE)),
    ]);

    assert_eq!(fate(&call, "Blackhole"), "applied on call");
    assert_eq!(fate(&call, "Slow"), "pre-empted on call by Blackhole");
    assert_eq!(
        fate(&call, "Stale watermark"),
        "pre-empted on orders.eu/3, orders.eu/4 by Blackhole"
    );
    assert_eq!(fate(&call, "Slow request"), "pre-empted on call by Blackhole");
    assert_eq!(call.fates().iter().filter(|fate| fate.counts()).count(), 1);

    // A delay on a call that a silence ended adds nothing.
    assert!(call.before_forwarding().delay().is_zero());
    assert!(call.after_response().changes_nothing());
}

#[test]
fn a_terminal_lie_that_was_shadowed_pre_empts_nothing() {
    let call = combine([
        lie("Refuse", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Blackhole", 1, silence()),
        lie("Slow", 2, delay(1000)),
        lie("Stale watermark", 3, stale_watermark(parts(&["orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "Slow"), "applied on call");
    assert_eq!(fate(&call, "Stale watermark"), "applied on orders.eu/4");
}

#[test]
fn a_hang_up_after_the_response_leaves_a_delay_before_forwarding_alone() {
    // The request waits and the backend sees it late, which really happens in the cluster.
    let call = combine([
        lie("Late disconnect", 0, late_hang_up()),
        lie("Slow request", 1, delay(200).at(BEFORE)),
        lie("Slow answer", 2, delay(1000)),
    ]);

    assert_eq!(fate(&call, "Late disconnect"), "applied on call");
    assert_eq!(fate(&call, "Slow request"), "applied on call");
    assert_eq!(fate(&call, "Slow answer"), "pre-empted on call by Late disconnect");

    assert_eq!(call.before_forwarding().terminal(), None);
    assert_eq!(call.before_forwarding().delay().length(), millis(200));
    assert_eq!(call.after_response().terminal(), Some(&"Late disconnect"));
    assert!(call.after_response().delay().is_zero());
}

// Modifiers

#[test]
fn delays_add_up_and_throttle_takes_the_maximum() {
    let call = combine([
        lie(
            "Delay 1 s",
            0,
            delay(1000).with_jitter(millis(200)).with_throttle(millis(300)),
        ),
        lie("Delay 100 ms", 1, delay(100).with_jitter(millis(100))),
        lie("Throttle", 2, TimingLie::new().with_throttle(millis(500))),
        lie("Throttle less", 3, TimingLie::new().with_throttle(millis(50))),
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
        lie("Slow request", 0, delay(200).at(BEFORE)),
        lie("Slower request", 1, delay(300).with_jitter(millis(30)).at(BEFORE)),
        lie("Slow answer", 2, delay(1000)),
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
        lie("Slow request", 0, delay(200).with_throttle(millis(700)).at(BEFORE)),
        lie("Throttle", 1, TimingLie::new().with_throttle(millis(500))),
    ]);

    assert_eq!(call.before_forwarding().throttle(), millis(0));
    assert_eq!(call.after_response().throttle(), millis(700));
}

#[test]
fn a_call_that_ends_after_the_response_has_no_answer_to_carry_a_throttle() {
    let call = combine([
        lie("Late disconnect", 0, late_hang_up()),
        lie("Slow request", 1, delay(200).with_throttle(millis(700)).at(BEFORE)),
    ]);

    // The delay held the request back, so the lie applies. Its throttle reaches no client.
    assert_eq!(fate(&call, "Slow request"), "applied on call");
    assert_eq!(call.before_forwarding().delay().length(), millis(200));

    let after = call.after_response();
    assert_eq!(after.terminal(), Some(&"Late disconnect"));
    assert_eq!(after.throttle(), millis(0));
}

#[test]
fn a_timing_lie_that_changes_nothing_still_applies() {
    let call = combine([lie("Nothing", 0, TimingLie::new())]);

    assert_eq!(fate(&call, "Nothing"), "applied on call");
    assert!(call.after_response().changes_nothing());
}

// The whole call as a target

#[test]
fn two_refusals_of_a_call_without_parts_conflict() {
    // A Heartbeat has no parts, so a refusal targets the whole response.
    let call = combine([
        lie("Coordinator gone", 0, refusal(Target::Call)),
        lie("Rebalance", 1, late_refusal(Target::Call)),
        lie("Slow", 2, delay(100)),
    ]);

    assert_eq!(fate(&call, "Coordinator gone"), "applied on call");
    assert_eq!(fate(&call, "Rebalance"), "shadowed on call by Coordinator gone");
    assert_eq!(fate(&call, "Slow"), "applied on call");

    let refusals = call.before_forwarding().refusals();
    assert_eq!(refusals.of_call(), Some(&"Coordinator gone"));
    assert_eq!(refusals.of(&part("orders.eu/3")), Some(&"Coordinator gone"));
    assert_eq!(held(refusals), ["call: Coordinator gone"]);
}

#[test]
fn a_refusal_of_the_whole_call_shadows_the_refusals_of_parts_below_it() {
    let call = combine([
        lie("Refuse all", 0, refusal(Target::Call)),
        lie("Refuse 3 and 4", 1, refusal(parts(&["orders.eu/3", "orders.eu/4"]))),
    ]);

    assert_eq!(fate(&call, "Refuse all"), "applied on call");
    assert_eq!(
        fate(&call, "Refuse 3 and 4"),
        "shadowed on orders.eu/3, orders.eu/4 by Refuse all"
    );
}

#[test]
fn a_refusal_of_the_whole_call_loses_the_whole_call_to_a_refusal_of_a_part_above_it() {
    // The whole call is one target: the refusal cannot apply around a part.
    let call = combine([
        lie("Refuse 4", 1, refusal(parts(&["orders.eu/4"]))),
        lie("Refuse 3", 0, refusal(parts(&["orders.eu/3"]))),
        lie("Refuse all", 2, refusal(Target::Call)),
    ]);

    assert_eq!(fate(&call, "Refuse all"), "shadowed on call by Refuse 3");
    assert_eq!(
        held(call.before_forwarding().refusals()),
        ["orders.eu/3: Refuse 3", "orders.eu/4: Refuse 4"]
    );
}

#[test]
fn rewrites_of_the_whole_call_conflict_per_aspect() {
    let call = combine([
        lie("Another controller", 0, rewrite(Aspect::Controller, Target::Call)),
        lie("No controller", 1, rewrite(Aspect::Controller, Target::Call)),
        lie("Missing broker", 2, rewrite(Aspect::Brokers, Target::Call)),
        lie(
            "Controller per topic",
            3,
            rewrite(Aspect::Controller, parts(&["orders.eu"])),
        ),
    ]);

    assert_eq!(fate(&call, "Another controller"), "applied on call");
    assert_eq!(fate(&call, "No controller"), "shadowed on call by Another controller");
    assert_eq!(fate(&call, "Missing broker"), "applied on call");
    assert_eq!(
        fate(&call, "Controller per topic"),
        "shadowed on orders.eu by Another controller"
    );

    let after = call.after_response();
    assert_eq!(after.rewrite_of_call(Aspect::Controller), Some(&"Another controller"));
    assert_eq!(after.rewrite_of_call(Aspect::Brokers), Some(&"Missing broker"));
    assert_eq!(after.rewrite_of_call(Aspect::Leader), None);
    assert_eq!(
        after.rewrite(Aspect::Controller, &part("orders.eu")),
        Some(&"Another controller")
    );
}

#[test]
fn a_refusal_of_the_whole_call_pre_empts_every_rewrite() {
    let call = combine([
        lie("Stale leader", 0, rewrite(Aspect::Leader, parts(&["orders.eu"]))),
        lie("Another controller", 1, rewrite(Aspect::Controller, Target::Call)),
        lie("Refuse all", 2, late_refusal(Target::Call)),
    ]);

    assert_eq!(fate(&call, "Stale leader"), "pre-empted on orders.eu by Refuse all");
    assert_eq!(fate(&call, "Another controller"), "pre-empted on call by Refuse all");
    assert_eq!(call.after_response().rewrites().count(), 0);
}

#[test]
fn a_rewrite_of_the_whole_call_keeps_something_to_change_when_a_part_is_refused() {
    let call = combine([
        lie("Unknown topic", 0, late_refusal(parts(&["orders.eu"]))),
        lie("Another controller", 1, rewrite(Aspect::Controller, Target::Call)),
    ]);

    assert_eq!(fate(&call, "Unknown topic"), "applied on orders.eu");
    assert_eq!(fate(&call, "Another controller"), "applied on call");
}

// Priority

#[test]
fn the_priority_orders_the_lies_and_not_the_order_they_come_in() {
    let partition = parts(&["orders.eu/3"]);
    let one = [
        lie("Low", 5, refusal(partition.clone())),
        lie("High", 2, refusal(partition.clone())),
        lie("Middle", 3, silence()),
    ];
    let mut other = one.clone();
    other.reverse();

    let call = combine(one);
    assert_eq!(call, combine(other));

    assert_eq!(ids(&call), ["High", "Middle", "Low"]);
    assert_eq!(fate(&call, "High"), "applied on orders.eu/3");
    assert_eq!(fate(&call, "Middle"), "shadowed on call by High");
    assert_eq!(fate(&call, "Low"), "shadowed on orders.eu/3 by High");

    let priorities: Vec<usize> = call.fates().iter().map(|fate| fate.priority().position()).collect();
    assert_eq!(priorities, [2, 3, 5]);
}

#[test]
fn lies_of_the_same_priority_keep_the_order_they_came_in() {
    let partition = parts(&["orders.eu/3"]);
    let call = combine([
        lie("First", 1, refusal(partition.clone())),
        lie("Second", 1, refusal(partition)),
    ]);

    assert_eq!(ids(&call), ["First", "Second"]);
    assert_eq!(fate(&call, "Second"), "shadowed on orders.eu/3 by First");
}

#[test]
fn a_fate_holds_the_lie_it_is_the_fate_of() {
    let fired = lie("Lost acknowledgement", 4, late_refusal(parts(&["orders.eu/3"])));
    let call = combine([fired.clone()]);
    let fate = &call.fates()[0];

    assert_eq!(fate.lie(), &fired);
    assert_eq!(fate.id(), &"Lost acknowledgement");
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

#[test]
fn a_lie_on_no_part_applies_nowhere() {
    // `Target::of_parts` never yields this target, and the variant is open to whoever builds it by hand.
    let nowhere = Target::Parts(std::collections::BTreeSet::new());
    let call = combine([lie("Nowhere", 0, stale_watermark(nowhere))]);

    assert_eq!(fate(&call, "Nowhere"), "nothing");
    assert!(!call.fate_of(&"Nowhere").unwrap().counts());
    assert!(call.after_response().changes_nothing());
}
