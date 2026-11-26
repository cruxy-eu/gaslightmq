//! The recent journal of an environment: it numbers what changed, keeps the latest entries, and only grows.

use gaslightmq_clock::{Duration, Instant};
use gaslightmq_journal::prelude::*;

#[test]
fn an_undo_adds_an_entry_next_to_the_change_it_took_back() {
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let rule: RecordedName = "slow-order-producers".parse().unwrap();
    let armed_at = Instant::ZERO + Duration::from_secs(60);
    let taken_back_at = armed_at + Duration::from_secs(5);
    let left_by_the_arm = Revision::new(7);
    let left_by_the_undo = Revision::new(8);

    // Someone arms the rule in a view of the UI, and takes that back with ⌘Z five seconds later.
    let arm = Change::RuleArmed { rule: rule.clone() };
    journal.record(armed_at, arm.clone(), ChangeSurface::Ui, Some(left_by_the_arm));
    let undo = ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo);
    let disarm = Change::RuleDisarmed { rule };
    journal.record(taken_back_at, disarm.clone(), undo, Some(left_by_the_undo));

    // The journal shows both: that someone armed the rule, and that they took it back.
    let entries: Vec<&JournalEntry> = journal.entries().collect();
    assert_eq!(
        entries,
        [
            &JournalEntry {
                id: JournalEntryId::FIRST,
                at: armed_at,
                revision: Some(left_by_the_arm),
                from: ChangeSource::new(ChangeSurface::Ui),
                change: arm,
            },
            &JournalEntry {
                id: JournalEntryId::FIRST.next(),
                at: taken_back_at,
                revision: Some(left_by_the_undo),
                from: undo,
                change: disarm,
            },
        ]
    );
}

#[test]
fn an_environment_counts_its_entries_from_one() {
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let rule: RecordedName = "slow-order-producers".parse().unwrap();
    assert!(journal.is_empty());

    // `gmq` fires the rule by hand three times. A firing leaves no revision.
    let ids: Vec<u64> = (0..3)
        .map(|_| {
            let fired = Change::RuleFiredByHand { rule: rule.clone() };
            journal
                .record(Instant::ZERO, fired, ChangeSurface::Cli, None)
                .id
                .value()
        })
        .collect();

    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(journal.len(), 3);
}

#[test]
fn a_change_that_nobody_asked_for_has_a_surface_too() {
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let rule: RecordedName = "slow-order-producers".parse().unwrap();
    let lifetime_ended_at = Instant::ZERO + Duration::from_secs(30);

    // The lifetime of the rule ends thirty seconds after its environment started.
    let disarmed = Change::RuleDisarmed { rule };
    let entry = journal.record(
        lifetime_ended_at,
        disarmed,
        ChangeSurface::Lifetime,
        Some(Revision::new(8)),
    );

    assert_eq!(entry.from, ChangeSource::new(ChangeSurface::Lifetime));
    assert_eq!(entry.from.cause, None);
    assert_eq!(entry.at, lifetime_ended_at);
}

#[test]
fn a_full_journal_lets_go_of_its_oldest_entry_and_counts_on() {
    let two_entries = RecentJournalSize::new(2).unwrap();
    let mut journal = RecentJournal::new(two_entries);
    let first_rule = Change::LiveRuleMade {
        rule: "delay-1s-on-order-producers".parse().unwrap(),
    };
    let second_rule = Change::LiveRuleMade {
        rule: "delay-1s-on-fetches".parse().unwrap(),
    };
    let third_rule = Change::LiveRuleMade {
        rule: "blackhole-on-metadata".parse().unwrap(),
    };

    // Someone presses three empty pads, one after the other, and each press makes the live rule of its pad.
    journal.record(Instant::ZERO, first_rule, ChangeSurface::Pad, Some(Revision::new(2)));
    journal.record(
        Instant::ZERO,
        second_rule.clone(),
        ChangeSurface::Pad,
        Some(Revision::new(3)),
    );
    let third = journal.record(
        Instant::ZERO,
        third_rule.clone(),
        ChangeSurface::Pad,
        Some(Revision::new(4)),
    );

    // The first entry is gone from memory. Its number is not: the third entry is entry 3.
    assert_eq!(third.id.value(), 3);
    let kept: Vec<&Change> = journal.entries().map(|entry| &entry.change).collect();
    assert_eq!(kept, [&second_rule, &third_rule]);
    assert_eq!(journal.len(), two_entries.entries());
}

#[test]
fn whoever_read_up_to_an_entry_reads_on_from_there() {
    let three_entries = RecentJournalSize::new(3).unwrap();
    let mut journal = RecentJournal::new(three_entries);
    let fired = Change::RuleFiredByHand {
        rule: "slow-order-producers".parse().unwrap(),
    };
    let first = journal
        .record(Instant::ZERO, fired.clone(), ChangeSurface::Test, None)
        .id;
    let second = journal
        .record(Instant::ZERO, fired.clone(), ChangeSurface::Test, None)
        .id;
    let third = journal
        .record(Instant::ZERO, fired.clone(), ChangeSurface::Test, None)
        .id;

    let after_the_first: Vec<u64> = journal.after(first).map(|entry| entry.id.value()).collect();
    let after_the_second: Vec<u64> = journal.after(second).map(|entry| entry.id.value()).collect();
    assert_eq!(after_the_first, [2, 3]);
    assert_eq!(after_the_second, [3]);
    assert_eq!(journal.after(third).next(), None);

    // Two more entries push the first two out. A reader that read up to the first one sees that entry 2 is missing.
    journal.record(Instant::ZERO, fired.clone(), ChangeSurface::Test, None);
    journal.record(Instant::ZERO, fired, ChangeSurface::Test, None);
    let after_the_first: Vec<u64> = journal.after(first).map(|entry| entry.id.value()).collect();
    assert_eq!(after_the_first, [3, 4, 5]);
    // An ID that the journal has not reached yet has no entry after it.
    let not_reached_yet = JournalEntryId::new(9).unwrap();
    assert_eq!(journal.after(not_reached_yet).next(), None);
}
