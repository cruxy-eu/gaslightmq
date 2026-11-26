//! The recent journal of an environment: it numbers what changed, keeps the latest entries, and only grows.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::{Duration, Instant};
use gaslightmq_journal::prelude::*;

/// The rule that the tests change.
fn slow_order_producers() -> RecordedName {
    "slow-order-producers".parse().unwrap()
}

/// The instant that many seconds after the environment started.
fn after_secs(secs: u64) -> Instant {
    Instant::ZERO + Duration::from_secs(secs)
}

#[test]
fn an_undo_adds_an_entry_next_to_the_change_it_took_back() {
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    let rule = slow_order_producers();
    let armed_at = Revision::new(7);
    let disarmed_at = Revision::new(8);

    // Someone arms the rule in a view of the UI, and takes that back with ⌘Z five seconds later.
    let arm = Change::RuleArmed { rule: rule.clone() };
    journal.record(after_secs(60), arm.clone(), ChangeSurface::Ui, Some(armed_at));
    let undo = ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo);
    let disarm = Change::RuleDisarmed { rule };
    journal.record(after_secs(65), disarm.clone(), undo, Some(disarmed_at));

    // The journal shows both: that someone armed the rule, and that they took it back.
    let entries: Vec<&JournalEntry> = journal.entries().collect();
    assert_eq!(
        entries,
        [
            &JournalEntry {
                id: JournalEntryId::FIRST,
                at: after_secs(60),
                revision: Some(armed_at),
                from: ChangeSource::new(ChangeSurface::Ui),
                change: arm,
            },
            &JournalEntry {
                id: JournalEntryId::FIRST.next(),
                at: after_secs(65),
                revision: Some(disarmed_at),
                from: undo,
                change: disarm,
            },
        ]
    );
}

#[test]
fn an_environment_counts_its_entries_from_one() {
    let mut journal = RecentJournal::new(RecentJournalSize::default());
    assert!(journal.is_empty());

    let ids: Vec<u64> = (0..3)
        .map(|_| {
            let fired = Change::RuleFiredByHand {
                rule: slow_order_producers(),
            };
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

    // The lifetime of the rule ends thirty seconds after someone armed it at a pad.
    let disarmed = Change::RuleDisarmed {
        rule: slow_order_producers(),
    };
    let entry = journal.record(
        after_secs(30),
        disarmed,
        ChangeSurface::Lifetime,
        Some(Revision::new(8)),
    );

    assert_eq!(entry.from, ChangeSource::new(ChangeSurface::Lifetime));
    assert_eq!(entry.from.cause, None);
}

#[test]
fn a_full_window_lets_go_of_its_oldest_entry_and_counts_on() {
    let two_entries = RecentJournalSize::new(2).unwrap();
    let mut journal = RecentJournal::new(two_entries);
    let made = |name: &str| Change::LiveRuleMade {
        rule: name.parse().unwrap(),
    };

    journal.record(
        after_secs(1),
        made("first-rule"),
        ChangeSurface::Pad,
        Some(Revision::new(2)),
    );
    journal.record(
        after_secs(2),
        made("second-rule"),
        ChangeSurface::Pad,
        Some(Revision::new(3)),
    );
    let third = journal.record(
        after_secs(3),
        made("third-rule"),
        ChangeSurface::Pad,
        Some(Revision::new(4)),
    );

    // The first entry is gone from memory. Its number is not: the third entry is entry 3.
    assert_eq!(third.id.value(), 3);
    let kept: Vec<&Change> = journal.entries().map(|entry| &entry.change).collect();
    assert_eq!(kept, [&made("second-rule"), &made("third-rule")]);
    assert_eq!(journal.len(), two_entries.entries());
}

#[test]
fn whoever_read_up_to_an_entry_reads_on_from_there() {
    let mut journal = RecentJournal::new(RecentJournalSize::new(3).unwrap());
    let fired = || Change::RuleFiredByHand {
        rule: slow_order_producers(),
    };
    let ids = |entries: Vec<&JournalEntry>| -> Vec<u64> { entries.iter().map(|entry| entry.id.value()).collect() };
    let first = journal.record(after_secs(1), fired(), ChangeSurface::Test, None).id;
    let second = journal.record(after_secs(2), fired(), ChangeSurface::Test, None).id;
    let third = journal.record(after_secs(3), fired(), ChangeSurface::Test, None).id;

    assert_eq!(ids(journal.after(first).collect()), [2, 3]);
    assert_eq!(ids(journal.after(second).collect()), [3]);
    assert!(journal.after(third).next().is_none());

    // Two more entries push the first two out. A reader that read up to the first one sees that entry 2 is missing.
    journal.record(after_secs(4), fired(), ChangeSurface::Test, None);
    journal.record(after_secs(5), fired(), ChangeSurface::Test, None);
    assert_eq!(ids(journal.after(first).collect()), [3, 4, 5]);
    // An ID that the journal has not reached yet has no entry after it.
    assert!(journal.after(JournalEntryId::new(9).unwrap()).next().is_none());
}
