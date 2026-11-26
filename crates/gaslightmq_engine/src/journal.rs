//! The journal of one proxy: what changed in its environment, as the engine keeps it in memory.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_clock::Clock;
use gaslightmq_journal::{
    Change, ChangeSource, JournalEntry, JournalEntryId, RecentJournal, RecentJournalSize, Revision,
};

use crate::EngineEvent;
use crate::events::EventBus;

/// The journal of one proxy: what changed in its environment, next to what happened on the wire.
///
/// One proxy is one environment, and the engine keeps its journal: it numbers the entries from one,
/// tells the time of each on its clock, and keeps the [recent ones](crate::EngineConfiguration::recent_journal) in
/// memory.
/// The engine changes nothing by itself here: whoever changes the environment [records](Self::record) the change,
/// with where it came from and the revision it left.
///
/// Every entry also leaves the engine with its [events](EngineEvent::Journaled), in the order of the entries.
/// A listener of every event therefore gets the entries between the calls, as a recording keeps them.
///
/// The type is a handle, and a clone leads to the same journal.
/// No method waits.
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}

struct Inner {
    clock: Arc<dyn Clock + Send + Sync>,
    /// The entries. Whoever records holds the lock until the entry left with the events,
    /// so the events carry the entries in the order of their IDs.
    recent: Mutex<RecentJournal>,
    events: Arc<EventBus>,
}

impl Journal {
    /// A journal without entries, which keeps `size` of them, tells their time on `clock`,
    /// and hands each one to the listeners of `events`.
    pub(crate) fn new(size: RecentJournalSize, clock: Arc<dyn Clock + Send + Sync>, events: Arc<EventBus>) -> Self {
        Self {
            inner: Arc::new(Inner {
                clock,
                recent: Mutex::new(RecentJournal::new(size)),
                events,
            }),
        }
    }

    /// Records one change of the environment, and returns its entry.
    ///
    /// The entry takes the next number of the journal, and the instant of the clock of the engine.
    /// `from` says where the change came from, and `revision` is the revision that the change left,
    /// when it has one. The entry leaves the engine as an [`EngineEvent::Journaled`].
    pub fn record(&self, change: Change, from: impl Into<ChangeSource>, revision: Option<Revision>) -> JournalEntry {
        let mut recent = self.recent();
        let entry = recent.record(self.inner.clock.now(), change, from, revision);
        self.inner.events.publish(EngineEvent::Journaled(entry.clone()));
        entry
    }

    /// The entries that the journal keeps in memory, the oldest first.
    pub fn entries(&self) -> Vec<JournalEntry> {
        self.recent().entries().cloned().collect()
    }

    /// The entries that came after the entry `id` and that the journal keeps in memory, the oldest first.
    ///
    /// Whoever read the journal up to an entry reads on from there.
    /// The first entry it gets has the next number, unless the journal let go of entries in between.
    pub fn after(&self, id: JournalEntryId) -> Vec<JournalEntry> {
        self.recent().after(id).cloned().collect()
    }

    /// The entries. They are whole whenever the lock is free, also after a panic of the task that held it:
    /// a change is one entry that is there or not.
    fn recent(&self) -> MutexGuard<'_, RecentJournal> {
        self.inner.recent.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes how many entries the journal keeps in memory.
impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Journal")
            .field("entries", &self.recent().len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Duration;
    use gaslightmq_clock_waitable::SteppedClock;
    use gaslightmq_journal::{ChangeSurface, RecordedName};

    use super::*;
    use crate::EventBacklog;

    fn journal(size: RecentJournalSize, clock: &SteppedClock) -> (Journal, Arc<EventBus>) {
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(size, Arc::new(clock.clone()), Arc::clone(&events));
        (journal, events)
    }

    fn rule() -> RecordedName {
        "slow-order-producers".parse().unwrap()
    }

    #[test]
    fn an_entry_takes_the_next_number_and_the_instant_of_the_clock() {
        let clock = SteppedClock::new();
        let (journal, _events) = journal(RecentJournalSize::default(), &clock);

        let armed = journal.record(
            Change::RuleArmed { rule: rule() },
            ChangeSurface::Pad,
            Some(Revision::new(2)),
        );
        let later = clock.advance(Duration::from_secs(30));
        let disarmed = journal.record(
            Change::RuleDisarmed { rule: rule() },
            ChangeSurface::Lifetime,
            Some(Revision::new(3)),
        );

        assert_eq!(armed.id, JournalEntryId::FIRST);
        assert_eq!(disarmed.id, JournalEntryId::FIRST.next());
        assert_eq!(disarmed.at, later);
        assert_eq!(disarmed.at - armed.at, Duration::from_secs(30));
        assert_eq!(journal.entries(), [armed.clone(), disarmed.clone()]);
        assert_eq!(journal.after(armed.id), [disarmed]);
    }

    #[test]
    fn a_clone_of_the_handle_leads_to_the_same_journal() {
        let (journal, _events) = journal(RecentJournalSize::default(), &SteppedClock::new());
        let clone = journal.clone();

        let fired = clone.record(Change::RuleFiredByHand { rule: rule() }, ChangeSurface::Cli, None);

        assert_eq!(journal.entries(), [fired]);
    }

    #[test]
    fn the_journal_keeps_as_many_entries_as_its_size_says() {
        let one_entry = RecentJournalSize::new(1).unwrap();
        let (journal, _events) = journal(one_entry, &SteppedClock::new());

        journal.record(
            Change::RuleArmed { rule: rule() },
            ChangeSurface::Test,
            Some(Revision::new(2)),
        );
        let disarmed = journal.record(
            Change::RuleDisarmed { rule: rule() },
            ChangeSurface::Test,
            Some(Revision::new(3)),
        );

        assert_eq!(journal.entries(), [disarmed]);
    }

    #[tokio::test]
    async fn every_entry_leaves_with_the_events_in_the_order_of_the_entries() {
        let (journal, events) = journal(RecentJournalSize::default(), &SteppedClock::new());
        let mut recording = events.listen_to_every_event();

        let armed = journal.record(
            Change::RuleArmed { rule: rule() },
            ChangeSurface::Ui,
            Some(Revision::new(2)),
        );
        let fired = journal.record(Change::RuleFiredByHand { rule: rule() }, ChangeSurface::Ui, None);

        for entry in [armed, fired] {
            let Some(EngineEvent::Journaled(journaled)) = recording.next().await else {
                panic!("the entry leaves with the events");
            };
            assert_eq!(journaled, entry);
        }
    }
}
