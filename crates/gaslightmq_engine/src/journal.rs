//! The journal of one proxy: what changed in its environment, as the engine keeps it in memory.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use gaslightmq_clock::Clock;
use gaslightmq_journal::{
    Change, ChangeSource, JournalEntry, JournalEntryId, RecentJournal, RecentJournalSize, RecordedName, Revision,
};
use gaslightmq_rule::{Rule, RuleName};
use gaslightmq_scenario::{Capture, Captured, CapturedRule};

use crate::events::EventBus;
use crate::{EngineEvent, StartCaptureError, StopCaptureError};

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
/// A handle keeps no engine running: once the engine stopped, its events are over, and an entry that is recorded
/// after that stays in the journal and leaves with no event.
///
/// # Capture
///
/// The journal also runs the capture of the environment: [`start_capture`](Self::start_capture) starts it with the
/// live set as it stands, and every entry that the journal records from then on reaches it, in the order of the
/// entries, until [`stop_capture`](Self::stop_capture) turns what it marked into the steps of a Sequence. A
/// [`Capture`] says what it marks. Whoever arms a live rule by hand [records the arming](Self::record_arming) with
/// the definition it arms with, so a captured rule keeps the parameters it had when it was armed.
///
/// The type is a handle, and a clone leads to the same journal.
/// No method waits.
#[derive(Clone)]
pub struct Journal {
    inner: Arc<Inner>,
}

struct Inner {
    clock: Arc<dyn Clock + Send + Sync>,
    /// The entries, and the capture that reads them. Whoever records holds the lock until the entry left with the
    /// events and reached the capture, so both take the entries in the order of their IDs.
    kept: Mutex<Kept>,
    /// The events of the engine, which end when the engine lets go of them.
    /// A journal does not hold them open: whoever waits for the end of the events, such as the task that hands
    /// the entries on, holds a journal itself.
    events: Weak<EventBus>,
}

/// What the journal keeps under its lock: the recent entries, and the capture that runs.
struct Kept {
    recent: RecentJournal,
    capture: Option<Capture>,
}

impl Journal {
    /// A journal without entries, which keeps `size` of them, tells their time on `clock`,
    /// and hands each one to the listeners of `events`.
    pub(crate) fn new(size: RecentJournalSize, clock: Arc<dyn Clock + Send + Sync>, events: &Arc<EventBus>) -> Self {
        Self {
            inner: Arc::new(Inner {
                clock,
                kept: Mutex::new(Kept {
                    recent: RecentJournal::new(size),
                    capture: None,
                }),
                events: Arc::downgrade(events),
            }),
        }
    }

    /// Records one change of the environment, and returns its entry.
    ///
    /// The entry takes the next number of the journal, and the instant of the clock of the engine.
    /// `from` says where the change came from, and `revision` is the revision that the change left,
    /// when it has one. The entry leaves the engine as an [`EngineEvent::Journaled`], unless the engine stopped.
    pub fn record(&self, change: Change, from: impl Into<ChangeSource>, revision: Option<Revision>) -> JournalEntry {
        self.record_as(change, from, revision, None)
    }

    /// Records that someone armed the rule `rule` by hand, as [`record`](Self::record) does a `rule-armed`, and
    /// tells the capture that runs the definition it armed with: `definition` is the live rule, or `None` for a rule
    /// of the workspace, which the capture names.
    pub fn record_arming(
        &self,
        rule: RecordedName,
        definition: Option<&Rule>,
        from: impl Into<ChangeSource>,
        revision: Option<Revision>,
    ) -> JournalEntry {
        self.record_as(Change::RuleArmed { rule }, from, revision, definition)
    }

    /// Starts the capture of the environment, with the live set as it stands: the rules that someone armed by hand,
    /// the one armed most recently first. Records that the capture started, and returns the entry: the capture reads
    /// every entry after it.
    ///
    /// # Errors
    ///
    /// [`StartCaptureError::AlreadyCapturing`] when a capture runs. An environment runs one capture at a time.
    pub fn start_capture(
        &self,
        live: impl IntoIterator<Item = CapturedRule>,
        from: impl Into<ChangeSource>,
    ) -> Result<JournalEntry, StartCaptureError> {
        let mut kept = self.kept();
        if kept.capture.is_some() {
            return Err(StartCaptureError::AlreadyCapturing);
        }
        let entry = self.add(&mut kept, Change::CaptureStarted, from.into(), None, None);
        kept.capture = Some(Capture::start(entry.at, live));
        Ok(entry)
    }

    /// Stops the capture of the environment, records that it stopped, and returns what it captured.
    ///
    /// # Errors
    ///
    /// [`StopCaptureError::NotCapturing`] when no capture runs.
    pub fn stop_capture(&self, from: impl Into<ChangeSource>) -> Result<Captured, StopCaptureError> {
        let mut kept = self.kept();
        let capture = kept.capture.take().ok_or(StopCaptureError::NotCapturing)?;
        let entry = self.add(&mut kept, Change::CaptureStopped, from.into(), None, None);
        Ok(capture.stop(entry.at))
    }

    /// Whether a capture of the environment runs.
    pub fn capturing(&self) -> bool {
        self.kept().capture.is_some()
    }

    /// Records one change, and hands it to the capture that runs, with the definition of a live rule that it arms.
    fn record_as(
        &self,
        change: Change,
        from: impl Into<ChangeSource>,
        revision: Option<Revision>,
        definition: Option<&Rule>,
    ) -> JournalEntry {
        let mut kept = self.kept();
        self.add(&mut kept, change, from.into(), revision, definition)
    }

    /// Adds one entry to the journal that `kept` holds: it leaves with the events, and reaches the capture that runs.
    fn add(
        &self,
        kept: &mut Kept,
        change: Change,
        from: ChangeSource,
        revision: Option<Revision>,
        definition: Option<&Rule>,
    ) -> JournalEntry {
        let entry = kept.recent.record(self.inner.clock.now(), change, from, revision);
        if let Some(events) = self.inner.events.upgrade() {
            events.publish(EngineEvent::Journaled(entry.clone()));
        }
        if let Some(capture) = kept.capture.as_mut() {
            let armed_as = |rule: &RuleName| definition.filter(|definition| definition.name() == rule).cloned();
            capture.read(&entry, armed_as);
        }
        entry
    }

    /// Records one change that leaves no revision, as [`record`](Self::record) does, and does `then` before any other
    /// change is recorded.
    pub(crate) fn record_then(
        &self,
        change: Change,
        from: impl Into<ChangeSource>,
        then: impl FnOnce(),
    ) -> JournalEntry {
        let mut kept = self.kept();
        let entry = self.add(&mut kept, change, from.into(), None, None);
        then();
        entry
    }

    /// Does `what` with the entries, and no change is recorded meanwhile.
    pub(crate) fn holding<R>(&self, what: impl FnOnce(&RecentJournal) -> R) -> R {
        what(&self.kept().recent)
    }

    /// The ID of the last entry, or `None` while the journal has none.
    pub(crate) fn last(&self) -> Option<JournalEntryId> {
        self.kept().recent.entries().next_back().map(|entry| entry.id)
    }

    /// The entries that the journal keeps in memory, the oldest first.
    pub fn entries(&self) -> Vec<JournalEntry> {
        self.kept().recent.entries().cloned().collect()
    }

    /// The entries that came after the entry `id` and that the journal keeps in memory, the oldest first.
    ///
    /// Whoever read the journal up to an entry reads on from there.
    /// The first entry it gets has the next number, unless the journal let go of entries in between.
    pub fn after(&self, id: JournalEntryId) -> Vec<JournalEntry> {
        self.kept().recent.after(id).cloned().collect()
    }

    /// The entries and the capture. They are whole whenever the lock is free, also after a panic of the task that
    /// held it: a change is one entry that is there or not.
    fn kept(&self) -> MutexGuard<'_, Kept> {
        self.inner.kept.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes how many entries the journal keeps in memory.
impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Journal")
            .field("entries", &self.kept().recent.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Duration;
    use gaslightmq_clock_waitable::SteppedClock;
    use gaslightmq_journal::ChangeSurface;
    use gaslightmq_lie::LieDefinition;
    use gaslightmq_scenario::ScenarioRule;
    use gaslightmq_selector::Conditions;

    use super::*;
    use crate::EventBacklog;

    #[test]
    fn an_entry_takes_the_next_number_and_the_instant_of_the_clock() {
        let clock = SteppedClock::new();
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(RecentJournalSize::default(), Arc::new(clock.clone()), &events);
        let rule: RecordedName = "slow-order-producers".parse().unwrap();
        let lifetime = Duration::from_secs(30);

        // Someone arms the rule at a pad, and its lifetime ends thirty seconds later.
        let armed = Change::RuleArmed { rule: rule.clone() };
        let armed = journal.record(armed, ChangeSurface::Pad, Some(Revision::new(2)));
        let ended_at = clock.advance(lifetime);
        let disarmed = Change::RuleDisarmed { rule };
        let disarmed = journal.record(disarmed, ChangeSurface::Lifetime, Some(Revision::new(3)));

        assert_eq!(armed.id, JournalEntryId::FIRST);
        assert_eq!(disarmed.id, JournalEntryId::FIRST.next());
        assert_eq!(disarmed.at, ended_at);
        assert_eq!(disarmed.at - armed.at, lifetime);
        assert_eq!(journal.entries(), [armed.clone(), disarmed.clone()]);
        assert_eq!(journal.after(armed.id), [disarmed]);
    }

    #[test]
    fn a_clone_of_the_handle_leads_to_the_same_journal() {
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(RecentJournalSize::default(), Arc::new(SteppedClock::new()), &events);
        let clone = journal.clone();
        let fired = Change::RuleFiredByHand {
            rule: "slow-order-producers".parse().unwrap(),
        };

        let fired = clone.record(fired, ChangeSurface::Cli, None);

        assert_eq!(journal.entries(), [fired]);
    }

    #[test]
    fn the_journal_keeps_as_many_entries_as_its_size_says() {
        let one_entry = RecentJournalSize::new(1).unwrap();
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(one_entry, Arc::new(SteppedClock::new()), &events);
        let rule: RecordedName = "slow-order-producers".parse().unwrap();

        let armed = Change::RuleArmed { rule: rule.clone() };
        journal.record(armed, ChangeSurface::Test, Some(Revision::new(2)));
        let disarmed = Change::RuleDisarmed { rule };
        let disarmed = journal.record(disarmed, ChangeSurface::Test, Some(Revision::new(3)));

        assert_eq!(journal.entries(), [disarmed]);
    }

    #[tokio::test]
    async fn every_entry_leaves_with_the_events_in_the_order_of_the_entries() {
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(RecentJournalSize::default(), Arc::new(SteppedClock::new()), &events);
        let mut recording = events.listen_to_every_event();
        let rule: RecordedName = "slow-order-producers".parse().unwrap();

        let armed = Change::RuleArmed { rule: rule.clone() };
        let armed = journal.record(armed, ChangeSurface::Ui, Some(Revision::new(2)));
        let fired = journal.record(Change::RuleFiredByHand { rule }, ChangeSurface::Ui, None);

        for entry in [armed, fired] {
            let Some(EngineEvent::Journaled(journaled)) = recording.next().await else {
                panic!("the entry leaves with the events");
            };
            assert_eq!(journaled, entry);
        }
    }

    #[test]
    fn a_capture_reads_every_entry_from_its_start_to_its_stop() {
        let clock = SteppedClock::new();
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(RecentJournalSize::default(), Arc::new(clock.clone()), &events);
        let slow = Rule::new(
            "slow-order-producers".parse().unwrap(),
            Conditions::new(),
            LieDefinition::Silence,
        );
        let recorded: RecordedName = slow.name().as_str().parse().unwrap();
        let lifetime = Duration::from_secs(30);
        let (arming, ending) = (Revision::new(2), Revision::new(3));

        // Someone captures while they arm a live rule, whose lifetime ends thirty seconds later.
        let started = journal.start_capture([], ChangeSurface::Ui).unwrap();
        journal.record_arming(recorded.clone(), Some(&slow), ChangeSurface::Pad, Some(arming));
        clock.advance(lifetime);
        let disarmed = Change::RuleDisarmed { rule: recorded };
        journal.record(disarmed, ChangeSurface::Lifetime, Some(ending));
        let again = journal.start_capture([], ChangeSurface::Ui);
        let captured = journal.stop_capture(ChangeSurface::Ui).unwrap();

        assert_eq!(started.change, Change::CaptureStarted);
        assert_eq!(again.unwrap_err(), StartCaptureError::AlreadyCapturing);
        assert_eq!(captured.steps()[1].arm(), [ScenarioRule::from(slow.name().clone())]);
        assert!(
            captured.steps()[2].arm().is_empty(),
            "the lifetime ended, and the rule left the live set"
        );
        assert_eq!(captured.rules(), [slow]);
        assert!(!journal.capturing());
        let last = journal.entries().pop().unwrap();
        assert_eq!(last.change, Change::CaptureStopped);
        assert_eq!(
            journal.stop_capture(ChangeSurface::Ui).unwrap_err(),
            StopCaptureError::NotCapturing
        );
    }

    #[tokio::test]
    async fn a_journal_that_outlives_its_engine_keeps_no_events_open() {
        let events = Arc::new(EventBus::new(EventBacklog::default()));
        let journal = Journal::new(RecentJournalSize::default(), Arc::new(SteppedClock::new()), &events);
        let mut recording = events.listen_to_every_event();
        let rule: RecordedName = "slow-order-producers".parse().unwrap();

        // The engine stops, and lets go of its events, while someone still holds the journal.
        drop(events);
        let disarmed = Change::RuleDisarmed { rule };
        let disarmed = journal.record(disarmed, ChangeSurface::Lifetime, Some(Revision::new(3)));

        assert!(recording.next().await.is_none(), "the events end with the engine");
        assert_eq!(journal.entries(), [disarmed]);
    }
}
