//! The recent journal of an environment: the entries that it keeps in memory.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;

use gaslightmq_clock::Instant;

use crate::{Change, ChangeSource, JournalEntry, JournalEntryId, Revision};

/// The recent journal of one environment: it numbers every entry, and keeps the latest ones in memory.
///
/// A journal only grows: [`record`](Self::record) adds an entry, and nothing changes one or takes one out.
/// The entries count from one, so a higher [ID](JournalEntryId) is a later entry.
/// Memory is not without end, so the recent journal keeps as many entries as its [size](RecentJournalSize) says,
/// and lets go of the oldest one when a new one would be one too many.
/// The count goes on: an entry that left the window keeps its number, and no later entry gets it.
///
/// The type does no IO and reads no clock: whoever records a change says when it was.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_journal::prelude::*;
///
/// let mut journal = RecentJournal::new(RecentJournalSize::new(2)?);
/// let rule: RecordedName = "slow-order-producers".parse().expect("a rule name");
/// let started = Instant::ZERO;
///
/// // Someone arms a rule at a pad, fires it once from `gmq`, and takes the arming back with ⌘Z.
/// let armed = Change::RuleArmed { rule: rule.clone() };
/// let first = journal.record(started, armed, ChangeSurface::Pad, Some(Revision::new(7))).id;
/// let fired = Change::RuleFiredByHand { rule: rule.clone() };
/// journal.record(started + Duration::from_secs(2), fired, ChangeSurface::Cli, None);
/// let disarmed = Change::RuleDisarmed { rule };
/// let undo = ChangeSource::new(ChangeSurface::Pad).with_cause(ChangeCause::Undo);
/// let third = journal.record(started + Duration::from_secs(5), disarmed, undo, Some(Revision::new(8))).id;
///
/// // The journal shows both the arming and that someone took it back, each as an entry of its own.
/// assert_eq!(first, JournalEntryId::FIRST);
/// assert_eq!(third, JournalEntryId::new(3).expect("a number that is at least one"));
///
/// // It keeps the two latest entries, and the first one left the window with its number.
/// let kept: Vec<u64> = journal.entries().map(|entry| entry.id.value()).collect();
/// assert_eq!(kept, [2, 3]);
/// # Ok::<(), RecentJournalSizeError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentJournal {
    size: RecentJournalSize,
    /// The ID of the next entry.
    next: JournalEntryId,
    /// The entries that the window keeps, the oldest first.
    entries: VecDeque<JournalEntry>,
}

impl RecentJournal {
    /// A journal without entries, which keeps as many as `size` says.
    pub fn new(size: RecentJournalSize) -> Self {
        Self {
            size,
            next: JournalEntryId::FIRST,
            entries: VecDeque::new(),
        }
    }

    /// Records one change, and returns its entry: the next entry of the journal, with the next number.
    ///
    /// `at` is the instant of the change on the clock of the environment, `from` says where the change came from,
    /// and `revision` is the revision that the change left, when it has one.
    /// The oldest entry leaves the window when the window is full.
    pub fn record(
        &mut self,
        at: Instant,
        change: Change,
        from: impl Into<ChangeSource>,
        revision: Option<Revision>,
    ) -> JournalEntry {
        if self.entries.len() == self.size.entries() {
            self.entries.pop_front();
        }
        let entry = JournalEntry {
            id: self.next,
            at,
            revision,
            from: from.into(),
            change,
        };
        self.next = entry.id.next();
        self.entries.push_back(entry.clone());
        entry
    }

    /// The entries that the window keeps, the oldest first.
    pub fn entries(&self) -> impl DoubleEndedIterator<Item = &JournalEntry> + ExactSizeIterator {
        self.entries.iter()
    }

    /// The entries that came after the entry `id` and that the window keeps, the oldest first.
    ///
    /// Whoever read the journal up to an entry reads on from there.
    /// The first entry it gets has the next number, unless the window let go of entries in between.
    pub fn after(&self, id: JournalEntryId) -> impl DoubleEndedIterator<Item = &JournalEntry> {
        // The entries stand in the order of their IDs, so the later ones begin where the first higher ID stands.
        let first_later = self.entries.partition_point(|entry| entry.id <= id);
        self.entries.range(first_later..)
    }

    /// How many entries the window keeps.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the window keeps no entry: nothing changed in the environment yet.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// How many entries a [`RecentJournal`] keeps in memory.
///
/// A journal keeps at least one entry.
/// It takes the room for an entry when the change comes, so a large window costs nothing until the entries are there.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let size = RecentJournalSize::new(250)?;
/// assert_eq!(size.entries(), 250);
/// assert_eq!(size.to_string(), "250 entries");
/// assert_eq!(RecentJournalSize::default().entries(), 1000);
/// assert_eq!(RecentJournalSize::new(0), Err(RecentJournalSizeError::Zero));
/// # Ok::<(), RecentJournalSizeError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecentJournalSize(NonZeroUsize);

impl RecentJournalSize {
    /// Check that `entries` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`RecentJournalSizeError::Zero`] when `entries` is zero.
    pub const fn new(entries: usize) -> Result<Self, RecentJournalSizeError> {
        match NonZeroUsize::new(entries) {
            Some(entries) => Ok(Self(entries)),
            None => Err(RecentJournalSizeError::Zero),
        }
    }

    /// The size as a plain number of entries.
    pub const fn entries(self) -> usize {
        self.0.get()
    }
}

/// 1000 entries.
impl Default for RecentJournalSize {
    fn default() -> Self {
        const DEFAULT: NonZeroUsize = NonZeroUsize::new(1000).unwrap();
        Self(DEFAULT)
    }
}

impl TryFrom<usize> for RecentJournalSize {
    type Error = RecentJournalSizeError;

    fn try_from(entries: usize) -> Result<Self, Self::Error> {
        Self::new(entries)
    }
}

impl From<RecentJournalSize> for usize {
    fn from(size: RecentJournalSize) -> Self {
        size.entries()
    }
}

/// Writes the number of entries: `1000 entries`, and `1 entry`.
impl fmt::Display for RecentJournalSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.entries() {
            1 => f.write_str("1 entry"),
            entries => write!(f, "{entries} entries"),
        }
    }
}

/// Why a number of entries is not a [`RecentJournalSize`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecentJournalSizeError {
    /// The size is zero, and a journal that keeps no entry shows nothing.
    #[error("a recent journal keeps at least one entry")]
    Zero,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_size_is_at_least_one_entry() {
        assert_eq!(RecentJournalSize::new(1).map(RecentJournalSize::entries), Ok(1));
        assert_eq!(RecentJournalSize::new(0), Err(RecentJournalSizeError::Zero));
        assert_eq!(RecentJournalSize::try_from(0), Err(RecentJournalSizeError::Zero));
        assert_eq!(usize::from(RecentJournalSize::try_from(512).unwrap()), 512);
        assert_eq!(RecentJournalSize::default().entries(), 1000);
    }

    #[test]
    fn a_size_writes_its_number_of_entries() {
        assert_eq!(RecentJournalSize::new(1).unwrap().to_string(), "1 entry");
        assert_eq!(RecentJournalSize::default().to_string(), "1000 entries");
        assert_eq!(
            RecentJournalSizeError::Zero.to_string(),
            "a recent journal keeps at least one entry"
        );
    }
}
