//! One entry of a journal.

use gaslightmq_clock::Instant;

use crate::{Change, ChangeSource, JournalEntryId, Revision};

/// One entry of the journal of an environment: one thing that changed there, when, and where the change came from.
///
/// | Part | Is |
/// |---|---|
/// | [`id`](Self::id) | Which entry of its journal this is: a number that the environment counts from one. |
/// | [`at`](Self::at) | The time, on the clock of the environment. |
/// | [`revision`](Self::revision) | The revision that the change left, when the change has one. |
/// | [`from`](Self::from) | The surface that the change came from, and its cause when it takes back or repeats an earlier change. |
/// | [`change`](Self::change) | What changed. |
///
/// A [`RecentJournal`](crate::RecentJournal) numbers an entry when it records it, so nobody else makes one up:
/// whoever reads a journal reads the entries that it recorded.
///
/// With the `serde` feature, an entry is the map of its change, with its other parts next to the keys of the change.
/// An entry without a revision leaves the key out.
///
/// ```
/// # #[cfg(feature = "serde")]
/// # {
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_journal::prelude::*;
///
/// // Someone pressed ⇧⌘Z, which armed a rule again 4.2 seconds after its environment started.
/// let json = r#"{
///     "id": 7,
///     "at": 4200000000,
///     "revision": 12,
///     "from": { "surface": "ui", "cause": "redo" },
///     "change": "rule-armed",
///     "rule": "slow-order-producers"
/// }"#;
/// let entry: JournalEntry = serde_json::from_str(json).expect("an entry of a journal");
///
/// assert_eq!(entry.id.value(), 7);
/// assert_eq!(entry.at, Instant::ZERO + Duration::from_millis(4200));
/// assert_eq!(entry.revision, Some(Revision::new(12)));
/// assert_eq!(entry.from, ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Redo));
/// let rule = "slow-order-producers".parse().expect("a rule name");
/// assert_eq!(entry.change, Change::RuleArmed { rule });
///
/// let written = serde_json::to_value(&entry).expect("an entry has a form");
/// assert_eq!(written, serde_json::from_str::<serde_json::Value>(json).expect("JSON"));
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JournalEntry {
    /// Which entry of its journal this is.
    pub id: JournalEntryId,
    /// When the change was recorded, on the clock of the environment.
    pub at: Instant,
    /// The revision that the change left: the one that the changed thing has from the change on.
    /// `None` for what leaves none, such as a firing by hand.
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    pub revision: Option<Revision>,
    /// Where the change came from.
    pub from: ChangeSource,
    /// What changed.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub change: Change,
}
