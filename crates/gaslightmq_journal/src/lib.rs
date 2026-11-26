//! The journal: what changed in an environment, next to what happened on the wire.
//!
//! Each environment has a journal of its own, and it only grows.
//! Someone arms a rule, turns an encoder, fires a rule by hand, keeps a live rule or changes a definition,
//! and each of those is one entry. The UI shows the history of a rule from it, a capture reads the arms and
//! disarms from it, and a recording keeps its entries between the calls.
//! This crate holds the journal as data, and it changes nothing itself.
//!
//! | Type | Is |
//! |---|---|
//! | [`JournalEntry`] | One entry: what changed, when, at which revision, and where the change came from. |
//! | [`JournalEntryId`] | Which entry of its journal this is: one number. |
//! | [`Change`] | What changed: a rule armed, a parameter changed, a definition was created, an incident was applied, a recording stopped. |
//! | [`RecordingStopCause`] | Why a recording stopped by itself: the recorder fell behind, or it could not write. |
//! | [`Parameter`] | One parameter of a rule, which a turn of an encoder changes. |
//! | [`RecordedName`], [`RecordedDefinition`] | The name a rule or a definition had when the entry was written, and a definition of the workspace with its kind. |
//! | [`ChangeSource`], [`ChangeSurface`], [`ChangeCause`] | Where a change came from: its surface, and `undo` or `redo` when it takes back or repeats an earlier change. |
//! | [`Revision`] | The revision that a change left. |
//! | [`RecentJournal`], [`RecentJournalSize`] | The recent journal of one environment, which numbers the entries and keeps the latest in memory, and how many it keeps. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! ```
//! use gaslightmq_clock::{Duration, Instant};
//! use gaslightmq_journal::prelude::*;
//!
//! let mut journal = RecentJournal::new(RecentJournalSize::default());
//! let rule: RecordedName = "slow-order-producers".parse()?;
//!
//! // Someone arms a rule in the UI, which leaves the rule at revision 7.
//! let armed = Change::RuleArmed { rule: rule.clone() };
//! journal.record(Instant::ZERO, armed, ChangeSurface::Ui, Some(Revision::new(7)));
//!
//! // Thirty seconds later the lifetime of the rule ends. Nobody asked for that, and it is a change all the same.
//! let ended = Instant::ZERO + Duration::from_secs(30);
//! let disarmed = Change::RuleDisarmed { rule };
//! let entry = journal.record(ended, disarmed, ChangeSurface::Lifetime, Some(Revision::new(8)));
//!
//! assert_eq!(entry.id, JournalEntryId::FIRST.next());
//! assert_eq!(entry.from.surface, ChangeSurface::Lifetime);
//! assert_eq!(journal.len(), 2);
//! # Ok::<(), RecordedNameError>(())
//! ```
//!
//! # An entry
//!
//! Every entry carries five things.
//!
//! - **Its ID.** A number that the environment counts from one, so a higher ID is a later entry.
//! - **The time**, as an instant on the clock of the environment: the clock that the calls of the environment tell
//!   their time on, so entries and calls stand on one timeline.
//! - **The revision that the change left**, when the change has one. One counter gives the revisions of a whole
//!   process, so the revision orders the entries of every environment of one run.
//! - **Where the change came from**: its [surface](ChangeSurface), and its [cause](ChangeCause) when the change
//!   takes back or repeats an earlier one. The journal records a surface, and not a person.
//! - **What changed**: a [`Change`].
//!
//! # The journal only grows
//!
//! Nothing takes an entry out of a journal.
//! An undo is a change like any other: it adds an entry, with the cause `undo`.
//! The journal then shows both: that someone armed a rule, and that they took it back.
//!
//! A [`RecentJournal`] is the part of a journal that an environment keeps in memory: the latest entries,
//! as many as its [size](RecentJournalSize) says. An entry that leaves it is gone from memory, and its number is not.
//!
//! # A journal speaks in its own words
//!
//! A journal says what changed about rules, definitions and incidents, and this crate knows neither a rule nor a
//! definition. It depends on vocabulary only, so it has words of its own for what it records:
//!
//! - A rule and a definition are the [name they had](RecordedName) when the entry was written.
//! - A parameter is a [`Parameter`], and its values are text, as a definition writes them: `1s`, `40%`.
//! - An incident is the [`ExpandedIncident`](gaslightmq_incident::ExpandedIncident) that a backend applied.
//!
//! Whoever changes an environment writes these down.
//!
//! # Errors
//!
//! A type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`JournalEntryId`] | [`JournalEntryIdError`] | The number is zero, or the text is no whole number. |
//! | [`RecordedName`] | [`RecordedNameError`] | The text breaks a part of the [rule for names](RecordedName#the-rule-for-names). |
//! | [`RecentJournalSize`] | [`RecentJournalSizeError`] | The size is zero. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every type but the [`RecentJournal`] has a form.
//!
//! | Type | Form |
//! |---|---|
//! | [`JournalEntryId`], [`Revision`] | A number. |
//! | [`RecordedName`] | The text. |
//! | [`RecordedDefinition`] | A map with one entry: the kind, and the name. `{ "rule": "slow-order-producers" }` |
//! | [`ChangeSurface`], [`ChangeCause`], [`Parameter`], [`RecordingStopCause`] | The name in kebab case: `quick-inject`. |
//! | [`ChangeSource`] | A map with `surface`, and with `cause` when it has one. |
//! | [`Change`] | The map of its fields, with a `change` key that names it: `{ "change": "rule-armed", "rule": "slow-order-producers" }`. |
//! | [`JournalEntry`] | The map of its change, with `id`, `at`, `from`, and `revision` when it has one, next to the keys of the change. |
//!
//! Reading builds every type through its own checks.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_incident` for the incidents that an entry records,
//! and on `gaslightmq_clock` for the instant of an entry.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.

mod change;
mod entry;
mod id;
mod name;
mod recent;
mod revision;
mod source;

pub use change::{Change, Parameter, RecordingStopCause};
pub use entry::JournalEntry;
pub use id::{JournalEntryId, JournalEntryIdError};
pub use name::{RecordedDefinition, RecordedName, RecordedNameError};
pub use recent::{RecentJournal, RecentJournalSize, RecentJournalSizeError};
pub use revision::Revision;
pub use source::{ChangeCause, ChangeSource, ChangeSurface};

/// Every type of this crate, with its error.
///
/// Import it whole wherever code speaks in journal entries, next to the prelude of `gaslightmq_incident`.
/// No name in it is a name of that prelude or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let third: Result<JournalEntryId, JournalEntryIdError> = JournalEntryId::new(3);
/// assert_eq!(third?, JournalEntryId::FIRST.next().next());
/// # Ok::<(), JournalEntryIdError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Change, ChangeCause, ChangeSource, ChangeSurface, JournalEntry, JournalEntryId, JournalEntryIdError, Parameter,
        RecentJournal, RecentJournalSize, RecentJournalSizeError, RecordedDefinition, RecordedName, RecordedNameError,
        RecordingStopCause, Revision,
    };
}
