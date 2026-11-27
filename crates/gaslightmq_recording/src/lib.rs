//! Recordings: what a recording holds, the contract that every storage of recordings keeps,
//! and the encoding of every message of a recording.
//!
//! A recording is append-only. It holds a description, and then the session events, the calls with their frames and
//! the journal entries of its environment, in the order in which they ended.
//! A recording only grows, and it can grow very large, so every reader is a stream over a stretch of it.
//!
//! This crate knows no layout. It says what a storage is asked, and it owns the bytes of an entry:
//! a layout decides where a batch lies, how it is compressed, how it is found and what is said about it,
//! and never what is in it. Every layout therefore keeps the same entries, and a reader is written once.
//!
//! | Type | Is |
//! |---|---|
//! | [`RecordingId`], [`UtcInstant`] | Which recording this is: its environment and the instant it started, which is the name of its directory too. |
//! | [`Description`], [`RecordedBackend`], [`ActiveScenario`] | What the recording is, written once when it starts. |
//! | [`Entry`] | One thing that a recording keeps, encoded where it is made: a session event, a call or a journal entry. |
//! | [`SessionEvent`], [`SessionChange`], [`Closer`] | An event of a session: it opened, it was identified, its software was named, or it closed. |
//! | [`CallFrames`], [`FrameSlot`], [`FrameState`], [`RecordedFrame`] | The four frames of a call: as the engine hands them over, and as a recording holds them. |
//! | [`Position`], [`Place`], [`Direction`], [`Frames`] | Which entry this is, and where and how a stream reads. |
//! | [`Summary`], [`SummaryValues`], [`Wanted`], [`Everything`] | What a stretch of a recording holds, and what a reader looks for as far as a summary can tell. |
//! | [`End`], [`Ending`], [`Extent`] | How a recording ended, and how far it goes. |
//! | [`RecordingStorage`], [`RecordingWriter`], [`RecordingReader`], [`RecordedEntries`] | The storage contract. |
//! | [`BatchEncoder`], [`EncodedEntry`] | The way of an entry into the bytes of a batch. |
//! | [`BatchBuffer`], [`Batch`] | The buffer that a storage unpacks a batch into, and the batch that it becomes. |
//! | [`EntryView`], [`CallView`], [`FieldsView`], [`FieldValueView`], [`PartView`], [`ExplanationView`], [`RuleExplanationView`], [`JournalEntryView`], [`SessionEventView`] | An entry read where it lies. |
//! | [`EncodingVersion`] | The version of the encoding that a recording was written in. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! # Writing
//!
//! The recorder makes an [`Entry`] of what it takes from its queue, and hands it to the writer of its storage.
//! Making the entry encodes it, and it is where a call loses what the recording must not keep:
//! [`Entry::call`] applies the mode of the recording and blanks credentials.
//! A storage takes entries, so it never sees a frame that a recording must not keep.
//!
//! A frame is handed over as the buffer that the session read from its socket: a [`Frame`](gaslightmq_wire::Frame)
//! shares its bytes, so nothing copies them on the way to the storage.
//!
//! # Reading
//!
//! A storage unpacks the entries of a batch into a [`BatchBuffer`], which is the only place where the bytes of an
//! entry are written when someone reads. From there on nothing copies them:
//!
//! - [`Batch::checked`] makes the buffer the batch, and checks it once.
//! - A [view](EntryView) reads an entry where it lies: a number where it stands, and a text as the bytes it is.
//! - A [frame](CallView::frame) is a slice of the buffer that the frames of the batch were unpacked into.
//! - Whoever asks for the type gets it built: [`CallView::to_call`].
//!
//! Whoever keeps a view or a frame keeps the buffer of its batch in memory.
//! A consumer that keeps entries beyond its stream therefore copies them out, with [`CallView::to_call`] for the
//! type, or with [`Batch::copy_of`] for the entry and its frames as they are.
//!
//! # The encoding has versions
//!
//! A message is the archive of a stored type: the type as it lies in memory.
//! That is why a view reads without decoding anything, and why a message cannot grow:
//! every change of what an entry holds is a new [version of the encoding](EncodingVersion).
//! The crate keeps the stored types of every version that a build ever wrote, and a reader reads all of them.
//! A recording of a version that this build does not know is [refused](EncodingVersion::read_by_this_build).
//!
//! # Errors
//!
//! The contract holds its errors, one for each thing the caller asks.
//!
//! | The caller | Error | Variants, each a decision of the caller |
//! |---|---|---|
//! | Starts a recording | [`StartError`] | A recording has the ID, or the storage could not keep it. |
//! | Appends, commits, finishes | [`WriteError`] | The storage did not write. The recorder stops the recording. |
//! | Lists | [`ListError`] | The storage did not answer. |
//! | Opens | [`OpenError`] | Not found, a version of the encoding or of the layout that this build does not read, or no recording at all. |
//! | Asks how far a recording goes | [`ExtentError`] | The storage did not answer. |
//! | Starts a stream | [`ReadError`] | The storage did not answer, or the readers of the process hold all they may. |
//! | Asks a stream for its next batch | [`NextBatchError`] | Positions are lost, the storage did not answer, or the consumer holds all that its reader may. |
//! | Reads a value through a view, or builds the type | [`EntryError`] | The bytes are no entry, or they hold a value that its type does not take. |
//! | Reads a description, a summary or an end | [`MessageError`] | The bytes are no such message. |
//! | Makes the entry of a call | [`CallEntryError`] | The call tells a lie that the encoding has no stored type for. |
//!
//! Every type that checks its value has an error of its own, named after it:
//! [`UtcInstantError`], [`RecordingIdError`] and [`EncodingVersionError`].
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # No IO
//!
//! The crate depends on the vocabulary of a call and of a journal, and on the Kafka codec for one thing:
//! to take out of a frame what a recording must not keep.
//! Its traits declare async methods, and it runs none: it has no async code, no IO, no system clock and no
//! randomness. Whoever starts a recording hands in the instant.
//! It does not compile to WebAssembly, because the Kafka codec does not.

mod batch;
mod description;
mod encoding;
mod entry;
mod error;
mod id;
mod position;
mod redact;
mod storage;
mod summary;
mod view;

pub use batch::{Batch, BatchBuffer, BatchEncoder, EncodedEntry};
pub use description::{ActiveScenario, Description, RecordedBackend};
pub use encoding::{EncodingVersion, EncodingVersionError};
pub use entry::{CallFrames, Closer, Entry, FrameSlot, FrameState, RecordedFrame, SessionChange, SessionEvent};
pub use error::{
    CallEntryError, EntryError, ExtentError, ListError, MessageError, NextBatchError, OpenError, ReadError, StartError,
    WriteError,
};
pub use id::{RecordingId, RecordingIdError, UtcInstant, UtcInstantError};
pub use position::{Direction, End, Ending, Extent, Frames, Place, Position};
pub use storage::{RecordedEntries, RecordingReader, RecordingStorage, RecordingWriter};
pub use summary::{Everything, Summary, SummaryValues, Wanted};
pub use view::{
    CallView, EntryView, ExplanationView, FieldValueView, FieldsView, JournalEntryView, PartView, RuleExplanationView,
    SessionEventView,
};

/// Every type of this crate, with its error.
///
/// Import it whole wherever code speaks of recordings,
/// next to the preludes of the crates of a call, a journal and the vocabulary of Kafka.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_recording::prelude::*;
///
/// let started: Result<UtcInstant, UtcInstantError> = "2026-11-21T14:02:11.527Z".parse();
/// assert_eq!(started?.unix_millis(), 1_795_269_731_527);
/// # Ok::<(), UtcInstantError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ActiveScenario, Batch, BatchBuffer, BatchEncoder, CallEntryError, CallFrames, CallView, Closer, Description,
        Direction, EncodedEntry, EncodingVersion, EncodingVersionError, End, Ending, Entry, EntryError, EntryView,
        Everything, ExplanationView, Extent, ExtentError, FieldValueView, FieldsView, FrameSlot, FrameState, Frames,
        JournalEntryView, ListError, MessageError, NextBatchError, OpenError, PartView, Place, Position, ReadError,
        RecordedBackend, RecordedEntries, RecordedFrame, RecordingId, RecordingIdError, RecordingReader,
        RecordingStorage, RecordingWriter, RuleExplanationView, SessionChange, SessionEvent, SessionEventView,
        StartError, Summary, SummaryValues, UtcInstant, UtcInstantError, Wanted, WriteError,
    };
}
