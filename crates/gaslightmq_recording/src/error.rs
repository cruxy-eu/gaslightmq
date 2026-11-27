//! The errors of the storage contract, one for each thing the caller asks.
//!
//! None of them names a file, a segment or a backing: a storage says what went wrong in the words of a recording,
//! and what the system or a library said is the text of a `reason`.

use crate::{EncodingVersionError, Position};

/// Why a recording did not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartError {
    /// A recording has the ID already: the environment and the instant of the description.
    #[error("a recording with this ID is there already")]
    AlreadyThere,
    /// The storage could not keep the recording.
    #[error("the recording did not start: {reason}")]
    NotStarted {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a writer did not take an entry, did not commit, or did not end its recording.
///
/// The recorder stops the recording, whatever stood in the way.
/// What the recording holds up to its last commit stays a recording.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    /// The storage did not write.
    #[error("the recording was not written: {reason}")]
    NotWritten {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a storage did not list its recordings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListError {
    /// The storage did not answer.
    #[error("the recordings were not listed: {reason}")]
    NotListed {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a recording did not open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    /// The storage has no recording with that ID.
    #[error("no recording has this ID")]
    NotFound,
    /// This build does not read the version of the encoding that the recording has.
    #[error("the recording does not open: {error}")]
    EncodingNotRead {
        /// The version that the recording has, and the newest that this build reads.
        error: EncodingVersionError,
    },
    /// This build does not read the version of the layout that the recording has.
    #[error("the recording has version {written} of its layout, and this build reads up to version {known}")]
    LayoutNotRead {
        /// The version of the layout that the recording has.
        written: u16,
        /// The newest version of that layout that this build reads.
        known: u16,
    },
    /// What is there is no recording.
    #[error("what is there is no recording: {reason}")]
    NotReadable {
        /// What is wrong with it.
        reason: String,
    },
}

/// Why a reader did not say how far its recording goes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExtentError {
    /// The storage did not answer, and the question can be asked again.
    #[error("the extent of the recording was not read: {reason}")]
    NotRead {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a stream of entries did not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// The storage did not answer, and the stream can be started again.
    #[error("the recording was not read: {reason}")]
    NotRead {
        /// What stood in the way.
        reason: String,
    },
    /// The readers of the process hold as many bytes as they may hold together, so the stream is refused.
    /// The caller tries again later, or tells whoever asked.
    #[error(
        "the readers of this process hold {held} bytes of recordings, and `recordings.all_readers_memory` allows \
         {budget}"
    )]
    AllReadersBudgetSpent {
        /// The bytes that the readers of the process hold.
        held: u64,
        /// The bytes that `recordings.all_readers_memory` allows.
        budget: u64,
    },
}

/// Why a stream of entries did not hand out its next batch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NextBatchError {
    /// The entries from `from` to `to` are lost, and the stream goes on behind them.
    #[error("the entries {from} to {to} of the recording are lost")]
    Damaged {
        /// The position of the first entry that is lost.
        from: Position,
        /// The position of the last entry that is lost.
        to: Position,
    },
    /// The storage did not answer, and the stream can ask again.
    #[error("the recording was not read: {reason}")]
    NotRead {
        /// What stood in the way.
        reason: String,
    },
    /// The consumer holds as many bytes of batches as its reader may hold.
    /// It lets go of some, or copies out what it keeps, and reads on.
    #[error(
        "the consumer of this stream holds {held} bytes of batches, and `recordings.reader_memory` allows {budget}"
    )]
    ReaderBudgetSpent {
        /// The bytes of batches that the consumer holds.
        held: u64,
        /// The bytes that `recordings.reader_memory` allows.
        budget: u64,
    },
}

/// Why a view did not hand out a value, or did not build the type of its entry.
///
/// The reader leaves the entry out, and says which position it had.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EntryError {
    /// The bytes are no entry, or they hold a value that its type does not take.
    #[error("the entry is damaged: {reason}")]
    Damaged {
        /// What is wrong with it.
        reason: String,
    },
}

/// Why bytes are no description, no summary or no end of a recording.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MessageError {
    /// The bytes are no such message, or they hold a value that its type does not take.
    #[error("the message is damaged: {reason}")]
    Damaged {
        /// What is wrong with it.
        reason: String,
    },
}

/// Why a call is no entry of a recording.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallEntryError {
    /// The call tells a lie that the encoding of this build has no stored type for.
    /// A lie comes with a crate of its own, and the encoding learns of it with a new version.
    #[error("the encoding of a recording has no stored type for the lie {lie}")]
    LieNotStored {
        /// The kind of the lie.
        lie: String,
    },
}
