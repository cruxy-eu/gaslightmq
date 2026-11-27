//! The storage contract: the traits through which a recording is started, written, listed, opened and read.

use crate::{
    Batch, Description, Direction, End, Entry, Extent, ExtentError, Frames, ListError, NextBatchError, OpenError,
    Place, Position, ReadError, RecordingId, StartError, Wanted, WriteError,
};

/// Where recordings are kept. A layout implements it, and the engine holds one and names no layout.
///
/// | A storage | Keeps to |
/// |---|---|
/// | Starts | It refuses an ID that a recording has. A recording that started is listed, and opens. |
/// | Appends | It gives every entry the next position. One writer writes a recording, and nobody writes to a recording again once its writer is gone. |
/// | Commits | Every entry that it took is readable, and is still there after a crash of the process or of the machine. A storage may commit by itself, when what it holds reaches a size. |
/// | Finishes | The recording has an end. A recording whose writer went away without one was cut short, and opens all the same. |
/// | Reads | As a stream: from the start, from the end or from a position, forwards or backwards. A reader sees every entry that a commit covers, also while the recording runs. Entries come in the order of their positions, a batch at a time. |
/// | Holds a bounded number of bytes | A reader holds the batches that it handed out and that someone still holds, and what it read ahead. A storage names the bound, never passes it by reading ahead, and says so when a consumer holds all of it. |
/// | Skips | It asks [`Wanted`] about every summary it has, at every level it has one, and reads no stretch that cannot hold what is wanted. A storage without summaries asks nothing and hands out everything. What it hands out is never less than what is wanted. |
/// | Hands out frames | Only when the reader asks for them. A reader that asks for none gets every call with the states and the lengths of its frames, and no storage reads a frame for it. |
/// | Keeps entries as they are | It stores the bytes of the entries as a [`BatchEncoder`](crate::BatchEncoder) wrote them, compressed or not, and looks into none of them. It keeps the [version of the encoding](crate::EncodingVersion) that they were written in, and says it with them. |
/// | Hands out entries where they lie | A batch is the buffer that the entries were put in. The storage fills a [`BatchBuffer`](crate::BatchBuffer), which starts at an address that is a multiple of 16, and makes the batch with [`Batch::checked`], which checks it once. No storage builds a type for a reader. |
/// | Says what it lost | A stretch that it cannot read is an error that names its positions, and the reader can go on behind it. |
///
/// The trait is a signature, and it names no runtime: a storage waits however it likes.
/// An implementation can write each of its async methods as an `async fn`.
pub trait RecordingStorage: Send + Sync {
    /// What writes a recording of this storage.
    type Writer: RecordingWriter;
    /// What reads a recording of this storage.
    type Reader: RecordingReader;

    /// Starts a recording. It exists from here on, and it is empty.
    ///
    /// The [ID](Description::id) of the recording is the environment and the instant of `description`.
    fn start(&self, description: Description) -> impl Future<Output = Result<Self::Writer, StartError>> + Send;

    /// The ID of every recording that started, in their order.
    fn recordings(&self) -> impl Future<Output = Result<Vec<RecordingId>, ListError>> + Send;

    /// Opens a recording to read it: one that ended, one that still runs, or one that was cut short.
    fn open(&self, recording: &RecordingId) -> impl Future<Output = Result<Self::Reader, OpenError>> + Send;
}

/// What writes one recording. A recording has one writer, and nobody writes to it again once the writer is gone.
///
/// A write that fails ends the recording: the writer takes nothing more.
pub trait RecordingWriter: Send {
    /// Takes the entry, and says which position it has.
    ///
    /// The entry is in the recording for good with the next [`commit`](Self::commit),
    /// or earlier, when the storage commits by itself.
    fn append(&mut self, entry: Entry) -> impl Future<Output = Result<Position, WriteError>> + Send;

    /// Every entry that was taken is in the recording when this returns:
    /// a reader sees it, and it is still there after a crash of the process or of the machine.
    fn commit(&mut self) -> impl Future<Output = Result<(), WriteError>> + Send;

    /// Commits, and ends the recording in order.
    fn finish(self, end: End) -> impl Future<Output = Result<(), WriteError>> + Send;
}

/// What reads one recording. Every reading is a stream: a pass over a stretch of the recording, in order.
pub trait RecordingReader: Send + Sync {
    /// A stream of the entries of the recording.
    type Entries: RecordedEntries;

    /// What the recording is.
    fn description(&self) -> &Description;

    /// How far the recording goes, and whether it ended in order.
    fn extent(&self) -> impl Future<Output = Result<Extent, ExtentError>> + Send;

    /// The entries from a place on, in one direction, without the stretches that cannot hold what is wanted.
    ///
    /// A stream sets the room for one batch aside when it starts, so it never stops half way for memory.
    /// When the readers of the process leave no room for that, the stream is refused, and nothing waits.
    fn read(
        &self,
        from: Place,
        direction: Direction,
        wanted: impl Wanted,
        frames: Frames,
    ) -> impl Future<Output = Result<Self::Entries, ReadError>> + Send;
}

/// A stream of the entries of a recording, a batch at a time.
pub trait RecordedEntries: Send {
    /// The next batch, or `None` at the end. A batch is the buffer that the storage has the entries in.
    ///
    /// A stream over a recording that still runs ends where the last commit ends,
    /// and the next call hands out what a later commit added.
    ///
    /// Nothing waits for memory. A consumer that holds as many bytes of batches as its reader may hold gets
    /// [`NextBatchError::ReaderBudgetSpent`]: it lets go of some, or copies out what it keeps, and asks again,
    /// and the stream hands out the batch that it did not hand out before.
    /// A reader may always hold one batch, however large it is.
    fn next(&mut self) -> impl Future<Output = Result<Option<Batch>, NextBatchError>> + Send;
}
