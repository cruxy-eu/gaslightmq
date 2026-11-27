//! A storage of recordings in memory: the smallest storage that keeps the contract.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};
use gaslightmq_recording::prelude::*;

/// A storage of recordings in memory.
///
/// It is the smallest storage that keeps the [contract](RecordingStorage), for the tests of whoever writes or reads
/// a recording and needs no file for it: a recorder, a replay, a query.
/// It keeps the bytes of the entries as a [`BatchEncoder`] wrote them, batch by batch, and looks into none of them.
///
/// | The contract | Here |
/// |---|---|
/// | A batch | The entries of one commit, cut where they reach [`with_batch_size`](Self::with_batch_size). |
/// | Commits | What a commit covers is in memory, and gone with the storage: nothing here outlives its process. |
/// | A bounded number of bytes | A reader holds at most [`with_reader_memory`](Self::with_reader_memory) bytes of entries that it unpacked, and reads nothing ahead. Frames are handed out as the buffers that the storage keeps, and do not count. |
/// | Skips | It keeps no summary, so it asks [`Wanted`] nothing, and hands out everything. |
///
/// A clone is the same storage.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_field::ConnectionNumber;
/// use gaslightmq_recording::prelude::*;
/// use gaslightmq_recording_conformance::MemoryStorage;
///
/// let storage = MemoryStorage::new();
/// let description = Description {
///     environment: "orders-sim".parse().expect("a name"),
///     started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
///     clock: Instant::ZERO,
///     mode: RecordingMode::Full,
///     backend: RecordedBackend::Simulated,
///     written_by: "0.1.0".to_owned(),
///     armed: Vec::new(),
///     scenario: None,
///     journal_after: None,
/// };
/// let closed = SessionEvent {
///     connection: ConnectionNumber::new(7),
///     at: Instant::ZERO,
///     change: SessionChange::Closed { by: Closer::Client },
/// };
///
/// block_on(async {
///     let mut writer = storage.start(description.clone()).await.expect("a new recording");
///     writer.append(Entry::session(&closed)).await.expect("the entry is taken");
///     writer.commit().await.expect("the entry is in the recording");
///
///     let reader = storage.open(&description.id()).await.expect("the recording opens");
///     let mut entries = reader
///         .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
///         .await
///         .expect("a stream");
///     let batch = entries.next().await.expect("the storage answers").expect("one batch");
///     let (position, entry) = batch.entries().next().expect("one entry");
///     let EntryView::Session(event) = entry.expect("an entry that passed the check") else { panic!("a session event") };
///     assert_eq!((position, event.to_session_event()), (Position::FIRST, Ok(closed)));
/// });
/// ```
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    recordings: Arc<Mutex<BTreeMap<RecordingId, Arc<Mutex<Recording>>>>>,
    batch_size: usize,
    reader_memory: usize,
}

/// What the storage keeps of one recording.
#[derive(Debug)]
struct Recording {
    description: Description,
    version: EncodingVersion,
    batches: Vec<KeptBatch>,
    end: Option<End>,
}

/// One batch as the storage keeps it: the bytes of its entries, one after the other, and its frames, back to back.
#[derive(Debug, Clone)]
struct KeptBatch {
    first: Position,
    count: u64,
    entries: Bytes,
    frames: Bytes,
}

impl KeptBatch {
    fn last(&self) -> Position {
        self.first.after(self.count - 1)
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MemoryStorage {
    /// The bytes of entries and frames at which the storage closes a batch, until someone sets another size:
    /// what `recordings.block_size` is in the segment layout.
    pub const DEFAULT_BATCH_SIZE: usize = 4 * 1024 * 1024;
    /// The bytes that one reader holds at most, until someone sets another number:
    /// what `recordings.reader_memory` is in the segment layout.
    pub const DEFAULT_READER_MEMORY: usize = 16 * 1024 * 1024;

    /// A storage that holds no recording.
    pub fn new() -> MemoryStorage {
        MemoryStorage {
            recordings: Arc::default(),
            batch_size: Self::DEFAULT_BATCH_SIZE,
            reader_memory: Self::DEFAULT_READER_MEMORY,
        }
    }

    /// The same storage, which closes a batch when its entries and frames reach `bytes`.
    /// An entry is never cut in two, so one entry that is larger is a batch of its own.
    #[must_use]
    pub fn with_batch_size(mut self, bytes: usize) -> MemoryStorage {
        self.batch_size = bytes;
        self
    }

    /// The same storage, whose readers each hold at most `bytes` bytes of batches that they handed out.
    /// A reader may always hold one batch, however large it is.
    #[must_use]
    pub fn with_reader_memory(mut self, bytes: usize) -> MemoryStorage {
        self.reader_memory = bytes;
        self
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a storage in memory has nothing to wait for"
)]
impl RecordingStorage for MemoryStorage {
    type Writer = MemoryWriter;
    type Reader = MemoryReader;

    async fn start(&self, description: Description) -> Result<MemoryWriter, StartError> {
        let mut recordings = locked(&self.recordings);
        let id = description.id();
        if recordings.contains_key(&id) {
            return Err(StartError::AlreadyThere);
        }
        let recording = Arc::new(Mutex::new(Recording {
            description,
            version: EncodingVersion::CURRENT,
            batches: Vec::new(),
            end: None,
        }));
        recordings.insert(id, Arc::clone(&recording));
        Ok(MemoryWriter {
            recording,
            batch_size: self.batch_size,
            next: Position::FIRST,
            open: OpenBatch::at(Position::FIRST),
            closed: Vec::new(),
        })
    }

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListError> {
        Ok(locked(&self.recordings).keys().cloned().collect())
    }

    async fn open(&self, recording: &RecordingId) -> Result<MemoryReader, OpenError> {
        let recording = locked(&self.recordings)
            .get(recording)
            .cloned()
            .ok_or(OpenError::NotFound)?;
        let description = locked(&recording).description.clone();
        Ok(MemoryReader {
            description,
            recording,
            reader_memory: self.reader_memory,
        })
    }
}

/// The batch that a writer fills.
#[derive(Debug)]
struct OpenBatch {
    first: Position,
    count: u64,
    encoder: BatchEncoder,
    entries: BytesMut,
    frames: BytesMut,
}

impl OpenBatch {
    fn at(first: Position) -> OpenBatch {
        OpenBatch {
            first,
            count: 0,
            encoder: BatchEncoder::new(),
            entries: BytesMut::new(),
            frames: BytesMut::new(),
        }
    }

    fn size(&self) -> usize {
        self.entries.len() + self.frames.len()
    }

    fn close(self) -> KeptBatch {
        KeptBatch {
            first: self.first,
            count: self.count,
            entries: self.entries.freeze(),
            frames: self.frames.freeze(),
        }
    }
}

/// What writes one recording of a [`MemoryStorage`].
#[derive(Debug)]
pub struct MemoryWriter {
    recording: Arc<Mutex<Recording>>,
    batch_size: usize,
    next: Position,
    open: OpenBatch,
    /// The batches that are full, and that no commit covers yet.
    closed: Vec<KeptBatch>,
}

impl MemoryWriter {
    fn close_the_open_batch(&mut self) {
        if self.open.count > 0 {
            let full = std::mem::replace(&mut self.open, OpenBatch::at(self.next));
            self.closed.push(full.close());
        }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a storage in memory has nothing to wait for"
)]
impl RecordingWriter for MemoryWriter {
    async fn append(&mut self, entry: Entry) -> Result<Position, WriteError> {
        let size = entry.size() + entry.frames_size();
        if self.open.size() + size > self.batch_size {
            self.close_the_open_batch();
        }
        // This is what a compressor does in a layout that compresses: the one copy of the bytes of a frame.
        let encoded = self.open.encoder.encode(entry);
        self.open.entries.extend_from_slice(encoded.entry());
        for frame in encoded.frames() {
            self.open.frames.extend_from_slice(frame);
        }
        self.open.count += 1;
        let position = self.next;
        self.next = position.next();
        Ok(position)
    }

    async fn commit(&mut self) -> Result<(), WriteError> {
        self.close_the_open_batch();
        locked(&self.recording).batches.append(&mut self.closed);
        Ok(())
    }

    async fn finish(mut self, end: End) -> Result<(), WriteError> {
        self.close_the_open_batch();
        let mut recording = locked(&self.recording);
        recording.batches.append(&mut self.closed);
        recording.end = Some(end);
        Ok(())
    }
}

/// What reads one recording of a [`MemoryStorage`].
#[derive(Debug)]
pub struct MemoryReader {
    description: Description,
    recording: Arc<Mutex<Recording>>,
    reader_memory: usize,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a storage in memory has nothing to wait for"
)]
impl RecordingReader for MemoryReader {
    type Entries = MemoryEntries;

    fn description(&self) -> &Description {
        &self.description
    }

    async fn extent(&self) -> Result<Extent, ExtentError> {
        let recording = locked(&self.recording);
        Ok(Extent {
            entries: recording.batches.iter().map(|batch| batch.count).sum(),
            end: recording.end,
        })
    }

    async fn read(
        &self,
        from: Place,
        direction: Direction,
        _wanted: impl Wanted,
        frames: Frames,
    ) -> Result<MemoryEntries, ReadError> {
        let last = locked(&self.recording).batches.last().map(KeptBatch::last);
        let next = match (from, direction) {
            (Place::Start, _) => Some(Position::FIRST),
            (Place::At(position), _) => Some(position),
            (Place::End, Direction::Forwards) => Some(last.unwrap_or(Position::FIRST)),
            (Place::End, Direction::Backwards) => last,
        };
        Ok(MemoryEntries {
            recording: Arc::clone(&self.recording),
            next,
            direction,
            frames,
            held: Arc::default(),
            reader_memory: self.reader_memory,
        })
    }
}

/// A stream of the entries of one recording of a [`MemoryStorage`].
#[derive(Debug)]
pub struct MemoryEntries {
    recording: Arc<Mutex<Recording>>,
    /// The position of the next entry that the stream hands out, or nothing behind the first entry of a stream
    /// backwards.
    next: Option<Position>,
    direction: Direction,
    frames: Frames,
    /// The bytes of the batches that this stream handed out, and that someone still holds.
    held: Arc<AtomicUsize>,
    reader_memory: usize,
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a storage in memory has nothing to wait for"
)]
impl RecordedEntries for MemoryEntries {
    async fn next(&mut self) -> Result<Option<Batch>, NextBatchError> {
        let Some(next) = self.next else {
            return Ok(None);
        };
        let (version, kept) = {
            let recording = locked(&self.recording);
            let holds = |batch: &&KeptBatch| batch.first <= next && next <= batch.last();
            let found = recording.batches.iter().find(holds).or_else(|| {
                // A stream backwards that starts behind the last entry starts at the last entry.
                let backwards = self.direction == Direction::Backwards;
                recording.batches.last().filter(|last| backwards && last.last() < next)
            });
            match found {
                Some(batch) => (recording.version, batch.clone()),
                None => return Ok(None),
            }
        };

        let held = self.held.load(Ordering::Acquire);
        if held > 0 && held + kept.entries.len() > self.reader_memory {
            return Err(NextBatchError::ReaderBudgetSpent {
                held: held as u64,
                budget: self.reader_memory as u64,
            });
        }

        // This is what a decompressor does: it writes the entries of the batch into their buffer, once.
        let size = kept.entries.len();
        self.held.fetch_add(size, Ordering::AcqRel);
        let counted = Arc::clone(&self.held);
        let mut entries = BatchBuffer::with_room(size).when_released(move || {
            counted.fetch_sub(size, Ordering::AcqRel);
        });
        entries.write(&kept.entries);
        let frames = match self.frames {
            Frames::With => Some(kept.frames.clone()),
            Frames::Without => None,
        };
        let batch = Batch::checked(version, kept.first, entries, frames).towards(self.direction);
        let batch = match self.direction {
            Direction::Forwards => {
                self.next = Some(kept.last().next());
                batch.starting_at(next)
            }
            Direction::Backwards => {
                self.next = kept.first.number().checked_sub(1).map(Position::new);
                batch.ending_at(next)
            }
        };
        Ok(Some(batch))
    }
}
