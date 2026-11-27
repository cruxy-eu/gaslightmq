//! A batch: a series of entries in the buffers that a storage has them in, and the way into and out of it.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::encoding::{self, Aligned, EncodingVersion, Head, Kind, Written, v1};
use crate::view::{CallView, EntryView, JournalEntryView, SessionEventView};
use crate::{Direction, Entry, EntryError, Position};

/// Writes the entries of one batch, one after the other.
///
/// A batch has two parts, and a storage keeps them apart:
///
/// | Part | Holds |
/// |---|---|
/// | Entries | The entries, one after the other. Each is a head of 16 bytes and then its message, which starts at a multiple of 16. The next head stands at the next multiple of 16 behind the message, and the bytes between them are zero. |
/// | Frames | The frames of the calls of the batch, back to back, without the four bytes of their length. |
///
/// The head of a call that the recording holds frames of says where they start among the frames of the batch,
/// so the encoder counts the frames that it handed out.
/// A storage starts a new encoder with every batch.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::ConnectionNumber;
/// use gaslightmq_recording::prelude::*;
///
/// let closed = SessionEvent {
///     connection: ConnectionNumber::new(7),
///     at: Instant::ZERO,
///     change: SessionChange::Closed { by: Closer::Client },
/// };
///
/// let mut encoder = BatchEncoder::new();
/// let encoded = encoder.encode(Entry::session(&closed));
///
/// // What the storage keeps is the bytes of the entry, and they are what a reader gets back.
/// let mut buffer = BatchBuffer::with_room(encoded.entry().len());
/// buffer.write(encoded.entry());
/// let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, buffer, None);
///
/// let (position, entry) = batch.entries().next().expect("one entry");
/// assert_eq!(position, Position::FIRST);
/// let EntryView::Session(event) = entry? else { panic!("a session event") };
/// assert_eq!(event.to_session_event()?, closed);
/// # Ok::<(), EntryError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct BatchEncoder {
    frames: u64,
}

impl BatchEncoder {
    /// An encoder at the start of a batch.
    pub const fn new() -> BatchEncoder {
        BatchEncoder { frames: 0 }
    }

    /// The bytes of `entry` as they stand in the batch, and its frames.
    pub fn encode(&mut self, entry: Entry) -> EncodedEntry {
        let (mut message, frames) = entry.into_parts();
        if !frames.is_empty() {
            message.set_frames_at(self.frames);
        }
        self.frames += frames.iter().map(|frame| frame.len() as u64).sum::<u64>();
        EncodedEntry {
            entry: message.into_bytes(),
            frames,
        }
    }

    /// The bytes of the frames that the encoder handed out so far: where the frames of the next call start.
    pub const fn frames_len(&self) -> u64 {
        self.frames
    }
}

/// One entry as it stands in a batch.
#[derive(Debug, Clone)]
pub struct EncodedEntry {
    entry: Bytes,
    frames: Vec<Bytes>,
}

impl EncodedEntry {
    /// The head of the entry, its message, and the zeros up to the next multiple of 16.
    /// A storage puts these bytes behind those of the entry before, as they are.
    pub const fn entry(&self) -> &Bytes {
        &self.entry
    }

    /// The frames that the recording holds of the entry, in their order.
    /// A storage puts them behind the frames of the entry before, back to back.
    /// A frame that the recording keeps whole is the buffer that the session read from its socket.
    pub fn frames(&self) -> &[Bytes] {
        &self.frames
    }
}

/// What a storage is told when the last holder of a buffer lets go.
type Released = Box<dyn FnOnce() + Send + Sync>;

/// The room for the entries of one batch.
///
/// It starts at an address that is a multiple of 16, it has its size from the start, and it never moves.
/// A storage unpacks the entries of a batch into it, and nothing copies them again:
/// [`Batch::checked`] makes the buffer the batch, and every view reads from it.
///
/// A storage counts what its readers hold. [`when_released`](Self::when_released) tells it when the last holder of
/// the buffer lets go: the batch, and every copy of the batch that a consumer made.
pub struct BatchBuffer {
    /// The bytes that were written, and the zeros behind them that someone asked for as room.
    /// The buffer has its whole room from the start, so it never grows, and it never moves.
    bytes: Aligned,
    written: usize,
    room: usize,
    released: Option<Released>,
}

impl BatchBuffer {
    /// A buffer with room for `bytes` bytes.
    pub fn with_room(bytes: usize) -> BatchBuffer {
        BatchBuffer {
            bytes: Aligned::with_capacity(bytes),
            written: 0,
            room: bytes,
            released: None,
        }
    }

    /// Calls `released` once, when the last holder of the buffer lets go of it.
    #[must_use]
    pub fn when_released(mut self, released: impl FnOnce() + Send + Sync + 'static) -> BatchBuffer {
        self.released = Some(Box::new(released));
        self
    }

    /// The bytes that the buffer has room for.
    pub const fn room(&self) -> usize {
        self.room
    }

    /// What was written so far. A decompressor looks back into it for what a later piece repeats.
    pub fn written(&self) -> &[u8] {
        &self.bytes[..self.written]
    }

    /// The room that is left, as zeros, for whoever writes into the buffer itself.
    /// [`advance`](Self::advance) says how much of it was written.
    ///
    /// Whoever has the bytes already hands them to [`write`](Self::write), which fills no room with zeros first.
    pub fn unwritten(&mut self) -> &mut [u8] {
        self.bytes.resize(self.room, 0);
        &mut self.bytes[self.written..]
    }

    /// Says that the first `bytes` bytes of [`unwritten`](Self::unwritten) were written.
    /// It stops at the end of the room.
    pub fn advance(&mut self, bytes: usize) {
        self.written = self.written.saturating_add(bytes).min(self.room);
        if self.bytes.len() < self.written {
            self.bytes.resize(self.written, 0);
        }
    }

    /// Writes `bytes` behind what was written, as far as the room goes, and says how many bytes it took.
    pub fn write(&mut self, bytes: &[u8]) -> usize {
        let taken = bytes.len().min(self.room - self.written);
        let end = self.written + taken;
        if self.bytes.len() == self.written {
            self.bytes.extend_from_slice(&bytes[..taken]);
        } else {
            // Someone asked for the room as zeros before, so the bytes go where the zeros are.
            let zeros = end.min(self.bytes.len());
            let (over_zeros, behind_them) = bytes[..taken].split_at(zeros - self.written);
            self.bytes[self.written..zeros].copy_from_slice(over_zeros);
            self.bytes.extend_from_slice(behind_them);
        }
        self.written = end;
        taken
    }
}

impl AsRef<[u8]> for BatchBuffer {
    fn as_ref(&self) -> &[u8] {
        self.written()
    }
}

impl Drop for BatchBuffer {
    fn drop(&mut self) {
        if let Some(released) = self.released.take() {
            released();
        }
    }
}

impl fmt::Debug for BatchBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchBuffer")
            .field("room", &self.room())
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

/// The kinds of message that are an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Session,
    Call,
    Journal,
}

/// Where one entry lies in the entries of its batch, and whether it passed the check.
#[derive(Debug)]
struct Checked {
    message: Range<usize>,
    kind: EntryKind,
    frames_at: u64,
    damaged: Option<String>,
}

/// Entries where they lie: what a storage hands out at a time.
///
/// A batch is the buffer that its entries were unpacked into, and the buffer of their frames.
/// It shares both with the storage, and its views borrow from it.
/// Nothing builds the type of an entry before someone asks for the type.
///
/// # A kept view keeps its batch
///
/// A view borrows from its batch, and a frame shares the buffer of its batch.
/// Whoever keeps one keeps the whole buffer in memory: megabytes, for an entry of a kilobyte or two.
/// The storage counts such a buffer against the budget of its reader until the last holder lets go.
/// So a consumer that keeps entries beyond its stream copies them out, and its code says so:
/// [`copy_of`](Self::copy_of) gives one entry and its frames in buffers of their own,
/// and [`CallView::to_call`] gives the type.
/// No consumer keeps a `Batch`, an [`EntryView`] or a [`RecordedFrame`](crate::RecordedFrame) in a field that
/// outlives its stream.
#[derive(Debug, Clone)]
pub struct Batch {
    entries: Bytes,
    frames: Option<Bytes>,
    first: Position,
    checked: Arc<[Checked]>,
    asked: Range<usize>,
    direction: Direction,
}

impl Batch {
    /// Takes the buffers that a storage filled, and checks the entries once.
    ///
    /// `version` is the version of the encoding that the recording says, and `first` is the position of the first
    /// entry. `frames` is the buffer of the frames of the batch, when the reader asked for frames.
    ///
    /// The check passes over the buffer once, from the first head to the last:
    ///
    /// | It checks | So that |
    /// |---|---|
    /// | Every head: its length stays inside the buffer, and its kind is one that an entry has. | The walk from entry to entry never leaves the buffer. |
    /// | Every reference in a message: what it points at lies inside the same message, at an address that the type of the value can stand at. | A view never reads outside its entry. |
    /// | Every length of a text or a list: its values lie inside the message. | The same. |
    /// | Every text: it is UTF-8. | A view hands out a text without a look at its bytes. |
    /// | Every choice: the number that says which case it is, is a case of its type. | A view never meets a case that its type does not have. |
    ///
    /// An entry that fails is left out, and the others are read: it comes out of [`entries`](Self::entries) as its
    /// error, at its position.
    /// A head that fails ends the walk, because nothing says where the next entry starts.
    /// The batch then holds the entries before it, and [`len`](Self::len) says how many those are.
    ///
    /// This is the only way to a view, and nothing makes a batch without the check.
    pub fn checked(version: EncodingVersion, first: Position, entries: BatchBuffer, frames: Option<Bytes>) -> Batch {
        let entries = match version.written() {
            // The current version: its entries are read where they lie.
            Written::V1 => Bytes::from_owner(entries),
        };
        let checked = check(&entries);
        Batch {
            entries,
            frames,
            first,
            asked: 0..checked.len(),
            checked: checked.into(),
            direction: Direction::Forwards,
        }
    }

    /// The same batch without the entries before `position`: for a stream that starts in the middle of a batch.
    #[must_use]
    pub fn starting_at(mut self, position: Position) -> Batch {
        let skipped = position.number().saturating_sub(self.first.number());
        let skipped = usize::try_from(skipped).unwrap_or(usize::MAX);
        self.asked.start = self.asked.start.max(skipped).min(self.asked.end);
        self
    }

    /// The same batch without the entries behind `position`: for a stream backwards that starts in the middle of a
    /// batch.
    #[must_use]
    pub fn ending_at(mut self, position: Position) -> Batch {
        let kept = match position.number().checked_sub(self.first.number()) {
            Some(before) => usize::try_from(before).unwrap_or(usize::MAX).saturating_add(1),
            None => 0,
        };
        self.asked.end = self.asked.end.min(kept).max(self.asked.start);
        self
    }

    /// The same batch for a stream in `direction`: [`entries`](Self::entries) hands them out in that order.
    #[must_use]
    pub const fn towards(mut self, direction: Direction) -> Batch {
        self.direction = direction;
        self
    }

    /// The entries that the reader asked for, in the order of the reading.
    ///
    /// An entry that failed the check comes as its error, in its place.
    /// A view reads a value where it lies: it copies no text, and it allocates nothing.
    pub fn entries(&self) -> impl Iterator<Item = (Position, Result<EntryView<'_>, EntryError>)> {
        let forwards = self.asked.clone();
        let backwards = self.asked.clone().rev();
        let (forwards, backwards) = match self.direction {
            Direction::Forwards => (Some(forwards), None),
            Direction::Backwards => (None, Some(backwards)),
        };
        let indexes = forwards.into_iter().flatten().chain(backwards.into_iter().flatten());
        indexes.map(|index| (self.first.after(index as u64), self.view(&self.checked[index])))
    }

    fn view(&self, entry: &Checked) -> Result<EntryView<'_>, EntryError> {
        if let Some(reason) = &entry.damaged {
            return Err(EntryError::Damaged { reason: reason.clone() });
        }
        let message = &self.entries[entry.message.clone()];
        // SAFETY: `check` passed for the type of this kind on these bytes, which have not moved since:
        // the buffer of a batch never changes, and `damaged` is `None` only for a message that passed.
        Ok(unsafe {
            match entry.kind {
                EntryKind::Session => EntryView::Session(SessionEventView::new(encoding::view(message))),
                EntryKind::Call => EntryView::Call(CallView::new(
                    encoding::view(message),
                    self.frames.as_ref(),
                    entry.frames_at,
                )),
                EntryKind::Journal => EntryView::Journal(JournalEntryView::new(encoding::view(message))),
            }
        })
    }

    /// The position of the first entry of the batch, whether the reader asked for it or not.
    pub const fn first(&self) -> Position {
        self.first
    }

    /// How many entries the check found in the buffer, whether the reader asked for them or not.
    /// A storage that knows how many entries it wrote reports the rest as lost.
    pub fn len(&self) -> usize {
        self.checked.len()
    }

    /// Whether the check found no entry in the buffer.
    pub fn is_empty(&self) -> bool {
        self.checked.is_empty()
    }

    /// The bytes that the batch keeps in memory: the buffer of its entries, and the buffer of its frames.
    pub fn size(&self) -> usize {
        self.entries.len() + self.frames.as_ref().map_or(0, Bytes::len)
    }

    /// One entry and its frames in buffers of their own: a batch of one, which keeps no other buffer in memory.
    ///
    /// This is how a consumer keeps an entry as it is beyond its stream.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the batch has no entry at `position`, or the entry failed the check.
    pub fn copy_of(&self, position: Position) -> Result<Batch, EntryError> {
        let index = position
            .number()
            .checked_sub(self.first.number())
            .and_then(|index| usize::try_from(index).ok());
        let Some(entry) = index.and_then(|index| self.checked.get(index)) else {
            return Err(EntryError::Damaged {
                reason: format!("the batch holds no entry at position {position}"),
            });
        };
        let view = self.view(entry)?;
        let frames = match (&view, &self.frames) {
            (EntryView::Call(call), Some(_)) => Some(Bytes::copy_from_slice(&call.kept_frames()?)),
            _ => None,
        };
        let head = Head {
            len: entry.message.len(),
            kind: match entry.kind {
                EntryKind::Session => Kind::Session,
                EntryKind::Call => Kind::Call,
                EntryKind::Journal => Kind::Journal,
            },
            frames_at: 0,
        };
        let mut copy = BatchBuffer::with_room(head.stride());
        head.write(copy.unwritten());
        copy.advance(Head::LEN);
        copy.write(&self.entries[entry.message.clone()]);
        // The zeros up to the next multiple of 16 are there already.
        copy.advance(head.stride());
        Ok(Batch::checked(EncodingVersion::CURRENT, position, copy, frames))
    }
}

/// Walks the entries of a batch from the first head to the last, and checks every message.
fn check(entries: &[u8]) -> Vec<Checked> {
    let mut checked = Vec::new();
    let mut at = 0;
    while at < entries.len() {
        // A head that fails ends the walk: nothing says where the next entry starts.
        let Ok(head) = Head::read(&entries[at..]) else {
            break;
        };
        let message = at + Head::LEN..at + Head::LEN + head.len;
        let bytes = &entries[message.clone()];
        let (kind, passed) = match head.kind {
            Kind::Session => (EntryKind::Session, encoding::check::<v1::ArchivedSessionEvent>(bytes)),
            Kind::Call => (EntryKind::Call, encoding::check::<v1::ArchivedCall>(bytes)),
            Kind::Journal => (EntryKind::Journal, encoding::check::<v1::ArchivedJournalEntry>(bytes)),
            // A kind that no entry has fails its head, as a kind that no message has does.
            Kind::Description | Kind::Summary | Kind::End => break,
        };
        checked.push(Checked {
            message,
            kind,
            frames_at: head.frames_at,
            damaged: passed.err(),
        });
        at += head.stride();
    }
    checked
}
