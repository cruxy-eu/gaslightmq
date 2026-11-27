//! The backing contract: the one place where the segment layout keeps its bytes, and the errors of it.

use std::fmt;

use bytes::Bytes;
use gaslightmq_recording::{Position, RecordingId};

/// The two kinds of object that grow: data segments and index chunks.
///
/// The manifest is the third kind of object, and it is replaced as a whole, so it has operations of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectKind {
    /// A data segment: the blocks of a recording, and its footer once it is sealed.
    Segment,
    /// An index chunk: the records that describe a series of segments.
    IndexChunk,
}

impl fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            ObjectKind::Segment => "segment",
            ObjectKind::IndexChunk => "index chunk",
        })
    }
}

/// One object that grows, named by its kind and by the position of the first entry that it holds or describes.
///
/// ```
/// use gaslightmq_recording::Position;
/// use gaslightmq_recording_segments::prelude::*;
///
/// let segment = Object::segment(Position::new(8_912_345));
/// assert_eq!(segment.kind, ObjectKind::Segment);
/// assert_eq!(segment.to_string(), "segment 8912345");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Object {
    /// A segment or an index chunk.
    pub kind: ObjectKind,
    /// The position of the first entry that the object holds, or that it describes.
    pub first: Position,
}

impl Object {
    /// The data segment whose first entry has the position `first`.
    pub const fn segment(first: Position) -> Object {
        Object {
            kind: ObjectKind::Segment,
            first,
        }
    }

    /// The index chunk that describes the segments from the position `first` on.
    pub const fn index_chunk(first: Position) -> Object {
        Object {
            kind: ObjectKind::IndexChunk,
            first,
        }
    }
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind, self.first)
    }
}

/// An object that a backing lists: its name, how long it is, and whether it is sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Listed {
    /// The position of the first entry that the object holds or describes, which is its name.
    pub first: Position,
    /// The bytes of the object.
    pub length: u64,
    /// Whether the object is sealed: it never changes again, and takes no append.
    pub sealed: bool,
}

/// Where the objects of recordings live. The segment layout works over any of them.
///
/// | Operation | Whole or not at all | When it returns |
/// |---|---|---|
/// | [Create](Self::create) a segment or an index chunk | Yes. It refuses one that is there. | The object is there, empty, and stays after a crash. |
/// | [Append](Self::append) | No. A crash or a failure can leave the first part of the pieces, and nothing else. It never changes a byte that was there. | Every byte is there after a crash of the process or of the machine. The pieces stand in the object in their order, as one stretch. |
/// | [Seal](Self::seal) | Yes. | The object never changes again, and takes no append. |
/// | [List](Self::list) | — | Every object of a kind from a position on, in order, each with its length and whether it is sealed. It lists what is there after a crash. |
/// | [Read](Self::read) | — | A stream of the bytes from an offset on. |
/// | [Put the manifest](Self::put_manifest) | Yes. It makes the manifest or replaces it, and a reader gets the old one or the new one. | The new manifest is there after a crash. |
/// | [Read the manifest](Self::manifest) | — | The manifest, whole. |
///
/// What a write returned is durable: the layout describes an object only once the backing has it for good.
/// An append may tear, and nothing else may: the layout frames what it appends, and finds the end of a torn object
/// by itself. A backing knows no entry and no codec: it keeps bytes under names, and never looks into them.
///
/// The trait is a signature, and it names no runtime: a backing waits however it likes.
/// An implementation can write each of its async methods as an `async fn`.
pub trait SegmentBacking: Send + Sync {
    /// A stream of the bytes of one object, from an offset on.
    type Stream: ByteStream;

    /// Every recording that has an object here, in the order of their IDs.
    fn recordings(&self) -> impl Future<Output = Result<Vec<RecordingId>, ListingError>> + Send;

    /// Creates an empty object. The recording has an object here from then on.
    ///
    /// It refuses an object that is there with [`StoreError::AlreadyThere`], sealed or not.
    fn create(&self, recording: &RecordingId, object: Object) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Puts `pieces` behind the bytes of `object`, in their order, as one stretch.
    ///
    /// A crash or a failure can leave the first part of the pieces, and nothing else.
    fn append(
        &self,
        recording: &RecordingId,
        object: Object,
        pieces: &[Bytes],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Seals `object`: it never changes again, and takes no append.
    fn seal(&self, recording: &RecordingId, object: Object) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Every object of `kind` whose first position is `from` or later, in the order of their positions.
    fn list(
        &self,
        recording: &RecordingId,
        kind: ObjectKind,
        from: Position,
    ) -> impl Future<Output = Result<Vec<Listed>, ListingError>> + Send;

    /// The bytes of `object` from the offset `from` on, as a stream.
    fn read(
        &self,
        recording: &RecordingId,
        object: Object,
        from: u64,
    ) -> impl Future<Output = Result<Self::Stream, FetchError>> + Send;

    /// Makes the manifest of the recording, or replaces it, in one step.
    fn put_manifest(
        &self,
        recording: &RecordingId,
        manifest: Bytes,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The manifest of the recording, whole.
    fn manifest(&self, recording: &RecordingId) -> impl Future<Output = Result<Bytes, FetchError>> + Send;
}

/// A place in an object that moves forward.
///
/// The layout always knows how much it wants next, so it asks for that many bytes, and a chunk comes as one buffer.
/// A piece is a shared buffer: a range of a mapped file, or what a backing fetched.
pub trait ByteStream: Send {
    /// The next `wanted` bytes as one buffer, fewer at the end of the object, and `None` behind it.
    fn next(&mut self, wanted: usize) -> impl Future<Output = Result<Option<Bytes>, FetchError>> + Send;
}

/// Why a backing did not create, append to, seal or put an object.
///
/// The layout ends the recording with either.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The object exists already.
    #[error("the object is there already")]
    AlreadyThere,
    /// The backing did not keep the bytes.
    #[error("the object was not stored: {reason}")]
    NotStored {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a backing did not list its recordings or the objects of one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListingError {
    /// The backing did not answer.
    #[error("the objects were not listed: {reason}")]
    NotListed {
        /// What stood in the way.
        reason: String,
    },
}

/// Why a backing did not hand out the bytes of an object.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// The backing has no such object. The layout rebuilds a description from below,
    /// or reports the positions of a segment as lost.
    #[error("the object is missing")]
    Missing,
    /// The backing did not answer, and the reading can be tried again.
    #[error("the object was not fetched: {reason}")]
    NotFetched {
        /// What stood in the way.
        reason: String,
    },
}
