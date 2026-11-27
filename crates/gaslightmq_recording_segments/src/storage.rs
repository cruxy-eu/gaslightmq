//! The segment layout as a storage of recordings: the one part of the crate that awaits, and what it awaits is the
//! backing it is handed.
//!
//! Everything that decides is a plain function over bytes: the [order of writing](crate::writing), the
//! [walks](crate::walk) that read what was written, and the [budgets](crate::budget). This module hands bytes to the
//! backing and takes bytes from it, and turns the errors of the backing into those of a recording.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use gaslightmq_recording::prelude::*;

use crate::backing::{ByteStream, FetchError, ListingError, Object, ObjectKind, SegmentBacking, StoreError};
use crate::budget::{ReadersMemory, Spent, StreamMemory};
use crate::codec::{Codec, Decompressor};
use crate::configuration::SegmentConfiguration;
use crate::framing::{
    BlockHeader, BlockRow, ChunkHeader, LAYOUT_VERSION, Manifest, Trailer, Versions, checksum, padding,
};
use crate::walk::{FooterWalk, IndexWalk, SegmentOutline, SegmentWalk, Walk};
use crate::writing::{Step, Writing};

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The segment layout over a backing: a storage of recordings.
///
/// | Object | Holds | Changes |
/// |---|---|---|
/// | Data segment | The blocks of the recording, one after the other, and when it is sealed a footer that sums them up. | It grows with every commit, until it is sealed. A sealed segment never changes. |
/// | Index chunk | For a series of segments: the summary of each one, the summary of each of its spans of blocks, and where each block is, with the positions it covers. | It grows by one item with every segment that is sealed, until it is full. A full index chunk never changes. |
/// | Manifest | The description of the recording, the list of its index chunks, each with what it covers, and the end of the recording. | It is replaced as a whole when an index chunk begins, and when the recording ends. |
///
/// The segments are the recording. An index chunk and the manifest hold nothing that the segments do not say,
/// so a reader rebuilds either of them from the segments, and nothing is lost when one is missing.
///
/// A clone is the same storage, and its readers share one budget: whoever puts a process together makes one storage.
/// The backing in memory, `MemoryBacking` behind the feature `memory`, shows it at work.
#[derive(Debug)]
pub struct SegmentStorage<B> {
    backing: Arc<B>,
    configuration: SegmentConfiguration,
    readers: ReadersMemory,
}

impl<B> Clone for SegmentStorage<B> {
    fn clone(&self) -> Self {
        SegmentStorage {
            backing: Arc::clone(&self.backing),
            configuration: self.configuration,
            readers: self.readers.clone(),
        }
    }
}

impl<B: SegmentBacking> SegmentStorage<B> {
    /// The segment layout over `backing`, with the properties of `configuration`.
    pub fn new(backing: B, configuration: SegmentConfiguration) -> SegmentStorage<B> {
        SegmentStorage {
            backing: Arc::new(backing),
            readers: ReadersMemory::new(configuration.all_readers_memory),
            configuration,
        }
    }
}

impl<B: SegmentBacking + 'static> RecordingStorage for SegmentStorage<B> {
    type Writer = SegmentWriter<B>;
    type Reader = SegmentReader<B>;

    async fn start(&self, description: Description) -> Result<SegmentWriter<B>, StartError> {
        let mut writing = Writing::start(&description, self.configuration, Codec::None);
        let mut writer = SegmentWriter {
            backing: Arc::clone(&self.backing),
            recording: description.id(),
            writing: None,
            failed: None,
        };
        // The first step creates the first segment, which refuses a recording that is there.
        let first = writing.commit();
        match writer.run(first).await {
            Ok(()) => {}
            Err(Failure {
                error: StoreError::AlreadyThere,
                ..
            }) => return Err(StartError::AlreadyThere),
            Err(failure) => {
                return Err(StartError::NotStarted {
                    reason: failure.reason(),
                });
            }
        }
        writer.writing = Some(writing);
        Ok(writer)
    }

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListError> {
        let mut recordings = self.backing.recordings().await.map_err(|error| match error {
            ListingError::NotListed { reason } => ListError::NotListed { reason },
        })?;
        recordings.sort();
        Ok(recordings)
    }

    async fn open(&self, recording: &RecordingId) -> Result<SegmentReader<B>, OpenError> {
        let opened = Opened::of(&*self.backing, recording).await?;
        let mut known = Known::new(&opened)?;
        known
            .refresh(&*self.backing, recording, opened.versions)
            .await
            .map_err(|trouble| OpenError::NotReadable {
                reason: trouble.reason(),
            })?;
        Ok(SegmentReader {
            shared: Arc::new(Shared {
                backing: Arc::clone(&self.backing),
                recording: recording.clone(),
                versions: opened.versions,
                version: opened.version,
                description: opened.description,
                configuration: self.configuration,
                readers: self.readers.clone(),
                known: Mutex::new(known),
            }),
        })
    }
}

/// A step that the backing did not do: which one, and what the backing said.
struct Failure {
    step: String,
    error: StoreError,
}

impl Failure {
    fn reason(&self) -> String {
        format!("{}: {}", self.step, self.error)
    }
}

/// What writes one recording of a [`SegmentStorage`].
///
/// It holds what the codec gave back since the last commit, the summary of the span that grows, and the records of
/// the spans of the segment that grows. A write that fails ends the recording: the writer takes nothing more.
#[derive(Debug)]
pub struct SegmentWriter<B> {
    backing: Arc<B>,
    recording: RecordingId,
    writing: Option<Writing>,
    failed: Option<String>,
}

impl<B: SegmentBacking> SegmentWriter<B> {
    /// Hands every step to the backing, in order, and stops at the first that fails.
    async fn run(&mut self, steps: Vec<Step>) -> Result<(), Failure> {
        let recording = &self.recording;
        for step in steps {
            let (done, what) = match step {
                Step::Create(object) => (
                    self.backing.create(recording, object).await,
                    format!("creating the {object}"),
                ),
                Step::Append(object, pieces) => (
                    self.backing.append(recording, object, &pieces).await,
                    format!("appending to the {object}"),
                ),
                Step::Seal(object) => (
                    self.backing.seal(recording, object).await,
                    format!("sealing the {object}"),
                ),
                Step::PutManifest(manifest) => (
                    self.backing.put_manifest(recording, manifest).await,
                    "putting the manifest".to_owned(),
                ),
            };
            if let Err(error) = done {
                let failure = Failure { step: what, error };
                self.failed = Some(failure.reason());
                return Err(failure);
            }
        }
        Ok(())
    }

    fn not_written(&self) -> WriteError {
        WriteError::NotWritten {
            reason: self.failed.clone().unwrap_or_else(|| "the recording ended".to_owned()),
        }
    }
}

impl<B: SegmentBacking + 'static> RecordingWriter for SegmentWriter<B> {
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the contract is async, and taking an entry waits for nothing: the commit hands it to the backing"
    )]
    async fn append(&mut self, entry: Entry) -> Result<Position, WriteError> {
        match (&self.failed, &mut self.writing) {
            (None, Some(writing)) => Ok(writing.append(entry)),
            _ => Err(self.not_written()),
        }
    }

    async fn commit(&mut self) -> Result<(), WriteError> {
        let steps = match (&self.failed, &mut self.writing) {
            (None, Some(writing)) => writing.commit(),
            _ => return Err(self.not_written()),
        };
        self.run(steps).await.map_err(|failure| WriteError::NotWritten {
            reason: failure.reason(),
        })
    }

    async fn finish(mut self, end: End) -> Result<(), WriteError> {
        let steps = match (&self.failed, self.writing.take()) {
            (None, Some(writing)) => writing.finish(&end),
            _ => return Err(self.not_written()),
        };
        self.run(steps).await.map_err(|failure| WriteError::NotWritten {
            reason: failure.reason(),
        })
    }
}

/// Why the layout could not read what it needed: the backing did not answer, or what is there is no recording.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Trouble {
    NotFetched(String),
    Unreadable(String),
}

impl Trouble {
    fn reason(&self) -> String {
        match self {
            Trouble::NotFetched(reason) | Trouble::Unreadable(reason) => reason.clone(),
        }
    }

    fn of_fetch(object: &str, error: &FetchError) -> Trouble {
        Trouble::NotFetched(format!("the {object} was not read: {error}"))
    }

    fn of_listing(error: &ListingError) -> Trouble {
        Trouble::NotFetched(error.to_string())
    }
}

/// Checks the versions that an object says against those that this build reads.
fn version_of(versions: Versions) -> Result<EncodingVersion, OpenError> {
    if versions.layout > LAYOUT_VERSION {
        return Err(OpenError::LayoutNotRead {
            written: versions.layout,
            known: LAYOUT_VERSION,
        });
    }
    if versions.layout == 0 {
        return Err(OpenError::NotReadable {
            reason: "the versions of the layout of a recording count from 1".to_owned(),
        });
    }
    EncodingVersion::read_by_this_build(versions.encoding).map_err(|error| match error {
        EncodingVersionError::Zero => OpenError::NotReadable {
            reason: error.to_string(),
        },
        EncodingVersionError::Newer { .. } => OpenError::EncodingNotRead { error },
    })
}

/// Every object of a recording says the same versions: one writer wrote it, with one build.
fn same_versions(recording: Versions, object: Versions, what: &str) -> Result<(), Trouble> {
    if recording != object {
        return Err(Trouble::Unreadable(format!(
            "the {what} says version {} of the layout and {} of the encoding, and the recording {} and {}",
            object.layout, object.encoding, recording.layout, recording.encoding
        )));
    }
    Ok(())
}

/// Feeds a walk the bytes of `object` from where it stands, until it is over or the bytes end.
async fn drive<B: SegmentBacking, W: Walk>(
    backing: &B,
    recording: &RecordingId,
    object: Object,
    walk: &mut W,
) -> Result<(), FetchError> {
    let mut stream = backing.read(recording, object, walk.offset()).await?;
    while let Some(wanted) = walk.wanted() {
        let bytes = exactly(&mut stream, wanted).await?;
        let ended = bytes.len() < wanted;
        walk.take(bytes);
        if ended || walk.stopped() {
            break;
        }
    }
    Ok(())
}

/// The next `wanted` bytes of a stream, or fewer when it ends before them.
async fn exactly<S: ByteStream>(stream: &mut S, wanted: usize) -> Result<Bytes, FetchError> {
    if wanted == 0 {
        return Ok(Bytes::new());
    }
    Ok(stream.next(wanted).await?.unwrap_or_default())
}

/// The outline of the sealed segment `first`, from its footer. `None` when the segment or its footer is lost.
async fn footer_of<B: SegmentBacking>(
    backing: &B,
    recording: &RecordingId,
    first: Position,
    footer: FooterAt,
) -> Result<Option<SegmentOutline>, FetchError> {
    let object = Object::segment(first);
    let footer_at = match footer {
        FooterAt::Offset(offset) => offset,
        FooterAt::Trailer(length) => {
            let Some(trailer_at) = length.checked_sub(Trailer::LEN as u64) else {
                return Ok(None);
            };
            let mut stream = match backing.read(recording, object, trailer_at).await {
                Ok(stream) => stream,
                Err(FetchError::Missing) => return Ok(None),
                Err(error) => return Err(error),
            };
            let trailer = exactly(&mut stream, Trailer::LEN).await?;
            let footer_at = Trailer::decode(&trailer)
                .ok()
                .and_then(|trailer| trailer_at.checked_sub(trailer.footer));
            match footer_at {
                Some(footer_at) => footer_at,
                None => return Ok(None),
            }
        }
    };
    let mut walk = FooterWalk::new(footer_at);
    match drive(backing, recording, object, &mut walk).await {
        Ok(()) => {}
        Err(FetchError::Missing) => return Ok(None),
        Err(error) => return Err(error),
    }
    Ok(walk.outline().ok().filter(|outline| outline.first == first))
}

/// What a reader takes from the top of a recording when it opens it: the manifest, or the header of a segment.
struct Opened {
    versions: Versions,
    version: EncodingVersion,
    description: Description,
    manifest: Option<Manifest>,
}

impl Opened {
    async fn of<B: SegmentBacking>(backing: &B, recording: &RecordingId) -> Result<Opened, OpenError> {
        let not_readable = |reason: String| OpenError::NotReadable { reason };
        let manifest = match backing.manifest(recording).await {
            Ok(bytes) => Manifest::decode(&bytes).ok(),
            Err(FetchError::Missing) => None,
            Err(error) => return Err(not_readable(Trouble::of_fetch("manifest", &error).reason())),
        };
        let (versions, description) = match &manifest {
            Some(manifest) => (manifest.versions, manifest.description.clone()),
            None => Self::from_the_first_segment(backing, recording).await?,
        };
        let version = version_of(versions)?;
        let description =
            Description::decode(version, &description).map_err(|error| not_readable(error.to_string()))?;
        Ok(Opened {
            versions,
            version,
            description,
            manifest,
        })
    }

    /// Without a manifest, the first segment says what the recording is: the versions and the description.
    async fn from_the_first_segment<B: SegmentBacking>(
        backing: &B,
        recording: &RecordingId,
    ) -> Result<(Versions, Bytes), OpenError> {
        let not_readable = |reason: String| OpenError::NotReadable { reason };
        let listed = backing
            .list(recording, ObjectKind::Segment, Position::FIRST)
            .await
            .map_err(|error| not_readable(Trouble::of_listing(&error).reason()))?;
        let Some(first) = listed.first() else {
            let chunks = backing
                .list(recording, ObjectKind::IndexChunk, Position::FIRST)
                .await
                .map_err(|error| not_readable(Trouble::of_listing(&error).reason()))?;
            if chunks.is_empty() {
                return Err(OpenError::NotFound);
            }
            return Err(not_readable("no segment says what the recording is".to_owned()));
        };
        let mut walk = SegmentWalk::new(first.first);
        drive(backing, recording, Object::segment(first.first), &mut walk)
            .await
            .map_err(|error| not_readable(Trouble::of_fetch("first segment", &error).reason()))?;
        let (Some(versions), Some(description)) = (walk.versions(), walk.description()) else {
            if let Some(versions) = walk.versions() {
                version_of(versions)?;
            }
            let flaw = walk.flaw().unwrap_or("its header is not whole");
            return Err(not_readable(format!("no segment says what the recording is: {flaw}")));
        };
        Ok((versions, description.clone()))
    }
}

/// An index chunk as a reader knows it.
#[derive(Debug, Clone)]
struct KnownChunk {
    first: Position,
    /// What a full chunk covers, as the manifest says it.
    summary: Option<Summary>,
    /// The position behind the last entry that the chunk describes, as far as it was read.
    described: Position,
    /// The walk over a chunk that the manifest does not say is full: it grows, or the manifest is behind.
    walk: Option<IndexWalk>,
}

/// A segment that no index chunk describes: one while a recording runs, two at most after a crash.
#[derive(Debug, Clone)]
struct TailSegment {
    outline: SegmentOutline,
    /// The walk over the segment, while it has no footer.
    walk: Option<SegmentWalk>,
    /// The bytes of the segment behind its last whole chunk.
    unread: u64,
}

impl TailSegment {
    /// A segment that the reader walks from its start.
    const fn walked(first: Position) -> TailSegment {
        TailSegment {
            outline: SegmentOutline {
                first,
                entries: 0,
                spans: Vec::new(),
                end: None,
                sealed: false,
            },
            walk: Some(SegmentWalk::new(first)),
            unread: 0,
        }
    }
}

/// What a reader knows of the levels of a recording: the index chunks, how far they describe it, and the segments
/// behind them. A stream takes a copy, and learns of later commits on its own.
#[derive(Debug, Clone)]
struct Known {
    chunks: Vec<KnownChunk>,
    described: Position,
    tail: Vec<TailSegment>,
    end: Option<Bytes>,
    version: EncodingVersion,
}

impl Known {
    fn new(opened: &Opened) -> Result<Known, OpenError> {
        let mut chunks = Vec::new();
        let mut end = None;
        if let Some(manifest) = &opened.manifest {
            for record in &manifest.chunks {
                let summary = match &record.summary {
                    Some(summary) => {
                        Some(
                            Summary::decode(opened.version, summary).map_err(|error| OpenError::NotReadable {
                                reason: error.to_string(),
                            })?,
                        )
                    }
                    None => None,
                };
                let full = summary.is_some();
                chunks.push(KnownChunk {
                    first: record.first,
                    described: record.first.after(record.entries),
                    walk: (!full).then(IndexWalk::new),
                    summary,
                });
            }
            end.clone_from(&manifest.end);
        }
        Ok(Known {
            described: chunks.last().map_or(Position::FIRST, |chunk| chunk.described),
            chunks,
            tail: Vec::new(),
            end,
            version: opened.version,
        })
    }

    /// How far the recording goes.
    fn entries(&self) -> u64 {
        let tail = self
            .tail
            .last()
            .map_or(Position::FIRST, |segment| segment.outline.end_position());
        self.described.max(tail).number()
    }

    fn extent(&self) -> Result<Extent, Trouble> {
        let end = match &self.end {
            Some(end) => Some(End::decode(self.version, end).map_err(|error| Trouble::Unreadable(error.to_string()))?),
            None => None,
        };
        Ok(Extent {
            entries: self.entries(),
            end,
        })
    }

    /// The bytes behind the last whole chunk of the segments behind the index.
    fn unread(&self) -> u64 {
        self.tail.iter().map(|segment| segment.unread).sum()
    }

    /// Learns what the index chunks and the segments say from where the reader knew them last:
    /// the index chunks that the manifest names and those that it does not name yet, and then the segments that no
    /// item describes, each walked from where the walk stood.
    async fn refresh<B: SegmentBacking>(
        &mut self,
        backing: &B,
        recording: &RecordingId,
        versions: Versions,
    ) -> Result<(), Trouble> {
        let from = self.chunks.last().map_or(Position::FIRST, |chunk| chunk.first);
        let listed = backing
            .list(recording, ObjectKind::IndexChunk, from)
            .await
            .map_err(|error| Trouble::of_listing(&error))?;
        for chunk in listed {
            if !self.chunks.iter().any(|known| known.first == chunk.first) {
                self.chunks.push(KnownChunk {
                    first: chunk.first,
                    summary: None,
                    described: chunk.first,
                    walk: Some(IndexWalk::new()),
                });
            }
        }
        for chunk in &mut self.chunks {
            let Some(walk) = &mut chunk.walk else {
                continue;
            };
            match drive(backing, recording, Object::index_chunk(chunk.first), walk).await {
                Ok(()) | Err(FetchError::Missing) => {}
                Err(error) => return Err(Trouble::of_fetch("index chunk", &error)),
            }
            if let Some(read) = walk.versions() {
                same_versions(versions, read, "index chunk")?;
            }
            walk.items().for_each(drop);
            if let Some(described) = walk.described() {
                chunk.described = chunk.described.max(described);
            }
            if let Some(end) = walk.end() {
                self.end = Some(end.clone());
            }
        }
        self.described = self
            .chunks
            .iter()
            .map(|chunk| chunk.described)
            .max()
            .unwrap_or(Position::FIRST);

        let described = self.described;
        self.tail.retain(|segment| segment.outline.first >= described);
        let listed = backing
            .list(recording, ObjectKind::Segment, described)
            .await
            .map_err(|error| Trouble::of_listing(&error))?;
        for segment in listed {
            let known = self.tail.iter().position(|tail| tail.outline.first == segment.first);
            let mut tail = match known {
                Some(index) => self.tail.remove(index),
                None if segment.sealed => {
                    let footer = footer_of(backing, recording, segment.first, FooterAt::Trailer(segment.length))
                        .await
                        .map_err(|error| Trouble::of_fetch("segment", &error))?;
                    match footer {
                        Some(outline) => TailSegment {
                            outline,
                            walk: None,
                            unread: 0,
                        },
                        None => TailSegment::walked(segment.first),
                    }
                }
                None => TailSegment::walked(segment.first),
            };
            if let Some(walk) = &mut tail.walk {
                match drive(backing, recording, Object::segment(segment.first), walk).await {
                    Ok(()) | Err(FetchError::Missing) => {}
                    Err(error) => return Err(Trouble::of_fetch("segment", &error)),
                }
                if let Some(read) = walk.versions() {
                    same_versions(versions, read, "segment")?;
                }
                tail.outline = walk.outline();
                tail.unread = segment.length.saturating_sub(walk.offset());
                if tail.outline.sealed {
                    tail.walk = None;
                    tail.unread = 0;
                }
            }
            if let Some(end) = &tail.outline.end {
                self.end = Some(end.clone());
            }
            self.tail.push(tail);
        }
        self.tail.sort_by_key(|segment| segment.outline.first);
        Ok(())
    }
}

/// What the reader and its streams share.
#[derive(Debug)]
struct Shared<B> {
    backing: Arc<B>,
    recording: RecordingId,
    versions: Versions,
    version: EncodingVersion,
    description: Description,
    configuration: SegmentConfiguration,
    readers: ReadersMemory,
    known: Mutex<Known>,
}

/// What reads one recording of a [`SegmentStorage`].
///
/// It holds the manifest while the recording is open, and what it knows of the segments that no index chunk
/// describes yet. It changes nothing in the backing.
#[derive(Debug)]
pub struct SegmentReader<B> {
    shared: Arc<Shared<B>>,
}

impl<B: SegmentBacking> SegmentReader<B> {
    /// The bytes of the recording behind its last whole chunk, which the reader left unread.
    ///
    /// They are a commit that is still written, or what a crash left half written: a recording with such bytes and
    /// without an end was cut short.
    pub fn bytes_left_unread(&self) -> u64 {
        locked(&self.shared.known).unread()
    }

    async fn refreshed(&self) -> Result<Known, Trouble> {
        let shared = &self.shared;
        let mut known = locked(&shared.known).clone();
        known
            .refresh(&*shared.backing, &shared.recording, shared.versions)
            .await?;
        let mut kept = locked(&shared.known);
        if known.entries() >= kept.entries() {
            *kept = known.clone();
        }
        Ok(known)
    }
}

impl<B: SegmentBacking + 'static> RecordingReader for SegmentReader<B> {
    type Entries = SegmentEntries<B>;

    fn description(&self) -> &Description {
        &self.shared.description
    }

    async fn extent(&self) -> Result<Extent, ExtentError> {
        let not_read = |trouble: Trouble| ExtentError::NotRead {
            reason: trouble.reason(),
        };
        self.refreshed().await.map_err(not_read)?.extent().map_err(not_read)
    }

    async fn read(
        &self,
        from: Place,
        direction: Direction,
        wanted: impl Wanted,
        frames: Frames,
    ) -> Result<SegmentEntries<B>, ReadError> {
        let shared = &self.shared;
        let configuration = shared.configuration;
        let memory = shared
            .readers
            .stream(configuration.block_size, configuration.reader_memory)
            .map_err(|Spent { held, budget }| ReadError::AllReadersBudgetSpent { held, budget })?;
        let known = self.refreshed().await.map_err(|trouble| ReadError::NotRead {
            reason: trouble.reason(),
        })?;
        let last = known.entries().checked_sub(1).map(Position::new);
        let next = match (from, direction) {
            (Place::Start, _) => Some(Position::FIRST),
            (Place::At(position), _) => Some(position),
            (Place::End, Direction::Forwards) => Some(last.unwrap_or(Position::FIRST)),
            (Place::End, Direction::Backwards) => last,
        };
        Ok(SegmentEntries {
            shared: Arc::clone(shared),
            known,
            wanted: Box::new(wanted),
            frames,
            direction,
            next,
            index: None,
            blocks: VecDeque::new(),
            damaged: None,
            memory,
        })
    }
}

/// Where the footer of a segment that an index chunk names stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FooterAt {
    /// The record of the segment says it.
    Offset(u64),
    /// Nothing says it but the trailer, at the end of a segment of this length.
    Trailer(u64),
}

/// A segment that a stream may read, as an index chunk or the listing of the backing names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentRef {
    first: Position,
    /// The position behind its last entry.
    end: Position,
    footer: FooterAt,
}

/// A block that a stream reads next: its segment, and its row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Located {
    segment: Position,
    row: BlockRow,
}

/// The index chunk that a stream walks, and the segments of it that are left to read.
#[derive(Debug)]
struct InChunk {
    chunk: usize,
    segments: VecDeque<SegmentRef>,
}

/// A stream of the entries of one recording of a [`SegmentStorage`].
///
/// It works from the top down: the index chunks whose summary can hold what is wanted, then the segments of each,
/// then the spans of each, and a block at a time of those. Behind the index it reads the segments that no item
/// describes yet, and when it reaches their end it looks again, so it hands out what a later commit added.
pub struct SegmentEntries<B> {
    shared: Arc<Shared<B>>,
    known: Known,
    wanted: Box<dyn Wanted>,
    frames: Frames,
    direction: Direction,
    /// The position of the next entry that the stream hands out, or nothing behind the first entry of a stream
    /// backwards.
    next: Option<Position>,
    index: Option<InChunk>,
    /// The blocks of one segment that are wanted and left, in the order of the reading.
    blocks: VecDeque<Located>,
    /// Positions that the stream lost, which it reports with the next call.
    damaged: Option<(Position, Position)>,
    memory: StreamMemory,
}

impl<B> std::fmt::Debug for SegmentEntries<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentEntries")
            .field("next", &self.next)
            .field("direction", &self.direction)
            .field("frames", &self.frames)
            .finish_non_exhaustive()
    }
}

/// What reading one block gave: the batch of its entries that are whole, and the positions that it lost.
struct BlockRead {
    batch: Option<Batch>,
    lost: Option<(Position, Position)>,
}

impl<B: SegmentBacking + 'static> SegmentEntries<B> {
    const fn forwards(&self) -> bool {
        matches!(self.direction, Direction::Forwards)
    }

    /// Whether a stretch that `summary` sums up can hold what the stream wants. A summary that does not read
    /// is read through: a stream hands out no less than what is wanted.
    fn may_be_in(&self, summary: Option<&Bytes>) -> bool {
        let Some(summary) = summary else {
            return true;
        };
        Summary::decode(self.shared.version, summary).map_or(true, |summary| self.wanted.may_be_in(&summary))
    }

    /// The rows of `outline` that the stream reads, in the order of the reading, from `next` on.
    fn blocks_of(&self, outline: &SegmentOutline, next: Position) -> VecDeque<Located> {
        let spans = outline
            .spans
            .iter()
            .filter(|span| self.may_be_in(span.summary.as_ref()));
        let rows = spans.flat_map(|span| span.rows.iter().copied());
        let forwards = self.forwards();
        let rows = rows.filter(|row| row.entries > 0 && if forwards { row.end() > next } else { row.first <= next });
        let mut located: VecDeque<Located> = rows
            .map(|row| Located {
                segment: outline.first,
                row,
            })
            .collect();
        if !forwards {
            located.make_contiguous().reverse();
        }
        located
    }

    /// Finds the next blocks to read from where the stream stands. `true` when the stream should look again:
    /// it found blocks, moved on to another level of the recording, or lost positions that it reports.
    /// `false` when there is nothing to read now.
    async fn fill(&mut self, looked_again: &mut bool) -> Result<bool, NextBatchError> {
        let Some(next) = self.next else {
            return Ok(false);
        };
        if next < self.known.described {
            return self.indexed(next).await;
        }
        self.index = None;
        let segments: Vec<&TailSegment> = if self.forwards() {
            self.known.tail.iter().collect()
        } else {
            self.known.tail.iter().rev().collect()
        };
        let blocks: VecDeque<Located> = segments
            .into_iter()
            .flat_map(|segment| self.blocks_of(&segment.outline, next))
            .collect();
        if !blocks.is_empty() {
            self.blocks = blocks;
            return Ok(true);
        }
        if self.forwards() {
            // At the end of what the stream knows: a later commit may have added more. It looks once a call.
            if *looked_again {
                return Ok(false);
            }
            *looked_again = true;
            let shared = &self.shared;
            self.known
                .refresh(&*shared.backing, &shared.recording, shared.versions)
                .await
                .map_err(|trouble| NextBatchError::NotRead {
                    reason: trouble.reason(),
                })?;
            return Ok(true);
        }
        // Backwards, the stream goes on in the segments that the index describes.
        self.next = self
            .known
            .described
            .number()
            .checked_sub(1)
            .map(|last| Position::new(last).min(next));
        Ok(self.next.is_some())
    }

    /// Finds the next blocks to read in the segments that the index describes. When there are none from `next` on,
    /// the stream moves on to where they are: behind the index forwards, and over backwards.
    async fn indexed(&mut self, next: Position) -> Result<bool, NextBatchError> {
        let forwards = self.forwards();
        loop {
            if self.index.is_none() {
                let containing = self.known.chunks.iter().rposition(|chunk| chunk.first <= next);
                let Some(chunk) = containing else {
                    // No index chunk starts before `next`: forwards the stream goes on at the first one,
                    // and backwards there is nothing before it.
                    self.next = match self.known.chunks.first() {
                        Some(first) if forwards => Some(first.first),
                        _ => None,
                    };
                    return Ok(self.next.is_some());
                };
                let segments = self.segments_of(chunk, next).await?;
                self.index = Some(InChunk { chunk, segments });
            }
            let Some(in_chunk) = &mut self.index else {
                continue;
            };
            let segment = if forwards {
                in_chunk.segments.front()
            } else {
                in_chunk.segments.back()
            };
            let Some(&segment) = segment else {
                // The chunk has nothing more: on to the next one in the direction of the reading.
                let chunk = in_chunk.chunk;
                self.index = None;
                self.next = if forwards {
                    let until = self
                        .known
                        .chunks
                        .get(chunk + 1)
                        .map_or(self.known.described, |later| later.first);
                    Some(until.max(next))
                } else {
                    let first = self.known.chunks[chunk].first.number();
                    first.checked_sub(1).map(|before| Position::new(before).min(next))
                };
                return Ok(self.next.is_some());
            };
            // A segment that did not read stays, so the stream reads it again when it is asked again.
            let outline = self.outline_of(segment).await?;
            if let Some(in_chunk) = &mut self.index {
                if forwards {
                    in_chunk.segments.pop_front();
                } else {
                    in_chunk.segments.pop_back();
                }
            }
            if let Some(outline) = outline {
                let blocks = self.blocks_of(&outline, next);
                if !blocks.is_empty() {
                    self.blocks = blocks;
                    return Ok(true);
                }
                continue;
            }
            // The segment is lost: its positions are, as far as the stream reads them.
            let last_of_segment = Position::new(segment.end.number().saturating_sub(1));
            let lost = if forwards {
                (segment.first.max(next), last_of_segment)
            } else {
                (segment.first, last_of_segment.min(next))
            };
            if lost.0 <= lost.1 {
                self.damaged = Some(lost);
                self.next = if forwards {
                    Some(segment.end)
                } else {
                    segment.first.number().checked_sub(1).map(Position::new)
                };
                return Ok(true);
            }
        }
    }

    /// The segments of an index chunk that the stream may read from `next` on, in the order of their positions.
    ///
    /// The index chunk gives the record of every segment whose item is whole. What no whole item describes,
    /// up to the next chunk, the listing of the backing gives, and their footers say the rest.
    async fn segments_of(&mut self, chunk: usize, next: Position) -> Result<VecDeque<SegmentRef>, NextBatchError> {
        let shared = Arc::clone(&self.shared);
        let not_read = |trouble: Trouble| NextBatchError::NotRead {
            reason: trouble.reason(),
        };
        let known = &self.known.chunks[chunk];
        let first = known.first;
        let until = self
            .known
            .chunks
            .get(chunk + 1)
            .map_or(self.known.described, |later| later.first);
        let forwards = self.forwards();
        let in_reach = |from: Position, end: Position| if forwards { end > next } else { from <= next };
        let mut segments = VecDeque::new();
        if !self.may_be_in_chunk(chunk) {
            return Ok(segments);
        }

        let mut walk = IndexWalk::new();
        let mut stream = match shared
            .backing
            .read(&shared.recording, Object::index_chunk(first), 0)
            .await
        {
            Ok(stream) => Some(stream),
            Err(FetchError::Missing) => None,
            Err(error) => return Err(not_read(Trouble::of_fetch("index chunk", &error))),
        };
        let mut described = first;
        if let Some(stream) = &mut stream {
            while let Some(wanted) = walk.wanted() {
                let bytes = exactly(stream, wanted)
                    .await
                    .map_err(|error| not_read(Trouble::of_fetch("index chunk", &error)))?;
                let ended = bytes.len() < wanted;
                walk.take(bytes);
                let items: Vec<_> = walk.items().collect();
                for item in items {
                    let segment = item.segment;
                    let end = segment.first.after(segment.entries);
                    described = end;
                    if in_reach(segment.first, end) && self.may_be_in(Some(&segment.summary)) {
                        segments.push_back(SegmentRef {
                            first: segment.first,
                            end,
                            footer: FooterAt::Offset(segment.footer_at),
                        });
                    }
                }
                if ended || walk.stopped() || described >= until {
                    break;
                }
            }
        }
        if described < until {
            // The index chunk is missing, or an item of it is not whole: the footers of the segments say it.
            let listed = shared
                .backing
                .list(&shared.recording, ObjectKind::Segment, described)
                .await
                .map_err(|error| not_read(Trouble::of_listing(&error)))?;
            let listed: Vec<_> = listed.into_iter().filter(|segment| segment.first < until).collect();
            for (index, segment) in listed.iter().enumerate() {
                let end = listed.get(index + 1).map_or(until, |later| later.first);
                if in_reach(segment.first, end) {
                    segments.push_back(SegmentRef {
                        first: segment.first,
                        end,
                        footer: FooterAt::Trailer(segment.length),
                    });
                }
            }
        }
        Ok(segments)
    }

    fn may_be_in_chunk(&self, chunk: usize) -> bool {
        let Some(summary) = &self.known.chunks[chunk].summary else {
            return true;
        };
        self.wanted.may_be_in(summary)
    }

    /// The outline of a sealed segment, from its footer. `None` when the segment is lost.
    async fn outline_of(&mut self, segment: SegmentRef) -> Result<Option<SegmentOutline>, NextBatchError> {
        let shared = &self.shared;
        footer_of(&*shared.backing, &shared.recording, segment.first, segment.footer)
            .await
            .map_err(|error| NextBatchError::NotRead {
                reason: Trouble::of_fetch("segment", &error).reason(),
            })
    }

    /// Reads one block: its header, and its chunks one after the other, each checked and unpacked into the buffer of
    /// the block, which counts against the budget of the stream.
    async fn read_block(&mut self, located: Located) -> Result<BlockRead, NextBatchError> {
        let shared = &self.shared;
        let row = located.row;
        let not_read = |error: FetchError| NextBatchError::NotRead {
            reason: Trouble::of_fetch("segment", &error).reason(),
        };
        let all_lost = BlockRead {
            batch: None,
            lost: Some((row.first, row.last())),
        };
        let object = Object::segment(located.segment);
        let mut stream = match shared.backing.read(&shared.recording, object, row.at).await {
            Ok(stream) => stream,
            Err(FetchError::Missing) => return Ok(all_lost),
            Err(error) => return Err(not_read(error)),
        };
        let header = exactly(&mut stream, BlockHeader::LEN).await.map_err(not_read)?;
        let Ok(header) = BlockHeader::decode(&header) else {
            return Ok(all_lost);
        };
        if header.first != row.first {
            return Ok(all_lost);
        }
        let Ok(codec) = Codec::from_id(header.codec) else {
            return Ok(all_lost);
        };
        let mut entries_codec = Decompressor::new(codec);

        let mut entries = self.memory.buffer(row.entries_room);
        let mut frames = match self.frames {
            Frames::With => Some(self.memory.buffer(row.frames_room)),
            Frames::Without => None,
        };
        let mut whole = 0;
        while whole < row.entries {
            let chunk = exactly(&mut stream, ChunkHeader::LEN).await.map_err(not_read)?;
            let Ok(chunk) = ChunkHeader::decode(&chunk) else {
                break;
            };
            let stored = exactly(&mut stream, chunk.entries_stored as usize)
                .await
                .map_err(not_read)?;
            if stored.len() != chunk.entries_stored as usize || checksum(&stored) != chunk.entries_checksum {
                break;
            }
            if entries_codec
                .read(&stored, chunk.entries_unpacked as usize, &mut entries)
                .is_err()
            {
                break;
            }
            let stored_frames = exactly(&mut stream, chunk.frames_stored as usize)
                .await
                .map_err(not_read)?;
            if let Some(frames) = &mut frames {
                // The frames of a chunk are a stream of their own, so a reader without frames steps over them unread.
                let whole_frames = stored_frames.len() == chunk.frames_stored as usize
                    && checksum(&stored_frames) == chunk.frames_checksum;
                let unpacked = Decompressor::new(codec).read(&stored_frames, chunk.frames_unpacked as usize, frames);
                if !whole_frames || unpacked.is_err() {
                    break;
                }
            }
            let stored_len = u64::from(chunk.entries_stored) + u64::from(chunk.frames_stored);
            let zeros = usize::try_from(padding(stored_len)).unwrap_or_default();
            exactly(&mut stream, zeros).await.map_err(not_read)?;
            whole += u64::from(chunk.entries);
        }

        let frames = frames.map(Bytes::from_owner);
        let batch = Batch::checked(shared.version, row.first, entries, frames);
        let read = batch.len() as u64;
        let lost = (read < row.entries).then(|| (row.first.after(read), row.last()));
        let batch = (read > 0).then_some(batch);
        Ok(BlockRead { batch, lost })
    }
}

impl<B: SegmentBacking + 'static> RecordedEntries for SegmentEntries<B> {
    async fn next(&mut self) -> Result<Option<Batch>, NextBatchError> {
        let mut looked_again = false;
        loop {
            if let Some((from, to)) = self.damaged.take() {
                return Err(NextBatchError::Damaged { from, to });
            }
            let Some(next) = self.next else {
                return Ok(None);
            };
            let Some(located) = self.blocks.front().copied() else {
                if self.fill(&mut looked_again).await? {
                    continue;
                }
                return Ok(None);
            };
            let row = located.row;
            let room = row.entries_room
                + match self.frames {
                    Frames::With => row.frames_room,
                    Frames::Without => 0,
                };
            self.memory
                .has_room_for(room)
                .map_err(|Spent { held, budget }| NextBatchError::ReaderBudgetSpent { held, budget })?;
            // A block that did not read stays, so the stream reads it again when it is asked again.
            let read = self.read_block(located).await?;
            self.blocks.pop_front();
            self.next = if self.forwards() {
                Some(row.end())
            } else {
                row.first.number().checked_sub(1).map(Position::new)
            };
            if let Some((from, to)) = read.lost {
                let asked = if self.forwards() {
                    (from.max(next), to)
                } else {
                    (from, to.min(next))
                };
                if asked.0 <= asked.1 {
                    self.damaged = Some(asked);
                }
            }
            let Some(batch) = read.batch else {
                continue;
            };
            let batch = if self.forwards() {
                batch.starting_at(next)
            } else {
                batch.ending_at(next).towards(Direction::Backwards)
            };
            if batch.entries().next().is_some() {
                return Ok(Some(batch));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_refuses_a_version_of_the_layout_or_of_the_encoding_above_its_own() {
        let current = Versions::current();
        let later_layout = Versions {
            layout: LAYOUT_VERSION + 1,
            ..current
        };
        let later_encoding = Versions {
            encoding: EncodingVersion::CURRENT.number() + 1,
            ..current
        };

        assert_eq!(version_of(current), Ok(EncodingVersion::CURRENT));
        assert_eq!(
            version_of(later_layout),
            Err(OpenError::LayoutNotRead {
                written: later_layout.layout,
                known: LAYOUT_VERSION
            })
        );
        assert_eq!(
            version_of(later_encoding),
            Err(OpenError::EncodingNotRead {
                error: EncodingVersionError::Newer {
                    written: later_encoding.encoding,
                    known: EncodingVersion::CURRENT.number()
                }
            })
        );
    }

    #[test]
    fn what_says_zero_as_a_version_is_no_recording() {
        let current = Versions::current();

        assert!(matches!(
            version_of(Versions { layout: 0, ..current }),
            Err(OpenError::NotReadable { .. })
        ));
        assert!(matches!(
            version_of(Versions { encoding: 0, ..current }),
            Err(OpenError::NotReadable { .. })
        ));
    }

    #[test]
    fn every_object_of_a_recording_says_the_same_versions() {
        let current = Versions::current();
        let other = Versions {
            layout: LAYOUT_VERSION + 1,
            ..current
        };

        assert_eq!(same_versions(current, current, "segment"), Ok(()));
        assert!(matches!(
            same_versions(current, other, "segment"),
            Err(Trouble::Unreadable(_))
        ));
    }
}
