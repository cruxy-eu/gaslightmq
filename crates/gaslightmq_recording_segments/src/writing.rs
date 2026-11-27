//! The order of writing: what the layout hands to its backing, and when, as a plain function of the entries.
//!
//! [`Writing`] takes entries and says the steps that keep them. It does no IO: the writer of the storage hands each
//! step to the backing, in order, and stops at the first that fails.
//!
//! Two rules make every crash harmless:
//!
//! - An object is written before the object that describes it.
//!   A segment is sealed before its item is in an index chunk,
//!   and an index chunk is there before the manifest names it.
//! - Every step leaves a recording. What a crash leaves half appended fails a checksum, and nothing else tears.

use bytes::Bytes;
use gaslightmq_recording::{BatchEncoder, Description, End, Entry, Position, Summary};

use crate::backing::Object;
use crate::codec::{Codec, Compressor};
use crate::configuration::SegmentConfiguration;
use crate::framing::{
    BlockHeader, BlockRow, ChunkHeader, IndexChunkRecord, IndexHeader, Manifest, Record, SegmentHeader, SegmentRecord,
    SpanRecord, Trailer, Versions, checksum, chunk_pieces,
};

/// One thing that the backing does for the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Create(Object),
    Append(Object, Vec<Bytes>),
    Seal(Object),
    PutManifest(Bytes),
}

/// The state of a recording that is written: the block, the span, the segment and the index chunk that are open,
/// and the steps that wait for the next commit.
#[derive(Debug)]
pub(crate) struct Writing {
    configuration: SegmentConfiguration,
    codec: Codec,
    description: Bytes,
    next: Position,
    segment: OpenSegment,
    block: Option<OpenBlock>,
    index: Option<OpenIndex>,
    /// The records of the index chunks, as the manifest has them.
    chunks: Vec<IndexChunkRecord>,
    steps: Vec<Step>,
}

/// The segment that grows.
#[derive(Debug)]
struct OpenSegment {
    first: Position,
    /// Its length as it is stored, with what waits for the commit.
    length: u64,
    /// What waits for the commit, in the order of the segment.
    waiting: Vec<Bytes>,
    /// The records of its spans that closed, as they stand behind their last blocks.
    spans: Vec<Bytes>,
    summary: Summary,
    span: OpenSpan,
}

impl OpenSegment {
    fn at(first: Position, length: u64) -> OpenSegment {
        OpenSegment {
            first,
            length,
            waiting: Vec::new(),
            spans: Vec::new(),
            summary: Summary::empty(),
            span: OpenSpan::new(),
        }
    }

    fn put(&mut self, bytes: Bytes) {
        self.length += bytes.len() as u64;
        self.waiting.push(bytes);
    }
}

/// The span that grows: the rows of its blocks that closed, and the summary of their entries.
#[derive(Debug)]
struct OpenSpan {
    rows: Vec<BlockRow>,
    summary: Summary,
    unpacked: u64,
}

impl OpenSpan {
    const fn new() -> OpenSpan {
        OpenSpan {
            rows: Vec::new(),
            summary: Summary::empty(),
            unpacked: 0,
        }
    }
}

/// The block that grows. Its header goes into the segment with its first chunk, so no block is ever empty.
#[derive(Debug)]
struct OpenBlock {
    first: Position,
    /// Where the block starts in its segment, once its first chunk is cut.
    at: Option<u64>,
    entries: u64,
    encoder: BatchEncoder,
    compressor: Compressor,
    entries_room: u64,
    frames_room: u64,
    chunk: OpenChunk,
}

impl OpenBlock {
    fn size(&self) -> u64 {
        self.entries_room + self.frames_room
    }
}

/// What the block took since its last chunk was cut.
#[derive(Debug)]
struct OpenChunk {
    entries: u32,
    entries_out: Vec<u8>,
    entries_unpacked: u64,
    frames: Compressor,
    frames_out: Vec<u8>,
    frames_unpacked: u64,
}

impl OpenChunk {
    fn new(codec: Codec) -> Result<OpenChunk, String> {
        Ok(OpenChunk {
            entries: 0,
            entries_out: Vec::new(),
            entries_unpacked: 0,
            frames: Compressor::new(codec)?,
            frames_out: Vec::new(),
            frames_unpacked: 0,
        })
    }
}

/// The index chunk that grows.
#[derive(Debug)]
struct OpenIndex {
    first: Position,
    length: u64,
    entries: u64,
    summary: Summary,
}

/// The most that a header of a chunk says of the bytes of its entries, or of its frames.
const CHUNK_LIMIT: u64 = u32::MAX as u64;

impl Writing {
    /// Starts a recording: the steps that create its first segment and its manifest wait for the first commit.
    ///
    /// The first segment comes first, because creating it refuses a recording that is there.
    pub(crate) fn start(description: &Description, configuration: SegmentConfiguration, codec: Codec) -> Writing {
        let description = description.encode();
        let first = Object::segment(Position::FIRST);
        let header = SegmentHeader {
            first: Position::FIRST,
            description: description.clone(),
        }
        .encode();
        let mut writing = Writing {
            configuration,
            codec,
            next: Position::FIRST,
            segment: OpenSegment::at(Position::FIRST, header.len() as u64),
            block: None,
            index: None,
            chunks: Vec::new(),
            steps: vec![Step::Create(first), Step::Append(first, vec![header])],
            description,
        };
        writing.put_manifest(None);
        writing
    }

    /// Takes `entry`, and says which position it has. It waits for the next commit.
    ///
    /// It fails with what the codec said, when the codec did not take the entry. The recording ends with it.
    pub(crate) fn append(&mut self, entry: Entry) -> Result<Position, String> {
        let entries = entry.size() as u64;
        let frames = entry.frames_size() as u64;
        if let Some(block) = &self.block
            && block.entries > 0
        {
            let over_its_size = block.size() + entries + frames > self.configuration.block_size;
            let over_a_header = block.entries_room + entries > CHUNK_LIMIT;
            if over_its_size || over_a_header {
                self.close_block()?;
            }
        }
        if let Some(block) = &self.block {
            let chunk = &block.chunk;
            if chunk.entries_unpacked + entries > CHUNK_LIMIT || chunk.frames_unpacked + frames > CHUNK_LIMIT {
                self.cut_chunk(false)?;
            }
        }
        if self.block.is_none() {
            self.block = Some(OpenBlock {
                first: self.next,
                at: None,
                entries: 0,
                encoder: BatchEncoder::new(),
                compressor: Compressor::new(self.codec)?,
                entries_room: 0,
                frames_room: 0,
                chunk: OpenChunk::new(self.codec)?,
            });
        }
        let block = self.block.as_mut().expect("the block that the entry goes into is open");
        self.segment.span.summary.add(&entry, self.configuration.summary_values);

        let encoded = block.encoder.encode(entry);
        let chunk = &mut block.chunk;
        block.compressor.write(encoded.entry(), &mut chunk.entries_out)?;
        for frame in encoded.frames() {
            chunk.frames.write(frame, &mut chunk.frames_out)?;
        }
        chunk.entries += 1;
        chunk.entries_unpacked += entries;
        chunk.frames_unpacked += frames;
        block.entries += 1;
        block.entries_room += entries;
        block.frames_room += frames;
        let full = block.size() >= self.configuration.block_size;

        let position = self.next;
        self.next = position.next();
        if full {
            self.close_block()?;
        }
        Ok(position)
    }

    /// The steps that put every entry that was taken into the recording for good, in their order.
    pub(crate) fn commit(&mut self) -> Result<Vec<Step>, String> {
        self.cut_chunk(false)?;
        self.hand_over_what_waits();
        Ok(std::mem::take(&mut self.steps))
    }

    /// The steps that commit, and end the recording in order.
    pub(crate) fn finish(mut self, end: &End) -> Result<Vec<Step>, String> {
        self.cut_chunk(true)?;
        if let Some(block) = self.block.take() {
            self.row_of(&block);
        }
        let end = end.encode();
        self.seal_segment(Some(&end));
        if let Some(index) = self.index.take() {
            self.steps.push(Step::Seal(Object::index_chunk(index.first)));
            self.chunk_is_full(&index);
        }
        self.put_manifest(Some(end));
        Ok(self.steps)
    }

    /// Ends the chunk that the open block took since the last one, and puts it behind the segment.
    /// `ends_block` finishes the stream of the entries of the block; otherwise the stream goes on behind the chunk.
    ///
    /// A block whose last chunk a commit already cut ends at that flush: no chunk is left to carry the end of the
    /// stream, and none is needed, because the chunks of a block say how many entries it holds.
    fn cut_chunk(&mut self, ends_block: bool) -> Result<(), String> {
        let Some(block) = &mut self.block else {
            return Ok(());
        };
        let chunk = &mut block.chunk;
        if chunk.entries == 0 {
            return Ok(());
        }
        if ends_block {
            block.compressor.finish_block(&mut chunk.entries_out)?;
        } else {
            block.compressor.flush(&mut chunk.entries_out)?;
        }
        // The frames of a chunk are a stream of their own.
        chunk.frames.finish_block(&mut chunk.frames_out)?;
        let entries = Bytes::from(std::mem::take(&mut chunk.entries_out));
        let frames = Bytes::from(std::mem::take(&mut chunk.frames_out));
        let header = ChunkHeader {
            entries: chunk.entries,
            entries_stored: u32::try_from(entries.len()).expect("a chunk cut before 4 GiB of entries"),
            entries_unpacked: u32::try_from(chunk.entries_unpacked).expect("a chunk cut before 4 GiB of entries"),
            frames_stored: u32::try_from(frames.len()).expect("a chunk cut before 4 GiB of frames"),
            frames_unpacked: u32::try_from(chunk.frames_unpacked).expect("a chunk cut before 4 GiB of frames"),
            entries_checksum: checksum(&entries),
            frames_checksum: checksum(&frames),
        };
        *chunk = OpenChunk::new(self.codec)?;
        if block.at.is_none() {
            block.at = Some(self.segment.length);
            let header = BlockHeader {
                first: block.first,
                codec: self.codec.id(),
            };
            self.segment.put(header.encode());
        }
        for piece in chunk_pieces(header, entries, frames) {
            self.segment.put(piece);
        }
        Ok(())
    }

    /// Ends the open block. A span or a segment that reaches its size with it closes too.
    fn close_block(&mut self) -> Result<(), String> {
        self.cut_chunk(true)?;
        let Some(block) = self.block.take() else {
            return Ok(());
        };
        self.row_of(&block);
        if self.segment.span.unpacked >= self.configuration.span_size {
            self.close_span();
        }
        if self.segment.length >= self.configuration.segment_size {
            self.seal_segment(None);
        }
        Ok(())
    }

    /// Puts the row of a block that ended into the span that grows.
    fn row_of(&mut self, block: &OpenBlock) {
        let Some(at) = block.at else {
            return;
        };
        let span = &mut self.segment.span;
        span.unpacked += block.size();
        span.rows.push(BlockRow {
            at,
            first: block.first,
            entries: block.entries,
            entries_room: block.entries_room,
            frames_room: block.frames_room,
        });
    }

    /// Puts the record of the span that grows behind its last block.
    fn close_span(&mut self) {
        let span = std::mem::replace(&mut self.segment.span, OpenSpan::new());
        let [first, ..] = span.rows[..] else {
            return;
        };
        let record = Record::Span(SpanRecord {
            start: first.at,
            stored: self.segment.length - first.at,
            first: first.first,
            entries: span.rows.iter().map(|row| row.entries).sum(),
            rows: span.rows,
            summary: span.summary.encode(),
        })
        .encode();
        self.segment.put(record.clone());
        self.segment.spans.push(record);
        self.segment
            .summary
            .merge(&span.summary, self.configuration.summary_values);
    }

    /// Seals the segment that grows with its footer, puts its item into the index chunk that grows,
    /// and creates the next segment, unless the recording ends with it.
    fn seal_segment(&mut self, end: Option<&Bytes>) {
        self.close_span();
        let segment = std::mem::replace(&mut self.segment, OpenSegment::at(self.next, 0));
        let object = Object::segment(segment.first);
        let record = Record::Segment(SegmentRecord {
            first: segment.first,
            entries: self.next.number() - segment.first.number(),
            footer_at: segment.length,
            spans: u32::try_from(segment.spans.len()).unwrap_or(u32::MAX),
            spans_len: segment.spans.iter().map(|span| span.len() as u64).sum(),
            summary: segment.summary.encode(),
        });
        let mut item = vec![record.encode()];
        item.extend(segment.spans.iter().cloned());
        item.extend(end.map(|end| Record::End(end.clone()).encode()));
        let item_len: u64 = item.iter().map(|piece| piece.len() as u64).sum();

        let mut pieces = segment.waiting;
        pieces.extend(item.iter().cloned());
        pieces.push(Trailer { footer: item_len }.encode());
        self.append_to(object, pieces);
        self.steps.push(Step::Seal(object));

        let entries = self.next.number() - segment.first.number();
        let index = match self.index.take() {
            Some(index) if index.length + item_len <= self.configuration.index_size => {
                self.append_to(Object::index_chunk(index.first), item);
                index
            }
            full => {
                if let Some(full) = full {
                    self.steps.push(Step::Seal(Object::index_chunk(full.first)));
                    self.chunk_is_full(&full);
                }
                let chunk = Object::index_chunk(segment.first);
                let header = IndexHeader::encode(segment.first);
                let length = header.len() as u64;
                self.steps.push(Step::Create(chunk));
                self.append_to(chunk, std::iter::once(header).chain(item).collect());
                self.chunks.push(IndexChunkRecord {
                    first: segment.first,
                    entries: 0,
                    summary: None,
                });
                if end.is_none() {
                    self.put_manifest(None);
                }
                OpenIndex {
                    first: segment.first,
                    length,
                    entries: 0,
                    summary: Summary::empty(),
                }
            }
        };
        let mut index = index;
        index.length += item_len;
        index.entries += entries;
        index.summary.merge(&segment.summary, self.configuration.summary_values);
        self.index = Some(index);

        if end.is_none() {
            let next = Object::segment(self.next);
            let header = SegmentHeader {
                first: self.next,
                description: self.description.clone(),
            }
            .encode();
            self.segment.length = header.len() as u64;
            self.steps.push(Step::Create(next));
            self.steps.push(Step::Append(next, vec![header]));
        }
    }

    /// The manifest says what a full index chunk covers from here on.
    fn chunk_is_full(&mut self, index: &OpenIndex) {
        if let Some(record) = self.chunks.iter_mut().find(|record| record.first == index.first) {
            record.entries = index.entries;
            record.summary = Some(index.summary.encode());
        }
    }

    fn put_manifest(&mut self, end: Option<Bytes>) {
        let manifest = Manifest {
            versions: Versions::current(),
            description: self.description.clone(),
            chunks: self.chunks.clone(),
            end,
        };
        self.steps.push(Step::PutManifest(manifest.encode()));
    }

    /// Hands what waits in the segment that grows to the steps, as one append.
    fn hand_over_what_waits(&mut self) {
        let waiting = std::mem::take(&mut self.segment.waiting);
        if !waiting.is_empty() {
            self.append_to(Object::segment(self.segment.first), waiting);
        }
    }

    /// An append of `pieces` to `object`, joined to the append before it when that is to the same object.
    fn append_to(&mut self, object: Object, pieces: Vec<Bytes>) {
        if let Some(Step::Append(last, waiting)) = self.steps.last_mut()
            && *last == object
        {
            waiting.extend(pieces);
            return;
        }
        self.steps.push(Step::Append(object, pieces));
    }
}
