//! What the layout says itself, as fixed binary: the headers of the objects, of a block and of a chunk,
//! the records of a span, of a segment, of an index chunk and of the end, the trailer of a segment, and the manifest.
//!
//! A number has a fixed size, and its lowest byte comes first. A length counts bytes, and a reader checks it against
//! the bytes that are there before it uses it. A checksum is a CRC-32C of bytes as they are stored.
//! A header, a record and a chunk start at a multiple of 16 of their object, behind zeros where they have to.
//! What has a structure is a message of `recording`, which the layout carries and never reads.
//!
//! Every reading here takes bytes and gives a value or the reason why the bytes are no such value.

use bytes::Bytes;
use gaslightmq_recording::{EncodingVersion, Position};

/// The version of the layout that this build writes, and the newest that it reads.
pub(crate) const LAYOUT_VERSION: u16 = 1;

/// A header, a record and a chunk start at a multiple of this many bytes of their object.
pub(crate) const ALIGNMENT: u64 = 16;

const ZEROS: [u8; 16] = [0; 16];

/// The mark of a data segment: `GMQSEG` between a byte that no text holds and a line feed.
const SEGMENT_MARK: [u8; 8] = [0x89, b'G', b'M', b'Q', b'S', b'E', b'G', 0x0A];
/// The mark of an index chunk: `GMQIDX`.
const INDEX_MARK: [u8; 8] = [0x89, b'G', b'M', b'Q', b'I', b'D', b'X', 0x0A];
/// The mark of a manifest: `GMQMAN`.
const MANIFEST_MARK: [u8; 8] = [0x89, b'G', b'M', b'Q', b'M', b'A', b'N', 0x0A];
/// The mark at the end of a sealed segment: `GMQEND`.
const END_MARK: [u8; 8] = [0x89, b'G', b'M', b'Q', b'E', b'N', b'D', 0x0A];

const BLOCK_MARK: [u8; 4] = *b"GMQB";
const CHUNK_MARK: [u8; 4] = *b"GMQC";
const SPAN_MARK: [u8; 4] = *b"GMQP";
const SEGMENT_RECORD_MARK: [u8; 4] = *b"GMQS";
const INDEX_RECORD_MARK: [u8; 4] = *b"GMQI";
const END_RECORD_MARK: [u8; 4] = *b"GMQE";

/// The zeros that stand behind `len` bytes up to the next multiple of 16.
pub(crate) fn padding(len: u64) -> u64 {
    len.next_multiple_of(ALIGNMENT) - len
}

/// `len` zeros, up to 15 of them, as a shared buffer.
fn zeros(len: u64) -> Bytes {
    let len = usize::try_from(len).unwrap_or(ZEROS.len()).min(ZEROS.len());
    Bytes::from_static(&ZEROS[..len])
}

/// The checksum of bytes as they are stored.
pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut number = [0; 4];
    number.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(number)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut number = [0; 8];
    number.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(number)
}

fn length_of(bytes: usize) -> u64 {
    bytes as u64
}

/// A length that a reader takes from the bytes, as a length in memory.
fn in_memory(length: u64) -> Result<usize, String> {
    usize::try_from(length).map_err(|_| format!("a length of {length} bytes does not fit into memory"))
}

/// The length of a message of `recording` behind its head at the start of `bytes`, with the zeros behind it.
fn message_stride(bytes: &[u8]) -> Result<usize, String> {
    if bytes.len() < 16 {
        return Err(format!(
            "{} bytes are left, and the head of a message is 16",
            bytes.len()
        ));
    }
    let len = u32_at(bytes, 0) as usize;
    let stride = 16 + len.next_multiple_of(16);
    if stride > bytes.len() {
        return Err(format!(
            "a message of {stride} bytes with its head, and {} bytes are left",
            bytes.len()
        ));
    }
    Ok(stride)
}

/// The versions that the header of an object says: of the layout, and of the encoding, as numbers.
///
/// A reader checks both before it reads anything else of the object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Versions {
    pub(crate) layout: u16,
    pub(crate) encoding: u16,
}

impl Versions {
    /// The versions that this build writes.
    pub(crate) const fn current() -> Versions {
        Versions {
            layout: LAYOUT_VERSION,
            encoding: EncodingVersion::CURRENT.number(),
        }
    }

    fn write(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.layout.to_le_bytes());
        out.extend_from_slice(&self.encoding.to_le_bytes());
    }

    fn read(bytes: &[u8], at: usize) -> Versions {
        Versions {
            layout: u16_at(bytes, at),
            encoding: u16_at(bytes, at + 2),
        }
    }
}

/// The header of a data segment: its mark, the versions, its first position, and the description of the recording.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 8 | The mark of a segment. |
/// | 8 | 2 | The version of the layout. |
/// | 10 | 2 | The version of the encoding. |
/// | 12 | 4 | The length of what follows, up to the checksum and with it. |
/// | 16 | 8 | The position of the first entry of the segment. |
/// | 24 | 8 | Zero. |
/// | 32 | … | The description of the recording, as a message behind its head. |
/// | … | 4 | The checksum of every byte of the header before it. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub(crate) first: Position,
    pub(crate) description: Bytes,
}

impl SegmentHeader {
    /// The bytes of the header that say how long the rest is.
    pub(crate) const START: usize = 16;

    pub(crate) fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(32 + self.description.len() + 16);
        out.extend_from_slice(&SEGMENT_MARK);
        Versions::current().write(&mut out);
        let follows = u32::try_from(16 + self.description.len() + 4).expect("a description of less than 4 GiB");
        out.extend_from_slice(&follows.to_le_bytes());
        out.extend_from_slice(&self.first.number().to_le_bytes());
        out.extend_from_slice(&[0; 8]);
        out.extend_from_slice(&self.description);
        out.extend_from_slice(&checksum(&out).to_le_bytes());
        out.resize(out.len().next_multiple_of(16), 0);
        Bytes::from(out)
    }

    /// The versions, and the bytes of the header behind its start, with the zeros up to the next multiple of 16.
    pub(crate) fn start(bytes: &[u8]) -> Result<(Versions, usize), String> {
        if bytes.len() < Self::START || bytes[..8] != SEGMENT_MARK {
            return Err("the object does not start with the mark of a segment".to_owned());
        }
        let follows = u64::from(u32_at(bytes, 12));
        let rest = follows + padding(16 + follows);
        Ok((Versions::read(bytes, 8), in_memory(rest)?))
    }

    /// Reads the header from its start and the rest that [`start`](Self::start) asked for.
    pub(crate) fn decode(start: &[u8], rest: &Bytes) -> Result<SegmentHeader, String> {
        let follows = u32_at(start, 12) as usize;
        if follows < 20 || follows > rest.len() {
            return Err("the header of a segment says a length that it does not have".to_owned());
        }
        let stored = u32_at(rest, follows - 4);
        let mut computed = crc32c::crc32c(&start[..Self::START]);
        computed = crc32c::crc32c_append(computed, &rest[..follows - 4]);
        if stored != computed {
            return Err("the header of a segment fails its checksum".to_owned());
        }
        Ok(SegmentHeader {
            first: Position::new(u64_at(rest, 0)),
            description: rest.slice(16..follows - 4),
        })
    }
}

/// The header of a block: where a stream can start.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 4 | The mark of a block: `GMQB`. |
/// | 4 | 4 | The checksum of the bytes 8 to 31. |
/// | 8 | 8 | The position of its first entry. |
/// | 16 | 1 | The codec, by its number. |
/// | 17 | 15 | Zero. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockHeader {
    pub(crate) first: Position,
    pub(crate) codec: u8,
}

impl BlockHeader {
    pub(crate) const LEN: usize = 32;

    pub(crate) fn encode(self) -> Bytes {
        let mut out = vec![0; Self::LEN];
        out[..4].copy_from_slice(&BLOCK_MARK);
        out[8..16].copy_from_slice(&self.first.number().to_le_bytes());
        out[16] = self.codec;
        let sum = checksum(&out[8..]);
        out[4..8].copy_from_slice(&sum.to_le_bytes());
        Bytes::from(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<BlockHeader, String> {
        if bytes.len() < Self::LEN || bytes[..4] != BLOCK_MARK {
            return Err("no block starts here".to_owned());
        }
        if u32_at(bytes, 4) != checksum(&bytes[8..Self::LEN]) {
            return Err("the header of a block fails its checksum".to_owned());
        }
        Ok(BlockHeader {
            first: Position::new(u64_at(bytes, 8)),
            codec: bytes[16],
        })
    }
}

/// The header of a chunk: what one commit wrote of a block, compressed.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 4 | The mark of a chunk: `GMQC`. |
/// | 4 | 4 | The checksum of the bytes 8 to 47. |
/// | 8 | 4 | The number of its entries. |
/// | 12 | 4 | Zero. |
/// | 16 | 4 | The length of its entries, as they are stored. |
/// | 20 | 4 | The length of its entries, unpacked. |
/// | 24 | 4 | The length of its frames, as they are stored. |
/// | 28 | 4 | The length of its frames, unpacked. |
/// | 32 | 4 | The checksum of its entries, as they are stored. |
/// | 36 | 4 | The checksum of its frames, as they are stored. |
/// | 40 | 8 | Zero. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkHeader {
    pub(crate) entries: u32,
    pub(crate) entries_stored: u32,
    pub(crate) entries_unpacked: u32,
    pub(crate) frames_stored: u32,
    pub(crate) frames_unpacked: u32,
    pub(crate) entries_checksum: u32,
    pub(crate) frames_checksum: u32,
}

impl ChunkHeader {
    pub(crate) const LEN: usize = 48;

    pub(crate) fn encode(self) -> Bytes {
        let mut out = vec![0; Self::LEN];
        out[..4].copy_from_slice(&CHUNK_MARK);
        out[8..12].copy_from_slice(&self.entries.to_le_bytes());
        out[16..20].copy_from_slice(&self.entries_stored.to_le_bytes());
        out[20..24].copy_from_slice(&self.entries_unpacked.to_le_bytes());
        out[24..28].copy_from_slice(&self.frames_stored.to_le_bytes());
        out[28..32].copy_from_slice(&self.frames_unpacked.to_le_bytes());
        out[32..36].copy_from_slice(&self.entries_checksum.to_le_bytes());
        out[36..40].copy_from_slice(&self.frames_checksum.to_le_bytes());
        let sum = checksum(&out[8..]);
        out[4..8].copy_from_slice(&sum.to_le_bytes());
        Bytes::from(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<ChunkHeader, String> {
        if bytes.len() < Self::LEN || bytes[..4] != CHUNK_MARK {
            return Err("no chunk starts here".to_owned());
        }
        if u32_at(bytes, 4) != checksum(&bytes[8..Self::LEN]) {
            return Err("the header of a chunk fails its checksum".to_owned());
        }
        let header = ChunkHeader {
            entries: u32_at(bytes, 8),
            entries_stored: u32_at(bytes, 16),
            entries_unpacked: u32_at(bytes, 20),
            frames_stored: u32_at(bytes, 24),
            frames_unpacked: u32_at(bytes, 28),
            entries_checksum: u32_at(bytes, 32),
            frames_checksum: u32_at(bytes, 36),
        };
        if header.entries == 0 {
            return Err("a chunk holds no entry".to_owned());
        }
        Ok(header)
    }

    /// The bytes of the chunk behind its header: its entries and its frames as stored, and the zeros behind them.
    pub(crate) fn body(self) -> u64 {
        let stored = u64::from(self.entries_stored) + u64::from(self.frames_stored);
        stored + padding(stored)
    }
}

/// A chunk as it is appended: its header, its entries and its frames as stored, and the zeros behind them.
pub(crate) fn chunk_pieces(header: ChunkHeader, entries: Bytes, frames: Bytes) -> [Bytes; 4] {
    let stored = length_of(entries.len() + frames.len());
    [header.encode(), entries, frames, zeros(padding(stored))]
}

/// What a row says about one block of a span: what a reader needs before it reads the block.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 8 | Where the block starts in its segment. |
/// | 8 | 8 | The position of its first entry. |
/// | 16 | 4 | The number of its entries. |
/// | 20 | 4 | The length of its entries, unpacked. |
/// | 24 | 8 | The length of its frames, unpacked. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockRow {
    pub(crate) at: u64,
    pub(crate) first: Position,
    pub(crate) entries: u64,
    pub(crate) entries_room: u64,
    pub(crate) frames_room: u64,
}

impl BlockRow {
    pub(crate) const LEN: usize = 32;

    /// The position of the last entry of the block.
    pub(crate) fn last(&self) -> Position {
        self.first.after(self.entries.saturating_sub(1))
    }

    /// The position behind the last entry of the block.
    pub(crate) fn end(&self) -> Position {
        self.first.after(self.entries)
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.at.to_le_bytes());
        out.extend_from_slice(&self.first.number().to_le_bytes());
        out.extend_from_slice(&u32::try_from(self.entries).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&u32::try_from(self.entries_room).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&self.frames_room.to_le_bytes());
    }

    fn read(bytes: &[u8]) -> BlockRow {
        BlockRow {
            at: u64_at(bytes, 0),
            first: Position::new(u64_at(bytes, 8)),
            entries: u64::from(u32_at(bytes, 16)),
            entries_room: u64::from(u32_at(bytes, 20)),
            frames_room: u64_at(bytes, 24),
        }
    }
}

/// The record of a span of blocks: where it is, the row of every block, and its summary.
///
/// Its fixed part is 48 bytes: where the span starts in its segment (8), its length as stored without this record (8),
/// the position of its first entry (8), the number of its entries (8), the number of its blocks (4), zero (12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpanRecord {
    pub(crate) start: u64,
    pub(crate) stored: u64,
    pub(crate) first: Position,
    pub(crate) entries: u64,
    pub(crate) rows: Vec<BlockRow>,
    pub(crate) summary: Bytes,
}

/// The record of a data segment: what it holds and where its footer starts, and its summary.
///
/// Its fixed part is 48 bytes: the position of its first entry (8), the number of its entries (8),
/// the length of the segment up to its footer (8), the number of its spans (4), zero (4),
/// the length of the records of its spans, which follow it (8), zero (8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentRecord {
    pub(crate) first: Position,
    pub(crate) entries: u64,
    pub(crate) footer_at: u64,
    pub(crate) spans: u32,
    pub(crate) spans_len: u64,
    pub(crate) summary: Bytes,
}

/// The record of an index chunk, in the manifest: where it starts, and once it is full what it covers.
///
/// Its fixed part is 16 bytes: the position of the first entry that it describes (8),
/// and the number of entries that it describes, which is zero while it grows (8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexChunkRecord {
    pub(crate) first: Position,
    pub(crate) entries: u64,
    pub(crate) summary: Option<Bytes>,
}

/// A record: what the layout says about a span, a segment or an index chunk, or how the recording ended.
///
/// ```text
/// record = record head  fixed part  row …  message …
/// ```
///
/// | Offset | Size | The record head holds |
/// |---|---|---|
/// | 0 | 4 | The mark of its kind. |
/// | 4 | 4 | The checksum of every byte of the record behind its head. |
/// | 8 | 4 | The number of those bytes. |
/// | 12 | 4 | Zero. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    Span(SpanRecord),
    Segment(SegmentRecord),
    IndexChunk(IndexChunkRecord),
    /// The end of the recording, as the message of `recording`.
    End(Bytes),
}

/// The head of a record: its mark, the checksum and the length of what follows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordHead {
    mark: [u8; 4],
    checksum: u32,
    pub(crate) len: u32,
}

impl RecordHead {
    pub(crate) const LEN: usize = 16;

    pub(crate) fn decode(bytes: &[u8]) -> Result<RecordHead, String> {
        if bytes.len() < Self::LEN {
            return Err(format!(
                "{} bytes are left, and the head of a record is 16",
                bytes.len()
            ));
        }
        let mark = [bytes[0], bytes[1], bytes[2], bytes[3]];
        let known = [SPAN_MARK, SEGMENT_RECORD_MARK, INDEX_RECORD_MARK, END_RECORD_MARK];
        if !known.contains(&mark) || u32_at(bytes, 12) != 0 {
            return Err("no record starts here".to_owned());
        }
        Ok(RecordHead {
            mark,
            checksum: u32_at(bytes, 4),
            len: u32_at(bytes, 8),
        })
    }

    /// The bytes of the record behind its head.
    pub(crate) fn body(self) -> usize {
        self.len as usize
    }
}

/// Whether `bytes` start with the mark of a block, of a chunk, or of neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Starts {
    Block,
    Chunk,
    Record,
    Trailer,
    Nothing,
}

impl Starts {
    /// What the 16 bytes at the start of an item say it is.
    pub(crate) fn of(bytes: &[u8]) -> Starts {
        if bytes.len() < 16 {
            return Starts::Nothing;
        }
        match [bytes[0], bytes[1], bytes[2], bytes[3]] {
            BLOCK_MARK => Starts::Block,
            CHUNK_MARK => Starts::Chunk,
            _ if bytes[8..16] == END_MARK => Starts::Trailer,
            _ if RecordHead::decode(bytes).is_ok() => Starts::Record,
            _ => Starts::Nothing,
        }
    }
}

impl Record {
    /// The record as it stands in an object: its head, its fixed part, its rows and its message.
    pub(crate) fn encode(&self) -> Bytes {
        let mut body = Vec::new();
        let (mark, message) = match self {
            Record::Span(span) => {
                body.extend_from_slice(&span.start.to_le_bytes());
                body.extend_from_slice(&span.stored.to_le_bytes());
                body.extend_from_slice(&span.first.number().to_le_bytes());
                body.extend_from_slice(&span.entries.to_le_bytes());
                let blocks = u32::try_from(span.rows.len()).unwrap_or(u32::MAX);
                body.extend_from_slice(&blocks.to_le_bytes());
                body.extend_from_slice(&[0; 12]);
                for row in &span.rows {
                    row.write(&mut body);
                }
                (SPAN_MARK, Some(&span.summary))
            }
            Record::Segment(segment) => {
                body.extend_from_slice(&segment.first.number().to_le_bytes());
                body.extend_from_slice(&segment.entries.to_le_bytes());
                body.extend_from_slice(&segment.footer_at.to_le_bytes());
                body.extend_from_slice(&segment.spans.to_le_bytes());
                body.extend_from_slice(&[0; 4]);
                body.extend_from_slice(&segment.spans_len.to_le_bytes());
                body.extend_from_slice(&[0; 8]);
                (SEGMENT_RECORD_MARK, Some(&segment.summary))
            }
            Record::IndexChunk(chunk) => {
                body.extend_from_slice(&chunk.first.number().to_le_bytes());
                body.extend_from_slice(&chunk.entries.to_le_bytes());
                (INDEX_RECORD_MARK, chunk.summary.as_ref())
            }
            Record::End(end) => (END_RECORD_MARK, Some(end)),
        };
        if let Some(message) = message {
            body.extend_from_slice(message);
        }
        let mut out = Vec::with_capacity(RecordHead::LEN + body.len());
        out.extend_from_slice(&mark);
        out.extend_from_slice(&checksum(&body).to_le_bytes());
        let len = u32::try_from(body.len()).expect("a record of less than 4 GiB");
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&body);
        Bytes::from(out)
    }

    /// Reads the record behind `head` from its body.
    pub(crate) fn decode(head: RecordHead, body: &Bytes) -> Result<Record, String> {
        if body.len() != head.body() {
            return Err(format!(
                "a record of {} bytes, and {} bytes are there",
                head.body(),
                body.len()
            ));
        }
        if checksum(body) != head.checksum {
            return Err("a record fails its checksum".to_owned());
        }
        let fixed = |len: usize| {
            if body.len() < len {
                return Err(format!("a record of {} bytes, and its fixed part is {len}", body.len()));
            }
            Ok(())
        };
        let message = |from: usize| -> Result<Bytes, String> {
            let message = body.slice(from..);
            if message_stride(&message)? != message.len() {
                return Err("a record holds more than one message".to_owned());
            }
            Ok(message)
        };
        match head.mark {
            SPAN_MARK => {
                fixed(48)?;
                let blocks = u32_at(body, 32) as usize;
                let rows_end = blocks
                    .checked_mul(BlockRow::LEN)
                    .and_then(|rows| rows.checked_add(48))
                    .filter(|end| *end <= body.len())
                    .ok_or_else(|| format!("the record of a span says {blocks} blocks, and has no room for them"))?;
                let (rows, _) = body[48..rows_end].as_chunks::<{ BlockRow::LEN }>();
                let rows = rows.iter().map(|row| BlockRow::read(row)).collect();
                Ok(Record::Span(SpanRecord {
                    start: u64_at(body, 0),
                    stored: u64_at(body, 8),
                    first: Position::new(u64_at(body, 16)),
                    entries: u64_at(body, 24),
                    rows,
                    summary: message(rows_end)?,
                }))
            }
            SEGMENT_RECORD_MARK => {
                fixed(48)?;
                Ok(Record::Segment(SegmentRecord {
                    first: Position::new(u64_at(body, 0)),
                    entries: u64_at(body, 8),
                    footer_at: u64_at(body, 16),
                    spans: u32_at(body, 24),
                    spans_len: u64_at(body, 32),
                    summary: message(48)?,
                }))
            }
            INDEX_RECORD_MARK => {
                fixed(16)?;
                let summary = if body.len() > 16 { Some(message(16)?) } else { None };
                Ok(Record::IndexChunk(IndexChunkRecord {
                    first: Position::new(u64_at(body, 0)),
                    entries: u64_at(body, 8),
                    summary,
                }))
            }
            _ => Ok(Record::End(message(0)?)),
        }
    }

    /// Reads one whole record at the start of `bytes`, and says how many bytes it took.
    pub(crate) fn decode_at(bytes: &Bytes) -> Result<(Record, usize), String> {
        let head = RecordHead::decode(bytes)?;
        let end = RecordHead::LEN + head.body();
        if end > bytes.len() {
            return Err(format!("a record of {end} bytes, and {} bytes are left", bytes.len()));
        }
        let record = Record::decode(head, &bytes.slice(RecordHead::LEN..end))?;
        Ok((record, end))
    }
}

/// The trailer of a sealed segment: the length of its footer, and the mark of its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trailer {
    pub(crate) footer: u64,
}

impl Trailer {
    pub(crate) const LEN: usize = 16;

    pub(crate) fn encode(self) -> Bytes {
        let mut out = Vec::with_capacity(Self::LEN);
        out.extend_from_slice(&self.footer.to_le_bytes());
        out.extend_from_slice(&END_MARK);
        Bytes::from(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Trailer, String> {
        if bytes.len() != Self::LEN || bytes[8..] != END_MARK {
            return Err("the segment does not end with the mark of a sealed segment".to_owned());
        }
        Ok(Trailer {
            footer: u64_at(bytes, 0),
        })
    }
}

/// The header of an index chunk.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 8 | The mark of an index chunk. |
/// | 8 | 2 | The version of the layout. |
/// | 10 | 2 | The version of the encoding. |
/// | 12 | 4 | Zero. |
/// | 16 | 8 | The position of the first entry that the chunk describes. |
/// | 24 | 8 | Zero. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexHeader {
    pub(crate) versions: Versions,
    pub(crate) first: Position,
}

impl IndexHeader {
    pub(crate) const LEN: usize = 32;

    pub(crate) fn encode(first: Position) -> Bytes {
        let mut out = Vec::with_capacity(Self::LEN);
        out.extend_from_slice(&INDEX_MARK);
        Versions::current().write(&mut out);
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&first.number().to_le_bytes());
        out.extend_from_slice(&[0; 8]);
        Bytes::from(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<IndexHeader, String> {
        if bytes.len() < Self::LEN || bytes[..8] != INDEX_MARK {
            return Err("the object does not start with the mark of an index chunk".to_owned());
        }
        Ok(IndexHeader {
            versions: Versions::read(bytes, 8),
            first: Position::new(u64_at(bytes, 16)),
        })
    }
}

/// The manifest of a recording: the description, a record for every index chunk, and the end once there is one.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 8 | The mark of a manifest. |
/// | 8 | 2 | The version of the layout. |
/// | 10 | 2 | The version of the encoding. |
/// | 12 | 4 | The length of what follows, up to the checksum and with it. |
/// | 16 | … | The description of the recording, as a message behind its head. |
/// | … | … | A record for every index chunk of the recording, in order. |
/// | … | … | The record of the end, once the recording ended in order. |
/// | … | 4 | The checksum of every byte before it. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub(crate) versions: Versions,
    pub(crate) description: Bytes,
    pub(crate) chunks: Vec<IndexChunkRecord>,
    pub(crate) end: Option<Bytes>,
}

impl Manifest {
    pub(crate) fn encode(&self) -> Bytes {
        let mut body = Vec::new();
        body.extend_from_slice(&self.description);
        for chunk in &self.chunks {
            body.extend_from_slice(&Record::IndexChunk(chunk.clone()).encode());
        }
        if let Some(end) = &self.end {
            body.extend_from_slice(&Record::End(end.clone()).encode());
        }
        let mut out = Vec::with_capacity(16 + body.len() + 4);
        out.extend_from_slice(&MANIFEST_MARK);
        self.versions.write(&mut out);
        let follows = u32::try_from(body.len() + 4).expect("a manifest of less than 4 GiB");
        out.extend_from_slice(&follows.to_le_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&checksum(&out).to_le_bytes());
        Bytes::from(out)
    }

    /// Reads a manifest. The versions are read as numbers: the reader checks them before the messages.
    pub(crate) fn decode(bytes: &Bytes) -> Result<Manifest, String> {
        if bytes.len() < 20 || bytes[..8] != MANIFEST_MARK {
            return Err("the manifest does not start with its mark".to_owned());
        }
        let follows = u32_at(bytes, 12) as usize;
        if 16 + follows != bytes.len() {
            return Err("the manifest says a length that it does not have".to_owned());
        }
        let sum_at = bytes.len() - 4;
        if u32_at(bytes, sum_at) != checksum(&bytes[..sum_at]) {
            return Err("the manifest fails its checksum".to_owned());
        }
        let versions = Versions::read(bytes, 8);
        let stride = message_stride(&bytes[16..sum_at])?;
        let description = bytes.slice(16..16 + stride);
        let mut at = 16 + stride;
        let (mut chunks, mut end) = (Vec::new(), None);
        while at < sum_at {
            let (record, taken) = Record::decode_at(&bytes.slice(at..sum_at))?;
            match record {
                Record::IndexChunk(chunk) if end.is_none() => chunks.push(chunk),
                Record::End(message) if end.is_none() => end = Some(message),
                _ => return Err("a manifest holds the records of index chunks, and then the end".to_owned()),
            }
            at += taken;
        }
        Ok(Manifest {
            versions,
            description,
            chunks,
            end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_and_a_record_stand_at_a_multiple_of_16() {
        let summary = gaslightmq_recording::Summary::empty().encode();
        let header = SegmentHeader {
            first: Position::new(8_912_345),
            description: summary.clone(),
        };
        let span = Record::Span(SpanRecord {
            start: 64,
            stored: 4096,
            first: Position::new(8_912_345),
            entries: 2300,
            rows: vec![BlockRow {
                at: 64,
                first: Position::new(8_912_345),
                entries: 2300,
                entries_room: 4_194_304,
                frames_room: 0,
            }],
            summary,
        });

        assert_eq!(header.encode().len() % 16, 0, "a header is padded to a multiple of 16");
        assert_eq!(span.encode().len() % 16, 0, "a record is a multiple of 16");
        assert_eq!(BlockHeader::LEN % 16, 0);
        assert_eq!(ChunkHeader::LEN % 16, 0);
    }

    #[test]
    fn a_record_reads_back_what_was_written() {
        let span = Record::Span(SpanRecord {
            start: 64,
            stored: 4096,
            first: Position::new(10),
            entries: 30,
            rows: vec![
                BlockRow {
                    at: 64,
                    first: Position::new(10),
                    entries: 20,
                    entries_room: 2048,
                    frames_room: 512,
                },
                BlockRow {
                    at: 2240,
                    first: Position::new(30),
                    entries: 10,
                    entries_room: 1024,
                    frames_room: 0,
                },
            ],
            summary: gaslightmq_recording::Summary::empty().encode(),
        });

        let bytes = span.encode();

        assert_eq!(Record::decode_at(&bytes), Ok((span, bytes.len())));
    }

    #[test]
    fn a_record_that_lost_a_byte_fails_its_checksum() {
        let end = Record::End(gaslightmq_recording::Summary::empty().encode());
        let mut bytes = end.encode().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;

        assert_eq!(
            Record::decode_at(&Bytes::from(bytes)),
            Err("a record fails its checksum".to_owned())
        );
    }
}
