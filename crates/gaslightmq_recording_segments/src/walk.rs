//! Recovery, and every other reading of what the layout wrote, as plain functions over bytes.
//!
//! A walk is fed the bytes of an object in the order in which they stand, as many at a time as it asks for,
//! and says what the object holds. It does no IO: the reader of the storage asks the backing for what the walk wants.
//!
//! | Walk | Over | Gives |
//! |---|---|---|
//! | [`SegmentWalk`] | A segment, from its start: its header, its blocks and chunks, the records of its spans, and its footer when it has one. | The [outline](SegmentOutline) of the segment, up to its last whole chunk, and the description of the recording. |
//! | [`IndexWalk`] | An index chunk, from its start. | The record of every segment whose item is whole, and the end of the recording when an item holds it. |
//! | [`FooterWalk`] | The footer of a sealed segment. | Its outline. |
//!
//! A walk follows lengths that a checksum covers, and never searches for a mark,
//! so the bytes of a frame can never pass for the start of a chunk.
//! Whatever is not whole stops a walk where the last whole item ends: a torn append, a chunk that a commit is still
//! writing, or bytes that are damaged. A walk that stopped goes on from there when it is fed again.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use gaslightmq_recording::Position;

use crate::framing::{
    BlockHeader, BlockRow, ChunkHeader, Record, RecordHead, SegmentHeader, SegmentRecord, SpanRecord, Starts, Trailer,
    Versions, checksum,
};

/// What a walk does with bytes: it says how many it wants next, and takes them.
pub(crate) trait Walk {
    /// The bytes that the walk wants next, or `None` when it is over.
    fn wanted(&self) -> Option<usize>;

    /// Takes the bytes that the walk wanted, or fewer when the object ends before them.
    fn take(&mut self, bytes: Bytes);

    /// Where the walk stands in its object: every byte before it is whole.
    fn offset(&self) -> u64;

    /// Whether the last bytes that the walk took stopped it where the last whole item ends.
    /// It then wants the bytes from [`offset`](Self::offset) on again.
    fn stopped(&self) -> bool;
}

/// What one span of a segment says to a reader: its summary, when it has a record, and the row of every block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpanOutline {
    /// The summary of the span. A span that still grows, or that a crash cut short, has none.
    pub(crate) summary: Option<Bytes>,
    pub(crate) rows: Vec<BlockRow>,
}

/// What a segment holds, as far as a reader needs before it reads a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentOutline {
    pub(crate) first: Position,
    pub(crate) entries: u64,
    pub(crate) spans: Vec<SpanOutline>,
    /// The end of the recording, when the footer of the segment holds it.
    pub(crate) end: Option<Bytes>,
    /// Whether the segment has its footer: it says everything about itself there.
    pub(crate) sealed: bool,
}

impl SegmentOutline {
    /// The position behind the last entry of the segment.
    pub(crate) fn end_position(&self) -> Position {
        self.first.after(self.entries)
    }

    fn of_footer(segment: &SegmentRecord, spans: Vec<SpanRecord>, end: Option<Bytes>) -> SegmentOutline {
        SegmentOutline {
            first: segment.first,
            entries: segment.entries,
            spans: spans
                .into_iter()
                .map(|span| SpanOutline {
                    summary: Some(span.summary),
                    rows: span.rows,
                })
                .collect(),
            end,
            sealed: true,
        }
    }
}

/// Joins two pieces of one header: they are small.
fn joined(start: &Bytes, rest: &Bytes) -> Bytes {
    let mut bytes = BytesMut::with_capacity(start.len() + rest.len());
    bytes.extend_from_slice(start);
    bytes.extend_from_slice(rest);
    bytes.freeze()
}

/// Checks that the records of the spans of a segment are as many as its record says, and reads them.
fn spans_of(segment: &SegmentRecord, bytes: &Bytes) -> Result<Vec<SpanRecord>, String> {
    let mut spans = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let (record, taken) = Record::decode_at(&bytes.slice(at..))?;
        let Record::Span(span) = record else {
            return Err("the record of a segment is followed by the records of its spans".to_owned());
        };
        spans.push(span);
        at += taken;
    }
    if spans.len() != segment.spans as usize {
        return Err(format!(
            "the record of a segment says {} spans, and {} follow it",
            segment.spans,
            spans.len()
        ));
    }
    Ok(spans)
}

/// A walk over the footer of a sealed segment: the record of the segment, the records of its spans,
/// the record of the end in the last segment, and the trailer.
#[derive(Debug, Clone)]
pub(crate) struct FooterWalk {
    offset: u64,
    state: FooterState,
}

#[derive(Debug, Clone)]
enum FooterState {
    Head,
    Body(RecordHead),
    /// The records of the spans, and the 16 bytes behind them: the trailer, or the head of the record of the end.
    Spans(SegmentRecord),
    /// The record of the end, and the trailer behind it.
    End(SegmentRecord, Vec<SpanRecord>, RecordHead),
    Done(SegmentOutline),
    Failed(String),
}

impl FooterWalk {
    /// A walk from the start of a footer.
    pub(crate) const fn new(offset: u64) -> FooterWalk {
        FooterWalk {
            offset,
            state: FooterState::Head,
        }
    }

    /// The outline of the segment, once the walk took its whole footer.
    pub(crate) fn outline(self) -> Result<SegmentOutline, String> {
        match self.state {
            FooterState::Done(outline) => Ok(outline),
            FooterState::Failed(reason) => Err(reason),
            FooterState::Head | FooterState::Body(_) | FooterState::Spans(_) | FooterState::End(..) => {
                Err("the footer of the segment ends before its trailer".to_owned())
            }
        }
    }

    fn failed(&mut self, reason: String) {
        self.state = FooterState::Failed(reason);
    }

    fn step(&mut self, bytes: &Bytes) -> Result<(), String> {
        let state = std::mem::replace(&mut self.state, FooterState::Head);
        self.state = match state {
            FooterState::Head => FooterState::Body(RecordHead::decode(bytes)?),
            FooterState::Body(head) => match Record::decode(head, bytes)? {
                Record::Segment(segment) => FooterState::Spans(segment),
                Record::Span(_) | Record::IndexChunk(_) | Record::End(_) => {
                    return Err("a footer starts with the record of its segment".to_owned());
                }
            },
            FooterState::Spans(segment) => {
                let split = bytes.len() - Trailer::LEN;
                let spans = spans_of(&segment, &bytes.slice(..split))?;
                let behind = bytes.slice(split..);
                if let Ok(Trailer { .. }) = Trailer::decode(&behind) {
                    FooterState::Done(SegmentOutline::of_footer(&segment, spans, None))
                } else {
                    FooterState::End(segment, spans, RecordHead::decode(&behind)?)
                }
            }
            FooterState::End(segment, spans, head) => {
                let split = bytes.len() - Trailer::LEN;
                let Record::End(end) = Record::decode(head, &bytes.slice(..split))? else {
                    return Err("only the end follows the records of the spans of a segment".to_owned());
                };
                Trailer::decode(&bytes[split..])?;
                FooterState::Done(SegmentOutline::of_footer(&segment, spans, Some(end)))
            }
            done @ (FooterState::Done(_) | FooterState::Failed(_)) => done,
        };
        Ok(())
    }
}

impl Walk for FooterWalk {
    fn wanted(&self) -> Option<usize> {
        match &self.state {
            FooterState::Head => Some(RecordHead::LEN),
            FooterState::Body(head) => Some(head.body()),
            FooterState::Spans(segment) => usize::try_from(segment.spans_len).ok().map(|len| len + Trailer::LEN),
            FooterState::End(_, _, head) => Some(head.body() + Trailer::LEN),
            FooterState::Done(_) | FooterState::Failed(_) => None,
        }
    }

    fn take(&mut self, bytes: Bytes) {
        if Some(bytes.len()) != self.wanted() {
            return self.failed("the footer of the segment ends before its trailer".to_owned());
        }
        self.offset += bytes.len() as u64;
        if let Err(reason) = self.step(&bytes) {
            self.failed(reason);
        }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn stopped(&self) -> bool {
        matches!(self.state, FooterState::Failed(_))
    }
}

/// A walk over a segment from its start, which is how a reader finds what no index describes.
///
/// It checks every mark and every checksum, and that every chunk is whole, and unpacks nothing.
/// It stops at the last chunk that is whole: what follows is a chunk that a commit still writes, or what a crash left
/// half written. Fed again from where it stands, it goes on.
#[derive(Debug, Clone)]
pub(crate) struct SegmentWalk {
    first: Position,
    offset: u64,
    /// The bytes of the item that the walk is in, so far.
    taken: u64,
    state: SegmentState,
    versions: Option<Versions>,
    description: Option<Bytes>,
    next: Position,
    spans: Vec<SpanOutline>,
    /// The rows of the span that has no record yet. The last one is the block that the walk is in, if any.
    rows: Vec<BlockRow>,
    in_a_block: bool,
    footer: Option<SegmentOutline>,
    /// Why the walk stands where it stands, when it is not the end of the bytes.
    flaw: Option<String>,
    stopped: bool,
}

#[derive(Debug, Clone)]
enum SegmentState {
    HeaderStart,
    HeaderRest(Bytes),
    Item,
    Block(Bytes),
    Chunk(Bytes),
    ChunkBody(ChunkHeader),
    Record(RecordHead),
    Footer(FooterWalk),
}

impl SegmentWalk {
    /// A walk over the segment whose first entry has the position `first`, from its start.
    pub(crate) const fn new(first: Position) -> SegmentWalk {
        SegmentWalk {
            first,
            offset: 0,
            taken: 0,
            state: SegmentState::HeaderStart,
            versions: None,
            description: None,
            next: first,
            spans: Vec::new(),
            rows: Vec::new(),
            in_a_block: false,
            footer: None,
            flaw: None,
            stopped: false,
        }
    }

    /// The versions that the header of the segment says, once the walk read its start.
    pub(crate) const fn versions(&self) -> Option<Versions> {
        self.versions
    }

    /// The description of the recording, as the header of the segment holds it.
    pub(crate) const fn description(&self) -> Option<&Bytes> {
        self.description.as_ref()
    }

    /// Why the walk stopped before the end of the bytes, when it did.
    pub(crate) fn flaw(&self) -> Option<&str> {
        self.flaw.as_deref()
    }

    /// What the segment holds up to its last whole chunk: everything, once the walk read its footer.
    pub(crate) fn outline(&self) -> SegmentOutline {
        if let Some(footer) = &self.footer {
            return footer.clone();
        }
        let mut spans = self.spans.clone();
        if !self.rows.is_empty() {
            spans.push(SpanOutline {
                summary: None,
                rows: self.rows.clone(),
            });
        }
        SegmentOutline {
            first: self.first,
            entries: self.next.number() - self.first.number(),
            spans,
            end: None,
            sealed: false,
        }
    }

    /// Stops at the last whole item: what was taken of the item that the walk is in does not count.
    fn stop(&mut self, flaw: Option<String>) {
        self.taken = 0;
        self.flaw = flaw;
        self.stopped = true;
        self.state = if self.versions.is_some() && self.description.is_some() {
            SegmentState::Item
        } else {
            SegmentState::HeaderStart
        };
    }

    /// The item that the walk is in is whole.
    fn whole(&mut self, state: SegmentState) {
        self.offset += self.taken;
        self.taken = 0;
        self.flaw = None;
        self.state = state;
    }

    fn step(&mut self, bytes: Bytes) -> Result<(), String> {
        let state = std::mem::replace(&mut self.state, SegmentState::Item);
        match state {
            SegmentState::HeaderStart => {
                let (versions, _) = SegmentHeader::start(&bytes)?;
                self.versions = Some(versions);
                self.state = SegmentState::HeaderRest(bytes);
            }
            SegmentState::HeaderRest(start) => {
                let header = SegmentHeader::decode(&start, &bytes)?;
                if header.first != self.first {
                    return Err(format!(
                        "the segment {} says that its first entry is {}",
                        self.first, header.first
                    ));
                }
                self.description = Some(header.description);
                self.whole(SegmentState::Item);
            }
            SegmentState::Item => match Starts::of(&bytes) {
                Starts::Block => self.state = SegmentState::Block(bytes),
                Starts::Chunk if self.in_a_block => self.state = SegmentState::Chunk(bytes),
                Starts::Chunk => return Err("a chunk stands outside a block".to_owned()),
                Starts::Record => self.state = SegmentState::Record(RecordHead::decode(&bytes)?),
                Starts::Trailer | Starts::Nothing => return Err("nothing that a segment holds starts here".to_owned()),
            },
            SegmentState::Block(start) => {
                let header = BlockHeader::decode(&joined(&start, &bytes))?;
                if header.first != self.next {
                    return Err(format!(
                        "a block says that its first entry is {}, and the entry behind the block before it is {}",
                        header.first, self.next
                    ));
                }
                self.rows.push(BlockRow {
                    at: self.offset,
                    first: header.first,
                    entries: 0,
                    entries_room: 0,
                    frames_room: 0,
                });
                self.in_a_block = true;
                self.whole(SegmentState::Item);
            }
            SegmentState::Chunk(start) => {
                let header = ChunkHeader::decode(&joined(&start, &bytes))?;
                self.state = SegmentState::ChunkBody(header);
            }
            SegmentState::ChunkBody(header) => {
                let entries_end = header.entries_stored as usize;
                let frames_end = entries_end + header.frames_stored as usize;
                if checksum(&bytes[..entries_end]) != header.entries_checksum {
                    return Err("the entries of a chunk fail their checksum".to_owned());
                }
                if checksum(&bytes[entries_end..frames_end]) != header.frames_checksum {
                    return Err("the frames of a chunk fail their checksum".to_owned());
                }
                let row = self.rows.last_mut().expect("a chunk is in a block");
                row.entries += u64::from(header.entries);
                row.entries_room += u64::from(header.entries_unpacked);
                row.frames_room += u64::from(header.frames_unpacked);
                self.next = self.next.after(u64::from(header.entries));
                self.whole(SegmentState::Item);
            }
            SegmentState::Record(head) => self.record(head, &bytes)?,
            SegmentState::Footer(mut footer) => {
                footer.take(bytes);
                if footer.wanted().is_some() {
                    self.state = SegmentState::Footer(footer);
                } else {
                    self.footer = Some(footer.outline()?);
                    self.offset += self.taken;
                    self.taken = 0;
                    self.state = SegmentState::Item;
                }
            }
        }
        Ok(())
    }

    /// The record of a span behind its last block, or the record of the segment, which starts its footer.
    fn record(&mut self, head: RecordHead, bytes: &Bytes) -> Result<(), String> {
        match Record::decode(head, bytes)? {
            Record::Span(span) => {
                let walked: u64 = self.rows.iter().map(|row| row.entries).sum();
                if span.rows.len() != self.rows.len() || span.entries != walked {
                    return Err("the record of a span does not say the blocks that stand before it".to_owned());
                }
                self.spans.push(SpanOutline {
                    summary: Some(span.summary),
                    rows: span.rows,
                });
                self.rows.clear();
                self.in_a_block = false;
                self.whole(SegmentState::Item);
            }
            Record::Segment(segment) => {
                if segment.first != self.first || segment.entries != self.next.number() - self.first.number() {
                    return Err("the footer of the segment does not say the entries that stand before it".to_owned());
                }
                let mut footer = FooterWalk::new(0);
                footer.state = FooterState::Spans(segment);
                self.state = SegmentState::Footer(footer);
            }
            Record::IndexChunk(_) | Record::End(_) => {
                return Err("a segment holds no such record outside its footer".to_owned());
            }
        }
        Ok(())
    }

    fn rest_of_header(&self) -> Option<usize> {
        match &self.state {
            SegmentState::HeaderRest(start) => SegmentHeader::start(start).ok().map(|(_, rest)| rest),
            _ => None,
        }
    }
}

impl Walk for SegmentWalk {
    fn wanted(&self) -> Option<usize> {
        if self.footer.is_some() {
            return None;
        }
        match &self.state {
            SegmentState::HeaderStart => Some(SegmentHeader::START),
            SegmentState::HeaderRest(_) => self.rest_of_header(),
            SegmentState::Item => Some(16),
            SegmentState::Block(_) => Some(BlockHeader::LEN - 16),
            SegmentState::Chunk(_) => Some(ChunkHeader::LEN - 16),
            SegmentState::ChunkBody(header) => usize::try_from(header.body()).ok(),
            SegmentState::Record(head) => Some(head.body()),
            SegmentState::Footer(footer) => footer.wanted(),
        }
    }

    fn take(&mut self, bytes: Bytes) {
        self.stopped = false;
        if Some(bytes.len()) != self.wanted() {
            // The bytes end inside an item: a commit that is still written, or what a crash left.
            return self.stop(None);
        }
        self.taken += bytes.len() as u64;
        if let Err(reason) = self.step(bytes) {
            self.stop(Some(reason));
        }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn stopped(&self) -> bool {
        self.stopped
    }
}

/// One item of an index chunk: the record of a segment, whose records of spans followed it whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexItem {
    pub(crate) segment: SegmentRecord,
}

/// A walk over an index chunk, record by record. It holds one record at a time, and hands out every item that is
/// whole: the record of its segment and the records of its spans, each with its checksum.
#[derive(Debug, Clone)]
pub(crate) struct IndexWalk {
    offset: u64,
    taken: u64,
    state: IndexState,
    versions: Option<Versions>,
    first: Option<Position>,
    /// The position behind the last entry that a whole item describes.
    described: Option<Position>,
    items: VecDeque<IndexItem>,
    end: Option<Bytes>,
    flaw: Option<String>,
    stopped: bool,
}

#[derive(Debug, Clone)]
enum IndexState {
    Header,
    Head,
    Body(RecordHead),
    Spans(SegmentRecord),
}

impl IndexWalk {
    /// A walk from the start of an index chunk.
    pub(crate) const fn new() -> IndexWalk {
        IndexWalk {
            offset: 0,
            taken: 0,
            state: IndexState::Header,
            versions: None,
            first: None,
            described: None,
            items: VecDeque::new(),
            end: None,
            flaw: None,
            stopped: false,
        }
    }

    /// The versions that the header of the index chunk says, once the walk read it.
    pub(crate) const fn versions(&self) -> Option<Versions> {
        self.versions
    }

    /// The position behind the last entry that a whole item describes, once the walk read the header.
    pub(crate) const fn described(&self) -> Option<Position> {
        self.described
    }

    /// The items that the walk read whole since this was asked last.
    pub(crate) fn items(&mut self) -> impl Iterator<Item = IndexItem> + '_ {
        self.items.drain(..)
    }

    /// The end of the recording, when the last item holds it.
    pub(crate) const fn end(&self) -> Option<&Bytes> {
        self.end.as_ref()
    }

    fn stop(&mut self, flaw: Option<String>) {
        self.taken = 0;
        self.flaw = flaw;
        self.stopped = true;
        self.state = if self.versions.is_some() {
            IndexState::Head
        } else {
            IndexState::Header
        };
    }

    fn whole(&mut self) {
        self.offset += self.taken;
        self.taken = 0;
        self.flaw = None;
        self.state = IndexState::Head;
    }

    fn step(&mut self, bytes: &Bytes) -> Result<(), String> {
        let state = std::mem::replace(&mut self.state, IndexState::Head);
        match state {
            IndexState::Header => {
                let header = crate::framing::IndexHeader::decode(bytes)?;
                self.versions = Some(header.versions);
                self.first = Some(header.first);
                self.described = Some(header.first);
                self.whole();
            }
            IndexState::Head => {
                if self.end.is_some() {
                    return Err("nothing follows the end of a recording".to_owned());
                }
                self.state = IndexState::Body(RecordHead::decode(bytes)?);
            }
            IndexState::Body(head) => match Record::decode(head, bytes)? {
                Record::Segment(segment) => {
                    if self.described != Some(segment.first) {
                        return Err("an item does not start where the item before it ends".to_owned());
                    }
                    self.state = IndexState::Spans(segment);
                }
                Record::End(end) => {
                    self.end = Some(end);
                    self.whole();
                }
                Record::Span(_) | Record::IndexChunk(_) => {
                    return Err("an item of an index chunk starts with the record of its segment".to_owned());
                }
            },
            IndexState::Spans(segment) => {
                spans_of(&segment, bytes)?;
                self.described = Some(segment.first.after(segment.entries));
                self.items.push_back(IndexItem { segment });
                self.whole();
            }
        }
        Ok(())
    }
}

impl Walk for IndexWalk {
    fn wanted(&self) -> Option<usize> {
        match &self.state {
            IndexState::Header => Some(crate::framing::IndexHeader::LEN),
            IndexState::Head => Some(RecordHead::LEN),
            IndexState::Body(head) => Some(head.body()),
            IndexState::Spans(segment) => usize::try_from(segment.spans_len).ok(),
        }
    }

    fn take(&mut self, bytes: Bytes) {
        self.stopped = false;
        if Some(bytes.len()) != self.wanted() {
            return self.stop(None);
        }
        self.taken += bytes.len() as u64;
        if let Err(reason) = self.step(&bytes) {
            self.stop(Some(reason));
        }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn stopped(&self) -> bool {
        self.stopped
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use gaslightmq_clock::Instant;
    use gaslightmq_environment::RecordingMode;
    use gaslightmq_field::ConnectionNumber;
    use gaslightmq_recording::prelude::*;

    use super::*;
    use crate::backing::Object;
    use crate::codec::Codec;
    use crate::configuration::SegmentConfiguration;
    use crate::framing::ChunkHeader;
    use crate::writing::{Step, Writing};

    /// Feeds a walk the bytes of an object from where it stands, as a stream of the backing hands them out:
    /// as many as it wants at a time, and fewer at the end.
    fn feed(walk: &mut impl Walk, bytes: &Bytes) {
        let mut at = usize::try_from(walk.offset()).expect("an offset in memory");
        while let Some(wanted) = walk.wanted() {
            let piece = bytes.slice(at.min(bytes.len())..(at + wanted).min(bytes.len()));
            at += piece.len();
            let ended = piece.len() < wanted;
            walk.take(piece);
            if ended || walk.stopped() {
                break;
            }
        }
    }

    #[test]
    fn wherever_a_crash_cuts_a_segment_the_walk_stops_at_the_last_whole_chunk_and_goes_on_from_there() {
        let description = Description {
            environment: "orders-sim".parse().expect("a name"),
            started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
            clock: Instant::ZERO,
            mode: RecordingMode::Full,
            backend: RecordedBackend::Simulated,
            written_by: "0.1.0".to_owned(),
            armed: Vec::new(),
            scenario: None,
            journal_after: None,
        };
        let closed = |connection| {
            Entry::session(&SessionEvent {
                connection: ConnectionNumber::new(connection),
                at: Instant::ZERO,
                change: SessionChange::Closed { by: Closer::Client },
            })
        };
        let entry_size = closed(0).size() as u64;
        let mut configuration = SegmentConfiguration::new();
        // A block of two entries, a span of two blocks and a commit of three entries, so the appends of the commits
        // hold chunks, headers of blocks and records of spans in every order.
        configuration.block_size = 2 * entry_size;
        configuration.span_size = 4 * entry_size;
        let mut writing = Writing::start(&description, configuration, Codec::None);
        let segment = Object::segment(Position::FIRST);
        let commits = [0..3, 3..6, 6..9, 9..12];

        // The bytes of the segment, and where each chunk that the writer cut ends, with the entries up to it.
        let mut bytes = BytesMut::new();
        let mut chunks = Vec::new();
        let mut entries = 0;
        for commit in commits.clone() {
            for connection in commit {
                writing.append(closed(connection)).expect("the codec takes every entry");
            }
            for step in writing.commit().expect("the codec takes every entry") {
                let Step::Append(object, pieces) = step else {
                    continue;
                };
                if object != segment {
                    continue;
                }
                for piece in pieces {
                    if let Ok(chunk) = ChunkHeader::decode(&piece) {
                        entries += u64::from(chunk.entries);
                        let end = bytes.len() as u64 + ChunkHeader::LEN as u64 + chunk.body();
                        chunks.push((end, entries));
                    }
                    bytes.extend_from_slice(&piece);
                }
            }
        }
        let bytes = bytes.freeze();
        assert_eq!(entries, commits.len() as u64 * 3, "the chunks hold every entry");

        for cut in 0..=bytes.len() {
            let mut walk = SegmentWalk::new(Position::FIRST);
            feed(&mut walk, &bytes.slice(..cut));
            let whole = chunks
                .iter()
                .filter(|(end, _)| *end <= cut as u64)
                .map(|(_, entries)| *entries)
                .max()
                .unwrap_or_default();
            assert_eq!(
                walk.outline().entries,
                whole,
                "a segment cut at {cut} of {} bytes holds what its whole chunks hold",
                bytes.len()
            );

            feed(&mut walk, &bytes);
            assert_eq!(
                walk.outline().entries,
                entries,
                "a walk that stopped at {cut} goes on from there, and reads every entry once"
            );
        }
    }

    #[test]
    fn a_walk_stops_at_a_chunk_whose_bytes_fail_their_checksum() {
        let description = Description {
            environment: "orders-sim".parse().expect("a name"),
            started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
            clock: Instant::ZERO,
            mode: RecordingMode::Full,
            backend: RecordedBackend::Simulated,
            written_by: "0.1.0".to_owned(),
            armed: Vec::new(),
            scenario: None,
            journal_after: None,
        };
        let event = SessionEvent {
            connection: ConnectionNumber::new(7),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        };
        let mut writing = Writing::start(&description, SegmentConfiguration::new(), Codec::None);
        let segment = Object::segment(Position::FIRST);
        let mut bytes = Vec::new();
        let mut first_commit = 0;
        for _ in 0..2 {
            writing
                .append(Entry::session(&event))
                .expect("the codec takes every entry");
            for step in writing.commit().expect("the codec takes every entry") {
                if let Step::Append(object, pieces) = step
                    && object == segment
                {
                    for piece in &pieces {
                        bytes.extend_from_slice(piece);
                    }
                }
            }
            if first_commit == 0 {
                first_commit = bytes.len();
            }
        }

        // The last byte of the second chunk goes bad: it is in its entries, behind its header.
        let last = bytes.len() - 1;
        bytes[last] = !bytes[last];
        let mut walk = SegmentWalk::new(Position::FIRST);
        feed(&mut walk, &Bytes::from(bytes));

        assert_eq!(
            walk.outline().entries,
            1,
            "the walk keeps the chunk before the damaged one"
        );
        assert_eq!(
            walk.offset(),
            first_commit as u64,
            "the walk stands behind the last whole chunk"
        );
        assert_eq!(walk.flaw(), Some("the entries of a chunk fail their checksum"));
    }

    #[test]
    fn a_walk_over_a_sealed_segment_reads_its_footer() {
        let description = Description {
            environment: "orders-sim".parse().expect("a name"),
            started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
            clock: Instant::ZERO,
            mode: RecordingMode::Full,
            backend: RecordedBackend::Simulated,
            written_by: "0.1.0".to_owned(),
            armed: Vec::new(),
            scenario: None,
            journal_after: None,
        };
        let event = SessionEvent {
            connection: ConnectionNumber::new(7),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        };
        let end = End {
            ending: Ending::Stopped,
            at: Instant::ZERO,
            entries: 1,
            calls: 0,
        };
        let mut writing = Writing::start(&description, SegmentConfiguration::new(), Codec::None);
        writing
            .append(Entry::session(&event))
            .expect("the codec takes every entry");
        let segment = Object::segment(Position::FIRST);
        let mut bytes = BytesMut::new();
        for step in writing.finish(&end).expect("the codec takes every entry") {
            if let Step::Append(object, pieces) = step
                && object == segment
            {
                for piece in &pieces {
                    bytes.extend_from_slice(piece);
                }
            }
        }

        let mut walk = SegmentWalk::new(Position::FIRST);
        feed(&mut walk, &bytes.freeze());

        let outline = walk.outline();
        assert!(outline.sealed, "the walk read the footer of the segment");
        assert_eq!(outline.entries, end.entries);
        assert_eq!(
            outline.end,
            Some(end.encode()),
            "the footer of the last segment holds the end"
        );
        assert!(
            outline.spans.iter().all(|span| span.summary.is_some()),
            "every span of a sealed segment has its summary"
        );
        assert_eq!(walk.wanted(), None, "a walk that read the footer is over");
    }
}
