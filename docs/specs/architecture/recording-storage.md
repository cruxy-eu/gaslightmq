# Recording storage

This document says how a [recording](storage.md#recordings) is kept:
the contract that every storage of recordings keeps,
the first layout behind it, which keeps a recording as compressed segments with indexes and a manifest,
and what that layout asks of the place where its bytes live.

A recording only grows, and it can grow very large.
The design follows from seven things that it must do:

- **A session never pays for it.**
  A session hands over what it already has in memory, and encodes nothing.
- **A reader streams.**
  Every reader is a pass over a stretch of the recording, in order: a replay, the index that the engine rebuilds, the seed of a World, a query with a selector.
  Nobody reads one entry out of the middle of a recording, so nothing is built for that.
- **Nothing is copied twice.**
  A segment is compressed, so reading writes the bytes of an entry once: where the decompressor puts them.
  From there on a reader reads an entry through views, and a frame as a slice, and nothing decodes an entry into a type before someone asks for the type.
- **A query reads a little.**
  A query over a recording of many terabytes streams the blocks that can match, and not the recording.
- **Nothing grows without a bound.**
  Every object that holds traffic or describes it closes at a size, and never changes afterwards.
- **A recording that was cut short still opens.**
  A crash, a full disk or a full queue leaves a recording that is complete up to a known point.
- **An old recording still opens.**
  A recording that someone kept a year ago opens in the build of today.

## The pipeline

Writing and reading are the same four steps, in opposite order.
Three of them only turn bytes into bytes or into entries, and one does IO.

```text
write   entries ──▶ batch ──▶ compressor ──▶ chunks of a block ──▶ bytes of a segment ──▶ backing

read    backing ──▶ bytes of a segment ──▶ chunks of a block ──▶ decompressor ──▶ batch ──▶ views of entries
```

| Step | Does | In | IO |
|---|---|---|---|
| Entries and batches | Writes entries into a [batch](#a-batch), and reads a batch through views. | `recording` | No. |
| The codec | Compresses the bytes of a batch, and unpacks them again. It is a [seam](#the-codec): Zstandard is the first codec, and none is one. | `recording_segments`, and a crate for a codec | No. |
| The layout | Cuts a segment into blocks and chunks, keeps the index and the manifest, and finds its way after a crash. | `recording_segments` | No. It awaits the backing, and nothing else. |
| The backing | Keeps bytes, and hands them out as a stream. | `recording_segments_fs` | Yes. |

There are three seams, and each has a contract:

| Seam | Its contract speaks of | Behind it |
|---|---|---|
| [Recording storage](#the-storage-contract) | Recordings and streams of their entries: session events, calls with their frames, journal entries. It owns the bytes of an entry, and knows nothing of where they are kept. | A layout. The segment layout is the first, and another format is another implementation. |
| [The codec](#the-codec) | Bytes in, bytes out, as a stream. It knows no entry and no segment. | A way to compress. Zstandard is the first, and it is the default. |
| [Segment backing](#the-segment-backing) | The three kinds of object of the segment layout, as streams of bytes. It knows no entry. | A place for bytes. Files are the first, and memory serves the tests. |

| Crate | Holds | IO |
|---|---|---|
| `recording` | The storage contract and its errors. What a recording holds: its ID, its description, the entries, a position, a summary, how it ended. The [encoding of an entry](#a-batch), the views that read it, and the way from a view to the types of `call` and `journal`. What the [recording mode](#what-a-recording-must-not-keep) drops, and the blanking of credentials. | Sans-IO. |
| `recording_conformance` | What every storage does, as a check. | Test support. |
| `recording_segments` | The segment layout, as a storage over any backing: the framing of segments, blocks and chunks, the summaries of each level, the index, the manifest and when it changes, and recovery. The codec contract, and the codec that does nothing. The backing contract and its errors. The backing in memory, behind a feature. | Sans-IO. It awaits the backing it is handed, and nothing else. |
| `recording_segments_zstd` | The Zstandard codec. It is a crate of its own because it brings the library that compresses. | Sans-IO. |
| `recording_segments_conformance` | What every backing does, as a check. | Test support. |
| `recording_segments_fs` | The backing on files: it opens, appends, syncs, renames, maps and reads. | This crate does IO. |

`recording_segments` awaits its backing as `workspace` awaits [its stores](crates.md#the-store-seam):
its rules are plain functions over bytes, and the one part that is async hands bytes to the backing and takes bytes from it.

The engine holds a storage and names no layout.
Whoever puts a process together picks one: `gmq` picks the segment layout on files with Zstandard, and a test picks it in memory.

## What a recording holds

The contract owns the words of a recording.
No layout adds one, and none leaves one out.

| Thing | Is |
|---|---|
| Recording ID | The [environment and the instant it started](identity.md#recording-id). |
| Description | What the recording is. It is written once, when the recording starts. |
| Entry | One thing that the recording keeps: a session event, a call or a journal entry. |
| Batch | The entries of one commit, encoded together. A storage keeps a [batch](#a-batch) as it is, and hands it out as it is. |
| Position | Which entry of its recording this is: a number that counts from 0. A higher position is a later entry. |
| Summary | What a stretch of a recording holds, so a reader knows whether to read it. |
| End | How the recording ended: someone stopped it, or the recorder fell behind. A recording without an end still runs, or was cut short. |

### Description

| Key | Holds |
|---|---|
| `environment` | The name of the environment. |
| `started` | The instant the recording started, in UTC and to the millisecond. Whoever starts the recording hands it in: no crate of a recording reads a clock. |
| `clock` | The instant of the clock of the environment at that moment. Every entry tells its time on that clock, so this pair says when an entry was. |
| `mode` | The recording mode: `full` or `metadata-only`. |
| `backend` | `upstream` or `simulated`: what stood behind the proxy. |
| `written-by` | The version of GaslightMQ that wrote the recording. |
| `armed` | The names of the rules that were armed when the recording started, the highest priority first. |
| `scenario` | The scenario that was active when the recording started, and its step, when one was. |
| `journal-after` | The ID of the last journal entry before the recording started, when there was one. |

The journal entries of a recording begin where the recording begins.
`armed` and `scenario` hold what the earlier entries would have said,
so a recording shows which rules were armed when its first call arrived.
They hold names, as an entry of the journal does.

### Entries

An entry stands where the recorder took it, so the entries are in the order in which things ended:

- **A journal entry** stands where its change happened. The IDs of the journal entries of a recording rise, without a gap.
- **A call** stands where it ended, because its record exists from then on.
  A Fetch that waited for records stands behind the heartbeats that came and went in the meantime.
  The IDs of the calls of a recording therefore do not always rise.
  The call ID gives the order in which the requests arrived, and the timing of a record gives every moment of its call.
- **A session event** stands where it happened.

A recording holds the calls whose request arrived after it started and that ended before it stopped.

| Entry | Holds |
|---|---|
| Session event | `opened`: the connection, the instant, the address and the endpoint. `identified`: the `client.id`, when the first request carried one. `software-named`: the `client.software`. `closed`: the instant, and who closed it: the client, the backend, a lie or the proxy. |
| Call | The [call record](../domains/observation.md#call), and up to four frames: the request, the upstream response when the overlay changed it, the truth, and the delivered response. |
| Journal entry | The [journal entry](../domains/observation.md#journal), as its crate has it. |

A recording starts with an `opened` event for every session that is open at that moment.
Its instant is when the session really opened, which is before the recording started,
and it holds what the session has learned by then.
Every connection number in a recording therefore has its session in the same recording.

A frame of a recorded call is in one of four states:

| State | Means |
|---|---|
| None | The call has no such frame: a lie ended it before forwarding, or the client received nothing. |
| Whole | The recording holds the frame as it was on the wire. |
| Redacted | The recording holds the frame without [what it must not keep](#what-a-recording-must-not-keep). It still decodes. |
| Withheld | The frame existed, and the recording holds none of it. |

Every frame that existed keeps the length it had on the wire, whatever the recording holds of it.
The size of the traffic is metadata, and a metadata-only recording keeps it.

### Summary

A summary says what a stretch of a recording holds.

| Key | Holds |
|---|---|
| `from`, `to` | The earliest and the latest instant that an entry of the stretch holds. |
| `calls` | The number of call records, and the lowest and the highest call ID among them. |
| `journal` | The number of journal entries, and the first and the last of their IDs. |
| `sessions` | The number of session events. |
| `fields` | For every [field](../domains/targeting.md#fields) that a call of the stretch has: its values. |

`fields` is a map from the name of a field to one of two things:

- **The list of its values**: every value that a call of the stretch has for the field, for the call as a whole or for one of its parts, each value once.
- **The text `any`**: the stretch has more values than `recordings.summary_values`, which is `256` until someone sets it.

A field that no call of the stretch has is not in the map.

Two summaries add up to the summary of both stretches: the wider ranges, the sums of the counts, and the values of both, or `any`.
A layout therefore sums up at every level it has, with one rule.

A list of values answers every operator of a [condition](../domains/targeting.md#conditions):

| Condition | The stretch cannot match when |
|---|---|
| `is`, `is any of` | The field is not in the map, or its list holds none of the values. |
| `matches` | The field is not in the map, or the pattern matches no value of its list. |
| `is not` | Never. It also matches a call without the field. |
| Any, on a field that says `any` | Never. |

A summary only proves that a stretch cannot match.
The reader decides every call that it does read with the selector itself, as a rule would.
Whether a selector can match a set of values is matching, so that check belongs to `selector`, and `recording` hands out the values.

`topic` and `partition` are summed up apart, although a selector tests them on the same partition.
A stretch with `orders.eu/1` and `payments.settled/3` is therefore read for `orders.eu/3`, and yields nothing.

## The storage contract

```rust
/// Where recordings are kept. A layout implements it.
pub trait RecordingStorage {
    type Writer: RecordingWriter;
    type Reader: RecordingReader;

    /// Starts a recording. It exists from here on, and it is empty.
    async fn start(&self, description: Description) -> Result<Self::Writer, StartError>;
    async fn recordings(&self) -> Result<Vec<RecordingId>, ListError>;
    async fn open(&self, recording: &RecordingId) -> Result<Self::Reader, OpenError>;
}

pub trait RecordingWriter {
    /// Takes the entry, and says which position it has.
    async fn append(&mut self, entry: Entry) -> Result<Position, WriteError>;
    /// Every entry that was taken is in the recording when this returns.
    async fn commit(&mut self) -> Result<(), WriteError>;
    /// Commits, and ends the recording in order.
    async fn finish(self, end: End) -> Result<(), WriteError>;
}

pub trait RecordingReader {
    type Entries: RecordedEntries;

    fn description(&self) -> &Description;
    /// How far the recording goes, and whether it ended in order.
    async fn extent(&self) -> Result<Extent, ReadError>;
    /// The entries from a place on, in one direction, without the stretches that cannot hold what is wanted.
    async fn read(&self, from: Place, direction: Direction, wanted: &impl Wanted, frames: Frames)
        -> Result<Self::Entries, ReadError>;
}

pub trait RecordedEntries {
    /// The next batch, or `None` at the end. A batch is the buffer that the storage has the entries in.
    async fn next(&mut self) -> Result<Option<Batch>, ReadError>;
}

/// Entries where they lie. The batch shares its buffer with the storage, and its views borrow from the batch.
impl Batch {
    /// The entries that the reader asked for, in the order of the reading.
    pub fn entries(&self) -> impl Iterator<Item = (Position, EntryView<'_>)>;
}

pub enum EntryView<'a> {
    Session(SessionEventView<'a>),
    Call(CallView<'a>),
    Journal(JournalEntryView<'a>),
}

impl<'a> CallView<'a> {
    pub fn id(&self) -> Result<CallId, EntryError>;
    /// The fields of the call, as a source that a selector matches.
    pub fn fields(&self) -> Result<FieldsView<'a>, EntryError>;
    pub fn explanation(&self) -> Result<ExplanationView<'a>, EntryError>;
    /// The frame, as a range of the buffer of the storage, when the reader asked for frames.
    pub fn frame(&self, slot: FrameSlot) -> Result<RecordedFrame, EntryError>;
    /// The call as its type, built through its checks. This is the one place that copies.
    pub fn to_call(&self) -> Result<Call, EntryError>;
}

/// What a reader looks for, as far as a summary can tell.
pub trait Wanted {
    fn may_be_in(&self, summary: &Summary) -> bool;
}
```

| A storage | Keeps to |
|---|---|
| Starts | It refuses an ID that a recording has. A recording that started is listed, and opens. |
| Appends | It gives every entry the next position. One writer writes a recording, and nobody writes to a recording again once its writer is gone. |
| Commits | Every entry that it took is readable, and is still there after a crash of the process or of the machine. A storage may commit by itself, when what it holds reaches a size. |
| Finishes | The recording has an end. A recording whose writer went away without one was cut short, and opens all the same. |
| Reads | As a stream: from the start, from the end or from a position, forwards or backwards. A reader sees every entry that a commit covers, also while the recording runs. Entries come in the order of their positions, a batch at a time. |
| Skips | It asks `Wanted` about every summary it has, at every level it has one, and reads no stretch that cannot hold what is wanted. A storage without summaries asks nothing and hands out everything. What it hands out is never less than what is wanted. |
| Hands out frames | Only when the reader asks for them. A reader that asks for none gets every call with the states and the lengths of its frames, and no storage reads a frame for it. |
| Keeps a batch as it is | It stores the bytes of a batch as `recording` wrote them, compressed or not, and looks into none of them. |
| Hands out entries where they lie | A batch is the buffer that the entries were put in: by a decompressor, or by nobody when the storage keeps them plain and maps them. It starts at an address that is a multiple of 8. No storage builds a type for a reader. |
| Says what it lost | A stretch that it cannot read is an error that names its positions, and the reader can go on behind it. |

`Wanted` carries a selector to the storage without the storage knowing one.
The engine implements it with the check of `selector`, and with a range of time or of call IDs when a reader asks for one.
A reader that wants everything hands in one that says yes.

`recording_conformance` holds this table as a check, for any storage.

### Nothing is copied twice

A reader gets a batch, and a batch gives views.

- **A segment is compressed, so reading writes the bytes of an entry once.**
  The decompressor puts the bytes of a batch into a buffer.
  That is a copy, and no layout that compresses can do without it.
  Zero-copy here means what happens after it: nothing copies those bytes again, and nothing decodes them into a type, on the way to the consumer.
  It does not mean that an entry is read from the file where it lies.
- **A batch is that buffer, and its owners share it.**
  The trait hands out no slice with a lifetime, so it needs no lending types, and a batch outlives the call that returned it.
  A storage that keeps its batches plain and maps its files hands out a range of the map, and then nothing was copied at all.
- **A view borrows from its batch.**
  A view of a call, of its fields, of its explanation, of a line of it, of a journal entry or of a session event is a place in the buffer and nothing more.
  Reading a value through a view reads the bytes of that value: a number where it stands, a text as the bytes it is.
- **A frame is a slice of the same kind.**
  On the way in, the frame of an entry is the buffer that the session read from its socket.
  On the way out, it is a slice of the buffer that the frames of its batch were unpacked into, and it goes into the codec of Kafka and into a pipeline as a frame from a socket does.
- **A view checks what it reads, when it reads it.**
  A recording from a disk is input from outside.
  Every step from one part of an entry to the next checks that it stays inside the buffer, and every text that it is text.
  So a value that is damaged is an error of the read that meets it.
- **A view hands out values, and the types check them.**
  A view of the fields gives a topic as text and an error code as a number.
  `to_call` builds the `Call` through the checks of its types, and it is the one place where a call that could not have been one is found.
  A reader that shows a call, or hands it to the control plane, asks for the type. A reader that replays, matches, counts or seeds does not.
- **A selector matches a view.**
  The view of the fields is a field source, as a live call and a call record are.
  Matching without a copy needs a field source that lends its values, which `field` does not have yet: its readings own what they hold.
  Until it does, a selector matches the fields that a view builds, and no more of the call.

`recording` owns the encoding of an entry, and so the views.
A layout decides where a batch lies, how it is compressed, how it is found and what is said about it, and never what is in it.
That is what lets another layout exist: it keeps the same batches in another way, and every reader reads them with the same views.

### A batch

A batch is what one commit writes: the entries since the last commit.
It has two parts, and a storage keeps them apart:

| Part | Holds | A reader |
|---|---|---|
| Entries | The entries, one after the other. Each is a head of 8 bytes, with its length and its kind, and then a message, at a multiple of 8. | Streams them in order, and reads each through a view. |
| Frames | The frames of the calls of the batch, back to back. | Takes them when it asked for frames, and never touches them when it did not. |

- **An entry is one message.**
  A call is its ID, its connection, its fields, its moments, what the client received, its explanation, and for each of its four frames a slot: its state, its length on the wire, and where it is in the frames of the batch.
- **An entry stands by itself.**
  It names no other entry and no table of the batch.
  What repeats from call to call, such as a topic and the name of a rule, is left to the [codec](#the-codec), which does it better than a table of our own: an entry of 1658 bytes is [121 bytes](#the-encoding-measured) in a segment.
- **The frames stand apart for the readers that want none.**
  The index, a query and a seed of the structure of a cluster read entries and no frame.
  In a full recording the frames are most of the bytes, and such a reader neither fetches them nor unpacks them.

### Errors

The contract holds its errors, one for each thing the caller asks.
None of them names a file, a segment or a backing.

| The caller | Error | Variants, each a decision of the caller |
|---|---|---|
| Starts a recording | `StartError` | `AlreadyThere`: a recording has the ID. `NotStarted { reason }`: the storage could not keep it. |
| Appends, commits, finishes | `WriteError` | `NotWritten { reason }`. The recorder stops the recording, whatever stood in the way. |
| Lists | `ListError` | `NotListed { reason }`. |
| Opens | `OpenError` | `NotFound`. `WrittenByNewerVersion { written, known }`: the reader is too old. `NotReadable { reason }`: what is there is no recording. |
| Reads | `ReadError` | `Damaged { from, to }`: these positions are lost, and the reading goes on behind them. `NotRead { reason }`: the storage did not answer, and the reading can be tried again. |
| Reads a value through a view, or builds the type | `EntryError` | `Damaged { reason }`: the bytes are no entry, or they hold a value that its type does not take. The reader leaves the entry out, and says which position it had. |

## Handing frames to the recorder

A call record holds no frame.
A session already has every frame of a call in memory, each as one buffer that several owners can share:
the request it read, the answer of the backend, and the bytes it sends to the client.

- **While a recording runs**, the session keeps a handle on each of those buffers until the call ends.
  It hands the handles over with the record.
  A handle shares the bytes, so the session copies nothing and encodes nothing.
- **While nothing records**, the session keeps no handle, and a call costs what it costs today.

The recorder takes the record and the handles from its queue, on its own thread.
Everything that costs time happens there: redaction, encoding, compression, checksums, and the wait for the storage.
The bytes of a frame are copied once, into the compressor.

A frame that waits in the queue keeps its bytes in memory.
A count of calls does not bound that: ten thousand fetches can wait with ten gigabytes.
The queue is therefore full when it holds `recordings.queue_entries` entries,
or when the frames that wait hold `recordings.queue_size` bytes, which is `256MiB` until someone sets it.

The recorder commits when `recordings.commit_interval` has passed since the last commit, which is `1s` until someone sets it,
when the entries and frames since the last commit reach `recordings.commit_size`, which is `16MiB`,
and before someone reads the recording that runs.
The interval says how much a crash can lose, and it runs on the time of the system: a virtual clock can stand still, and a crash does not.

## What a recording must not keep

`recording` applies the [recording mode](storage.md#privacy) and blanks [credentials](storage.md#credentials-on-the-wire).
Nobody else does, so nobody can forget it, and no storage has to:

- An entry for a call is made in one way: from the record, the frames as they were on the wire, and the mode of the recording.
  That way redacts.
- A storage takes entries, so it never sees a frame that a recording must not keep.

For every frame, `recording` reads the API of its call from the header of the request, and then does one of three things.

| The frame | In a full recording | In a metadata-only recording |
|---|---|---|
| A `SaslAuthenticate` request, or a response to one | Redacted: its authentication bytes are empty. | The same. |
| A frame that holds records: a Produce request, and a Fetch or a `ShareFetch` response | Whole. | Redacted: every record is there without its key, its value and its headers. |
| A frame of any other API that the codec reads | Whole. | Whole. |
| A frame that the codec cannot read: its API is unknown, or its header or its body does not decode | Withheld. | Withheld. |

- **A redacted frame is a frame.**
  It decodes at the version it had, and it says everything else that it said.
  A record batch keeps its base offset, its number of records, its timestamps, and the ID, the epoch and the sequence of its producer.
  A record keeps its offset and its timestamp.
  The batch is kept without compression and with a new checksum.
  It keeps its attributes too, such as the type of its timestamps.
  The rewrite in `wire` loses the type of the timestamps today, because the encoder it uses drops it, and that is a gap to close.
- **The response to `SaslAuthenticate` is blanked too.**
  With SCRAM, what the server answers is enough to test guessed passwords against, next to what an attacker can guess.
- **A control batch stays whole.**
  It marks the end of a transaction, and holds nothing that a client wrote.
- **Whatever cannot be read is withheld, in a full recording too.**
  A frame that nobody could read can hold records, or a password, and nobody can tell.
  The price is that a full recording does not keep the one frame that the codec failed on: it keeps its length, and the call record says what GaslightMQ made of it.

`recording` needs the Kafka codec for this, and for nothing else.

## The segment layout

The segment layout keeps a recording as three kinds of object.
Each kind describes the one below it.

```text
manifest                      one, small, replaced as a whole
  └─ index chunk              up to `recordings.index_size` each
       └─ data segment        up to `recordings.segment_size` each, as it is stored
            └─ block          up to `recordings.block_size` of entries and frames: where a stream can start
                 └─ chunk     what one commit wrote, compressed
```

| Object | Holds | Changes |
|---|---|---|
| Data segment | The blocks of the recording, one after the other, and when it is sealed a footer that sums them up. | It grows by one chunk with every commit, until it is sealed. A sealed segment never changes. |
| Index chunk | For a series of segments: the summary of each one, and where each of its blocks is, with the positions, the time and the call IDs it covers and its summary. | It grows by one item with every segment that is sealed, until it is full. A full index chunk never changes. |
| Manifest | The description of the recording, the list of its index chunks, each with what it covers, and the end of the recording. | It is replaced as a whole when an index chunk begins, and when the recording ends. |

The segments are the recording.
An index chunk and the manifest hold nothing that the segments do not say:
a reader rebuilds either of them from the segments, and nothing is lost when one is missing.

### Blocks and chunks

A block is a stretch of a segment that a reader can start at.
A chunk is what one commit added to a block.

| | A chunk | A block |
|---|---|---|
| Is | The [batch](#a-batch) of one commit, compressed. | A series of chunks whose entries are compressed as one stream. |
| Closes when | The recorder commits. | Its entries and frames reach `recordings.block_size`, before they are compressed. |
| Is the unit of | What a crash can lose, and what a reader holds in memory: one batch. | Where a stream starts, what one summary covers, and what readers share out between them. |

The two were one thing in an earlier form of this layout, a block of 4 MiB, and that made the block as small as a commit.
They have different reasons, so they have different sizes.

- **Compression is by block, and not by segment.**
  A reader that starts in the middle of a segment starts at a block, with a codec that knows nothing yet.
  A segment that was compressed as one stream could only be read from its start: a gibibyte before the entry that someone wants.
- **A chunk goes on where the chunk before it stopped.**
  The entries of a block are one stream for the codec, which it flushes at every commit.
  A chunk of one second of light traffic is small, and it still compresses well, because the codec remembers the block so far.
- **The frames of a chunk are compressed by themselves.**
  A reader that asked for no frames steps over them by their length, and a reader that wants the frames of one call unpacks one chunk of them.
- **A torn tail is a chunk that is not whole.**
  Every chunk says its length and has a checksum of its bytes as they are stored.
  A reader checks both before it hands a byte to the codec, so the codec never sees what a crash left half written.
  The block that was open in a crash is good up to its last whole chunk.

What limits the size of a block, now that nothing decodes one as a whole:

| A larger block | By how much |
|---|---|
| Makes a reader unpack more before the entry it wants. A stream that starts at a position starts at the block that holds it. | 25 ms for 64 MiB, and 0.4 s for a gibibyte, at the [speed measured](#the-encoding-measured). |
| Makes one summary cover more, so it rules out less, and says `any` sooner. | In step with the size. |
| Loses more to one damaged chunk: the rest of its block, which the codec cannot read without it. | In step with the size. |
| Gives readers fewer pieces to share out. A block is what one reader takes while another takes the next. | In step with the size. |

| A larger block does not | Because |
|---|---|
| Compress better. | A call takes [99 bytes](#the-encoding-measured) in blocks of 1 MiB and 95 in blocks of 256 MiB. The codec looks back a few megabytes, and no further. |
| Read faster or slower. | The time to stream a recording was the same for blocks of 1 MiB to 256 MiB. |
| Cost memory. | The writer holds one commit, and a reader holds one batch. Neither holds a block. |
| Lose more in a crash. | A commit is a chunk, and a block is many. |

So the default is 64 MiB: sixteen times the earlier block, and small enough that starting anywhere costs 25 ms.

A block and a segment are not the same thing.
A segment is as large as one file should be, and it is counted as it is stored.
A block is as large as a stream can afford to skip, and it is counted before compression.
With the ratio of the measurement a segment of a gibibyte holds about two hundred blocks.

### The codec

The codec stands between a batch and the bytes of a chunk.
It is a seam: a recording says which codec wrote it, and a reader takes the codec from there.

```rust
/// One way to compress. A block says which one wrote it.
pub trait Codec {
    type Compressor: Compressor;
    type Decompressor: Decompressor;

    /// The number that a block names the codec by.
    fn id(&self) -> CodecId;
    /// A compressor that knows nothing yet: for a block, or for the frames of one chunk.
    fn compressor(&self) -> Self::Compressor;
    fn decompressor(&self) -> Self::Decompressor;
}

pub trait Compressor {
    /// Takes bytes. What it has ready goes after `out`.
    fn write(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError>;
    /// Lets go of everything it took, so a reader can unpack all of it. The stream goes on.
    fn flush(&mut self, out: &mut Vec<u8>) -> Result<(), CodecError>;
}

pub trait Decompressor {
    /// Unpacks the next chunk of the stream after `out`. Chunks come in the order they were written.
    fn read(&mut self, chunk: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError>;
}
```

| Codec | Is |
|---|---|
| Zstandard | The first codec, and the default. `recordings.compression_level` says how hard it works, and is `3` until someone sets it. A reader needs no level. |
| None | The bytes of a batch as they are. A backing that maps hands them out without a copy. |

- **A codec is a transform.**
  It takes bytes and gives bytes, and it does no IO, so a test drives it with bytes in memory.
- **A codec knows no entry.**
  It never sees where one entry ends.
  The layout flushes it at a commit, which is always between two entries.
- **A recording has one codec, and a block names it.**
  `recordings.compression` says which codec a new recording takes: `zstd` or `none`.
  A reader that meets a codec it does not know says so, and reads nothing of that block.
- **A codec fails in one way.**
  `CodecError::NotUnpacked { reason }` means that the bytes are not what the codec wrote.
  The layout reports the positions of the block as lost.

Why Zstandard at level 3, from the [measurement](#the-encoding-measured):
it takes a call from 1658 bytes to 121, costs 1.4 µs for a call on the thread that records, and unpacks 2.7 GB of entries a second.
From the disk a compressed recording streams faster than a plain one, because the disk has a fourteenth to read.

### What it takes from Iceberg

The model is the tree of metadata of Apache Iceberg: a root that lists small files of metadata, which describe the files of data and say what is in each, so a query prunes at every level.

| From that model | Here |
|---|---|
| Files never change once they are written. | A sealed segment and a full index chunk never change. |
| New metadata is written beside the old, and one step makes it the current one. | The manifest is replaced in one step: a reader sees the old one or the new one, and never a mix. |
| Every level says what is below it, with ranges, so a reader skips what cannot match. | The manifest sums up every full index chunk, and an index chunk sums up every segment and every block. |
| No file of metadata grows with the table. | An index chunk closes at a size. The manifest grows by one small item with every index chunk, which is one for about 600 GiB of segments. |

| Left out | Because |
|---|---|
| A catalog | A recording is where its ID says, and its manifest has one name. |
| Snapshots, and the history of the metadata | A recording only grows. Nobody asks what it looked like an hour ago. |
| Commits of several writers | A recording has one writer, so nothing has to settle who was first. |
| Rewriting, compacting and deleting data | Nobody changes a recording. |
| Columns | A reader streams entries whole. |
| A change of metadata for every write | A commit is one append to the segment that grows, and a sealed segment is one append to the index chunk that grows. The chunks at the end describe themselves, so nothing is rewritten for them. |

### Bytes

- **A number has a fixed size, and its lowest byte comes first.**
- **A length counts bytes.**
  A reader checks every length against the bytes that are there before it uses it.
  A recording can come from anywhere, so a reader trusts nothing that it says.
- **A checksum is a CRC-32C**, the checksum that Kafka gives its record batches.
  It covers bytes as they are stored, so a reader checks it before it unpacks anything.
- **What has a structure is a Cap'n Proto message, without packing.**
  A description, a summary, an entry, a footer, the items of an index chunk and the manifest are messages.
  A message is laid out as a reader reads it, so nothing is decoded: a reader follows a pointer to the part it wants.
- **A message starts at a multiple of 8, in the buffer it is read from.**
  For an entry that is the buffer of its batch, which the decompressor wrote: the entries of a batch keep their multiples of 8 in there, and where a chunk lies in a file does not matter.
  A footer, an item of an index chunk and the manifest are not compressed, and start at a multiple of 8 in their object.
- **What is a frame is the frame.**
  The bytes of a Kafka frame stand in the frames of a batch as they were on the wire, without the four bytes of their length.

The layout therefore has two layers.
Its framing is fixed binary: marks, lengths and checksums, which a reader finds without reading a message and without unpacking anything.
What it says is in messages.

### The schema

`recording` holds the schema of what the contract owns: an entry, a description and a summary.
`recording_segments` holds the schema of its own objects, which names those: a footer, the item of an index chunk, and the manifest.

- **A value is a number where it is one.**
  An API, an error code, an instant and a count are numbers, and not the text that a person reads.
- **The schema is a file of the crate.**
  A tool in another language reads a recording with it.
- **Writing is one conversion, and it is checked against the types.**
  `recording` writes an entry into a batch from the types of `call` and `journal`, and a view builds them again.
  A test writes values of every type and reads them back, so a type that grows without its message fails a test.

The control plane keeps its own encoding.
It sends a call from a recording as it sends a live one, from the type.

Every object starts with a mark of 8 bytes and the [version](#versions) of the layout.

### A data segment

```text
segment = segment header  block …  segment footer
block   = block header  chunk …  block footer
chunk   = chunk header  entries, as stored  frames, as stored
```

| Offset | Size | The segment header holds |
|---|---|---|
| 0 | 8 | The mark of a segment: the bytes `89 47 4D 51 53 45 47 0A`, which read `GMQSEG` between a byte that no text holds and a line feed. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | Zero. |
| 12 | 4 | The length of what follows. |
| 16 | … | A message: the [description](#description) of the recording, and `first`, the position of the first entry of the segment. |
| … | 4 | The checksum of every byte of the header before it. |

Every segment holds the description, so every segment says which recording it belongs to, and a recording without a manifest still knows what it is.

| Offset | Size | The block header holds |
|---|---|---|
| 0 | 4 | The mark of a block: `GMQB`. |
| 4 | 4 | The checksum of the bytes 8 to 23. |
| 8 | 8 | The position of its first entry. |
| 16 | 1 | The codec: `0` none, `1` Zstandard. |
| 17 | 7 | Zero. |

| Offset | Size | The chunk header holds |
|---|---|---|
| 0 | 4 | The mark of a chunk: `GMQC`. |
| 4 | 4 | The checksum of the bytes 8 to 31. |
| 8 | 4 | The number of its entries. |
| 12 | 4 | The checksum of its entries and its frames, as they are stored. |
| 16 | 4 | The length of its entries, as they are stored. |
| 20 | 4 | The length of its entries, unpacked. |
| 24 | 4 | The length of its frames, as they are stored. |
| 28 | 4 | The length of its frames, unpacked. |

A reader makes the room for a batch from the unpacked lengths, and the codec writes into it once.

The writer ends a block with a footer when the block is full, and a segment with a footer when it seals it.
A footer is a mark and a checksum, a message, and 16 bytes at its end with its own length and a mark, so a reader finds the footer of a sealed segment from the last bytes of the segment.

| Footer | Its message holds |
|---|---|
| Of a block: `GMQF` | The number of its entries, its lengths as stored and unpacked, the ranges of time and of call IDs it covers, and its [summary](#summary). |
| Of a segment: `GMQS` | The [summary](#summary) of the segment, the list of its blocks as the [index chunk](#an-index-chunk) has it, and the end of the recording in its last segment. |

A block without a footer is the one that still grows, or the one that a crash cut short, and so is a segment without one.

The end of a recording says how it ended: someone stopped it, or its environment stopped, or the recorder fell behind.
It holds the instant, and the numbers of the entries and the calls of the recording.

A recording that [stops by itself](storage.md#recordings) still ends in order.
The recorder writes everything it took from its queue, and then the footers with the end.
Its last entry is the journal entry that says why it stopped, and the end says it again, so whoever lists the recordings unpacks no entry.
A recording that could not be written has no end, because a recording cannot say that it could not be written.
The journal in memory and the control plane say it.

### An index chunk

An index chunk describes a series of segments, one after the other.

```text
index chunk = index header  item …
```

| Offset | Size | The index header holds |
|---|---|---|
| 0 | 8 | The mark of an index chunk: `89 47 4D 51 49 44 58 0A`, which reads `GMQIDX`. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 6 | Zero. |
| 16 | 8 | The position of the first entry that the chunk describes. |

An item is a length of 4 bytes, a checksum of 4 bytes, and a message that describes one segment.
It is the message of the footer of that segment:

| Key | Holds |
|---|---|
| `first` | The position of the first entry of the segment, which is the name of the segment. |
| `entries` | The number of its entries. |
| `length` | The length of the segment in bytes. |
| `summary` | The [summary](#summary) of the segment: the sum of the summaries of its blocks. |
| `blocks` | For every block of the segment: where it starts, its length as stored, the position of its first entry, the number of its entries, the ranges of time and of call IDs it covers, and its summary. |

An index chunk holds everything that a reader needs to pick a block, and to start a stream there.

An index chunk grows as a segment does: the layout appends an item when it seals a segment.
Nothing of an index chunk is written twice, and an item that a crash left half written fails its checksum, and is read again from the footer of its segment.

### The manifest

| Offset | Size | The manifest holds |
|---|---|---|
| 0 | 8 | The mark of a manifest: `89 47 4D 51 4D 41 4E 0A`, which reads `GMQMAN`. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | Zero. |
| 12 | 4 | The length of what follows. |
| 16 | … | A message. |
| … | 4 | The checksum of every byte before it. |

| Key | Holds |
|---|---|
| The keys of the [description](#description) | The description of the recording. |
| `indexes` | The index chunks of the recording, in order. Each one has `first`, the position of the first entry it describes. A full one also has `entries`, the number of entries it describes, and `summary`, the sum of the summaries of its segments. |
| `ended` | Once the recording ended in order: how, when, and the numbers of its entries and its calls. |

The manifest lists the index chunks, and says what each full one covers.
That is what the root of the model does, and it is what lets a reader go straight to the index chunk that holds a position or a moment, and past every index chunk that cannot match.
With a list of names alone, every reader would read every index chunk.

### What closes each level

| Level | Closes when | Property | Default |
|---|---|---|---|
| Chunk | The recorder commits: after an interval, or when the entries and frames that wait reach a size. | `recordings.commit_interval`, `recordings.commit_size` | `1s`, `16MiB` |
| Block | Its entries and frames reach a size, before compression. A chunk is never cut in two. | `recordings.block_size` | `64MiB` |
| Data segment | It reaches a size, as it is stored. A block is never cut in two. | `recordings.segment_size` | `1GiB` |
| Index chunk | The next item would take it over a size. | `recordings.index_size` | `256MiB` |
| Summary | A field has more values than a count, and says `any`. | `recordings.summary_values` | `256` |

Neither a count of entries nor an age closes a segment.
A thousand heartbeats and a thousand fetches differ a thousandfold in bytes, so a count bounds nothing.
An age is for a log that deletes its old segments, and nothing deletes a part of a recording.

What speaks against a larger value of each, and against a smaller one:

| Property | A larger value | A smaller value |
|---|---|---|
| `commit_interval` | Loses more in a crash, and leaves a reader of a recording that runs further behind. | Makes more chunks, and more waits for the disk. |
| `commit_size` | Holds more in memory: in the writer before a commit, and in a reader as one batch. | Commits more often under heavy traffic, for nothing. |
| `block_size` | See [blocks and chunks](#blocks-and-chunks). | Makes a larger index, and summaries of very little. |
| `segment_size` | Leaves more for a reader to walk after a crash, and makes a longer list of blocks in one item of the index. | Makes more files: a pebibyte is a million segments of a gibibyte. |
| `index_size` | Makes a reader of one position take in a larger object. | Makes a longer manifest. |
| `summary_values` | Makes a larger index. | Lets a summary say `any` for a field that a query asks for. |
| `compression_level` | Costs more on the thread that records, for a few bytes: level 1 stores 142 bytes for a call and level 3 stores 121. | Stores more. |

### Writing

The layout writes an entry into the batch that grows, and hands its bytes to the compressor of the block as it comes.
It holds what the compressor gave back since the last commit, and no more.

| When | The layout writes, in this order |
|---|---|
| The recording starts | The manifest, with the description and no index chunk. Then the first segment, with its header and the header of its first block. |
| A commit | It flushes the compressor, and appends one chunk to the segment that grows. |
| The block is full | It appends the footer of the block, and the header of the next one, with the next chunk. |
| The segment reached its size | It appends the footers of the block and of the segment, and seals the segment. It appends the item of the segment to the index chunk that grows. It creates the next segment. |
| The index chunk is full | It seals that index chunk, and creates the next one with the item that did not fit. Then it replaces the manifest, which lists the new one and sums up the full one. |
| The recording ends | A commit. The footers, with the end. Then it seals the segment, appends its item, seals the index chunk, and replaces the manifest, which now holds the end. |

Two rules make every crash harmless:

- **An object is written before the object that describes it.**
  Whatever an index chunk or the manifest names is there, whole and for good.
  After a crash, the most that is missing is a description, and a description can be rebuilt from below.
- **Every step leaves a recording.**
  The backing gives each write [what it promises](#the-segment-backing), so no step leaves the manifest half replaced, and what a crash leaves half appended fails a checksum.

A write that fails ends the recording.
The layout says so and takes nothing more.
Nobody opens a recording to write to it again: a new recording has a new [ID](identity.md#recording-id).

### Opening

A reader works from the top down, and trusts each level only as far as it reaches.

1. **The manifest.**
   It gives the description, the index chunks, and the end when there is one.
2. **The index chunks behind the manifest.**
   The reader lists the index chunks from the last one that the manifest names.
   One that the manifest does not name yet was written just before a crash, and counts.
3. **The segments behind the index chunks.**
   The reader lists the segments from the first position that no item describes.
   That is one segment while a recording runs, and two at most after a crash.
   A segment with a footer says everything about itself there.
   The reader walks a segment without one: block header, then chunk header after chunk header, and a block footer where a block has one.
   It checks every mark and every checksum, and that the whole chunk is there, and steps over the chunk by its length. It unpacks nothing.
4. **The end of the recording.**
   The end in the footer of the last segment means that the recording ended in order.
   Otherwise the last chunk that is whole is where the recording stops: it still runs, or it was cut short.

A recording that was cut short opens like any other.
The reader says that it was cut short, and how many bytes behind the last whole chunk it left unread.
It changes nothing in the backing.

The walk follows lengths that a checksum covers, and never searches for a mark,
so the bytes of a frame can never pass for the start of a chunk.

A block without a footer has no summary, and needs none: a reader streams it.

| What is missing or damaged | The reader |
|---|---|
| The manifest | Lists the index chunks and the segments itself, and takes the description from the header of a segment. |
| An index chunk, or an item of one | Reads the footers of the segments that it described. |
| A chunk in the middle of a block | Reports the positions from that chunk to the end of its block as lost, and goes on with the next block. |
| The end of the segment that grew | Stops at the last whole chunk. |

Recovery is a function: it takes what the backing lists and the bytes it reads, and returns what the recording holds.
It runs in a test without a file.

### How a stream starts

Every reader is a stream, and the levels above the segments say where it starts and what it leaves out.

1. It reads the manifest, and keeps the index chunks whose summary can hold what it wants.
2. It reads those index chunks, and keeps the segments, and then the blocks, whose summary can hold it.
3. It asks the backing for the bytes of a segment from the start of a block, takes them chunk by chunk, and hands each to the codec.
4. It hands out each batch as the codec fills it, and the consumer reads its entries through views.
5. It steps over the frames of a chunk when the reader asked for none.

| A reader that wants | Streams |
|---|---|
| Everything, as a replay does | Every block of every segment, in order. |
| A position, a moment or a call ID | The block that holds it, from its start, and the entries before it without a look at them. |
| The latest calls | The last block, and hands out its batches in turn from the last. |
| The calls a selector matches | The blocks whose summaries can match, and the selector decides every call in them. |

A summary gets coarser with every level, so the levels prune different things.
A range of time, of positions or of call IDs prunes at every level, down to a block.
A value that is rare prunes at every level too.
A value that half of the calls have prunes nowhere, and such a query streams the recording.

Blocks unpack without each other, so two readers take two blocks at once, and a stream over a long recording can be shared out between the cores of a machine.

### Sizes

The numbers below are estimates.
They take the [measured](#the-encoding-measured) ratio of entries without frames, 14 to 1, which real traffic and frames will change, and about 2 KiB of index for a block.

| | With the defaults |
|---|---|
| A chunk | One second of traffic, and 16 MiB at most before compression. |
| A block | 64 MiB of entries: about 40 000 calls, and about 5 MiB as stored. |
| A segment | 1 GiB as stored: about 200 blocks, 9 million calls, and 400 KiB of index. |
| An index chunk | 256 MiB: about 600 segments. |
| An item of the manifest | About 40 KiB for an index chunk. |

| A recording of, as stored | Segments | Blocks | Index chunks | All index chunks | The manifest |
|---|---|---|---|---|---|
| 100 GiB | 100 | 20 000 | 1 | 40 MiB | 1 KiB |
| 10 TiB | 10 240 | 2 million | 17 | 4 GiB | 700 KiB |
| 1 PiB | 1 048 576 | 210 million | 1700 | 420 GiB | 70 MiB |

- The index is about 0.04% of what it describes.
- Nothing of the index is written twice: a segment that is sealed adds 400 KiB to the index chunk that grows.
- The manifest is replaced once for every 600 GiB of segments.
- A recording of 10 TiB opens with one read of 700 KiB.
- A stream reads half a million calls a second on one core: a segment of a gibibyte in 18 seconds, and 10 TiB in two days. Blocks unpack without each other, so eight cores can share that out.
- A recording of a pebibyte is a million files in one directory. It wants a larger `segment_size`, which the property gives it.

## The segment backing

The segment layout asks one contract for a place to keep its bytes.
The contract speaks of the three kinds of object, and each is named by the position of the first entry it holds or describes.
It hands bytes out as a stream.

```rust
/// Where the objects of recordings live. The segment layout works over any of them.
pub trait SegmentBacking {
    type Bytes: ByteStream;

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListingError>;

    // Data segments and index chunks grow the same way. `Object` says which of the two, and its first position.
    async fn create(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError>;
    async fn append(&self, recording: &RecordingId, object: Object, pieces: &[Bytes]) -> Result<(), StoreError>;
    async fn seal(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError>;
    async fn list(&self, recording: &RecordingId, kind: ObjectKind, from: Position) -> Result<Vec<Listed>, ListingError>;
    /// The bytes of the object from an offset on.
    async fn read(&self, recording: &RecordingId, object: Object, from: u64) -> Result<Self::Bytes, FetchError>;

    // The manifest is replaced as a whole.
    async fn put_manifest(&self, recording: &RecordingId, manifest: Bytes) -> Result<(), StoreError>;
    async fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError>;
}

/// A place in an object that moves forward.
pub trait ByteStream {
    /// The next `wanted` bytes as one buffer, fewer at the end of the object, and `None` behind it.
    async fn next(&mut self, wanted: usize) -> Result<Option<Bytes>, FetchError>;
}
```

| Operation | Whole or not at all | When it returns |
|---|---|---|
| Create a segment or an index chunk | Yes. It refuses one that is there. | The object is there, empty, and stays after a crash. |
| Append | No. A crash or a failure can leave the first part of the pieces, and nothing else. It never changes a byte that was there. | Every byte is there after a crash of the process or of the machine. The pieces stand in the object in their order, as one stretch. |
| Seal | Yes. | The object never changes again, and takes no append. |
| List | — | Every object of a kind from a position on, in order, each with its length and whether it is sealed. It lists what is there after a crash. |
| Read | — | A stream of the bytes from an offset on. |
| Put the manifest | Yes. It makes the manifest or replaces it, and a reader gets the old one or the new one. | The new manifest is there after a crash. |
| Read the manifest | — | The manifest, whole. |

- **What a write returned is durable.**
  The layout builds its order of writing on that: it describes an object only once the backing has it for good.
- **An append may tear, and nothing else may.**
  The layout frames what it appends, so it finds the end of a torn segment and of a torn index chunk by itself.
  It frames nothing against a manifest that is half new, so the backing must never show one.
- **Reading is a stream that the layout pulls.**
  The layout always knows how much it wants next: the 32 bytes of a chunk header, and then the bytes that the header names.
  It asks for that many, so a chunk comes as one buffer, and the layout joins nothing.
  A reader that starts at a block opens the stream at the offset that the index gives.
- **A piece of a stream is a shared buffer.**
  The backing on files maps a sealed segment and hands out ranges of the map, so the compressed bytes go from the cache of the system into the codec without a copy on the way.
  A backing that fetches hands out what it fetched.
- **An append takes pieces.**
  The layout hands over the headers and the compressed bytes as the buffers they are, and never joins them.
- **A backing knows no entry and no codec.**
  It keeps bytes under names, and it never looks into them.

### Errors

| The layout | Error | Variants, each a decision of the layout |
|---|---|---|
| Creates, appends, seals, puts | `StoreError` | `AlreadyThere`: for an object that exists. `NotStored { reason }`. The layout ends the recording with either. |
| Lists | `ListingError` | `NotListed { reason }`. |
| Reads | `FetchError` | `Missing`: the backing has no such object, so the layout rebuilds a description from below, or reports the positions of a segment as lost. `NotFetched { reason }`: the backing did not answer, and the layout hands that on as a reading that can be tried again. |

The layout turns each of them into an error of the [storage contract](#errors), in the words of a recording.

### Backings

| Backing | Keeps the objects | Crate |
|---|---|---|
| Files | As files in one directory for each recording. See [files](#files). | `recording_segments_fs` |
| Memory | In memory, for tests, and for an engine whose recordings need not outlive it. | `recording_segments`, behind a feature |
| An object store | Later. A sealed segment, a full index chunk and the manifest are one object each, and a stream is a read of a range. The object that grows is what such a backing must solve, for instance as parts that a seal joins. | — |

`recording_segments_conformance` holds what every backing does, as a check.
The tests of the backing in memory run it, and so do the tests of the backing on files, so files are held to what memory does.

## Files

The backing on files keeps the recordings of a workspace under `recordings.directory`, one directory each.
The name of the directory is the [ID of the recording](identity.md#recording-id).

```text
recordings/
└── orders-sim.2026-11-21T14-02-11.527Z/
    ├── manifest
    ├── 00000000000000000000.index.open
    ├── 00000000000000000000.segment
    ├── 00000000000008912345.segment
    └── 00000000000017840022.segment.open
```

A name is the position of the first entry, with 20 digits, so the names sort as the positions do.

| Operation | On files |
|---|---|
| Create | Creates `….segment.open` or `….index.open`, and syncs the directory. |
| Append | Writes the pieces with one write, and syncs the data of the file. |
| Seal | Syncs the file, renames it to the name without `.open`, and syncs the directory. |
| Put the manifest | Writes a file beside the old one, syncs it, renames it over the old one, and syncs the directory. |
| Read | Maps a sealed file once, and hands out ranges of the map. It reads from a file that grows into buffers. |
| List | Reads the names of the directory. |

A file with another name is no object of the recording, and the backing leaves it alone.
The file that a `put` did not rename yet is such a file.

Mapping a file is unsafe code, and it is in this crate alone.
A sealed file never changes, which is what makes a map of it sound.

A recording is a directory, and it can hold many gigabytes.
Whether a recording also exports to one file is [not decided](../open-questions.md#storage).

## Versions

Every object holds the version of the layout.

- **A reader opens every version up to its own.**
  It keeps the reading code of every version that was ever released.
- **A reader refuses a version that is newer than itself**, and the message names both versions.
- **Every change of the layout takes a new number**:
  a new kind of entry, a new state of a slot, a new level, a new field or a new value in a message.
  A new codec takes a new number of its own, which a block names, and no new version.
- **A message only grows.**
  A later version adds a field that a reader of an old recording does without, or a value that an old recording never has.
  A field keeps its number, its type and its meaning for good, and a new field takes the next number.
  An old recording then reads with the schema of today: a field that it does not have reads as its default.
  When a message must change in another way, the reader keeps the old schema for the old version.
- **The bytes that are zero today are free for a later version.**
- **Version 1 is fixed with the first release.**
  Until then it changes with the types it holds, and no recording of an earlier build has to open.

The tests of `recording_segments` keep one small recording of every released version, and open each of them.
That makes "an old recording still opens" a check, and not a promise.

A new layout is not a new version.
It is another implementation of the storage contract, and the contract does not change for it.

## The browser

Nothing of these crates runs in the browser.
`recording` needs the Kafka codec, which never [compiles to WebAssembly](crates.md#what-compiles-to-webassembly),
and a browser opens no file: the UI asks the control plane, and the engine answers from the recording.

What the UI shares is the type of an entry.
A call record and a journal entry are the types of `call` and `journal`, which the UI has already.
The control plane sends a call from a recording in the encoding it sends every call in.

## Who reads a recording

| Reader | Streams | The storage gives it |
|---|---|---|
| [Replay](crates.md#the-pipeline-is-sans-io) | Every entry in order, with the request and the truth of every call. | The rules that were armed at the start, the journal entries that changed them, the sessions, and the frames as slices, which go into a pipeline without a copy. The view of every call holds the verdict and the explanation to compare with. |
| The [index](../domains/observation.md#index), which the engine rebuilds when it opens a recording | The fields of every call, once. | The entries of every block in order, and no frame: it neither fetches nor unpacks one. The index of the engine is not stored. |
| A [query with a selector](control-plane.md#drafts), saved or draft, from the UI, the CLI or a test | The entries of the blocks that can match. | `Wanted`, which skips at every level. The manifest and the index chunks that a query read stay in memory, so a draft that changes with every key reads them once. |
| A [test](control-plane.md#tests) that asserts on verdicts and explanations | The same. | The explanation is a part of every call. |
| The UI that shows the latest calls | The last block, backwards. | A stream from the end. |
| [Seeding a World](../domains/world.md#seeding) | The entries, and the frames of the calls that describe the cluster or carry records. | `Wanted` for `Metadata`, Produce and Fetch calls, and a stream with frames. |

A metadata-only recording serves all of them.
Its frames still decode, so a replay runs and a World gets its topics, its partitions and its offsets.
It gives no record contents: a seeded World has no records from it, and a lie that changes records cannot be replayed on it.

A call whose truth is withheld cannot be replayed, and the reader sees that in the state of its frame.

### What a replay still needs

A recording holds names, and no definitions.
A replay takes the definitions of the rules from a workspace.

A call record does not say which snapshot of the armed rules its call saw.
The instants of the calls and of the journal entries relate the two, and two things at one instant have no order between them.
The design of replay settles what it needs.
A key in the call record or in the description carries it.

## What a recording does not hold

| Not in a recording | Because |
|---|---|
| The index of the engine, with its facets | The engine rebuilds it when it opens the recording. |
| The definitions of rules, selectors and presets | The journal names a definition, and the workspace holds it. |
| The request as the backend saw it | No reader of today needs it: a replay gets it from the pipeline it drives. A later version adds a slot when a reader does. |
| A client's credentials | `recording` [blanks](#what-a-recording-must-not-keep) them in every recording. |
| A run ID or a revision of its own | No file holds a run ID. A journal entry holds the revision that its change left. |

## Properties

```yaml
# gaslight.yaml
recordings:
  directory: recordings
  queue_entries: 10000      # the entries that may wait for the recorder
  queue_size: 256MiB        # the bytes of the frames that may wait with them
  commit_interval: 1s       # how long an entry waits at most before it is in the recording for good
  commit_size: 16MiB        # the entries and frames that wait for a commit at most, before it comes early
  compression: zstd         # the codec of a new recording: zstd, or none
  compression_level: 3      # how hard the codec works, in the levels of the codec
  block_size: 64MiB         # the entries and frames of one block, before they are compressed
  segment_size: 1GiB        # the size at which a data segment is sealed, as it is stored
  index_size: 256MiB        # the size at which an index chunk is full
  summary_values: 256       # the values of one field that a summary lists
```

A size is a number of bytes with its unit.
`queue_entries`, `queue_size`, `commit_interval` and `commit_size` are the recorder's, whatever the layout is.
The others are the segment layout's.
A recording says its codec itself, so a reader needs none of these.

## Decisions

| Question | Decision | What it replaced, and why |
|---|---|---|
| Is a recording tied to one format? | No. A storage contract speaks of entries, and a layout implements it. | A reader and a writer of one file format. A second format would have been a second set of readers. |
| What is the first layout? | Data segments of compressed blocks, index chunks that describe them, and a manifest that lists the index chunks. | One file with a summary in every chunk. A query had to walk every chunk of it, and a recording could not grow past one file. |
| How does a reader read? | As a stream: every reader is a pass over the blocks that the summaries leave. | Reading one entry, or one field, out of the middle. No reader does that, and a layout that serves it pays for it in every stream. |
| Is a segment compressed? | Yes, by block, behind a seam. Zstandard at level 3 is the default, and none is a choice. | No compression: fourteen times the bytes, and slower from the disk. Compression by segment: a reader could only start at the start of a segment. |
| What does zero-copy mean? | Nothing copies or decodes an entry after the decompressor wrote it. A reader reads it through views, and a frame as a slice. | Reading an entry from the file where it lies, which compression rules out. Decoding every entry into its type, which most readers never look at. |
| Which encoding? | Framing of our own, and Cap'n Proto messages, without packing, for what has a structure. See [the encoding, measured](#the-encoding-measured). | FlatBuffers: a check of every entry before the first read, which makes it the slowest stream of the three. An archive of the layout of the types in memory: three times as fast, with no way to grow a type and no reader in another language. Protocol Buffers: ahead of Cap'n Proto on every number, and decoded. JSON and CBOR: far behind. |
| Who owns the encoding of an entry? | The contract. A layout keeps batches and never looks into one. | The layout. Views would be the layout's, and a reader would be written against one layout. |
| What does the manifest say about an index chunk? | Where it starts, and for a full one what it covers and the sum of its summaries. | A list of names alone. A reader would read every index chunk to find a position, a moment or a topic. |
| Where do the bytes of the layout live? | Behind a third contract of three kinds of object, which hands out streams. Files are the first backing. | A layout that opens files itself. It could not be tested without a disk, and could never move to an object store. |
| What closes each level? | A size, and for a chunk an interval. | A count of entries, which bounds neither memory nor loss. An age for segments, which only a log with retention needs. |
| Is a block a commit, or a segment? | Neither. A commit writes a chunk, a block is where a stream can start, and a segment is a file. | A block of 4 MiB that was also the commit. It was small for a reader that decodes a block as a whole, and no reader does. |
| What is in a summary? | The values of every field, as lists with a limit. | Bloom filters, which answer `is` and cannot answer `matches`. Ranges of values, which no operator asks for. |
| What is left after a crash? | Everything that was committed. The segments describe themselves, so a missing index chunk or manifest is rebuilt from them. | A manifest that must be right. A crash between two writes would then lose what the segments still hold. |
| How does metadata change? | An object is written before what describes it. Segments and index chunks only grow, and the manifest is replaced in one step. | An index chunk that is written again for every segment: more to write with every level of size. |
| Where is the recording mode applied? | In `recording`, where an entry is made. A storage never sees what must not be kept. | In the engine, where every caller would have to remember it. In a layout, where every layout would have to. |
| What does metadata-only keep of a frame? | The frame, without the keys, values and headers of its records. | No frame at all. The usual recording of a real cluster could then seed no World and run no replay. |
| Is a recording one file? | No. It is a directory of segments, and whoever reads it reads it through the storage contract. Whether it exports to one file is not decided. | One file for each recording. A recording can be many gigabytes. |

### The encoding, measured

One call record in six encodings, built from the real types: a Produce for two partitions with three lines of explanation, and a Fetch for five partitions with two.
The calls differ as traffic does: a few hundred clients, connections and topics that come back, and moments, latencies and counts that do not.
Frames are not a part of it, and every read starts from bytes that nobody trusts.

The machine: an AMD Ryzen 7 255 with 8 cores and 7 GiB of memory, Debian 13, ext4 on a disk that gave 0.8 GiB a second.
The code was built for that processor.
Every number is the median of its runs, on one core.
A read from the disk starts with none of the pages of the file in memory.

**A stream of a gibibyte of entries**, in blocks of 64 MiB, each compressed with Zstandard at level 3.
The reader unpacks a block and reads every entry: through a view where the encoding has views, and by decoding it where it has none.

| | Bytes of an entry | As stored | Write: encode, compress | Whole entries a second | From the disk | Their fields alone | To a `Call` |
|---|---|---|---|---|---|---|---|
| JSON | 1554 | 135 | 4.5 µs, 1.6 µs | 147 000 | 144 000 | 568 000 | 145 000 |
| CBOR | 1200 | 118 | 5.1 µs, 1.3 µs | 92 000 | 91 000 | 118 000 | 91 000 |
| Protocol Buffers | 648 | 95 | 3.0 µs, 1.0 µs | 635 000 | 609 000 | 1 401 000 | 268 000 |
| Cap'n Proto | 1658 | 121 | 3.2 µs, 1.4 µs | 505 000 | 479 000 | 956 000 | 202 000 |
| FlatBuffers | 1449 | 108 | 4.4 µs, 1.3 µs | 309 000 | 301 000 | 336 000 | 163 000 |
| An archive of the layout of the types in memory | 1780 | 112 | 3.1 µs, 1.3 µs | 1 482 000 | 1 312 000 | 1 525 000 | 270 000 |

The first three are decoded, and are here to compare with.
The last three are read through views of the buffer that the decompressor wrote.

**The same stream without a codec**, where a reader reads the mapped file itself:

| | As stored | Whole entries a second | From the disk | Their fields alone | To a `Call` |
|---|---|---|---|---|---|
| JSON | 1554 | 156 000 | 138 000 | 786 000 | 156 000 |
| CBOR | 1200 | 95 000 | 89 000 | 124 000 | 95 000 |
| Protocol Buffers | 648 | 741 000 | 589 000 | 2 199 000 | 294 000 |
| Cap'n Proto | 1658 | 670 000 | 451 000 | 1 722 000 | 221 000 |
| FlatBuffers | 1449 | 364 000 | 286 000 | 390 000 | 173 000 |
| An archive of the layout in memory | 1780 | 4 639 000 | 516 000 | 5 102 000 | 294 000 |

**What the codec costs and saves**, for Cap'n Proto:

| | As stored | Compress, for an entry | Whole entries a second | From the disk |
|---|---|---|---|---|
| None | 1658 | — | 670 000 | 451 000 |
| Zstandard, level 1 | 142 | 1.3 µs | 496 000 | 471 000 |
| Zstandard, level 3 | 121 | 1.4 µs | 505 000 | 479 000 |

Zstandard unpacks 2.7 GB of these entries a second, which is 0.6 µs of the 2.0 µs that a whole entry takes.

**What the size of the stretch does**, for Cap'n Proto at level 3, as milliseconds from memory and from the disk:

| 64 MiB of entries | 256 MiB | 1 GiB |
|---|---|---|
| 81, 85 | 328, 340 | 1290, 1360 |

**What the size of a block does**, as stored bytes for a call, with 256 MiB of entries at level 3:

| Blocks of | 1 MiB | 4 MiB | 16 MiB | 64 MiB | 256 MiB |
|---|---|---|---|---|---|
| Protocol Buffers | 99 | 97 | 95 | 95 | 95 |
| Cap'n Proto | 123 | 123 | 121 | 121 | 121 |

The time to stream them was the same for every size of block.

What the numbers say:

- **Compression levels the sizes.**
  The four that matter differ nearly threefold before the codec, and by a quarter after it.
  A layout of our own that held each text once would save what the codec saves anyway.
- **A compressed recording is not slower to read.**
  From the disk it is faster than a plain one, because the disk has a fourteenth of the bytes to give.
  From memory it costs a quarter of the speed.
- **The time grows with the size, in a straight line, and the size of a block changes nothing.**
  Nothing in a stream asks for a small block, and nothing in the codec asks for a large one.
- **Reading through views is not faster by itself.**
  Cap'n Proto checks every step that a reader takes, and a reader of a whole entry takes them all: 2.0 µs for an entry, against 1.6 µs to decode the same entry from Protocol Buffers.
  Only the archive reads a whole entry faster than a decoder does, three times as fast.
- **A reader of the fields alone gains, where the encoding lets it.**
  Cap'n Proto reads twice as many entries when it leaves the explanation alone.
  FlatBuffers gains nothing: it checks a whole entry before the first read, at 2.4 µs.
- **To a `Call`, every encoding is slow, and they are close.**
  Building a `Call` from plain values through its checks takes 2 µs, whatever the encoding is.
  A reader that wants the type gains nothing from views.

What decides between the three that are read through views is what each asks before a reader may trust what it reads:

| | Checks | Grows | Asks of the address | Read in other languages |
|---|---|---|---|---|
| Cap'n Proto | Every step, when a reader takes it. No pass over the message, and no unsafe code in a reader. | A field takes the next number, and an old message reads with the new schema. | A multiple of 8. | Java, Python, C++, Go and more. |
| FlatBuffers | The whole entry, before the first read. Without it a reader is unsafe code. | A table takes a field at its end, and an old buffer reads with the new schema. | Nothing, in this implementation. | Java, Python, C++, Go and more, each from the project itself. |
| An archive of the layout in memory | The whole archive, before the first read: 0.15 µs for an entry. | It does not. A type that changes is another layout, and the reader keeps every old type for good. Its bytes are also those of one version of the library that writes them. | A multiple of 8. | None. |

- **Cap'n Proto is the encoding that is read through views and can grow, at a speed that a stream can live with.**
  It is not the fastest and not the smallest.
- **Protocol Buffers is ahead of it on every number of the first table**, and is the choice if views are not a requirement.
- **The archive is three times as fast**, and is the choice if a recording never has to open in a later build or in another tool.

The second schema is the same work in all of them: a message for every type of an entry, and the way from the types to a message and back.
For the calls of this measurement that was about 300 lines for the types and 300 to 500 for each encoding, and `Explanation` is most of it.

An earlier round measured a search for one field of every entry.
A layout that keeps the fields of a block side by side answers it thirty times faster than any of these.
No reader asks for it, and a stream reads whole entries, so the layout does not do it.
