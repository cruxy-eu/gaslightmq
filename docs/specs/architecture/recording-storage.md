# Recording storage

This document says how a [recording](storage.md#recordings) is kept:
the contract that every storage of recordings keeps,
the first layout behind it, which keeps a recording as compressed segments with indexes and a manifest,
and what that layout asks of the place where its bytes live.

A recording only grows, and it can grow very large.
The design follows from eight things that it must do:

- **A session never pays for it.**
  A session hands over what it already has in memory, and encodes nothing.
- **A reader streams.**
  Every reader is a pass over a stretch of the recording, in order: a replay, the index that the engine rebuilds, the seed of a World, a query with a selector.
  Nobody reads one entry out of the middle of a recording, so nothing is built for that.
- **Nothing is copied twice.**
  A segment is compressed, so reading writes the bytes of an entry once: where the decompressor puts them.
  From there on a reader reads an entry through a view, and a frame as a slice, and nothing builds the type of an entry before someone asks for the type.
- **Memory has a budget.**
  A writer and a reader each hold a number of bytes that a property names, whatever the size of the recording.
  What a reader holds is a few blocks, so a block is small.
- **A query reads a little.**
  A query over a recording of many terabytes streams the blocks that can match, and not the recording.
- **Nothing grows without a bound.**
  Every object that holds traffic or describes it closes at a size, and never changes afterwards.
- **A recording that was cut short still opens.**
  A crash, a full disk or a full queue leaves a recording that is complete up to a known point.
- **An old recording still opens.**
  A recording that someone kept a year ago opens in the build of today.
  It says the version that it was written in, and a reader keeps the way from every version to its own.

## The pipeline

Writing and reading are the same steps, in opposite order.
One of them does IO, and the others turn bytes into bytes, or bytes into entries.

```text
write   entries ──▶ encoder ──▶ compressor ──▶ chunks of a block ──▶ backing

read    backing ──▶ stream of bytes ──▶ decompressor ──▶ stream of blocks ──▶ decoder ──▶ stream of entries
```

A reader is a pipeline of streams, and each stream pulls from the one before it:

1. **The backing gives a stream of bytes**: the bytes of a segment from an offset on, as they are stored.
2. **The codec turns it into a stream of blocks.**
   It unpacks the chunks of a block into one buffer, which is the only place where the bytes of an entry are written.
3. **The decoder turns a block into a stream of entries.**
   It [checks the block](#the-check-of-a-block) once.
   From there an entry is a [view](#nothing-is-copied-twice) into the buffer of its block, and a frame is a slice of the buffer of its frames.

A block that an older build wrote takes one more step, between the codec and the decoder:
`recording` writes it again in the [version](#versions) of this build, and the decoder reads that.

| Step | Does | In | IO |
|---|---|---|---|
| The encoder and the decoder | Write entries into the bytes of a block. Check the buffer of a block, and read it as entries. Convert a block of an older version. | `recording` | No. |
| The codec | Compresses the bytes of a block, and unpacks them again. It is a [seam](#the-codec): Zstandard is the first codec, and none is one. | `recording_compression` | No. |
| The layout | Cuts a segment into blocks and chunks, keeps the index and the manifest, holds the readers to their [memory](#memory), and finds its way after a crash. | `recording_segments` | No. It awaits the backing, and nothing else. |
| The backing | Keeps bytes, and hands them out as a stream. | `recording_segments_fs` | Yes. |

There are three seams, and each has a contract:

| Seam | Its contract speaks of | Behind it |
|---|---|---|
| [Recording storage](#the-storage-contract) | Recordings and streams of their entries: session events, calls with their frames, journal entries. It owns the bytes of an entry, and knows nothing of where they are kept. | A layout. The segment layout is the first, and another format is another implementation. |
| [The codec](#the-codec) | Bytes in, bytes out, as a stream. It knows no entry and no segment. | A way to compress, out of a closed set. Zstandard is the first, and it is the default. A reader takes the codec at run time, from the number that the header of a block names it by. |
| [Segment backing](#the-segment-backing) | The three kinds of object of the segment layout, as streams of bytes. It knows no entry. | A place for bytes. Files are the first, and memory serves the tests. |

| Crate | Holds | IO |
|---|---|---|
| `recording` | The storage contract and its errors. What a recording holds: its ID, its description, the entries, a position, a summary, how it ended. The [encoding](#a-message) of every message of a recording, as stored types for every [version](#versions), with the way from each version to the next. The [check of a block](#the-check-of-a-block), the views, and the way from a view to the types of `call` and `journal`. What the [recording mode](#what-a-recording-must-not-keep) drops, and the blanking of credentials. | Sans-IO. |
| `recording_conformance` | What every storage does, as a check. A storage in memory, which is the smallest storage that keeps the contract: the check is held to it, and it is no layout. | Test support. |
| `recording_segments` | The segment layout, as a storage over any backing: the framing of segments, [spans](#spans), blocks and chunks, the [records](#records) of each level, the index, the manifest and when it changes, recovery, and the [memory budgets](#memory). It has no message of its own. The backing contract and its errors. The backing in memory, behind a feature. | Sans-IO. It awaits the backing it is handed, and nothing else. |
| `recording_compression` | The [codecs](#the-codec): a compressor and a decompressor over a closed set of codecs, which is none and Zstandard with its level. A block is a frame of its own. It is a crate of its own because it brings the library that compresses. | Sans-IO. |
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
| Batch | A series of entries in one buffer: what a storage hands out at a time. A storage keeps the bytes of a [batch](#a-batch) as they were written. In the segment layout a batch is a [block](#blocks-and-chunks). |
| Message | The bytes of one entry, description, summary or end, in the [encoding](#a-message) that `recording` owns. |
| Position | Which entry of its recording this is: a number that counts from 0. A higher position is a later entry. |
| Summary | What a stretch of a recording holds, so a reader knows whether to read it. |
| End | How the recording ended: someone stopped it, its environment stopped, or the recorder fell behind. A recording without an end still runs, or was cut short. |

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
| Session event | `opened`: the connection, the instant, the address and the endpoint, and what the session has learned by then when the recording started later: its `client.id` and its `client.software`. `identified`: the `client.id`, when the first request carried one. `software-named`: the `client.software`. `closed`: the instant, and who closed it: the client, the backend, a lie or the proxy. |
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
pub trait RecordingStorage: Send + Sync {
    type Writer: RecordingWriter;
    type Reader: RecordingReader;

    /// Starts a recording. It exists from here on, and it is empty.
    async fn start(&self, description: Description) -> Result<Self::Writer, StartError>;
    async fn recordings(&self) -> Result<Vec<RecordingId>, ListError>;
    async fn open(&self, recording: &RecordingId) -> Result<Self::Reader, OpenError>;
}

pub trait RecordingWriter: Send {
    /// Takes the entry, and says which position it has.
    async fn append(&mut self, entry: Entry) -> Result<Position, WriteError>;
    /// Every entry that was taken is in the recording when this returns.
    async fn commit(&mut self) -> Result<(), WriteError>;
    /// Commits, and ends the recording in order.
    async fn finish(self, end: End) -> Result<(), WriteError>;
}

pub trait RecordingReader: Send + Sync {
    type Entries: RecordedEntries;

    fn description(&self) -> &Description;
    /// How far the recording goes, and whether it ended in order.
    async fn extent(&self) -> Result<Extent, ExtentError>;
    /// The entries from a place on, in one direction, without the stretches that cannot hold what is wanted.
    async fn read(&self, from: Place, direction: Direction, wanted: impl Wanted, frames: Frames)
        -> Result<Self::Entries, ReadError>;
}

pub trait RecordedEntries: Send {
    /// The next batch, or `None` at the end. A batch is the buffer that the storage has the entries in.
    async fn next(&mut self) -> Result<Option<Batch>, NextBatchError>;
}

/// One thing that a recording keeps. Making it encodes it, and for a call it redacts.
impl Entry {
    pub fn session(event: &SessionEvent) -> Entry;
    pub fn call(call: Call, frames: CallFrames, mode: RecordingMode) -> Result<Entry, CallEntryError>;
    pub fn journal(entry: &JournalEntry) -> Entry;
}

/// Writes the entries of one batch, one after the other. A storage starts a new one with every batch.
impl BatchEncoder {
    pub fn new() -> BatchEncoder;
    /// The bytes of the entry as they stand in the batch, and its frames, each as a shared buffer.
    pub fn encode(&mut self, entry: Entry) -> EncodedEntry;
}

/// The room for the entries of one batch. It starts at an address that is a multiple of 16, it has its size
/// from the start, and it never moves.
impl BatchBuffer {
    pub fn with_room(bytes: usize) -> BatchBuffer;
    /// Calls `released` once, when the last holder of the buffer lets go of it.
    pub fn when_released(self, released: impl FnOnce() + Send + Sync + 'static) -> BatchBuffer;
    /// The room that is left, and how much of it was written.
    pub fn unwritten(&mut self) -> &mut [u8];
    pub fn advance(&mut self, bytes: usize);
}

/// Entries where they lie. The batch shares its buffers with the storage, and its views borrow from the batch.
impl Batch {
    /// Takes the buffers that a storage filled, and checks the entries once. `version` is the version of the
    /// encoding that the recording says, and `first` is the position of the first entry.
    pub fn checked(version: EncodingVersion, first: Position, entries: BatchBuffer, frames: Option<Bytes>) -> Batch;
    /// The same batch for a stream that starts or ends in the middle of it, and for a stream in a direction.
    pub fn starting_at(self, position: Position) -> Batch;
    pub fn ending_at(self, position: Position) -> Batch;
    pub fn towards(self, direction: Direction) -> Batch;
    /// The entries that the reader asked for, in the order of the reading. An entry that failed the check
    /// comes as its error, in its place.
    pub fn entries(&self) -> impl Iterator<Item = (Position, Result<EntryView<'_>, EntryError>)>;
    /// One entry and its frames in buffers of their own: a batch of one, which keeps no other buffer in memory.
    pub fn copy_of(&self, position: Position) -> Result<Batch, EntryError>;
}

pub enum EntryView<'a> {
    Session(SessionEventView<'a>),
    Call(CallView<'a>),
    Journal(JournalEntryView<'a>),
}

impl<'a> CallView<'a> {
    pub fn id(&self) -> Result<CallId, EntryError>;
    /// The fields of the call, where they lie. `to_fields` builds the source that a selector matches.
    pub fn fields(&self) -> FieldsView<'a>;
    pub fn explanation(&self) -> ExplanationView<'a>;
    /// The frame, as a slice of the buffer of the storage, when the reader asked for frames. It keeps that buffer in memory.
    pub fn frame(&self, slot: FrameSlot) -> Result<RecordedFrame, EntryError>;
    /// The call as its type, built through its checks. It owns what it holds, and keeps no buffer in memory.
    pub fn to_call(&self) -> Result<Call, EntryError>;
}

/// What a reader looks for, as far as a summary can tell. A function of a summary is one.
pub trait Wanted: Send + 'static {
    fn may_be_in(&self, summary: &Summary) -> bool;
}
```

The traits declare their async methods as the [backend contract](crates.md#the-backend-seam) does: each returns a future that is `Send`.

| A storage | Keeps to |
|---|---|
| Starts | It refuses an ID that a recording has. A recording that started is listed, and opens. |
| Lists | Every recording that started, in the order of their IDs: by the environment, and then by the instant it started. |
| Appends | It gives every entry the next position. One writer writes a recording, and nobody writes to a recording again once its writer is gone. |
| Commits | Every entry that it took is readable, and is still there after a crash of the process or of the machine. A storage may commit by itself, when what it holds reaches a size. |
| Finishes | The recording has an end. A recording whose writer went away without one was cut short, and opens all the same. |
| Reads | As a stream: from the start, from the end or from a position, forwards or backwards. A reader sees every entry that a commit covers, also while the recording runs: a stream that reached the end hands out what a later commit added, when it is asked again. Entries come in the order of their positions, a batch at a time. |
| Holds a bounded number of bytes | A reader holds the batches that it handed out and that someone still holds, and what it read ahead. A storage names the bound, never passes it by reading ahead, and says so when a consumer holds all of it. See [memory](#memory). |
| Skips | It asks `Wanted` about every summary it has, at every level it has one, and reads no stretch that cannot hold what is wanted. A storage without summaries asks nothing and hands out everything. What it hands out is never less than what is wanted. |
| Hands out frames | Only when the reader asks for them. A reader that asks for none gets every call with the states and the lengths of its frames, and no storage reads a frame for it. |
| Keeps entries as they are | It hands every entry to a `BatchEncoder`, and stores the bytes that come out as they are, compressed or not. It looks into none of them. It keeps the [version of the encoding](#versions) that they were written in, and says it with them. |
| Hands out entries where they lie | A batch is the buffer that the entries were put in. The storage fills a `BatchBuffer`, which starts at an address that is a multiple of 16, and makes the batch with `Batch::checked`, which [checks it once](#the-check-of-a-block). No storage builds a type for a reader. |
| Says what it lost | A stretch that it cannot read is an error that names its positions, and the reader can go on behind it. |

`Wanted` carries a selector to the storage without the storage knowing one.
The engine implements it with the check of `selector`, and with a range of time or of call IDs when a reader asks for one.
A storage keeps it for as long as its stream runs, so a stream takes it whole, and a function of a summary is one.
A reader that wants everything hands in `Everything`, which says yes.

`recording_conformance` holds this table as a check, for any storage.
It checks what shows from outside.
That a commit outlives a crash, that a storage reads no stretch that a summary rules out, and what a storage says about a stretch that it lost, are for the tests of a layout.

The same crate holds a storage in memory, which keeps every batch as the bytes that were written, and keeps no summary.
The check is held to it, and the tests of a recorder or of a reader use it where they need no layout.

### Nothing is copied twice

A reader gets a batch, and a batch gives views.

- **A segment is compressed, so reading writes the bytes of an entry once.**
  The decompressor puts the bytes of a batch into a buffer.
  That is a copy, and no layout that compresses can do without it.
  Zero-copy here means what happens after it: nothing copies those bytes again on the way to the consumer.
  It does not mean that an entry is read from the file where it lies.
- **A batch is that buffer, and its owners share it.**
  The trait hands out no slice with a lifetime, so it needs no lending types, and a batch outlives the call that returned it.
- **A view is a place in the buffer.**
  A view of a call, of its fields, of its explanation, of a line of it, of a journal entry or of a session event points at the entry where it lies.
  Reading a value through a view reads the bytes of that value: a number where it stands, and a text as the bytes it is.
  No view copies a text, no view allocates, and none is the type of `call`.
- **A frame is a slice of the same kind.**
  On the way in, the frame of an entry is the buffer that the session read from its socket.
  On the way out, it is a slice of the buffer that the frames of its batch were unpacked into, and it goes into the codec of Kafka and into a pipeline as a frame from a socket does.
- **A batch is checked once, before its first view.**
  A recording from a disk is input from outside.
  A view that followed a reference on trust would read whatever a damaged or a forged recording points it at.
  So `recording` passes over the buffer [once](#the-check-of-a-block), when the storage makes the batch, and a view reads without a check after that.
- **Whoever keeps a view keeps its buffer.**
  A view borrows from its batch, and a frame shares the buffer of its batch.
  So one call that a consumer keeps holds the whole buffer in memory: megabytes, for an entry of a kilobyte or two.
  The storage counts such a buffer against the [budget of its reader](#memory) until the last holder lets go, and the buffer tells it so: `BatchBuffer::when_released`.
  A consumer that keeps entries beyond its stream therefore [copies them out](#a-kept-view-keeps-its-block), and says so in its code:
  `copy_of` gives one entry and its frames in buffers of their own, and `to_call` gives the type.
- **A view hands out values, and the types check them.**
  A view of the fields gives a topic as text and an error code as a number.
  The check of a batch says that a text is text, and not that it is the name of a topic.
  `to_call` builds the `Call` through the checks of its types, and it is the one place where a call that could not have been one is found.
  A reader that shows a call, or hands it to the control plane, asks for the type. A reader that replays, matches, counts or seeds does not.
- **A selector matches a view.**
  The view of the fields is a field source, as a live call and a call record are.
  Matching without a copy needs a field source that lends its values, which `field` does not have yet: its readings own what they hold.
  Until it does, a selector matches the fields that a view builds, and no more of the call.
- **This holds for a recording of the current version.**
  A batch of an [older version](#an-older-version-is-converted) is written again in the current one before its first view, which is one more copy.

`recording` owns the encoding of an entry, and so the decoder and the views.
A layout decides where a batch lies, how it is compressed, how it is found and what is said about it, and never what is in it.
That is what lets another layout exist: it keeps the same entries in another way, and every reader reads them with the same views.

### A batch

A batch is a series of entries that a storage hands out together, in the buffers that it has them in.
It has two parts, and a storage keeps them apart:

| Part | Holds | A reader |
|---|---|---|
| Entries | The entries, one after the other. Each is a head of 16 bytes and then a [message](#a-message), which starts at a multiple of 16. | Streams them in order, and reads each through a view. |
| Frames | The frames of the calls of the batch, back to back. | Takes them when it asked for frames, and never touches them when it did not. |

| Offset | Size | The head of an entry holds |
|---|---|---|
| 0 | 4 | The length of its message. |
| 4 | 1 | The kind of its message: `1` a session event, `2` a call, `3` a journal entry. |
| 5 | 3 | Zero. |
| 8 | 8 | For a call that the recording holds frames of: where they start in the frames of the batch. Zero for every other message. |

The next head stands at the next multiple of 16 behind the message, and the bytes between them are zero.

- **An entry is one message.**
  A call is its ID, its connection, its fields, its moments, what the client received, its explanation, and for each of its four frames a slot: its state, its length on the wire, and the length that the recording holds of it.
  The frames of a call stand back to back in the frames of the batch, in the order of the slots, and the head of the call says where they start.
- **A message is made once.**
  The recorder makes the message of an entry before a layout knows which batch the entry goes into.
  Where the frames of the entry lie in that batch is therefore in the head, which the encoder of the batch writes, and not in the message.
  A storage keeps the message as it was made.
- **An entry stands by itself.**
  It names no other entry and no table of the batch.
  What repeats from call to call, such as a topic and the name of a rule, is left to the [codec](#the-codec), which does it better than a table of our own:
  a call of 1780 bytes is [122 bytes](#the-encoding-measured) in a segment.
- **The frames stand apart for the readers that want none.**
  The index, a query and a seed of the structure of a cluster read entries and no frame.
  In a full recording the frames are most of the bytes, and such a reader neither fetches them nor unpacks them.

### A message

A message is the bytes of one thing that the contract owns: an entry, a description, a summary or an end.
`recording` writes every message and reads every message, and no layout has one of its own.

- **A message is the archive of a stored type.**
  `recording` has a stored type for every message: a plain type of numbers, texts, bytes, lists, choices and other stored types.
  A stored type holds no value that a check guards, and it is not the type of `call` or `journal`.
  The archive of a value is that value as its type lies in memory, made free of the place it lies at:
  a number has a fixed size and its lowest byte first,
  and a text or a list is a length and a reference, which is the distance in bytes from where the reference stands to where the values are.
- **Nothing in it is decoded.**
  A view is a reference to the archive, and reading a value is reading memory.
  That is why this encoding reads a whole call [more than twice as fast](#the-encoding-measured) as the next one, and allocates nothing.
- **It asks for an address.**
  A message starts at a multiple of 16, in the buffer that it is read from.
  16 is the most that a value of a stored type can ask for, which a number of 128 bits does, so no later stored type changes the rule.
  The buffer that a codec unpacks a block into therefore starts at a multiple of 16, and so does every message in it.
- **The same value gives the same bytes.**
  No stored type holds a collection without an order of its own, and the bytes that pad a value are zero.
  That lets a [test](#a-type-cannot-change-without-a-version) compare bytes.
- **It cannot grow.**
  A field that is added, taken out, moved or widened gives other bytes for every value, and a reader with the other type reads them wrong.
  Every change of a stored type is therefore a new [version of the encoding](#versions).
  That is the price of the speed, and versions are how it is paid.
- **The types of `call` and `journal` are not archived themselves.**
  A change in one of them would change every recording without anyone deciding it.
  Writing is one conversion, from the type to its stored type, and `to_call` is the way back.

### The check of a block

In the segment layout a batch is a block, so the check of a batch is the check of a block.
`Batch::checked` passes over the buffer once, from the first head to the last, when the storage makes the batch.
The pass checks one message after the other, each by itself, and it is done before the batch hands out its first view.
Nothing is checked later, as a reader walks the entries, so a batch never hands out a view of an entry that was not checked.

| It checks | So that |
|---|---|
| Every head: its length stays inside the buffer, its kind is one that an entry of the version has, and its message starts at a multiple of 16. | The walk from entry to entry never leaves the buffer. |
| Every reference in a message: what it points at lies inside the same message, at an address that the type of the value can stand at. | A view never reads outside its entry. |
| Every length of a text or a list: its values lie inside the message. | The same. |
| Every text: it is UTF-8. | A view hands out a text without a look at its bytes. |
| Every choice: the number that says which case it is, is a case of its type. So for a value that may be missing, and for a yes or no. | A view never meets a case that its type does not have. |

- **It is one pass, and it costs little.**
  It took 0.15 µs for a call in the [measurement](#the-encoding-measured), which is 0.35 ms for a block of 4 MiB.
  Every speed of the measurement includes it.
- **After it, a view reads without a check.**
  Making a view of bytes is unsafe code, and the check is what makes it sound.
  It is in `recording` alone: `Batch::checked` is the only way to a view, and nothing makes a `Batch` without the check.
- **An entry that fails is left out, and the others are read.**
  It comes out of `entries` as an `EntryError`, at its position.
  Its head still says where the next entry starts, so it hides no entry behind it.
  A head that fails ends the walk, because nothing says where the next entry starts.
  The batch then holds the entries before it, and the layout, which knows from its chunks how many entries the block has, reports the rest as lost.
- **It does not check what a type checks.**
  That a text is the name of a topic, or that a number is a call ID, is found by the value object that takes it.
- **It does not check the frames.**
  A frame is the bytes of Kafka, and nothing follows a reference into them.
  A slot that says a frame outside the frames of the batch is found when a reader asks for that frame, as an `EntryError`.
- **The other messages are checked in the same way.**
  A description, a summary and an end pass the same check when `recording` reads them.

A checksum already covers every byte as it is stored.
The check of a block is for what a checksum does not see: a recording that someone built, and a build that wrote something wrong.

### Errors

The contract holds its errors, one for each thing the caller asks.
None of them names a file, a segment or a backing.

| The caller | Error | Variants, each a decision of the caller |
|---|---|---|
| Starts a recording | `StartError` | `AlreadyThere`: a recording has the ID. `NotStarted { reason }`: the storage could not keep it. |
| Appends, commits, finishes | `WriteError` | `NotWritten { reason }`. The recorder stops the recording, whatever stood in the way. |
| Lists | `ListError` | `NotListed { reason }`. |
| Opens | `OpenError` | `NotFound`. `EncodingNotRead { error }`: this build does not read the [version of the encoding](#a-version-that-a-reader-does-not-know) that the recording has. The error is an `EncodingVersionError`, which names that version and the newest that the build reads. `LayoutNotRead { written, known }`: the same for the version of the layout. `NotReadable { reason }`: what is there is no recording. |
| Asks how far a recording goes | `ExtentError` | `NotRead { reason }`: the storage did not answer, and the question can be asked again. |
| Starts a stream | `ReadError` | `NotRead { reason }`: the storage did not answer, and the stream can be started again. `AllReadersBudgetSpent { held, budget }`: the readers of the process hold as much as they may together, which is `recordings.all_readers_memory`, so the stream is refused, and the caller tries again later, or tells whoever asked. |
| Asks a stream for its next batch | `NextBatchError` | `Damaged { from, to }`: these positions are lost, and the reading goes on behind them. `NotRead { reason }`: the storage did not answer, and the stream can ask again. `ReaderBudgetSpent { held, budget }`: the consumer holds as many bytes of batches as its reader may hold, which is `recordings.reader_memory`, so it lets go of some, or copies out what it keeps, and reads on. |
| Reads a value through a view, or builds the type | `EntryError` | `Damaged { reason }`: the bytes are no entry, or they hold a value that its type does not take. The reader leaves the entry out, and says which position it had. |
| Reads a description, a summary or an end | `MessageError` | `Damaged { reason }`: the bytes are no such message. A layout reads the message from another place where it has one, or says that what is there is no recording. |
| Makes the entry of a call | `CallEntryError` | `LieNotStored { lie }`: the call tells a lie that this version of the encoding has no stored type for. The recorder stops the recording. |

Reading has three errors, because its three callers decide on different things, and no caller writes an arm for a variant that cannot come from what it called.
In both errors of a budget, `held` is the bytes that are held and `budget` is the bytes that the property allows.

A lie comes with a crate of its own, and its definition is open to new cases.
The encoder therefore cannot name every case, and a lie without a stored type is an error where the entry is made, and no silent loss.
The encoding learns of a new lie with a new [version](#versions).

## Memory

A recording can be any size, and what a process holds of it cannot.
Three bounds say what it holds, and each is made of properties.

| Who | Holds at most | Property | Default |
|---|---|---|---|
| One reader, which is one stream | The unpacked blocks that it handed out and that someone still holds, and their frames. The segment layout reads nothing ahead. | `recordings.reader_memory` | `16MiB` |
| All readers of a process | The same, for all of them together. | `recordings.all_readers_memory` | `256MiB` |
| The writer | The frames that wait in its queue, what waits for a commit, and the block that is open. | `recordings.queue_size`, `recordings.commit_size` and `recordings.block_size`, added up | `276MiB` |

A [block](#blocks-and-chunks) is small so that these numbers can be.
Blocks of 64 MiB cost [72 MiB](#the-encoding-measured) of peak RSS (VmHWM) for one reader and 536 MiB for eight.
Blocks of 4 MiB cost 7 MiB and 38 MiB.

### What a reader holds

- **One block at a time.**
  That is the buffer of its entries, and the buffer of its frames when the reader asked for them: 4 MiB together, with the default.
- **Nothing that it read ahead.**
  The segment layout reads nothing ahead: it awaits its backing and starts no task, so a block that it read ahead
  would be read in the same wait as the block that the consumer asked for.
  Reading ahead belongs to the backing, which can start the next read while the layout unpacks a block:
  the backing on files tells the system which ranges of a mapped segment come next, and a backing that fetches can
  fetch the next range.
- **Not what the backing holds ahead.**
  The backing contract has no way for a backing to say how many bytes it holds ahead, so no budget counts them.
  What the backing on files reads ahead is pages of the cache of the system, which the system takes back, and which
  no budget of a process counts in any case.
  A backing that fetches into buffers of its own holds them outside the budgets, and bounds them itself.
- **One block, however large it is.**
  A call with a frame of 100 MiB makes a block of 100 MiB, and an entry is never cut in two.
  A reader may always hold one block with its frames, also when that is more than its budget, and nothing else while it does.
  A budget never makes a recording unreadable.
- **The bytes as they are stored, when the backing read them into a buffer.**
  A backing that maps a file hands out ranges of the map, which the system keeps and takes back, and they do not count.
- **One record of the index at a time, and the rows of one segment.**
  A reader streams an [index chunk](#an-index-chunk) record by record, and keeps of it the place of each segment that can hold what it wants: a few numbers for each.
  It takes the records of the spans of a segment from the [footer](#a-data-segment) of that segment, and keeps the rows of the spans that can hold what it wants, for one segment at a time: about 0.1 MiB for a segment of a gibibyte.
  The manifest stays in memory for as long as the recording is open: about 2 KiB for every index chunk.
- **A block of an older version, twice.**
  The [conversion](#an-older-version-is-converted) holds the block that it reads and the block that it writes, and both count.

### A kept view keeps its block

A view borrows from its batch, and a frame shares the buffer of its batch.
Whoever keeps one keeps the block, and the storage counts the block against the budget of the reader until the last holder lets go.

In the [measurement](#the-encoding-measured), a consumer that kept every thousandth call of a gibibyte of entries as a view held the whole gibibyte.
The same consumer held 1 MiB when it copied each of them out, and its peak RSS (VmHWM) was 11 MiB instead of 1028 MiB.

So a consumer that keeps entries beyond its stream copies them out, and its code says so.

| Consumer | Keeps beyond its stream | It copies out with |
|---|---|---|
| The engine, when it answers the control plane: for the UI that shows the latest calls or the result of a query, and for a test that collects calls and asserts on them | The calls that it sends. | `to_call`. It sends the type, so it needs it anyway. |
| The engine, when it rebuilds its [index](../domains/observation.md#index) | The values of the fields. | The index takes each value into its own facets. |
| [Seeding a World](../domains/world.md#seeding) | Topics, partitions, offsets, and records when the recording has them. | The seed builds the definition, and copies the bytes of a record that it keeps. |
| A replay | Nothing. A frame goes into the pipeline, and the replay lets go of it when the call is compared. | — |
| Anyone else who reads a storage and collects entries, such as a test of a storage | The entries. | `to_call` for the type, or `copy_of` for the entry and its frames as they are, which gives a batch of one. |

No consumer keeps a `Batch`, an `EntryView` or a `RecordedFrame` in a field that outlives its stream.
`recording_conformance` reads a recording to its end with a consumer that keeps nothing, and reads it again with one that keeps every batch, which gets `ReaderBudgetSpent` and loses no entry when it lets go.

### When a budget is reached

Nothing waits for memory.

| The budget is reached | The storage |
|---|---|
| Of a reader, by batches that the consumer still holds | Fails the next read with `NextBatchError::ReaderBudgetSpent { held, budget }`, which names `recordings.reader_memory`. The consumer lets go, or copies out, and reads on from the position it had. |
| Of all readers, when a stream starts and there is no room for one block | Refuses the stream with `ReadError::AllReadersBudgetSpent { held, budget }`, which names `recordings.all_readers_memory`. The room for one block is `recordings.block_size`. When no reader holds anything, a stream is never refused. |
| Of all readers, while a stream runs | Reads on. A stream set the room for one block aside when it started, so it never stops half way. |

- **A reader that waited would wait for its own process.**
  The consumer that holds the bytes may be the one that asks, and then nothing ever lets go.
  A test with two streams over one recording is such a consumer.
- **A refusal says what is wrong, and a wait says nothing.**
  A query from the UI that is refused says that the engine is busy reading, and the UI asks again.
  A query that waits looks like a recording that is slow.
- **A stream counts what it holds, and at least the room it set aside.**
  All readers together hold the sum of that, so a stream that holds less than one block still counts one.
  What a consumer still holds when its stream ends stays counted until it lets go.
- **A stream that starts asks for one block.**
  With the defaults sixty-four streams read at once before a stream is refused, and sixteen of them can each hold
  all that one reader may.
- **A stream that waited for room, with a limit on the wait, was the other way.**
  It hides the bound until the limit passes, and then it is a refusal all the same.

The message of either error says the bytes that are held, the bytes that the budget allows, and the property that sets it.
Whoever gets `AllReadersBudgetSpent` raises `recordings.all_readers_memory`, or reads when fewer streams run.

### What the writer holds

| Part | At most | With the defaults |
|---|---|---|
| The frames that wait in the queue | `recordings.queue_size` | 256 MiB |
| The call records and journal entries that wait in the queue | `recordings.queue_entries` of them | 10 000 entries |
| What waits for the next commit, compressed | `recordings.commit_size` | 16 MiB |
| The block that is open: what the codec remembers of it | `recordings.block_size` | 4 MiB |
| The records of the spans of the segment that is open, for its footer, and the summary of the span that is open | One [record](#records) for a span | About 0.6 MiB |

The first, the third and the fourth are bytes that a property names, and they add up to 276 MiB.
The second and the fifth are counts of small things.
The writer of the [measurement](#the-encoding-measured), with a queue of 10 000 calls that was full the whole time and held no frame, had a peak RSS (VmHWM) of 52 MiB.
So a writer holds about 330 MiB at most with the defaults, and nearly all of it is frames that wait.

A writer that would pass `queue_entries` or `queue_size` stops the recording, as [the recorder](#handing-frames-to-the-recorder) says.

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
| A frame that the codec cannot read: the API of its call is unknown, the header of its request does not decode, or its body does not decode where the recording has to change it | Withheld. | Withheld. |

- **A redacted frame is a frame.**
  It decodes at the version it had, and it says everything else that it said.
  A record batch keeps its base offset, its number of records, its timestamps, and the ID, the epoch and the sequence of its producer.
  A record keeps its offset and its timestamp.
  The batch is kept without compression and with a new checksum.
  It keeps its other attributes, such as the type of its timestamps and whether it is transactional.
- **The response to `SaslAuthenticate` is blanked too.**
  With SCRAM, what the server answers is enough to test guessed passwords against, next to what an attacker can guess.
- **A control batch stays whole.**
  It marks the end of a transaction, and holds nothing that a client wrote.
- **Whatever cannot be read is withheld, in a full recording too.**
  The API of a call is in the header of its request.
  When that header does not decode, or names an API that the codec does not know, a frame of the call can hold records, or a password, and nobody can tell: every frame of the call is withheld.
- **A body is decoded only where it has to change.**
  The API says what a frame can hold, so a frame that holds nothing to take out is kept whole without a look at its body.
  Decoding every body would cost a decode for every frame on the thread that records, and take nothing more out.
  A frame that has to change, and whose body does not decode, is withheld.
  The price is that a full recording does not keep the one frame that the codec failed on: it keeps its length, and the call record says what GaslightMQ made of it.

`recording` needs the Kafka codec for this, and for nothing else.

## The segment layout

The segment layout keeps a recording as three kinds of object.
Each kind describes the one below it.

```text
manifest                      one, small, replaced as a whole
  └─ index chunk              up to `recordings.index_size` each
       └─ data segment        up to `recordings.segment_size` each, as it is stored
            └─ span           up to `recordings.span_size` of entries and frames: what one summary covers
                 └─ block     up to `recordings.block_size` of entries and frames: what a reader holds, and where a stream can start
                      └─ chunk  what one commit wrote of a block, compressed
```

| Object | Holds | Changes |
|---|---|---|
| Data segment | The blocks of the recording, one after the other, and when it is sealed a footer that sums them up. | It grows with every commit, until it is sealed. A sealed segment never changes. |
| Index chunk | For a series of segments: the summary of each one, the summary of each of its [spans](#spans) of blocks, and where each block is, with the positions it covers. | It grows by one item with every segment that is sealed, until it is full. A full index chunk never changes. |
| Manifest | The description of the recording, the list of its index chunks, each with what it covers, and the end of the recording. | It is replaced as a whole when an index chunk begins, and when the recording ends. |

The segments are the recording.
An index chunk and the manifest hold nothing that the segments do not say:
a reader rebuilds either of them from the segments, and nothing is lost when one is missing.

### Blocks and chunks

A block is what a reader holds at a time, and where a stream can start.
A chunk is what one commit added to a block.

| | A chunk | A block |
|---|---|---|
| Is | The entries and frames that one commit added to a block, compressed. | A series of chunks whose entries are compressed as one stream. |
| Closes when | The recorder commits, or its block closes. | Its entries and frames reach `recordings.block_size`, before they are compressed. |
| Is the unit of | What a crash can lose. | What a reader holds in memory, where a stream starts, and what readers share out between them. |

The two have different reasons, so they are two things.
Under light traffic a block takes many commits, and a second of calls is a chunk of it.
Under heavy traffic a commit covers several blocks, and writes them with one append.

- **Compression is by block, and not by segment.**
  A reader that starts in the middle of a segment starts at a block, with a codec that knows nothing yet.
  A segment that was compressed as one stream could only be read from its start: a gibibyte before the entry that someone wants.
- **A chunk goes on where the chunk before it stopped.**
  The entries of a block are one stream for the codec, which it flushes at every commit.
  A chunk of one second of light traffic is small, and it still compresses well, because the codec remembers the block so far.
- **The frames of a chunk are compressed by themselves.**
  A reader that asked for no frames steps over them by their length.
  Nobody measured this: [no measurement](#known-limits) held a frame, so what the frames of a real recording compress to is not known.
- **A block may end at a flush.**
  The writer finishes the stream of a block with its last chunk when the block fills between two commits.
  When a commit cut the last chunk already, no chunk is left to carry the end of the stream, and none is needed:
  the chunks of a block say how many entries it holds, and a reader stops there.
- **A torn tail is a chunk that is not whole.**
  Every chunk says its length and has a checksum of its bytes as they are stored.
  A reader checks both before it hands a byte to the codec, so the codec never sees what a crash left half written.
  The block that was open in a crash is good up to its last whole chunk.

The default is 4 MiB, because a reader holds a block.

| A larger block | By how much |
|---|---|
| Makes every reader hold more. | A reader held [7 MiB](#the-encoding-measured) with blocks of 4 MiB and 72 MiB with blocks of 64 MiB, and eight readers held 38 MiB and 536 MiB, as peak RSS (VmHWM). |
| Makes one kept view keep more. | A view keeps its block: 4 MiB or 64 MiB for one call. |
| Makes a reader unpack more before the entry it wants. A stream that starts at a position starts at the block that holds it. | 1.5 ms for 4 MiB and 30 ms for 64 MiB, to unpack it and read all of it. |
| Loses more to one damaged chunk: the rest of its block, which the codec cannot read without it. | In step with the size. |

| A smaller block | By how much |
|---|---|
| Makes more rows in the index: every block has a row in the [record](#records) of its span. | 32 bytes for a block. The summary is not the block's: it covers a [span](#spans). |
| Stores a little more, because the codec starts anew with every block. | A call took 122 bytes in blocks of 4 MiB and 117 in blocks of 64 MiB. |

| The size of a block does not | Because |
|---|---|
| Make a stream slower. | A reader read 1.53 million calls a second from blocks of 4 MiB, and 1.28 million from blocks of 64 MiB. |
| Decide what a crash loses. | A crash loses what no commit covers. A commit covers one chunk of a block under light traffic, and several blocks under heavy traffic. |
| Decide what the writer holds. | The writer hands an entry to the codec as it comes, and holds what waits for the commit, which `recordings.commit_size` bounds. |

A block and a segment are not the same thing.
A segment is as large as one file should be, and it is counted as it is stored.
A block is as large as a reader should hold, and it is counted before compression.
With the ratio of the measurement a segment of a gibibyte holds about 3700 blocks, in 230 spans.

### Spans

A span is a series of blocks that one summary covers.

| | A block | A span |
|---|---|---|
| Is | What a reader holds, and what the codec compresses as one stream. | The blocks that one [summary](#summary) covers, one after the other. |
| Closes when | Its entries and frames reach `recordings.block_size`. | The entries and frames of its blocks reach `recordings.span_size`, which is `64MiB` until someone sets it. Or its segment is sealed. |
| In the index | A row of 32 bytes. | A [record](#records) with a summary, of about 2 KiB. |

- **Memory wants a small block, and the index wants few summaries.**
  A summary is about 2 KiB and a block as it is stored is about 280 KiB, so a summary for every block made the index 0.7% of a recording.
  A summary for a span of sixteen blocks takes it back to 0.06%.
- **A span is nothing to the codec, and nothing to the memory of a reader.**
  The blocks of a span unpack without each other, and a reader holds one of them.
- **A span is whole blocks of one segment.**
  A block is never cut in two, and a span that is open when its segment is sealed closes there.
- **The record of a span has a row for every block.**
  So a stream starts at any block, and readers share out the blocks of a span between them.

How fine a reader skips:

| A reader that looks for | Skips down to | Because | It reads at most, next to what it wants |
|---|---|---|---|
| A position | A block. | The row of a block says its first position and the number of its entries. | The rest of one block: 4 MiB, in 1.5 ms. |
| A moment, or a call ID | A span. | The summary of the span holds the ranges, and a row holds none. | The rest of one span: 64 MiB, in 25 ms on one core. |
| A value of a field | A span. | The summary of the span lists the values. | The rest of every span that holds the value: 64 MiB for each. |

| Property | A larger value | A smaller value |
|---|---|---|
| `span_size` | Makes one summary cover more, so it rules out less and says `any` sooner, and makes a reader read more next to what it wants. | Makes a larger index: a summary for every span. |

A span of 64 MiB is what a block was in an earlier form of this layout, so a summary covers what it covered then.

### The codec

The codec stands between the bytes of a block and the bytes of its chunks.
It is a seam: the header of a block names the codec that wrote it, by a number, and a reader takes the codec from there, at run time.
The codecs are a closed set, in one crate, `recording_compression`.

```rust
/// The ways to compress a recording, with what each needs to know: a closed set.
pub enum Compression {
    None,
    Zstandard { level: ZstandardLevel },
}

/// A codec of this build. The header of a block names it by its number: `0` is none, `1` is Zstandard.
pub enum Codec {
    None,
    Zstandard,
}

impl Compression {
    /// The codec that unpacks what this compression packed. A reader needs no level.
    pub const fn codec(&self) -> Codec;
}

impl Codec {
    pub const fn id(self) -> u8;
    pub const fn from_id(id: u8) -> Result<Codec, CodecError>;
}

/// Packs what it is handed, one block at a time. What it packed goes behind what `out` holds.
impl Compressor {
    pub fn new(compression: Compression) -> Result<Compressor, CompressError>;
    pub fn write(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError>;
    /// A commit: a reader unpacks every byte that the compressor was handed. The block goes on.
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError>;
    /// Ends the block: its frame is finished, and the next byte starts a new one.
    pub fn finish_block(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError>;
}

/// Unpacks a block with the codec that a header names.
impl Decompressor {
    pub fn new(codec: Codec) -> Decompressor;
    /// Unpacks one block from the bytes of its chunks, one after the other, behind what `out` holds.
    pub fn read_block<'a>(
        &mut self,
        chunks: impl IntoIterator<Item = &'a [u8]>,
        out: &mut BatchBuffer,
    ) -> Result<UnpackedBlock, DecompressError>;
}

pub struct UnpackedBlock {
    pub end: BlockEnd,
    /// The bytes of the chunks that the block took. What follows them is the start of the next block.
    pub taken: usize,
}

pub enum BlockEnd {
    /// Where its writer finished it.
    Finished,
    /// The bytes ran out before the frame ended, after its last whole flush.
    Unfinished,
}
```

| Codec | Is |
|---|---|
| Zstandard | The first codec, and the default. `recordings.compression_level` says how hard it works, and is `3` until someone sets it. A reader needs no level. |
| None | The bytes of a block as they are. It copies a chunk into the buffer of its block, which is the one write that every codec does. A block ends where its buffer is full. |

- **A codec is a transform.**
  It takes bytes and gives bytes, and it does no IO, so a test drives it with bytes in memory.
  It is not async and starts no thread: it works on the thread that calls it.
  The recorder writes on a thread of its own, so no session waits for the codec.
  A reader on an async runtime hands the unpacking to a blocking task, where it does its IO.
- **A block is a frame of its own.**
  `finish_block` finishes the frame of the block, and the next byte starts a new one.
  A reader starts at any block that the index names, with a decompressor that knows nothing yet.
  A reader unpacks a frame that is not finished up to the last flush whose bytes are all there, as `BlockEnd::Unfinished`.
- **A block may end at a flush.**
  When a commit already cut the last chunk of a block and the block then closes, no chunk is left to carry the end of its frame.
  Such a block is whole, and it unpacks in full as `BlockEnd::Unfinished`.
  A block that a crash cut short ends the same way, and the codec cannot tell the two apart:
  the layout finds a torn tail from the lengths and the checksums of its chunks, and not from the codec.
- **A flush is a commit.**
  After a flush a reader unpacks every byte that the compressor was handed, and the frame goes on.
  The layout flushes at a commit, which is always between two entries, so a chunk is what one commit added to the frame of its block.
  A flush and the end of a block that nothing was handed to write nothing.
- **A block unpacks in one call.**
  The layout hands `read_block` the chunks of a block, without their headers, after it checked the length and the checksum of each.
  The block takes the bytes of its frame, and `taken` says how many, so the layout knows where the next block starts.
  The next call starts a block of its own, and unpacks behind what the buffer already holds:
  the frames of the chunks of a block, which are a frame each, unpack one after the other into one buffer.
- **A compressor serves a stream for its whole life.**
  After `finish_block` it is ready for the next block, with nothing to reset.
  Making one costs the context of the codec, so the frames of each chunk, which are a stream of their own that ends at every chunk, keep one compressor.
- **A codec writes a block once, into the buffer that the reader hands out.**
  The chunks of a block unpack one after the other into one `BatchBuffer`, which the layout makes with the room for the whole block.
  Zstandard looks back into that buffer for what a chunk repeats, and keeps no window and no second copy of the block.
  That is why a block unpacks in one call: the decoder needs the same room from the first byte of the frame to its end.
  A block that unpacks into more than its room is not unpacked.
- **The buffer starts at a multiple of 16.**
  The layout makes it so, and the codec writes from its start.
  An entry keeps its multiple of 16 in there, whatever the place of its chunk in the file is, and a [message](#a-message) asks for no more.
- **The frames of a block unpack into a second buffer**, chunk after chunk, when the reader asked for frames.
  A slot of a call says where its frame is in that buffer.
- **A codec knows no entry.**
  It never sees where one entry ends.
- **A recording has one codec, and a block names it.**
  `recordings.compression` says which codec a new recording takes: `zstd` or `none`.
  A writer makes its compressor from that property, and a reader makes its decompressor from the number in the header of the block, at run time.
  No reader is built for one codec, and none is told which codec to expect.
- **The codecs are a closed set.**
  A compressor and a decompressor are one type each, over the codecs that this build has, and not a trait that a crate implements.
  A new codec is a new case, with a new number.
  A reader that meets a number it does not know says so, with `CodecError`, and reads nothing of that block.
- **A codec fails in one way each.**
  `DecompressError { reason }` means that the bytes are not what the codec wrote, or that they unpack into more than the room of the buffer.
  The layout reports the positions of the block as lost.
  `CompressError { reason }` means that the codec failed to pack, and the block and the compressor are lost.

Why Zstandard at level 3, from the [measurement](#the-encoding-measured):
it takes a call from 1780 bytes to 122, and costs 1.3 µs for a call on the thread that records.
From the disk a compressed recording streams more than twice as fast as a plain one, because the disk has a fourteenth of the bytes to give.

Why none is not the default, although a plain recording that is already in memory reads three times as fast:
a recording is read from a disk far more often than from memory, and it is kept for a long time.

### What it takes from Iceberg

The model is the tree of metadata of Apache Iceberg: a root that lists small files of metadata, which describe the files of data and say what is in each, so a query prunes at every level.

| From that model | Here |
|---|---|
| Files never change once they are written. | A sealed segment and a full index chunk never change. |
| New metadata is written beside the old, and one step makes it the current one. | The manifest is replaced in one step: a reader sees the old one or the new one, and never a mix. |
| Every level says what is below it, with ranges, so a reader skips what cannot match. | The manifest sums up every full index chunk, and an index chunk sums up every segment and every span of blocks. |
| No file of metadata grows with the table. | An index chunk closes at a size. The manifest grows by one small record with every index chunk, which is one for about 420 GiB of segments. |

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
- **What the layout says itself is fixed binary.**
  A header and a [record](#records) are marks, numbers, lengths and checksums at offsets that this document gives.
  A reader finds them without reading a message and without unpacking anything.
- **What has a structure is a message of `recording`.**
  A description, a summary, an entry and an end are [messages](#a-message).
  The layout carries them and has none of its own, so it has no stored types, and the [version of the encoding](#versions) is `recording`'s alone.
- **A message stands behind a head of 16 bytes, at a multiple of 16.**
  The head is the [head of an entry](#a-batch): the length of the message, and its kind.
  The kinds that an entry does not have are `4` a description, `5` a summary and `6` an end.
- **A header, a record and a chunk start at a multiple of 16 of their object.**
  The bytes that stand before one for that reason are zero.
  A message that is not compressed therefore stands at a multiple of 16 of its object, and a backing that maps hands it out at such an address.
  A reader that gets one at another address copies it to one first: these messages are small.
- **What is a frame is the frame.**
  The bytes of a Kafka frame stand in the frames of a block as they were on the wire, without the four bytes of their length.

Every object starts with a mark of 8 bytes, the [version](#versions) of the layout and the version of the encoding.

### Records

A record is what the layout says about a span of blocks, a segment or an index chunk, or how the recording ended.
It is a head, a fixed part, rows where it has them, and the messages that it carries.

```text
record = record head  fixed part  row …  message …
```

| Offset | Size | The record head holds |
|---|---|---|
| 0 | 4 | The mark of its kind. |
| 4 | 4 | The checksum of every byte of the record behind its head. |
| 8 | 4 | The number of those bytes. |
| 12 | 4 | Zero. |

| Record | Mark | Its fixed part holds | Its rows | Its messages |
|---|---|---|---|---|
| Of a span | `GMQP` | 48 bytes: where the span starts in its segment (8), its length as it is stored, without this record (8), the position of its first entry (8), the number of its entries (8), the number of its blocks (4), zero (12). | One row of 32 bytes for every block of the span: where the block starts in its segment (8), the position of its first entry (8), the number of its entries (4), the length of its entries, unpacked (4), the length of its frames, unpacked (8). | The [summary](#summary) of the span. |
| Of a segment | `GMQS` | 48 bytes: the position of its first entry, which is the name of the segment (8), the number of its entries (8), the length of the segment up to its footer, which is where the footer starts (8), the number of its spans (4), zero (4), the length of the records of its spans, which follow (8), zero (8). | None. | The summary of the segment: the sum of the summaries of its spans. |
| Of an index chunk | `GMQI` | 16 bytes: the position of the first entry that it describes (8), and the number of entries that it describes, which is zero while it grows (8). | None. | The summary of the index chunk, once it is full: the sum of the summaries of its segments. |
| Of the end | `GMQE` | Nothing. | None. | The end of the recording. |

- **A record is the same bytes wherever it stands.**
  The record of a span stands behind the last block of the span, it stands again in the footer of its segment, and again in the index chunk.
  The writer makes it once.
- **A row says what a reader needs before it reads a block.**
  The unpacked lengths are the room that a reader makes for the entries and for the frames.
  A block is as long as the way to the start of the next one, and the last block of a span ends where the span does.
- **A summary says whether to read the span at all.**
  The instants, the call IDs and the counts that a span covers are in its [summary](#summary), so the fixed part does not hold them again.
- **A block has no summary of its own.**
  One summary covers a [span](#spans), and a row of 32 bytes is all that the index holds of a block.
- **The record of a segment says where its footer starts.**
  A reader goes from the item of a segment straight to its footer, without the trailer.

### A data segment

```text
segment        = segment header  span …  segment footer
span            = block …  record of the span
block          = block header  chunk …
chunk          = chunk header  entries, as stored  frames, as stored  zeros up to a multiple of 16
segment footer = record of the segment  record of a span …  record of the end, in the last segment  trailer
```

| Offset | Size | The segment header holds |
|---|---|---|
| 0 | 8 | The mark of a segment: the bytes `89 47 4D 51 53 45 47 0A`, which read `GMQSEG` between a byte that no text holds and a line feed. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | The version of the encoding: `1`. |
| 12 | 4 | The length of what follows. |
| 16 | 8 | The position of the first entry of the segment. |
| 24 | 8 | Zero. |
| 32 | … | The [description](#description) of the recording, as a message behind its head. |
| … | 4 | The checksum of every byte of the header before it. |

Every segment holds the description, so every segment says which recording it belongs to, and a recording without a manifest still knows what it is.

| Offset | Size | The block header holds |
|---|---|---|
| 0 | 4 | The mark of a block: `GMQB`. |
| 4 | 4 | The checksum of the bytes 8 to 31. |
| 8 | 8 | The position of its first entry. |
| 16 | 1 | The codec: `0` none, `1` Zstandard. |
| 17 | 15 | Zero. |

| Offset | Size | The chunk header holds |
|---|---|---|
| 0 | 4 | The mark of a chunk: `GMQC`. |
| 4 | 4 | The checksum of the bytes 8 to 47. |
| 8 | 4 | The number of its entries, which is never zero. |
| 12 | 4 | Zero. |
| 16 | 4 | The length of its entries, as they are stored. |
| 20 | 4 | The length of its entries, unpacked. |
| 24 | 4 | The length of its frames, as they are stored. |
| 28 | 4 | The length of its frames, unpacked. |
| 32 | 4 | The checksum of its entries, as they are stored. |
| 36 | 4 | The checksum of its frames, as they are stored. |
| 40 | 8 | Zero. |

The entries and the frames of a chunk have a checksum each.
A reader that asked for no frames steps over them unread, and still checks every byte that it unpacks.

The unpacked lengths of the entries of the chunks of a block add up to the room that a reader makes for the block.
It makes that room once, before the codec writes into it.
The row of the block says the sum.
A block of the span that still grows has no row yet, so the reader takes its chunks first, and adds up their headers.

A block ends where the next header of a block, the record of its span or the end of the segment stands.
The header of a block goes into the segment with its first chunk, so no block is ever empty.
The writer ends a span with its record when the span is full, or when it seals the segment, and it ends a segment with a footer.
A span without a record is the one that still grows, or the one that a crash cut short, and so is a segment without a footer.

The footer of a segment is the record of the segment and the records of its spans, as the [index chunk](#an-index-chunk) has them.
The footer of the last segment also holds the record of the end.
The trailer is 16 bytes: the length of the footer before it, in 8 bytes, and the mark `89 47 4D 51 45 4E 44 0A`, which reads `GMQEND`.
A reader finds the footer of a sealed segment from the last bytes of the segment.

The end of a recording says how it ended: someone stopped it, or its environment stopped, or the recorder fell behind.
It holds the instant, and the numbers of the entries and the calls of the recording.

A recording that [stops by itself](storage.md#recordings) still ends in order.
The recorder writes everything it took from its queue, and then the footer with the end.
Its last entry is the journal entry that says why it stopped, and the end says it again, so whoever lists the recordings unpacks no entry.
A recording that could not be written has no end, because a recording cannot say that it could not be written.
The journal in memory and the control plane say it.

### An index chunk

An index chunk describes a series of segments, one after the other.

```text
index chunk = index header  item …
item        = record of a segment  record of a span …
```

| Offset | Size | The index header holds |
|---|---|---|
| 0 | 8 | The mark of an index chunk: `89 47 4D 51 49 44 58 0A`, which reads `GMQIDX`. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | The version of the encoding: `1`. |
| 12 | 4 | Zero. |
| 16 | 8 | The position of the first entry that the chunk describes. |
| 24 | 8 | Zero. |

An item describes one segment, and it is the footer of that segment without its trailer.
The item of the last segment of a recording that ended in order therefore holds the record of the end.

- **An index chunk holds everything that a reader needs to pick a span and a block**, and to start a stream there.
- **A reader takes an item record by record.**
  The record of the segment comes first, with the summary of the segment and the length of the records of its spans.
  A reader that the summary rules out steps over that length, and reads no record of a span.
  A reader holds one record at a time, so an item never has to fit in memory.
- **An index chunk grows as a segment does.**
  The layout appends an item when it seals a segment, and nothing of an index chunk is written twice.
- **An item that a crash left half written is not whole.**
  Its records end before the length that the record of its segment names, or one of them fails its checksum.
  The reader reads that item again from the footer of its segment.

### The manifest

| Offset | Size | The manifest holds |
|---|---|---|
| 0 | 8 | The mark of a manifest: `89 47 4D 51 4D 41 4E 0A`, which reads `GMQMAN`. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | The version of the encoding: `1`. |
| 12 | 4 | The length of what follows. |
| 16 | … | The [description](#description) of the recording, as a message behind its head. |
| … | … | A record for every index chunk of the recording, in order. |
| … | … | The record of the end, once the recording ended in order. |
| … | 4 | The checksum of every byte before it. |

The manifest lists the index chunks, and says what each full one covers.
That is what the root of the model does, and it is what lets a reader go straight to the index chunk that holds a position or a moment, and past every index chunk that cannot match.
With a list of names alone, every reader would read every index chunk.

### What closes each level

| Level | Closes when | Property | Default |
|---|---|---|---|
| Chunk | The recorder commits: after an interval, or when the entries and frames that wait reach a size. Or its block closes. | `recordings.commit_interval`, `recordings.commit_size` | `1s`, `16MiB` |
| Block | Its entries and frames reach a size, before compression. An entry is never cut in two, and neither are its frames. | `recordings.block_size` | `4MiB` |
| Span | The entries and frames of its blocks reach a size, before compression. Or its segment is sealed. A block is never cut in two. | `recordings.span_size` | `64MiB` |
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
| `commit_size` | Holds more in memory, in the writer before a commit, and loses more in a crash under heavy traffic. | Commits more often under heavy traffic, for nothing. |
| `block_size` | Makes every reader hold more, and one kept view keep more. See [blocks and chunks](#blocks-and-chunks). | Makes more rows in the index, and stores a little more. |
| `span_size` | Makes one summary cover more, so it rules out less. See [spans](#spans). | Makes a larger index. |
| `segment_size` | Leaves more for a reader to walk after a crash, makes a longer item in the index, and makes the writer hold more records of spans. | Makes more files: a pebibyte is a million segments of a gibibyte. |
| `index_size` | Makes one summary of the manifest cover more, so it rules out less. | Makes a longer manifest, which is replaced more often. |
| `summary_values` | Makes a larger index. | Lets a summary say `any` for a field that a query asks for. |
| `compression_level` | Costs more on the thread that records, for a few bytes. | Stores more: a sixth more at level 1 than at level 3, in the [measurement](#the-encoding-measured). |

### Writing

`recording` encodes an entry, and the layout hands its bytes to the compressor of the block as they come.
The layout holds what the compressor gave back since the last commit, the summary of the span that grows, and the [records](#records) of the spans of the segment that grows.

| When | The layout writes, in this order |
|---|---|
| The recording starts | The first segment, with its header. Then the manifest, with the description and no index chunk. Creating the first segment comes first because it refuses a recording that is there: the manifest is replaced as a whole, and would replace the manifest of that recording. |
| The block is full | It ends the chunk and the stream of the compressor. The header of the next block goes into the segment with the first chunk of that block. All of it waits for the commit. |
| The span is full | It puts the record of the span behind its last block, before the header of the next block. It waits for the commit too. |
| A commit | It flushes the compressor, and appends what waits to the segment that grows, as one append: one chunk under light traffic, and the chunks and headers of the blocks that filled since the last commit, with the records of the spans that did, under heavy traffic. |
| The segment reached its size | It appends the record of the span that is open and the footer of the segment, and seals the segment. It appends the item of the segment, which is that footer without its trailer, to the index chunk that grows. The first item creates the first index chunk, and the manifest is replaced to name it. It creates the next segment. The writer checks the size when a block closes, so a segment is sealed between two blocks. |
| The index chunk is full | It seals that index chunk, and creates the next one with the item that did not fit. Then it replaces the manifest, which has a record for the new one and the summary of the full one. |
| The recording ends | A commit. The record of the span that is open, and the footer of the segment with the record of the end. Then it seals the segment, appends its item, seals the index chunk, and replaces the manifest, which now holds the end. |

Two rules make every crash harmless:

- **An object is written before the object that describes it.**
  Whatever an index chunk or the manifest names is there, whole and for good.
  After a crash, the most that is missing is a description, and a description can be rebuilt from below.
- **Every step leaves a recording.**
  The backing gives each write [what it promises](#the-segment-backing), so no step leaves the manifest half replaced, and what a crash leaves half appended fails a checksum.

The order of writing is a plain function: it takes the entries and the commits, and gives the steps for the backing, in order.
The writer hands each step to the backing and stops at the first that fails, so it runs in a test without a backing.

A write that fails ends the recording.
The layout says so and takes nothing more.
Nobody opens a recording to write to it again: a new recording has a new [ID](identity.md#recording-id).

### Opening

A reader works from the top down, and trusts each level only as far as it reaches.
The header of every object says the [versions](#versions) of the layout and of the encoding, and a reader that does not read one of them [refuses the recording](#a-version-that-a-reader-does-not-know) before it reads anything else.

1. **The manifest.**
   It gives the description, the index chunks, and the end when there is one.
2. **The index chunks behind the manifest.**
   The reader lists the index chunks from the last one that the manifest names.
   One that the manifest does not name yet was written just before a crash, and counts.
3. **The segments behind the index chunks.**
   The reader lists the segments from the first position that no item describes.
   That is one segment while a recording runs, and two at most after a crash.
   A segment with a footer says everything about itself there.
   The reader walks a segment without one: block header, then chunk header after chunk header, and the record of a span where a span has one.
   It checks every mark and every checksum, and that the whole chunk is there, and steps over the chunk by its length, to the next multiple of 16. It unpacks nothing.
   A sealed segment that no item describes yet says everything in its footer, which the reader finds from the trailer.
4. **The end of the recording.**
   The end in the footer of the last segment means that the recording ended in order.
   Otherwise the last chunk that is whole is where the recording stops: it still runs, or it was cut short.

A recording that was cut short opens like any other.
The reader says that it was cut short, and how many bytes behind the last whole chunk it left unread.
It changes nothing in the backing.

The walk stops at whatever is not whole: a chunk that a crash left half written, a chunk that a commit is still writing, and a chunk whose bytes fail their checksum, which look the same at the end of a segment that grows.
A reader of a recording that runs walks on from there when it looks again, and finds what later commits added.

The walk follows lengths that a checksum covers, and never searches for a mark,
so the bytes of a frame can never pass for the start of a chunk.

A span without a record has no summary, and needs none: a reader streams it.

| What is missing or damaged | The reader |
|---|---|
| The manifest | Lists the index chunks and the segments itself, and takes the description from the header of a segment. |
| An index chunk, or an item of one | Reads the footers of the segments that it described: the items that are whole come from the index chunk, and the backing lists the segments that the rest described. |
| A segment that an item names | Reports the positions of the segment as lost, and goes on with the next one. |
| A chunk in the middle of a block | Reports the positions from that chunk to the end of its block as lost, and goes on with the next block. |
| The end of the segment that grew | Stops at the last whole chunk. |

Recovery is a function: it takes what the backing lists and the bytes it reads, and returns what the recording holds.
It runs in a test without a file.
Each walk, over a segment, over an index chunk and over a footer, says how many bytes it wants next and takes them, and the reader asks the backing for them.

### How a stream starts

Every reader is a stream, and the levels above the segments say where it starts and what it leaves out.

1. It reads the manifest, and keeps the index chunks whose summary can hold what it wants.
2. It streams those index chunks record by record, and keeps the segments whose summary can hold it.
   It takes the records of the spans of each such segment from its footer, which holds the same bytes as its item, and keeps the [spans](#spans) whose summary can hold it.
   A stream backwards takes the segments of an index chunk from the last, so it reads them in the order that it needs them.
3. It asks the backing for the bytes of a segment from the start of a block, which a row of the span gives, takes them chunk by chunk, and hands each to the codec.
4. It makes a batch of each block once the codec has filled its buffer, which [checks the block](#the-check-of-a-block), and the consumer reads its entries through views.
5. It steps over the frames of a chunk when the reader asked for none.
6. It reads nothing ahead: the backing does, as [memory](#what-a-reader-holds) says.

| A reader that wants | Streams |
|---|---|
| Everything, as a replay does | Every block of every segment, in order. |
| A position | The block that holds it, as a whole, and hands out the entries from that one on. |
| A moment, or a call ID | The span that holds it, from its first block, and hands out the entries from that one on. |
| The latest calls | The last blocks, in turn from the last, and the entries of each from its last. |
| The calls a selector matches | The spans whose summaries can match, and the selector decides every call in them. |

A summary gets coarser with every level, so the levels prune different things.
A range of time or of call IDs prunes at every level, down to a span, and a range of positions down to a block.
A value that is rare prunes at every level too.
A value that half of the calls have prunes nowhere, and such a query streams the recording.

Blocks unpack without each other, so two readers take two blocks at once, and a stream over a long recording can be shared out between the cores of a machine.

A reader that follows a recording as it runs takes the block that grows again when a commit added to it, and hands out the entries that are new.
A block is small, so that costs a millisecond or two.

### Sizes

The numbers below are estimates.
They take the [measured](#the-encoding-measured) ratio of entries without frames, 14 to 1, which real traffic and frames will change.
They take about 2 KiB for the summary of a span, which nobody measured, and 32 bytes for the row of a block.

| | With the defaults |
|---|---|
| A chunk | One second of traffic, and 16 MiB at most before compression. |
| A block | 4 MiB of entries: about 2300 calls, and about 280 KiB as stored. |
| A span | 64 MiB of entries: 16 blocks, about 37 000 calls, about 4.5 MiB as stored, and 2.6 KiB of index. |
| A segment | 1 GiB as stored: about 3700 blocks in 230 spans, 8.8 million calls, and 0.6 MiB of index. |
| An index chunk | 256 MiB: about 420 segments, which are 420 GiB as stored. |
| A record of the manifest | About 2 KiB for an index chunk. |

| A recording of, as stored | Segments | Blocks | Spans | Index chunks | All index chunks | The manifest |
|---|---|---|---|---|---|---|
| 100 GiB | 100 | 370 000 | 23 000 | 1 | 60 MiB | 1 KiB |
| 10 TiB | 10 240 | 38 million | 2.4 million | 25 | 6 GiB | 50 KiB |
| 1 PiB | 1 048 576 | 3.9 billion | 245 million | 2500 | 610 GiB | 5 MiB |

- **The index is about 0.06% of what it describes.**
  It was 0.04% when a block was 64 MiB, and 0.7% with a summary for every block of 4 MiB.
  Four fifths of it are the summaries of the spans, as before, and a fifth is new: the rows of the blocks.
- **A reader streams the index, and holds one record of it.**
  It steps over the records of every segment that a summary rules out.
  No reader holds an index chunk, and none holds the index.
- **A query that no level above the spans can rule out reads the whole index.**
  That is 0.06% of the recording, to find the spans that it then streams.
  A query for a range of time, of positions or of call IDs never does: the manifest gives it the index chunk, and the record of a segment gives it the spans.
- **Skipping is as fine as a span for a value, a moment and a call ID, and as fine as a block for a position.**
  See [spans](#spans).
- **Nothing of the index is written twice**: a segment that is sealed adds 0.6 MiB to the index chunk that grows.
- **The manifest is replaced once for every 420 GiB of segments.**
- **A recording of 10 TiB opens with one read of 50 KiB.**
- **A stream reads 1.5 million calls a second on one core**: a segment of a gibibyte in 6 seconds, and 10 TiB in 16 hours.
  Blocks unpack without each other: eight readers on eight cores read 0.8 million calls a second each, which is 6.7 million together.
- **A recording of a pebibyte is a million files in one directory.**
  It wants a larger `segment_size`, which the property gives it.

## The segment backing

The segment layout asks one contract for a place to keep its bytes.
The contract speaks of the three kinds of object, and each is named by the position of the first entry it holds or describes.
It hands bytes out as a stream.

```rust
/// Where the objects of recordings live. The segment layout works over any of them.
pub trait SegmentBacking: Send + Sync {
    type Stream: ByteStream;

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListingError>;

    // Data segments and index chunks grow the same way. `Object` says which of the two, and its first position.
    async fn create(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError>;
    async fn append(&self, recording: &RecordingId, object: Object, pieces: &[Bytes]) -> Result<(), StoreError>;
    async fn seal(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError>;
    async fn list(&self, recording: &RecordingId, kind: ObjectKind, from: Position) -> Result<Vec<Listed>, ListingError>;
    /// The bytes of the object from an offset on.
    async fn read(&self, recording: &RecordingId, object: Object, from: u64) -> Result<Self::Stream, FetchError>;

    // The manifest is replaced as a whole.
    async fn put_manifest(&self, recording: &RecordingId, manifest: Bytes) -> Result<(), StoreError>;
    async fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError>;
}

/// A place in an object that moves forward.
pub trait ByteStream: Send {
    /// The next `wanted` bytes as one buffer, fewer at the end of the object, and `None` behind it.
    async fn next(&mut self, wanted: usize) -> Result<Option<Bytes>, FetchError>;
}

/// A segment or an index chunk, named by the position of the first entry that it holds or describes.
pub struct Object { pub kind: ObjectKind, pub first: Position }
pub enum ObjectKind { Segment, IndexChunk }
/// What a listing says of an object.
pub struct Listed { pub first: Position, pub length: u64, pub sealed: bool }
```

The traits declare their async methods as the storage contract does: each returns a future that is `Send`.

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
  The layout always knows how much it wants next: the 48 bytes of a chunk header, and then the bytes that the header names.
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
| Memory | In memory, for tests, and for an engine whose recordings need not outlive it. It keeps each append as one buffer, and hands out a header or a chunk as a slice of it. For the tests of recovery it also tears an object, damages a byte, and takes an object or the manifest away. | `recording_segments`, behind the feature `memory` |
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

A recording has two versions, and every object says both in its header.

| Version | Counts | Its owner | It rises when |
|---|---|---|---|
| Of the encoding | What a [message](#a-message) is in bytes: the stored types of an entry, a description, a summary and an end. | `recording` | A stored type changes in any way. |
| Of the layout | The framing: the marks, the headers, the [records](#records), the levels and the order of writing. | `recording_segments` | A header, a record or a level changes. |

A new codec takes a new number of its own, which a block names, and no new version.
A new layout is not a new version either: it is another implementation of the storage contract, and it carries the same messages.

One writer writes a recording, with one build, so every object of a recording says the same two versions.
A reader that finds two in one recording says that the recording is not readable.

| Version of the encoding | Written since | What changed |
|---|---|---|
| 1 | Not released yet. | The first. |

| Version of the layout | Written since | What changed |
|---|---|---|
| 1 | Not released yet. | The first. |

### The stored types of a version

A message [cannot grow](#a-message), so the encoding does not change: it gets a new version, and the old one stays.

- **`recording` has one module for every version of the encoding.**
  It holds the stored types of that version, for every message.
  A stored type that did not change from one version to the next is the type of the version before, under its name.
  A stored type that changed is written anew, and so is every stored type that holds it.
- **A reader reads every version that was ever written.**
  A build holds the module of every version that a release wrote, and none is ever taken out.
  A version costs a module that nobody edits, its samples and one conversion: for the calls of the measurement the stored types were about 300 lines.
- **A module never changes once a release wrote its version.**
  Nobody edits a stored type of a released version, for any reason.
  A [test](#a-type-cannot-change-without-a-version) holds every released module to its bytes.
- **Each version converts to the next.**
  The module of a version holds the way from every stored type of the version before to its own.
  The way from an old version to the current one is those steps in a row, so a change of a type takes one conversion, and no conversion is ever written again.
- **A conversion invents nothing.**
  A value that an old recording cannot know becomes the value that says so: nothing where a value may be missing, and an empty list.
  A stored type that gains a value which every old entry would have to make up takes it as one that may be missing.
- **The current version alone has an encoder, views and the way to the types.**
  The encoder writes the current version, and no build writes an older one.
  `CallView`, `to_call` and their like read the stored types of the current version, and nothing else.
- **A version is fixed by the release that first writes it.**
  Until then it is open: its stored types change in place, and several changes between two releases share one number.
  A recording that a build between two releases wrote opens in that build, and nothing promises more.
- **Version 1 is fixed with the first release.**
  Until then no recording of an earlier build has to open.

`EncodingVersion` is the version as `recording` hands it around.
It exists only for a number that this build reads.
Inside `recording` it is a closed set of the versions that a build ever wrote, and whatever reads a message decides on that set without an arm for the rest.
So a new version does not compile until every reader has the way from the version before.

| | |
|---|---|
| `EncodingVersion::CURRENT` | The version that this build writes. |
| `EncodingVersion::read_by_this_build(number)` | The version, or `EncodingVersionError::Newer { written, known }` for a number above the current one, or `EncodingVersionError::Zero` for zero, which no build writes. |

### An older version is converted

A reader reads the current version through views, and every older one through its conversion.

1. `Batch::checked` gets the version that the recording says.
2. For the current version it checks the buffer, and that is all.
3. For an older version it checks the buffer with the stored types of that version, builds each entry as a stored type, converts it step by step to the current version, and writes it into a new buffer.
   The batch is that new buffer, and the views read it as they read any other.

- **An old recording is not read in place.**
  Its entries are built and written once more, so it reads at the speed of a decoder, and each block is in memory twice for a moment.
  Nobody measured that speed. A reader that built a `Call` from every entry read [270 000 entries a second](#the-encoding-measured), which is the size of it.
- **The frames are not converted.**
  A frame is the bytes of Kafka, and a slot keeps its place.
- **A description, a summary and an end convert in the same way**, when `recording` reads them.
- **Views of every version were the other way.**
  Every reading of every view would then have a case for every version, and each new version would touch all of them.
  The conversion keeps the readers to one set of types, and puts the cost on the recordings that are old.

### A version that a reader does not know

A reader looks at the versions in the header of an object before it reads anything else of it.

| The recording has | The reader |
|---|---|
| A version of the encoding above its own | Refuses the recording with `OpenError::EncodingNotRead { error }`. The error is `EncodingVersionError::Newer { written, known }`, and the message names the version that the recording has and the newest that this build reads. |
| Zero as the version of the encoding | Says that what is there is no recording: `OpenError::NotReadable { reason }`. |
| A version of the layout above its own | Refuses the recording with `OpenError::LayoutNotRead { written, known }`, and the message names the same two versions of the layout. |

A build reads [every version](#the-stored-types-of-a-version) that a release wrote, so no reader meets one that is too old for it, and no error says so.
A reader never guesses.
It reads no message of a version that it does not have the stored types of.

### A type cannot change without a version

`recording` keeps two things for every version of the encoding, next to its tests:

- **The samples of the version**, as a function.
  It builds a value of every stored type, with every case of every choice at least once, a text that is empty and one that is not, a list that is empty and one that is not, and a value that is missing and one that is there.
  It names every field and matches every choice in full, so a new field or a new case does not compile until it has a sample.
- **The bytes of those samples**, as a file that is committed.

| A test | Fails when |
|---|---|
| Writing the samples of a version gives the bytes of its file, byte for byte. | A stored type of that version changed: a field more or fewer, another order, another size, another order of the cases. Or the library that archives lays a type out in another way. |
| The file of a version passes the [check](#the-check-of-a-block), and reads as the samples of that version. | The reading of that version changed. |
| The samples of a version, converted, are the values that the test names for the next version. | A conversion changed, or loses a value. |
| Every value of `call` and `journal` that the tests build goes into an entry and comes back the same through `to_call`. | A type and its stored type no longer say the same. |

- **The file of a released version is never written again.**
  A change to one is a change of a file that nobody has a reason to touch, and a review refuses it.
  The command that writes the file of the open version writes no other: it is a test of the module of that version that is ignored, and it goes away with the release that first writes the version.
- **The bytes are the test, and not a fingerprint of the layout.**
  A fingerprint of the sizes, the alignments and the offsets of the archived types fails on the same changes of a type.
  It says nothing about how a text or a list is written, nothing about a conversion, and it needs a fingerprint for every type, by hand or by a macro of our own.
  The bytes are what a reader will meet.
- **A new release of the library that archives is a change like any other.**
  If it gives other bytes, the first test fails, and the upgrade takes a new version of the encoding.
  The modules of the older versions keep the release that wrote them, and a build can hold both.

The tests of `recording_segments` keep one small recording of every released version of the layout, and open each of them.
That makes "an old recording still opens" a check, and not a promise.

### When `Call` or a journal entry changes

1. **Change the type**, in `call` or in `journal`.
2. **See what fails in `recording`.**
   The encoder takes a type apart field by field and case by case, so a new field or a new case does not compile there.
   A change that compiles and still changes what an entry says fails the test that sends every value through an entry and back.
   A change that changes nothing in an entry, such as a new method, ends here.
3. **See whether the current version is open.**
   It is open when no release wrote it yet.
   Then change its stored types in place, and go on at step 6.
4. **Add the module of the next version.**
   Write anew the stored type that changes, and every stored type that holds it.
   Take every other stored type from the version before, under its name.
5. **Write the conversion from the version before**, in the new module, for every stored type that was written anew.
   A value that an old entry cannot know becomes one that is missing, or an empty list.
6. **Move the encoder, the views and `to_call`** to the stored types of the new version, and raise `EncodingVersion::CURRENT`.
7. **Add the samples.**
   Write the samples of the new version, with a sample for what is new, and the values that the samples of the version before convert to.
   Write the file of their bytes with the command for it.
8. **Add a row to the table of versions** at the top of this section.
9. **Run `cargo xtask`**, and let CI run the tests.

`recording` owns all of it: the stored types of every version, the conversions, the samples and their bytes.
A change of `call` or `journal` therefore changes `recording`, and no other crate of a recording.
`recording_segments` hands the version on and never reads a message.

A change of the layout takes the same steps in `recording_segments`, with the reading code of the old version in the place of a module of stored types.

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
| A [query with a selector](control-plane.md#drafts), saved or draft, from the UI, the CLI or a test | The entries of the spans that can match. | `Wanted`, which skips at every level. The manifest stays in memory while the recording is open. A draft that changes with every key streams the index chunks again, which on files come from the cache of the system. |
| A [test](control-plane.md#tests) that asserts on verdicts and explanations | The same. | The explanation is a part of every call. |
| The UI that shows the latest calls | The last blocks, backwards. | A stream from the end. |
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

## Known limits

| Limit | What it means | What there is instead |
|---|---|---|
| No reader in another language | A message is the archive of a type of this code base. There is no schema that a tool in Java, Python or Go reads a recording with, and none is planned. | Another tool asks the control plane, which sends a call in its own encoding. |
| A message cannot grow | Every change of what an entry holds is a new [version of the encoding](#versions), with a module, a conversion and samples. A build holds every version that was ever written. | The steps are [written down](#when-call-or-a-journal-entry-changes), and a test fails when one is left out. |
| The bytes follow the library that archives | A release of that library that lays a type out in another way is a new version of the encoding. | The [samples](#a-type-cannot-change-without-a-version) fail on it. |
| An old recording is not read in place | A block of an older version is [converted](#an-older-version-is-converted): one more copy, at the speed of a decoder. That speed was not measured. | A recording of the current version, which is every recording that a build writes itself. |
| Nobody measured frames | The measurement held entries and no frame. What the frames of a real recording compress to, and what compressing them apart from the entries costs, is not known. | — |
| Nobody measured a summary | The [size of the index](#sizes) rests on an estimate of 2 KiB for the summary of a span. | `recordings.summary_values` and `recordings.span_size` bound it. |
| A recording does not export to one file | It is a directory. | [Not decided](../open-questions.md#storage). |

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
  block_size: 4MiB          # the entries and frames of one block, before they are compressed: what a reader holds at a time
  span_size: 64MiB          # the entries and frames of the blocks that one summary covers, before they are compressed
  segment_size: 1GiB        # the size at which a data segment is sealed, as it is stored
  index_size: 256MiB        # the size at which an index chunk is full
  summary_values: 256       # the values of one field that a summary lists
  reader_memory: 16MiB      # the unpacked blocks that one reader holds at most, with what it read ahead
  all_readers_memory: 256MiB  # the unpacked blocks that all readers of the process hold together at most
```

A size is a number of bytes with its unit.
`queue_entries`, `queue_size`, `commit_interval` and `commit_size` are the recorder's, whatever the layout is.
The others are the segment layout's.
`reader_memory` and `all_readers_memory` are the only two that a reader needs: a recording says its codec and its versions itself, and its objects say their sizes.

What changed with blocks of 4 MiB, and what did not:

| Property | Default | Why |
|---|---|---|
| `block_size` | `4MiB`, from `64MiB` | A reader holds a block. |
| `span_size` | `64MiB`, which is new | A summary covers a span of blocks, and not a block, so the index stays small. 64 MiB is what a block was, and what a summary covered. |
| `summary_values` | `256`, as before | A summary covers what it covered before. |
| `commit_interval`, `commit_size` | `1s` and `16MiB`, as before | They say what a crash loses and what waits in the writer, and neither is about a reader. A commit of 16 MiB covers four blocks, and is still one append and one wait for the disk. |
| `segment_size` | `1GiB`, as before | It is the size of a file. |
| `index_size` | `256MiB`, as before | A reader streams an index chunk, so its size bounds no memory. It decides how long the manifest is: one record for 420 GiB of segments. |

## Decisions

| Question | Decision | What it replaced, and why |
|---|---|---|
| Is a recording tied to one format? | No. A storage contract speaks of entries, and a layout implements it. | A reader and a writer of one file format. A second format would have been a second set of readers. |
| What is the first layout? | Data segments of compressed blocks, index chunks that describe them, and a manifest that lists the index chunks. | One file with a summary in every chunk. A query had to walk every chunk of it, and a recording could not grow past one file. |
| How does a reader read? | As a pipeline of streams: the backing gives bytes, the codec gives blocks, and the decoder gives entries. Every reader is a pass over the blocks that the summaries leave. | Reading one entry, or one field, out of the middle. No reader does that, and a layout that serves it pays for it in every stream. |
| Is a segment compressed? | Yes, by block, behind a seam. Zstandard at level 3 is the default, and none is a choice. A reader takes the codec from the header of the block, at run time. | No compression: fourteen times the bytes, and slower from the disk. Compression by segment: a reader could only start at the start of a segment. |
| What does zero-copy mean? | Nothing copies the bytes of an entry after the decompressor wrote them. A reader reads an entry through a view, and a text and a frame as slices of the buffers of their block. | Reading an entry from the file where it lies, which compression rules out. Building the type of every entry, which most readers never look at. A decoder that copies every text into a string of its own. |
| Which encoding? | A message is the archive of a stored type: the type as it lies in memory. A reader checks a block once and reads it through views. See [the encoding, measured](#the-encoding-measured). | Encodings that are decoded: less than half the speed, and an allocation for every entry. Encodings that are read through views and can grow: a third of the speed, or less. |
| How does an entry change, when a message cannot grow? | The encoding has [versions](#versions). `recording` keeps the stored types of every version and the way from each to the next, and a reader converts a block of an older version. A reader reads every version that was ever written. | A schema whose messages grow, which the encoding does not have. Views of every version, which would put a case for every version into every reading. |
| What keeps a type from changing without a version? | The bytes of samples of every version, committed, and tests that write them and read them. | A fingerprint of the layout of the types, which says nothing about a conversion and has to be written for every type. |
| Who has a message? | `recording`, for all of them: an entry, a description, a summary and an end. A layout carries them between records of fixed binary. | Messages of the layout for its footers, its index and its manifest. The layout would have had stored types and versions of its own, with the summaries of `recording` inside them. |
| What does a reader do before the first view? | `recording` checks the block once: every head, every reference, every length, every text and every choice. | A check at every step of every view, which costs a reader of a whole entry more than decoding it. No check, which reads whatever a forged recording points at. |
| What bounds memory? | Named budgets: one for a reader, one for all readers of a process, and for the writer the sizes of its queue, of a commit and of a block. A block is small, so a budget is a few blocks. See [memory](#memory). | Blocks of 64 MiB, which a reader holds whole: eight readers held 0.5 GiB. A reader without a bound, whose read-ahead and kept views decide what it holds. |
| What happens when a budget is reached? | [Nothing waits](#when-a-budget-is-reached). A reader stops reading ahead, a consumer that holds too much gets `ReaderBudgetSpent`, and a stream that finds no room for one block is refused with `AllReadersBudgetSpent`. Each error names its budget. | A reader that waits for room. It can wait for its own consumer, and a wait says nothing to whoever asked. |
| Who pays for an entry that is kept? | Whoever keeps it: a kept view or frame keeps its block in memory, and counts against the budget of its reader. A consumer that keeps entries beyond its stream copies them out. | A storage that copies every entry it hands out, so that nothing is ever kept alive: every reader would pay for the few that keep. |
| Who owns the encoding of an entry? | The contract. A layout keeps batches and never looks into one. | The layout. Views would be the layout's, and a reader would be written against one layout. |
| What does the manifest say about an index chunk? | Where it starts, and for a full one what it covers and the sum of its summaries. | A list of names alone. A reader would read every index chunk to find a position, a moment or a topic. |
| Where do the bytes of the layout live? | Behind a third contract of three kinds of object, which hands out streams. Files are the first backing. | A layout that opens files itself. It could not be tested without a disk, and could never move to an object store. |
| What closes each level? | A size, and for a chunk an interval. | A count of entries, which bounds neither memory nor loss. An age for segments, which only a log with retention needs. |
| What does a summary cover? | A [span](#spans) of blocks: 64 MiB of entries and frames, by a property. A block has a row of 32 bytes in the record of its span. | A summary for every block of 4 MiB: sixteen times the index, 0.7% of a recording. |
| Is a block a commit, or a segment? | Neither. A commit writes a chunk, a block is what a reader holds and where a stream can start, and a segment is a file. | A block that was also the commit: under light traffic a second of calls is a block of its own, which compresses badly and fills the index. A block of 64 MiB, which costs every reader that much memory. |
| What is in a summary? | The values of every field, as lists with a limit. | Bloom filters, which answer `is` and cannot answer `matches`. Ranges of values, which no operator asks for. |
| What is left after a crash? | Everything that was committed. The segments describe themselves, so a missing index chunk or manifest is rebuilt from them. | A manifest that must be right. A crash between two writes would then lose what the segments still hold. |
| How does metadata change? | An object is written before what describes it. Segments and index chunks only grow, and the manifest is replaced in one step. | An index chunk that is written again for every segment: more to write with every level of size. |
| Where is the recording mode applied? | In `recording`, where an entry is made. A storage never sees what must not be kept. | In the engine, where every caller would have to remember it. In a layout, where every layout would have to. |
| What does metadata-only keep of a frame? | The frame, without the keys, values and headers of its records. | No frame at all. The usual recording of a real cluster could then seed no World and run no replay. |
| Is a recording one file? | No. It is a directory of segments, and whoever reads it reads it through the storage contract. Whether it exports to one file is not decided. | One file for each recording. A recording can be many gigabytes. |

### The encoding, measured

This is the record of why a message is an archive.
The tables name what was measured, and the design names no library.

One call record in six encodings, built from the real types: a Produce for two partitions with three lines of explanation, and a Fetch for five partitions with two.
The calls differ as traffic does: a few hundred clients, connections and topics that come back, and moments, latencies and counts that do not.
Frames are not a part of it, and every read starts from bytes that nobody trusts.

The machine: an AMD Ryzen 7 255 with 8 cores and 7 GiB of memory, Debian 13, ext4 on a disk that gave 0.8 GiB a second.
The code was built for that processor.
Every number is the median of its measurements.
A read from the disk starts with none of the pages of the file in memory.

#### Speed

**A stream of a gibibyte of entries**, on one core, in blocks of 64 MiB, each compressed with Zstandard at level 3.
The reader unpacks a block and reads every entry: through a view where the encoding has views, and by decoding it where it has none.

| | Bytes of an entry | As stored | Write: encode, compress | Whole entries a second | From the disk | Their fields alone | To a `Call` |
|---|---|---|---|---|---|---|---|
| JSON | 1554 | 135 | 4.5 µs, 1.6 µs | 147 000 | 144 000 | 568 000 | 145 000 |
| CBOR | 1200 | 118 | 5.1 µs, 1.3 µs | 92 000 | 91 000 | 118 000 | 91 000 |
| Protocol Buffers | 648 | 95 | 3.0 µs, 1.0 µs | 635 000 | 609 000 | 1 401 000 | 268 000 |
| Cap'n Proto | 1658 | 121 | 3.2 µs, 1.4 µs | 505 000 | 479 000 | 956 000 | 202 000 |
| FlatBuffers | 1449 | 108 | 4.4 µs, 1.3 µs | 309 000 | 301 000 | 336 000 | 163 000 |
| rkyv: an archive of the types as they lie in memory | 1780 | 112 | 3.1 µs, 1.3 µs | 1 482 000 | 1 312 000 | 1 525 000 | 270 000 |

The first three are decoded.
The last three are read through views of the buffer that the decompressor wrote.

**The same stream without a codec**, where a reader reads the mapped file itself:

| | As stored | Whole entries a second | From the disk | Their fields alone | To a `Call` |
|---|---|---|---|---|---|
| JSON | 1554 | 156 000 | 138 000 | 786 000 | 156 000 |
| CBOR | 1200 | 95 000 | 89 000 | 124 000 | 95 000 |
| Protocol Buffers | 648 | 741 000 | 589 000 | 2 199 000 | 294 000 |
| Cap'n Proto | 1658 | 670 000 | 451 000 | 1 722 000 | 221 000 |
| FlatBuffers | 1449 | 364 000 | 286 000 | 390 000 | 173 000 |
| rkyv | 1780 | 4 639 000 | 516 000 | 5 102 000 | 294 000 |

**What the codec costs and saves**, measured with Cap'n Proto:

| | As stored | Compress, for an entry | Whole entries a second | From the disk |
|---|---|---|---|---|
| None | 1658 | — | 670 000 | 451 000 |
| Zstandard, level 1 | 142 | 1.3 µs | 496 000 | 471 000 |
| Zstandard, level 3 | 121 | 1.4 µs | 505 000 | 479 000 |

**What the size of the stretch does**, with Cap'n Proto at level 3, as milliseconds from memory and from the disk:

| 64 MiB of entries | 256 MiB | 1 GiB |
|---|---|---|
| 81, 85 | 328, 340 | 1290, 1360 |

**What the size of a block does**, as stored bytes for a call, with 256 MiB of entries at level 3:

| Blocks of | 1 MiB | 4 MiB | 16 MiB | 64 MiB | 256 MiB |
|---|---|---|---|---|---|
| Protocol Buffers | 99 | 97 | 95 | 95 | 95 |
| Cap'n Proto | 123 | 123 | 121 | 121 | 121 |

The time to stream them was the same for every size of block.

#### Memory

A second measurement ran a writer and readers as the layout has them.
The writer takes calls from a queue of 10 000, which three sessions kept full, and commits every 16 MiB of entries.
It writes blocks of 4 MiB or of 64 MiB, in chunks, until the file is 1 GiB as stored.
A reader reads the file through the cache of the system, unpacks each block into a buffer of its own, reads every entry, and lets the block go.

- **Peak RSS is `VmHWM`** of `/proc/self/status`, for the whole process.
- **The eight readers are threads of one process**, and each reads the whole file.
  Their peak RSS is that of all eight together.
  Their entries a second are what each one of them read, so the eight together read eight times that.
- **The heap for an entry** is what a counting allocator saw a reader ask for while it read one entry.
- **The stored bytes are a little above those of the first table.**
  This writer flushes the codec at every commit, and the first one compressed each block in one piece.

| Encoding | Block | Stored bytes for a call | Writer: peak RSS (VmHWM) | One reader: peak RSS (VmHWM), entries a second | Eight readers: peak RSS (VmHWM), entries a second for each | Heap for an entry read |
|---|---|---|---|---|---|---|
| rkyv | 4 MiB | 122 | 52 MiB | 7 MiB, 1 530 000 | 38 MiB, 834 000 | 0 |
| rkyv | 64 MiB | 117 | 56 MiB | 72 MiB, 1 280 000 | 536 MiB, 586 000 | 0 |
| Protocol Buffers, a text as a shared slice | 4 MiB | 102 | 59 MiB | 8 MiB, 627 000 | 41 MiB, 238 000 | 3.3 KB, in 9.5 allocations |
| Protocol Buffers, a text as a borrowed slice | 4 MiB | — | — | 8 MiB, 528 000 | 40 MiB, 287 000 | 2.9 KB, in 9.5 allocations |
| Protocol Buffers, a text as a copy | 4 MiB | — | — | 8 MiB, 535 000 | 41 MiB, 110 000 | 3.3 KB, in 33.5 allocations |
| Cap'n Proto | 4 MiB | 128 | 60 MiB | 7 MiB, 429 000 | 38 MiB, 228 000 | 0 |
| FlatBuffers | 4 MiB | 116 | 52 MiB | 8 MiB, 313 000 | 38 MiB, 239 000 | 0 |
| JSON | 4 MiB | 143 | 54 MiB | 8 MiB, 137 000 | 40 MiB, 34 000 | 7.7 KB, in 94.5 allocations |
| CBOR | 4 MiB | 128 | 56 MiB | 8 MiB, 86 000 | 40 MiB, 28 000 | 6.6 KB, in 91 allocations |

With blocks of 64 MiB every encoding had a peak RSS (VmHWM) of 72 to 79 MiB for one reader, and of 550 to 600 MiB for eight.

**A view that is kept**, with rkyv and blocks of 4 MiB, over a gibibyte of entries, which are 603 000 calls:

| The consumer keeps | As | It holds | Peak RSS (VmHWM) |
|---|---|---|---|
| Every 1000th call: 604 calls | A view | 1024 MiB: every block | 1028 MiB |
| | A copy of the entry | 1.1 MiB | 11 MiB |
| | A `Call` | 2.5 MiB | 12 MiB |
| Every 100 000th call: 7 calls | A view | 28 MiB: seven blocks | 35 MiB |
| | A copy of the entry | 12 KiB | 11 MiB |

#### What the numbers say

- **The archive reads a whole entry more than twice as fast as any other encoding, and allocates nothing.**
  It read 1.48 million entries a second where the next one read 0.64 million.
  With eight readers it read 834 000 for each, and no other encoding passed 290 000.
- **What a reader holds follows the block, and not the encoding.**
  Every encoding held 7 or 8 MiB for a reader with blocks of 4 MiB, and 72 to 79 MiB with blocks of 64 MiB.
- **A decoder pays for every entry in the heap.**
  The decoders asked for 3 to 8 KB with every entry, in 10 to 95 allocations, and eight readers that do so get in each other's way.
  A view asks for nothing.
- **A kept view keeps its block.**
  604 calls of 1.8 KB each held a gibibyte.
- **Compression levels the sizes.**
  The encodings differ nearly threefold before the codec, and by less than half after it.
  A layout of our own that held each text once would save what the codec saves anyway.
- **A compressed recording is not slower to read from a disk.**
  It is faster than a plain one, because the disk has a fourteenth of the bytes to give.
  From memory a plain archive reads three times as fast, and a mapped file of it is what the second table shows.
- **A small block costs a few bytes, and no speed.**
  A call took 122 bytes in blocks of 4 MiB and 117 in blocks of 64 MiB, and the stream over the small blocks was the faster one.
- **Reading through views is not faster by itself.**
  Cap'n Proto checks every step that a reader takes, and a reader of a whole entry takes them all: 2.0 µs for an entry, against 1.6 µs to decode the same entry from Protocol Buffers.
  FlatBuffers checks a whole entry before the first read, at 2.4 µs.
  The archive checks a whole entry before the first read too, at 0.15 µs.
- **To a `Call`, every encoding is slow, and they are close.**
  Building a `Call` from plain values through its checks takes 2 µs, whatever the encoding is.
  A reader that wants the type gains nothing from views.

#### What the archive gives up

| | Checks | Grows | Asks of the address | Read in other languages |
|---|---|---|---|---|
| Cap'n Proto | Every step, when a reader takes it. No pass over the message, and no unsafe code in a reader. | A field takes the next number, and an old message reads with the new schema. | A multiple of 8. | Java, Python, C++, Go and more. |
| FlatBuffers | The whole entry, before the first read. Without it a reader is unsafe code. | A table takes a field at its end, and an old buffer reads with the new schema. | Nothing, in this implementation. | Java, Python, C++, Go and more, each from the project itself. |
| rkyv | The whole archive, before the first read: 0.15 µs for an entry. | It does not. A type that changes is another layout, and the reader keeps every old type. Its bytes are also those of one version of the library that writes them. | A multiple of 8 for the types of this measurement, and of 16 at most. | None. |

The archive was taken for its speed and for what a reader of it holds in the heap, which is nothing.
It gives up two things.
A message cannot grow, and [versions](#versions) pay for that.
No tool in another language reads a recording, which is a [known limit](#known-limits).

A second set of types is the same work in every encoding: a plain type for every type of an entry, and the way from the types to it and back.
For the calls of this measurement that was about 300 lines for the types and 300 to 500 for each encoding, and `Explanation` is most of it.

An earlier round measured a search for one field of every entry.
A layout that keeps the fields of a block side by side answers it thirty times faster than any of these.
No reader asks for it, and a stream reads whole entries, so the layout does not do it.
