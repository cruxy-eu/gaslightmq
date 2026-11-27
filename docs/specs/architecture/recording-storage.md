# Recording storage

This document says how a [recording](storage.md#recordings) is kept:
the contract that every storage of recordings keeps,
the first layout behind it, which keeps a recording as segments with indexes and a manifest,
and what that layout asks of the place where its bytes live.

A recording only grows, and it can grow very large.
The design follows from six things that it must do:

- **A session never pays for it.**
  A session hands over what it already has in memory, and encodes nothing.
- **A reader copies nothing, and decodes nothing.**
  It reads an entry where it lies, in the bytes that the storage has mapped: its fields, its verdict, its explanation.
  It reads a frame the same way.
  A reader that looks at one field of a call pays for that field, and for nothing else of the call.
- **A query reads a little.**
  A query over a recording of many terabytes reads a few small objects, and not the recording.
- **Nothing grows without a bound.**
  Every object that holds traffic or describes it closes at a size, and never changes afterwards.
- **A recording that was cut short still opens.**
  A crash, a full disk or a full queue leaves a recording that is complete up to a known point.
- **An old recording still opens.**
  A recording that someone kept a year ago opens in the build of today.

## Two seams

```text
engine · world_seed · tests
        │  entries
        ▼
  recording storage        the contract of `recording`
        │
  segment layout           `recording_segments`: segments of batches, index chunks, the manifest
        │  bytes
        ▼
  segment backing          the contract of `recording_segments`
        │
  files │ memory │ later: an object store
```

| Seam | Its contract speaks of | Behind it |
|---|---|---|
| [Recording storage](#the-storage-contract) | Recordings and their entries: session events, calls with their frames, journal entries. It owns the bytes of an entry, and knows nothing of where they are kept. | A layout. The segment layout is the first, and another format is another implementation. |
| [Segment backing](#the-segment-backing) | The three kinds of object of the segment layout, as bytes. It knows no entry. | A place for bytes. Files are the first, and memory serves the tests. |

| Crate | Holds | IO |
|---|---|---|
| `recording` | The storage contract and its errors. What a recording holds: its ID, its description, the entries, a position, a summary, how it ended. The [encoding of an entry](#entries-are-read-in-place), the views that read it in place, and the way from a view to the types of `call` and `journal`. What the [recording mode](#what-a-recording-must-not-keep) drops, and the blanking of credentials. | Sans-IO. |
| `recording_conformance` | What every storage does, as a check. | Test support. |
| `recording_segments` | The segment layout, as a storage over any backing: the framing of its objects, the summaries of each level, the manifest and when it changes, and recovery. The backing contract and its errors. The backing in memory, behind a feature. | Sans-IO. It awaits the backing it is handed, and nothing else. |
| `recording_segments_conformance` | What every backing does, as a check. | Test support. |
| `recording_segments_fs` | The backing on files: it opens, appends, syncs, renames and maps. | This crate does IO. |

Everything that decides is sans-IO, and a test drives it with bytes in memory:

| Sans-IO, in `recording` | Sans-IO, in `recording_segments` | IO, in `recording_segments_fs` |
|---|---|---|
| The entries: their encoding, and the views that read them in place. What a summary holds, and how summaries add up. Redaction of frames. | The framing of batches and segments. Index chunks. What the manifest holds and when it changes. Recovery, as a function of a listing and of bytes. | Opening, appending, syncing, renaming and mapping files. |

`recording_segments` awaits its backing as `workspace` awaits [its stores](crates.md#the-store-seam):
its rules are plain functions over bytes, and the one part that is async hands bytes to the backing and takes bytes from it.

The engine holds a storage and names no layout.
Whoever puts a process together picks one: `gmq` picks the segment layout on files, and a test picks it in memory.

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
- **The text `any`**: the stretch has more values than `recordings.summary_values`, which is `1024` until someone sets it.

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
| Reads | From the start, from the end or from a position, forwards or backwards. A reader sees every entry that a commit covers, also while the recording runs. Entries come in the order of their positions, a batch at a time. |
| Skips | It asks `Wanted` about every summary it has, at every level it has one, and reads no stretch that cannot hold what is wanted. A storage without summaries asks nothing and hands out everything. What it hands out is never less than what is wanted. |
| Hands out frames | Only when the reader asks for them. A reader that asks for none gets every call with the states and the lengths of its frames, and no storage reads a frame for it. |
| Keeps a batch as it is | It stores the bytes of a batch as `recording` wrote them, in one stretch, and decodes none of them. |
| Hands out entries where they lie | A batch is the buffer that the storage has the entries in: a range of a mapped file, or what it fetched. It starts at an address that is a multiple of 8. No storage builds a type for a reader. |
| Says what it lost | A stretch that it cannot read is an error that names its positions, and the reader can go on behind it. |

`Wanted` carries a selector to the storage without the storage knowing one.
The engine implements it with the check of `selector`, and with a range of time or of call IDs when a reader asks for one.
A reader that wants everything hands in one that says yes.

`recording_conformance` holds this table as a check, for any storage.

### Entries are read in place

A reader gets a batch, and a batch gives views.

- **A batch is a buffer that its owners share.**
  It is whatever the storage has the bytes in.
  A storage over a file that is mapped into memory hands out a range of the map, and copies nothing.
  A storage that fetched the bytes hands out what it fetched.
  The trait hands out no slice with a lifetime, so it needs no lending types, and a batch outlives the call that returned it.
- **A view borrows from its batch.**
  A view of a call, of its fields, of its explanation, of a line of it, of a journal entry or of a session event is a place in the [batch](#a-batch) and nothing more.
  Reading a value through a view reads the bytes of that value: a number where it stands, a text as the bytes it is.
- **A view checks what it reads, when it reads it.**
  A recording from a disk is input from outside.
  Every step from one part of an entry to the next checks that it stays inside the buffer, and every text that it is text.
  So a value that is damaged is an error of the read that meets it, and no view checks an entry that nobody reads.
- **A view hands out values, and the types check them.**
  A view of the fields gives a topic as text and an error code as a number.
  `to_call` builds the `Call` through the checks of its types, and it is the one place where a call that could not have been one is found.
  A reader that shows a call, or hands it to the control plane, asks for the type. A reader that matches, counts or skips does not.
- **A selector matches a view.**
  The view of the fields is a field source, as a live call and a call record are.
  Matching in place needs a field source that lends its values, which `field` does not have yet: its readings own what they hold.
  Until it does, a selector matches the fields that a view builds, and no more of the call.
- **A frame is a range of the same kind.**
  On the way in, the frame of an entry is the buffer that the session read from its socket.
  On the way out, it is a range of the buffer of the storage, and it goes into the codec and into a pipeline as a frame from a socket does.

`recording` owns the encoding of an entry, and so the views.
A layout decides where a batch lies, how it is found and what is said about it, and never what is in it.
That is what lets another layout exist: it keeps the same batches in another way, and every reader reads them with the same views.
A layout that cannot hand out the bytes of a batch in one stretch cannot be a storage.

### A batch

A batch is what one commit writes: the entries since the last commit.
`recording` lays it out, and the layout follows from what readers do.
Almost every reader reads the fields of many calls, and almost none reads the explanation of many.

```text
batch = rows  details  frames
```

| Part | Holds | A reader |
|---|---|---|
| Rows | One message: the table of the texts of the batch, a row for every entry, and a row for every part of a call. A row has a fixed size, and the rows of a kind lie side by side. | Reads the rows of a whole batch in one pass: a scan, the index, a query. |
| Details | For every call, a message of its own: its explanation. | Reads the details of one call, when someone looks at that call. |
| Frames | The frames of the calls, back to back. | Reads one frame, when someone asks for it. |

- **A row of a call holds everything that is not its explanation.**
  Its ID, its connection, its fields, its moments, what the client received, whether a lie applied, and how the response answered each part.
  It says where its details and its frames are, how long each is, and the checksum of each.
- **A text is a number.**
  The table of the batch holds each text once: a topic, a `client.id`, an address, the name of a rule.
  A row holds the number of the text, and so does the explanation of a call.
- **A selector compares numbers.**
  A reader looks a value up in the table of the batch once.
  A batch whose table does not have the value has no such call, and the reader reads none of its rows.
  In every other batch it compares one number in each row, and a pattern is matched once against each text of the table and never against a row.
- **A part is a row too.**
  A call names its parts as a range of the part rows of the batch, and its explanation names a part by its place in that range.
  A part is therefore there once for its call, however many lines of the explanation name it.
- **The table of a batch is its finest summary.**
  It holds every text of the batch, without a limit, so it never says `any`.
- **Each part is checked when it is read.**
  The rows have one checksum, which a reader checks the first time it reads the rows of the batch while the recording is open.
  The details and the frames of a call have theirs in its row, and a reader checks one when it reads what it covers.
  The checks of a view keep a reader inside its buffer, and they do not see a bit that flipped inside a text: the checksum does.

The [measurement](#the-encoding-measured) is why.
The fields of the 668 000 calls of a gibibyte take 82 MiB as rows, and a reader finds the calls of one `client.id` among them in 4 ms, or in 58 ms when the disk has to give them first.
The same search over the same calls as entries of their own, each with its explanation, takes 144 ms and 1.2 s: reading in place saves the decoding, and only rows save the reading.

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
Everything that costs time happens there: redaction, encoding, checksums, and the wait for the storage.
The bytes of a frame are copied once, into the storage.

A frame that waits in the queue keeps its bytes in memory.
A count of calls does not bound that: ten thousand fetches can wait with ten gigabytes.
The queue is therefore full when it holds `recordings.queue_entries` entries,
or when the frames that wait hold `recordings.queue_size` bytes, which is `256MiB` until someone sets it.

The recorder commits when `recordings.commit_interval` has passed since the last commit, which is `1s` until someone sets it,
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
       └─ data segment        up to `recordings.segment_size` each
            └─ batch          what one commit wrote: rows, details and frames
```

| Object | Holds | Changes |
|---|---|---|
| Data segment | The [batches](#a-batch) of the recording, one after the other, and when it is sealed a footer that sums them up. | It grows by one batch with every commit, until it is sealed. A sealed segment never changes. |
| Index chunk | For a series of segments: the summary of each one, and where each of its batches is, with the positions, the time and the call IDs it covers. | It is replaced as a whole when a segment is sealed, until it is full. A full index chunk never changes. |
| Manifest | The description of the recording, the list of its index chunks, each with what it covers, and the end of the recording. | It is replaced as a whole when an index chunk begins, and when the recording ends. |

The segments are the recording.
An index chunk and the manifest hold nothing that the segments do not say:
a reader rebuilds either of them from the segments, and nothing is lost when one is missing.

### Three sizes that were one

An earlier form of this layout cut a segment into blocks of 4 MiB.
A block was three things at once: what one commit wrote, what one summary covered, and what the writer held in memory.
It was small because a reader decoded a block as a whole.
A reader that reads in place decodes nothing, so each of the three gets the size it needs:

| What | Is now | Limited by | Property | Default |
|---|---|---|---|---|
| What a crash can lose | The entries since the last commit. | Time. | `recordings.commit_interval` | `1s` |
| What the writer holds in memory | One batch: its rows and details, and the handles of its frames. A commit comes early when they reach a size. | Memory, and how long one write keeps the recorder from its queue. | `recordings.batch_size` | `64MiB` |
| What one summary covers | A segment. | How much a reader scans when the summary cannot rule the segment out: [4 ms for a gibibyte](#the-encoding-measured), or 58 ms from the disk. | `recordings.segment_size` | `1GiB` |

So a block and a segment are the same thing now: the stretch that is one file, that is sealed once, and that one summary covers.
A batch is not a level of the index.
It is how a segment grows, and the index chunk lists the batches of a segment only to find a position or a moment in it.

Inside a segment a reader needs no finer summary, because the [table of every batch](#a-batch) is one: exact, and read in place.

### What it takes from Iceberg

The model is the tree of metadata of Apache Iceberg: a root that lists small files of metadata, which describe the files of data and say what is in each, so a query prunes at every level.

| From that model | Here |
|---|---|
| Files never change once they are written. | A sealed segment and a full index chunk never change. |
| New metadata is written beside the old, and one step makes it the current one. | The manifest and the index chunk that still grows are replaced in one step: a reader sees the old one or the new one, and never a mix. |
| Every level says what is below it, with ranges, so a reader skips what cannot match. | The manifest sums up every full index chunk, an index chunk sums up every segment, and a batch holds the table of its texts. |
| No file of metadata grows with the table. | An index chunk closes at a size. The manifest grows by one small item with every index chunk, which is one for about a tebibyte of traffic. |

| Left out | Because |
|---|---|
| A catalog | A recording is where its ID says, and its manifest has one name. |
| Snapshots, and the history of the metadata | A recording only grows. Nobody asks what it looked like an hour ago. |
| Commits of several writers | A recording has one writer, so nothing has to settle who was first. |
| Rewriting, compacting and deleting data | Nobody changes a recording. |
| Columns | A row of a fixed size is as near as a recording comes to one. A reader of one field still reads its row, and a row is small. |
| A change of metadata for every write | A commit of entries is one append to the segment that grows. The batches at the end describe themselves, so no metadata changes with them. |

### Bytes

- **A number has a fixed size, and its lowest byte comes first.**
- **What is read in place starts at a multiple of 8.**
  A segment header, a batch, each part of a batch, the details of each call, a footer, the items of an index chunk and the manifest each start at an offset that is a multiple of 8, with zero bytes before them where they must.
  A file that is mapped starts at an address that is a multiple of a page, so each of them is at an address that is a multiple of 8, which the encoding needs.
  A frame starts wherever the frame before it ends.
- **A length counts bytes.**
  A reader checks every length against the bytes that are there before it uses it.
  A recording can come from anywhere, so a reader trusts nothing that it says.
- **A checksum is a CRC-32C**, the checksum that Kafka gives its record batches.
- **What has a structure is a Cap'n Proto message, without packing.**
  A description, a summary, the rows and the details of a batch, a footer, the items of an index chunk and the manifest are messages.
  A message is laid out as a reader reads it, so nothing is decoded: a reader follows a pointer to the part it wants.
- **What is a frame is the frame.**
  The bytes of a Kafka frame stand in a segment as they were on the wire, without the four bytes of their length.

The layout therefore has two layers.
Its framing is fixed binary: marks, lengths and checksums, which a reader finds without reading a message.
What it says is in messages, which a reader reads where they lie.

### The schema

`recording` holds the schema of what the contract owns: the rows and the details of a batch, a description and a summary.
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
segment = segment header  batch …  footer
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

A batch in a segment has a header of 48 bytes, and then its three parts:

| Offset | Size | The batch header holds |
|---|---|---|
| 0 | 4 | The mark of a batch: `GMQB`. |
| 4 | 4 | The checksum of the bytes 8 to 47. |
| 8 | 8 | The length of the batch, with its frames. |
| 16 | 8 | The position of its first entry. |
| 24 | 4 | The number of its entries. |
| 28 | 4 | The length of the rows. |
| 32 | 4 | The checksum of the rows. |
| 36 | 1 | The compression of the details: `0`, none. See [compression](#compression). |
| 37 | 3 | Zero. |
| 40 | 8 | The length of the details. |

The frames take the rest of the batch.

The writer ends a segment with a footer when it seals it:

| The footer holds | |
|---|---|
| The mark of a footer: `GMQF`, and the checksum of the footer. | 8 bytes. |
| A message: the [summary](#summary) of the segment, the list of its batches as the [index chunk](#an-index-chunk) has it, and the end of the recording in its last segment. | |
| The length of the footer, and the mark of an end: `GMQE`. | 16 bytes, the last of the segment. |

A reader finds the footer of a sealed segment from its last 16 bytes.
A segment without a footer is the one that still grows, or the one that a crash cut short.

The end of a recording says how it ended: someone stopped it, or its environment stopped, or the recorder fell behind.
It holds the instant, and the numbers of the entries and the calls of the recording.

A recording that [stops by itself](storage.md#recordings) still ends in order.
The recorder writes everything it took from its queue, and then the footer with the end.
Its last entry is the journal entry that says why it stopped, and the end says it again, so whoever lists the recordings reads no entry.
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
| `summary` | The [summary](#summary) of the segment. |
| `batches` | For every batch of the segment: where it starts, the lengths of its rows and its details, the position of its first entry, the number of its entries, and the ranges of time and of call IDs it covers. |

An index chunk holds everything that a reader needs to pick a segment, and to go to the batch that holds a position, a moment or a call ID.
The list of the batches is a list of rows of a fixed size, which a reader searches in place.

An index chunk describes sealed segments only.

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
| Batch | The recorder commits, or the rows, details and frames that wait reach a size. The call that reaches the size still belongs to the batch. | `recordings.commit_interval`, `recordings.batch_size` | `1s`, `64MiB` |
| Data segment | A commit takes it to a size. A batch is never cut in two. | `recordings.segment_size` | `1GiB` |
| Index chunk | The next segment would take it over a size. | `recordings.index_size` | `32MiB` |
| Summary | A field has more values than a count, and says `any`. | `recordings.summary_values` | `1024` |

Neither a count of entries nor an age closes a segment.
A thousand heartbeats and a thousand fetches differ a thousandfold in bytes, so a count bounds nothing.
An age is for a log that deletes its old segments, and nothing deletes a part of a recording.

What speaks for each default, and against a larger one:

| Property | A larger value | A smaller value |
|---|---|---|
| `commit_interval` | Loses more in a crash. | Makes more batches, each with a table of its own, which repeats the texts that the batch before it had. |
| `batch_size` | Holds more in memory, and keeps the recorder longer at one write: 64 MiB take about 60 ms at a gibibyte a second. | Commits more often under heavy traffic, for nothing. |
| `segment_size` | Makes a reader scan more when a summary cannot rule a segment out, and a summary says `any` sooner. Leaves more for a reader to walk after a crash. | Makes more files: a pebibyte is a million segments of a gibibyte. |
| `index_size` | Rewrites more with every segment: half of it on average, which is 1.6% of a gibibyte at 32 MiB. | Makes a longer manifest. |
| `summary_values` | Makes a larger index. | Lets a summary say `any` for a field that a query asks for. |

### Writing

The layout holds the entries since the last commit in memory, as a batch that `recording` builds, with the handles of their frames.

| When | The layout writes, in this order |
|---|---|
| The recording starts | The manifest, with the description and no index chunk. Then the first segment, with its header. |
| A commit | One batch, appended to the segment that grows. |
| The segment reached its size | It appends the footer, and seals the segment. It writes the index chunk that grows again, now with this segment. It creates the next segment. |
| An index chunk begins: the first one, or the one after a full one | It writes the new index chunk, with its first segment. Then it replaces the manifest, which lists the new one and sums up the full one before it. |
| The recording ends | A commit. The footer, with the end. Then it seals the segment, writes the index chunk again, and replaces the manifest, which now holds the end. |

Two rules make every crash harmless:

- **An object is written before the object that describes it.**
  Whatever an index chunk or the manifest names is there, whole and for good.
  After a crash, the most that is missing is a description, and a description can be rebuilt from below.
- **Every step leaves a recording.**
  The backing gives each write [what it promises](#the-segment-backing), so no step leaves a description half replaced.

A write that fails ends the recording.
The layout says so and takes nothing more.
Nobody opens a recording to write to it again: a new recording has a new [ID](identity.md#recording-id).

The layout holds at most one batch in memory: `recordings.batch_size`, and one call more.

### Opening

A reader works from the top down, and trusts each level only as far as it reaches.

1. **The manifest.**
   It gives the description, the index chunks, and the end when there is one.
2. **The index chunks behind the manifest.**
   The reader lists the index chunks from the last one that the manifest names.
   One that the manifest does not name yet was written just before a crash, and counts.
3. **The segments behind the index chunks.**
   The reader lists the segments from the first position that no index chunk describes.
   That is one segment while a recording runs, and one after a crash.
   A segment with a footer says everything about itself there.
   The reader walks a segment without one: the segment header, then batch header after batch header.
   It checks every mark and every checksum, and that the whole batch is there, and steps over the batch by its length.
4. **The end of the recording.**
   The end in the footer of the last segment means that the recording ended in order.
   Otherwise the last batch that is whole is where the recording stops: it still runs, or it was cut short.

A recording that was cut short opens like any other.
The reader says that it was cut short, and how many bytes behind the last whole batch it left unread.
It changes nothing in the backing.

The walk follows lengths that a checksum covers, and never searches for a mark,
so the bytes of a frame can never pass for the start of a batch.
It reads 48 bytes of every batch, and a segment that grew for an hour at one commit a second has 3600 of them.

The segment that still grows has no summary, and needs none: a reader reads the table of each of its batches.

| What is missing or damaged | The reader |
|---|---|
| The manifest | Lists the index chunks and the segments itself, and takes the description from the header of a segment. |
| An index chunk, or an item of one | Reads the footers of the segments that it described. |
| A batch in the middle of a sealed segment | Steps over it with the length that the index chunk has, and reports its positions as lost. |
| The end of the segment that grew | Stops at the last whole batch. |

Recovery is a function: it takes what the backing lists and the bytes it reads, and returns what the recording holds.
It runs in a test without a file.

### How a query walks the levels

A query hands in what it wants, and reads from the top down:

1. It reads the manifest, and keeps the index chunks whose summary can hold what it wants.
2. It reads those index chunks, and keeps the segments whose summary can hold it, and of each segment the batches in the range of time or of call IDs that it asks for.
3. It looks its values up in the table of each batch that is left, and reads no row of a batch that does not have them.
4. It reads the rows of the batches that do, and compares numbers.
5. It reads the details of a call only when someone looks at the call, and a frame only when someone asks for the frame.

| A reader that wants | Reads |
|---|---|
| The latest calls | The manifest, the last index chunk, and the rows of the last batches. |
| A position, a moment or a call ID | The manifest, one index chunk, and one row of one batch. |
| The calls a selector matches | The manifest, the index chunks and the segments that can match, the tables of their batches, and the rows of the batches that have the value. |
| Everything, as the index of the engine does when a recording opens | The rows of every batch, in order, and neither details nor frames. |

A summary gets coarser with every level, so the levels prune different things.
A range of time, of positions or of call IDs prunes at every level, down to a batch.
A value that is rare prunes at every level too, and the table of a batch is exact.
A value that half of the calls have prunes nowhere, and such a query reads the rows of the whole recording: less than a tenth of it.

### Sizes

The numbers below are estimates for a metadata-only recording: about 1.5 KiB for a call with its frames, and about 32 KiB of index for a segment.
The rows of the fields [measured](#the-encoding-measured) 130 bytes for a call.
A row that also holds the moments of its call and what the client received is larger, and is not measured yet.
Neither are the details, the frames and the index.

| | With the defaults |
|---|---|
| A batch | One second of traffic, and 64 MiB at most. |
| A segment | 1 GiB: about 700 000 calls, with 82 MiB of rows. |
| An index chunk | 32 MiB: about 1000 segments, which is a tebibyte of traffic. |
| An item of the manifest | About 40 KiB for an index chunk. |

| A recording of | Segments | Index chunks | All index chunks | The manifest |
|---|---|---|---|---|
| 100 GiB | 100 | 1 | 3 MiB | 1 KiB |
| 10 TiB | 10 240 | 10 | 320 MiB | 400 KiB |
| 1 PiB | 1 048 576 | 1024 | 32 GiB | 40 MiB |

- The index is about 0.003% of the traffic it describes.
- The index chunk that grows is written again for every segment: 16 MiB on average for a gibibyte of traffic, which is 1.6% more to write.
- The manifest is replaced once for every tebibyte of traffic.
- A recording of 10 TiB opens with one read of 400 KiB, and its latest calls take three reads.
- A query that no summary helps reads the rows of every segment: 4 ms for each from memory, 58 ms from the disk of the measurement, which is 10 minutes for 10 TiB on one core and that disk.
- A recording of a pebibyte is a million files in one directory. It wants a larger `segment_size`, which the property gives it.

### Keeping entries small

A message that is read in place is larger than one that is decoded: it keeps the room of every number, and a pointer to every part.
A call record [measured](#the-encoding-measured) 1500 to 1700 bytes as a message of its own, against 470 to 700 as a message that is decoded.
A [batch](#a-batch) brings that down, and each of these leaves a reader reading in place:

| What | Saves | State |
|---|---|---|
| A batch holds each text once. | The texts of a call are 650 of its 1600 bytes, and 100 as numbers into the table. | Measured for the fields: 130 bytes for a call, with its parts and its share of the table. |
| A call names each of its parts once. Its explanation points at them. | The parts that an explanation repeats, line after line. | In the design. The details are not measured yet. |
| A filesystem that compresses. | Whatever it saves, under the map. | Nothing of ours. |

The packed form of the encoding is not one of them.
It leaves out the zero bytes, and a reader must unpack a message before it reads it.

### Compression

Version 1 compresses nothing.

The rows are never compressed: they are what a reader scans in place.
Compressing the details of a batch would save bytes, and a reader would unpack them into memory before the first view of an explanation: one copy, and still no decoding.
The byte for it is in the header of a batch, so a later version can offer it as a choice of a recording.
Whether it should is not decided, and wants a measurement on real traffic: the calls of the bench are too alike to say what compression saves.
The frames stay plain in every version: a frame that is compressed cannot be handed out without a copy,
and the record batches in it are often compressed already.

## The segment backing

The segment layout asks one contract for a place to keep its bytes.
The contract speaks of the three kinds of object, and each is named by the position of the first entry it holds or describes.

```rust
/// Where the objects of recordings live. The segment layout works over any of them.
pub trait SegmentBacking {
    async fn recordings(&self) -> Result<Vec<RecordingId>, ListingError>;

    // Data segments.
    async fn create_segment(&self, recording: &RecordingId, first: Position) -> Result<(), StoreError>;
    async fn append(&self, recording: &RecordingId, segment: Position, pieces: &[Bytes]) -> Result<(), StoreError>;
    async fn seal(&self, recording: &RecordingId, segment: Position) -> Result<(), StoreError>;
    async fn segments(&self, recording: &RecordingId, from: Position) -> Result<Vec<SegmentListed>, ListingError>;
    async fn read(&self, recording: &RecordingId, segment: Position, at: u64, at_most: u64) -> Result<Bytes, FetchError>;

    // Index chunks.
    async fn put_index(&self, recording: &RecordingId, first: Position, chunk: Bytes) -> Result<(), StoreError>;
    async fn indexes(&self, recording: &RecordingId, from: Position) -> Result<Vec<Position>, ListingError>;
    async fn index(&self, recording: &RecordingId, first: Position) -> Result<Bytes, FetchError>;

    // The manifest.
    async fn put_manifest(&self, recording: &RecordingId, manifest: Bytes) -> Result<(), StoreError>;
    async fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError>;
}
```

| Operation | Whole or not at all | When it returns |
|---|---|---|
| Create a segment | Yes. It refuses a segment that is there. | The segment is there, empty, and stays after a crash. |
| Append to a segment | No. A crash or a failure can leave the first part of the pieces, and nothing else. It never changes a byte that was there. | Every byte is there after a crash of the process or of the machine. The pieces stand in the segment in their order, as one stretch. |
| Seal a segment | Yes. | The segment never changes again, and takes no append. |
| List segments | — | Every segment from a position on, in order, each with its length and whether it is sealed. It lists what is there after a crash. |
| Read from a segment | — | The bytes from an offset on, at most as many as asked, and fewer at the end of the segment. A buffer that the reader shares with the backing. |
| Put an index chunk | Yes. It makes the chunk or replaces it, and a reader gets the old one or the new one. | The new chunk is there after a crash. |
| List index chunks, read one | — | Every index chunk from a position on, in order. A chunk is read whole. |
| Put the manifest | Yes, as an index chunk. | The new manifest is there after a crash. |
| Read the manifest | — | The manifest, whole. |

- **What a write returned is durable.**
  The layout builds its order of writing on that: it describes an object only once the backing has it for good.
- **An append may tear, and nothing else may.**
  The layout frames what it appends, so it finds the end of a torn segment by itself.
  It frames nothing against a manifest that is half new, so the backing must never show one.
- **A read is a pull, and a segment is a stream of pulls.**
  The layout always knows what it wants next: 48 bytes of a batch header, the rows of a batch, the details of one call, one frame.
  It asks for that range, and no read is larger than the rows of a batch.
  A reader that wants a segment from an offset on pulls it piece by piece.
- **A read hands out a shared buffer.**
  The backing on files maps a segment into memory and hands out a range of the map: the system reads from the disk only the pages that someone looks at, and no byte is copied.
  A backing that fetches hands out what it fetched.
  The layout cuts rows, details and frames out of that buffer, and every view and every frame it hands on shares it.
- **A read keeps the multiples of 8.**
  The bytes of an offset that is a multiple of 8 stand at an address that is a multiple of 8.
  A map does that by itself, and a backing that fetches makes its room that way.
  The same holds for an index chunk and for the manifest, which are read from their start.
- **An append takes pieces.**
  The layout hands over the header, the rows and the details of a batch as buffers of its own, and every frame as the buffer it already is.
  It never joins them, so a frame is copied once, by the backing, into the place where it stays.
- **A backing knows no entry.**
  It keeps bytes under names, and it never looks into them.

### Errors

| The layout | Error | Variants, each a decision of the layout |
|---|---|---|
| Creates, appends, seals, puts | `StoreError` | `AlreadyThere`: for a segment that exists. `NotStored { reason }`. The layout ends the recording with either. |
| Lists | `ListingError` | `NotListed { reason }`. |
| Reads | `FetchError` | `Missing`: the backing has no such object, so the layout rebuilds a description from below, or reports the positions of a segment as lost. `NotFetched { reason }`: the backing did not answer, and the layout hands that on as a reading that can be tried again. |

The layout turns each of them into an error of the [storage contract](#errors), in the words of a recording.

### Backings

| Backing | Keeps the objects | Crate |
|---|---|---|
| Files | As files in one directory for each recording. See [files](#files). | `recording_segments_fs` |
| Memory | In memory, for tests, and for an engine whose recordings need not outlive it. | `recording_segments`, behind a feature |
| An object store | Later. A sealed segment, an index chunk and the manifest are one object each, and each `put` is one request. The segment that grows is what such a backing must solve, for instance as parts that a seal joins. | — |

`recording_segments_conformance` holds what every backing does, as a check.
The tests of the backing in memory run it, and so do the tests of the backing on files, so files are held to what memory does.

## Files

The backing on files keeps the recordings of a workspace under `recordings.directory`, one directory each.
The name of the directory is the [ID of the recording](identity.md#recording-id).

```text
recordings/
└── orders-sim.2026-11-21T14-02-11.527Z/
    ├── manifest
    ├── 00000000000000000000.index
    ├── 00000000000000000000.segment
    ├── 00000000000000262144.segment
    └── 00000000000000518003.segment.open
```

A name is the position of the first entry, with 20 digits, so the names sort as the positions do.

| Operation | On files |
|---|---|
| Create a segment | Creates `….segment.open`, and syncs the directory. |
| Append | Writes the pieces with one write, and syncs the data of the file. |
| Seal | Syncs the file, renames it to `….segment`, and syncs the directory. |
| Put an index chunk, put the manifest | Writes a file beside the old one, syncs it, renames it over the old one, and syncs the directory. |
| Read | Maps a sealed segment once, and hands out ranges of the map. It reads from the segment that grows into a buffer. |
| List | Reads the names of the directory. |

A file with another name is no object of the recording, and the backing leaves it alone.
The file that a `put` did not rename yet is such a file.

Mapping a file is unsafe code, and it is in this crate alone.
A sealed segment never changes, which is what makes a map of it sound.

A recording is a directory, and it can hold many gigabytes.
Whether a recording also exports to one file is [not decided](../open-questions.md#storage).

## Versions

Every object holds the version of the layout.

- **A reader opens every version up to its own.**
  It keeps the reading code of every version that was ever released.
- **A reader refuses a version that is newer than itself**, and the message names both versions.
- **Every change of the layout takes a new number**:
  a new kind of entry, a new state of a slot, a compression, a new level, a new field or a new value in a message.
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

| Reader | Reads | The storage gives it |
|---|---|---|
| The [index](../domains/observation.md#index), which the engine rebuilds when it opens a recording | The fields of every call, once. | The rows of every batch in order. It reads the fields in place, and neither an explanation nor a frame. The index of the engine is not stored. |
| A [query with a selector](control-plane.md#drafts), saved or draft, from the UI, the CLI or a test | The rows of the batches that can match, and the details of the calls that someone then looks at. | `Wanted`, which skips at every level. The manifest and the index chunks that a query read stay in memory, so a draft that changes with every key reads them once. |
| A [test](control-plane.md#tests) that asserts on verdicts and explanations | The same. | The explanation is a part of every call record. |
| The UI that shows the latest calls | The last entries. | A reading from the end, backwards. |
| [Replay](crates.md#the-pipeline-is-sans-io) | The description, every entry, and the request and the truth of every call. | The rules that were armed at the start, the journal entries that changed them, the sessions, and the frames, which go into a pipeline without a copy. The record of every call holds the verdict and the explanation to compare with. |
| [Seeding a World](../domains/world.md#seeding) | The index, and the frames of the calls that describe the cluster or carry records. | `Wanted` for `Metadata`, Produce and Fetch calls, and only their frames are read. |

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
  queue_entries: 10000    # the entries that may wait for the recorder
  queue_size: 256MiB      # the bytes of the frames that may wait with them
  commit_interval: 1s     # how long an entry waits at most before it is in the recording for good
  batch_size: 64MiB       # what waits for a commit at most, before the commit comes early
  segment_size: 1GiB      # the size at which a data segment is sealed
  index_size: 32MiB       # the size at which an index chunk is full
  summary_values: 1024    # the values of one field that a summary lists
```

A size is a number of bytes with its unit.
`queue_entries`, `queue_size` and `commit_interval` are the recorder's, whatever the layout is.
The others are the segment layout's.

## Decisions

| Question | Decision | What it replaced, and why |
|---|---|---|
| Is a recording tied to one format? | No. A storage contract speaks of entries, and a layout implements it. | A reader and a writer of one file format. A second format would have been a second set of readers. |
| What is the first layout? | Data segments, index chunks that describe them, and a manifest that lists the index chunks. | One file with a summary in every chunk. A query had to walk every chunk of it, and a recording could not grow past one file. |
| What does the manifest say about an index chunk? | Where it starts, and for a full one what it covers and the sum of its summaries. | A list of names alone. A reader would read every index chunk to find a position, a moment or a topic. |
| Where do the bytes of the layout live? | Behind a second contract of three kinds of object. Files are the first backing. | A layout that opens files itself. It could not be tested without a disk, and could never move to an object store. |
| Does a reader copy or decode? | Neither. A batch is a buffer that is shared, and a reader reads an entry through views into it. A frame is a range of the same buffer. Only a reader that asks for a `Call` gets one built. | Entries that are decoded into their types. Every reader would pay for the whole of every call, also for the one field it looks at. |
| Which encoding? | Framing of our own around the frames, and Cap'n Proto messages, without packing, for what has a structure. See [the encoding, measured](#the-encoding-measured). | FlatBuffers: half the bytes when a block is one buffer, and a check of the whole buffer before the first read of it. An archive of the layout of the types in memory: the fastest, with no way to grow a type and no reader in another language. Protocol Buffers and JSON: smaller or plainer, and decoded. |
| Who owns the encoding of an entry? | The contract. A layout keeps batches and never looks into one. | The layout. Views would be the layout's, and a reader would be written against one layout. |
| What closes each level? | A size, and for a batch an interval. | A count of entries, which bounds neither memory nor loss. An age for segments, which only a log with retention needs. |
| Is a block a level of its own? | No. A commit writes a batch, a summary covers a segment, and the table of a batch is exact. See [three sizes that were one](#three-sizes-that-were-one). | Blocks of 4 MiB, each with a summary. They were small for a reader that decodes a block, and a reader that reads in place does not. |
| How is a batch laid out? | The fields of its calls as rows side by side, each text once, and the explanation of a call apart. | Each entry as a message of its own. A reader of one field would still read past every explanation, and from a disk that is the whole cost. |
| What is in a summary? | The values of every field, as lists with a limit. | Bloom filters, which answer `is` and cannot answer `matches`. Ranges of values, which no operator asks for. |
| What is left after a crash? | Everything that was committed. The segments describe themselves, so a missing index chunk or manifest is rebuilt from them. | A manifest that must be right. A crash between two writes would then lose what the segments still hold. |
| How does metadata change? | An object is written before what describes it, and a description is replaced in one step. | Changing an index or a manifest in place, which a crash can leave half done. |
| Where is the recording mode applied? | In `recording`, where an entry is made. A storage never sees what must not be kept. | In the engine, where every caller would have to remember it. In a layout, where every layout would have to. |
| What does metadata-only keep of a frame? | The frame, without the keys, values and headers of its records. | No frame at all. The usual recording of a real cluster could then seed no World and run no replay. |
| Is a recording one file? | No. It is a directory of segments, and whoever reads it reads it through the storage contract. Whether it exports to one file is not decided. | One file for each recording. A recording can be many gigabytes, and one file of that size has to be walked to be queried. |
| Is a recording compressed? | Not in version 1. Whether a later version offers it for the details of a batch is [not decided](#compression). Never the rows, and never the frames. | Compression of whole batches from the start, which ends reading from the map before anyone measured what it saves. |

### The encoding, measured

One call record in six encodings, built from the real types: a Produce for two partitions with three lines of explanation, and a Fetch for five partitions with two.
Frames are not a part of it, and every read starts from bytes that nobody trusts.

The machine: an AMD Ryzen 7 255 with 8 cores and 7 GiB of memory, Debian 13, ext4 on a disk that gave 0.8 GiB a second.
The code was built for that processor.
Every number is the median of its runs, on one core.

**One entry.** The mean of the two calls.

| | Bytes | Write | Check before a read | One field | Every value, through views | To a `Call` |
|---|---|---|---|---|---|---|
| JSON | 1475 | 4.0 µs | — | 1.17 µs | — | 6.2 µs |
| CBOR | 1110 | 4.5 µs | — | 7.95 µs | — | 10.4 µs |
| Protocol Buffers | 580 | 2.2 µs | — | 0.27 µs | — | 3.3 µs |
| Cap'n Proto | 1600 | 2.5 µs | None: it checks each step | 0.06 µs | 1.5 µs | 4.2 µs |
| FlatBuffers | 1390 | 4.0 µs | 2.6 µs | 0.01 µs | 0.23 µs | 5.5 µs |
| An archive of the layout of the types in memory | 1730 | 2.5 µs | 0.14 µs | 0.005 µs | 0.05 µs | 2.9 µs |

The first three are decoded, and are here to compare with: their one field is the least decoding that finds it.
For the last three, the one field and every value are read through a view, after the check where there is one.
A journal entry is 100 to 130 bytes in the last three.

**A scan for one `client.id`**, over entries that stand one after the other, each a message of its own.
A block of 4 MiB in memory, and then files through a map: from the disk, with none of their pages in memory, and again from memory.

| | 4 MiB | 64 MiB: disk, memory | 256 MiB | 1 GiB | Calls in the gibibyte | For a call: memory | Disk |
|---|---|---|---|---|---|---|---|
| JSON | 3.3 ms | 86 ms, 54 ms | 352, 217 | 1353, 874 | 715 000 | 1.2 µs | 1.9 µs |
| CBOR | 29.7 ms | 507, 470 | 1990, 1867 | 7994, 7457 | 935 000 | 8.0 µs | 8.6 µs |
| Protocol Buffers | 1.9 ms | 88, 34 | 354, 131 | 1274, 520 | 1 785 000 | 0.29 µs | 0.71 µs |
| Cap'n Proto | 0.16 ms | 75, 8.5 | 288, 36 | 1190, 144 | 668 000 | 0.22 µs | 1.8 µs |
| FlatBuffers | 7.4 ms | 188, 128 | 716, 512 | 2893, 2031 | 766 000 | 2.7 µs | 3.8 µs |
| An archive of the layout in memory | 0.33 ms | 79, 7.0 | 317, 26 | 1254, 107 | 619 000 | 0.17 µs | 2.0 µs |
| **Cap'n Proto, as [rows](#a-batch)**: the fields of the calls of the Cap'n Proto line | | 3.9, 0.03 | 16, 0.25 | 58, 4.4 | 668 000, in 82 MiB | 0.007 µs | 0.09 µs |

- **The time grows with the size, in a straight line.**
  A file of four times the size takes four times as long, from the disk and from memory, in every encoding.
  Nothing in a scan gets worse with a larger stretch, so nothing in a scan asks for a small one.
- **From the disk, the bytes are the cost.**
  Every encoding reads at the speed of the disk, so what counts is how many bytes a call takes.
  Entries that are read in place are large, and from the disk a scan of them is slower for a call than one that decodes a small message: 1.8 µs against 0.71 µs.
- **From memory, reading in place wins, by less than one entry promises.**
  A field of one entry takes 0.06 µs. Over a gibibyte it is 0.22 µs, because the processor waits for the memory of the explanations between the fields.
- **Rows are where reading in place pays.**
  The fields of the same calls, side by side and with each text once, are a twelfth of the bytes.
  A scan of them is 20 times faster than the entries from the disk and 30 times from memory, and 8 and 40 times faster for a call than decoding the smallest message.
- **Half of the time to a `Call` is the types.**
  Building a `Call` from plain values through its checks takes 2 µs, whatever the encoding is.
  A reader that wants the type gains nothing from reading in place, and a reader that wants a field gains all of it.

What decides between the three that are read in place is what each asks before a reader may trust what it reads:

| | Checks | Grows | Asks of the address | Read in other languages |
|---|---|---|---|---|
| Cap'n Proto | Every step, when a reader takes it. No pass over the message, and no unsafe code in a reader. A read of every value costs 1.5 µs for that reason. | A field takes the next number, and an old message reads with the new schema. | A multiple of 8. | Java, Python, C++, Go and more. |
| FlatBuffers | The whole buffer, before the first read: 2.6 µs for a call, which is what makes its scan the slowest but one. After it a read checks nothing. Without it a reader is unsafe code. | A table takes a field at its end, and an old buffer reads with the new schema. | Nothing, in this implementation. | Java, Python, C++, Go and more, each from the project itself. |
| An archive of the layout in memory | The whole archive, before the first read: 0.14 µs for a call. | It does not. A type that changes is another layout, and the reader keeps every old type for good. | A multiple of 8. | None. |

- **Cap'n Proto is the one that pays for what it reads.**
  A reader of one field of one call checks the steps to that field, and nothing else.
- **FlatBuffers is small where texts repeat, and pays with its check.**
  With a whole block as one buffer it holds each text once, and a call is 710 bytes in it.
  The check of the buffer comes before the first read of it, at 2.5 µs for a call.
- **The archive is the fastest, and it cannot grow.**
  Its bytes are the layout of the types of one build.
  "An old recording still opens" would mean a copy of every type for every version, and no tool but ours reads it.
- **With rows, the three are closer than this table says.**
  A row of a fixed size is the same thing in each of them, and a list of rows needs little checking in any.
  What is left to tell them apart is the details of a call, how a schema grows, and who else reads it.

The second schema is the same work in all of them: a message for every type of an entry, and the way from the types to a message and back.
For the calls of this measurement that was about 300 lines for the types and 300 to 500 for each encoding, and `Explanation` is most of it.
