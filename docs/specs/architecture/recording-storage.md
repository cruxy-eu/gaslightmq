# Recording storage

This document says how a [recording](storage.md#recordings) is kept:
the contract that every storage of recordings keeps,
the first layout behind it, which keeps a recording as segments with indexes and a manifest,
and what that layout asks of the place where its bytes live.

A recording only grows, and it can grow very large.
The design follows from six things that it must do:

- **A session never pays for it.**
  A session hands over what it already has in memory, and encodes nothing.
- **A reader copies no frame.**
  A frame can be many megabytes, and most readers never look at it.
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
  segment layout           `recording_segments`: blocks, segments, index chunks, the manifest
        │  bytes
        ▼
  segment backing          the contract of `recording_segments`
        │
  files │ memory │ later: an object store
```

| Seam | Its contract speaks of | Behind it |
|---|---|---|
| [Recording storage](#the-storage-contract) | Recordings and their entries: session events, calls with their frames, journal entries. It knows no bytes. | A layout. The segment layout is the first, and another format is another implementation. |
| [Segment backing](#the-segment-backing) | The three kinds of object of the segment layout, as bytes. It knows no entry. | A place for bytes. Files are the first, and memory serves the tests. |

| Crate | Holds | IO |
|---|---|---|
| `recording` | The storage contract and its errors. What a recording holds: its ID, its description, the entries, a position, a summary, how it ended. What the [recording mode](#what-a-recording-must-not-keep) drops, and the blanking of credentials. | Sans-IO. |
| `recording_conformance` | What every storage does, as a check. | Test support. |
| `recording_segments` | The segment layout, as a storage over any backing: every encoding, the summaries of each level, the manifest and when it changes, and recovery. The backing contract and its errors. The backing in memory, behind a feature. | Sans-IO. It awaits the backing it is handed, and nothing else. |
| `recording_segments_conformance` | What every backing does, as a check. | Test support. |
| `recording_segments_fs` | The backing on files: it opens, appends, syncs, renames and maps. | This crate does IO. |

Everything that decides is sans-IO, and a test drives it with bytes in memory:

| Sans-IO, in `recording` | Sans-IO, in `recording_segments` | IO, in `recording_segments_fs` |
|---|---|---|
| The entries. What a summary holds, and how summaries add up. Redaction of frames. | Encoding and decoding entries. The framing of blocks and segments. Index chunks. What the manifest holds and when it changes. Recovery, as a function of a listing and of bytes. | Opening, appending, syncing, renaming and mapping files. |

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
- **The text `any`**: the stretch has more values than `recordings.summary_values`, which is `64` until someone sets it.

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
    /// The next entries, each with its position, or `None` at the end.
    async fn next(&mut self) -> Result<Option<Vec<Recorded>>, ReadError>;
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
| Hands out an entry | Built through the checks of its type. A storage can never hand out a call that could not have been one. |
| Says what it lost | A stretch that it cannot read is an error that names its positions, and the reader can go on behind it. |

`Wanted` carries a selector to the storage without the storage knowing one.
The engine implements it with the check of `selector`, and with a range of time or of call IDs when a reader asks for one.
A reader that wants everything hands in one that says yes.

`recording_conformance` holds this table as a check, for any storage.

### Frames are shared, not borrowed

An entry holds a frame as the codec has it: a buffer that its owners share.
It holds no slice with a lifetime.

- **On the way in**, the frame of an entry is the buffer that the session read from its socket.
- **On the way out**, the frame is whatever buffer the storage has the bytes in.
  A storage over a file that is mapped into memory hands out a range of the map, and copies nothing.
  A storage that fetched the bytes from somewhere hands out what it fetched.
  A storage that holds them in memory hands out what it holds.

So the contract lets every storage hand out a frame without a copy, and forces none to map anything.
A frame from a recording goes into the codec and into a pipeline as a frame from a socket does.

A call record is decoded, and not borrowed.
A record is a few hundred bytes of small values, and its types check what they hold.
Types that borrow from a storage would be a second set of the same vocabulary.

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
            └─ block          up to `recordings.block_size` each: entries, and the frames of their calls
```

| Object | Holds | Changes |
|---|---|---|
| Data segment | The entries and their frames, as [blocks](#a-block). Each block says what it holds. | It grows by one block with every commit, until it is sealed. A sealed segment never changes. |
| Index chunk | For a series of segments: where each one is, what each of its blocks holds, and their summaries. | It is replaced as a whole when a segment is sealed, until it is full. A full index chunk never changes. |
| Manifest | The description of the recording, the list of its index chunks, each with what it covers, and the end of the recording. | It is replaced as a whole when an index chunk begins, and when the recording ends. |

The segments are the recording.
An index chunk and the manifest hold nothing that the segments do not say:
a reader rebuilds either of them from the segments, and nothing is lost when one is missing.

The model is the tree of metadata of Apache Iceberg: a root that lists small files of metadata, which describe the files of data and say what is in each, so a query prunes at every level.

| From that model | Here |
|---|---|
| Files never change once they are written. | A sealed segment and a full index chunk never change. |
| New metadata is written beside the old, and one step makes it the current one. | The manifest and the index chunk that still grows are replaced in one step: a reader sees the old one or the new one, and never a mix. |
| Every level says what is below it, with ranges, so a reader skips what cannot match. | The manifest sums up every full index chunk, and an index chunk sums up every segment and every block. |
| No file of metadata grows with the table. | An index chunk closes at a size. The manifest grows by one small item with every index chunk, which is one for about 16 GiB of traffic. |

| Left out | Because |
|---|---|
| A catalog | A recording is where its ID says, and its manifest has one name. |
| Snapshots, and the history of the metadata | A recording only grows. Nobody asks what it looked like an hour ago. |
| Commits of several writers | A recording has one writer, so nothing has to settle who was first. |
| Rewriting, compacting and deleting data | Nobody changes a recording. |
| Columns | A reader takes an entry whole, and a frame is bytes that only the codec reads. |
| A change of metadata for every write | A commit of entries is one append to the segment that grows. The blocks at the end describe themselves, so no metadata changes with them. |

### Bytes

- **A number has a fixed size, and its lowest byte comes first.**
  A reader reads a number from its bytes, wherever they stand.
  Nothing is aligned, and nothing is padded.
- **A length counts bytes.**
  A reader checks every length against the bytes that are there before it uses it.
  A recording can come from anywhere, so a reader trusts nothing that it says.
- **A checksum is a CRC-32C**, the checksum that Kafka gives its record batches.
- **What has a structure is JSON.**
  A description, a summary, the body of an entry and the items of an index chunk are JSON, in UTF-8 and without white space between its parts.
  JSON describes itself, which the forms of a call record and of a journal entry need.
- **What is a frame is the frame.**
  The bytes of a Kafka frame stand in a segment as they were on the wire, without the four bytes of their length.

The layout therefore has two layers.
Its framing is fixed binary: marks, lengths and checksums, which a reader finds without decoding anything.
What it says is in JSON, in the forms that the types already have.
A call record and a journal entry have one form each, the form that their crate gives them, and one encoding:
a segment holds the bytes that the control plane sends.
Anyone reads an entry with the tools they have.

Every object starts with a mark of 8 bytes and the [version](#versions) of the layout.

### A data segment

```text
segment = segment header  block …  end
```

| Offset | Size | The segment header holds |
|---|---|---|
| 0 | 8 | The mark of a segment: the bytes `89 47 4D 51 53 45 47 0A`, which read `GMQSEG` between a byte that no text holds and a line feed. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | Zero. |
| 12 | 4 | The length of what follows. |
| 16 | … | A JSON map: the [description](#description) of the recording, and `first`, the position of the first entry of the segment. |
| … | 4 | The checksum of every byte of the header before it. |

Every segment holds the description, so every segment says which recording it belongs to, and a recording without a manifest still knows what it is.

The last segment of a recording that ended in order ends with the end:

| Offset | Size | The end holds |
|---|---|---|
| 0 | 4 | The mark of the end: `GMQX`. |
| 4 | 4 | The checksum of the bytes 8 to 39. |
| 8 | 8 | The number of entries of the recording. |
| 16 | 8 | The number of its calls. |
| 24 | 8 | The instant the recording ended, on the clock of the environment. |
| 32 | 1 | How it ended: `1` someone stopped it, or its environment stopped, `2` the recorder fell behind. |
| 33 | 7 | Zero. |

A recording that [stops by itself](storage.md#recordings) still ends in order.
The recorder writes everything it took from its queue, and then the end.
Its last entry is the journal entry that says why it stopped, and the end says it again, so whoever lists the recordings reads no entry.
A recording that could not be written has no end, because a recording cannot say that it could not be written.
The journal in memory and the control plane say it.

### A block

A block is what one commit writes: the entries since the last commit, and the frames of their calls.

```text
block = block header  summary  entries  frames
```

| Offset | Size | The block header holds |
|---|---|---|
| 0 | 4 | The mark of a block: `GMQB`. |
| 4 | 4 | The checksum of the bytes 8 to 47. |
| 8 | 8 | The length of the block. |
| 16 | 8 | The position of its first entry. |
| 24 | 4 | The number of its entries. |
| 28 | 4 | The length of the summary. |
| 32 | 4 | The length of the entries. |
| 36 | 4 | The checksum of the summary and the entries. |
| 40 | 1 | The compression of the entries: `0`, none. See [compression](#compression). |
| 41 | 7 | Zero. |

The summary is the [summary](#summary) of the block, in JSON.
The frames take the rest of the block.

The entries and the frames stand apart on purpose.
The entries are small and dense, and almost every reader wants all of them: the index, a query, a replay.
The frames are large, and most readers want none of them.
A reader of the entries of a recording never touches a byte of a frame.

An entry:

| Offset | Size | Holds |
|---|---|---|
| 0 | 4 | The length of the body. |
| 4 | 1 | The kind: `1` a session event, `2` a call, `3` a journal entry. |
| 5 | … | The body: JSON, and for a call 72 bytes before it. |

The 72 bytes of a call say where its frames are:

| Offset | Size | Holds |
|---|---|---|
| 0 | 8 | Where the frames of the call start, in bytes from the start of the frames of the block. |
| 8 | 16 | The slot of the request. |
| 24 | 16 | The slot of the upstream response. |
| 40 | 16 | The slot of the truth. |
| 56 | 16 | The slot of the delivered response. |

| Offset | Size | A slot holds |
|---|---|---|
| 0 | 1 | The [state](#entries) of the frame: `0` none, `1` whole, `2` redacted, `3` withheld, `4` the same as the truth. |
| 1 | 3 | Zero. |
| 4 | 4 | The length the frame had on the wire. |
| 8 | 4 | The length of the frame in the block. |
| 12 | 4 | The checksum of the frame in the block. |

The frames of a call stand in the order of their slots, back to back.
State `4` is for the delivered response alone: the client received the truth, byte for byte, which is the usual case, and the block holds it once.

The slots are fixed binary, and not a part of the record, for two readers:
one that wants a frame decodes no record, and one that wants a record skips 72 bytes.

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

An item is a length of 4 bytes, a checksum of 4 bytes, and a JSON map that describes one segment:

| Key | Holds |
|---|---|
| `first` | The position of the first entry of the segment, which is the name of the segment. |
| `entries` | The number of its entries. |
| `length` | The length of the segment in bytes. |
| `summary` | The [summary](#summary) of the segment: the sum of the summaries of its blocks. |
| `blocks` | For every block of the segment: where it starts, its length, the position of its first entry, the number of its entries, the lengths of its summary and its entries, and its summary. |

An index chunk holds everything that a reader needs to pick a block, and to read the entries of that block with one read.
It finds an entry by its position, by its time and by its call ID, because every block says its range of each.

An index chunk describes sealed segments only.

### The manifest

| Offset | Size | The manifest holds |
|---|---|---|
| 0 | 8 | The mark of a manifest: `89 47 4D 51 4D 41 4E 0A`, which reads `GMQMAN`. |
| 8 | 2 | The version of the layout: `1`. |
| 10 | 2 | Zero. |
| 12 | 4 | The length of what follows. |
| 16 | … | A JSON map. |
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
| Block | The recorder commits, or the entries and frames that wait reach a size. The call that reaches the size still belongs to the block. | `recordings.block_size` | `4MiB` |
| Data segment | A commit takes it to a size. A block is never cut in two. | `recordings.segment_size` | `256MiB` |
| Index chunk | The next segment would take it over a size. | `recordings.index_size` | `8MiB` |
| Summary | A field has more values than a count, and says `any`. | `recordings.summary_values` | `64` |

Neither a count of entries nor an age closes a segment.
A thousand heartbeats and a thousand fetches differ a thousandfold in bytes, so a count bounds nothing.
An age is for a log that deletes its old segments, and nothing deletes a part of a recording.
What a crash can lose is bounded by `recordings.commit_interval`, and not by the size of a segment.

### Writing

The layout holds the entries since the last commit in memory, encoded, with the handles of their frames.

| When | The layout writes, in this order |
|---|---|
| The recording starts | The manifest, with the description and no index chunk. Then the first segment, with its header. |
| A commit | One block, appended to the segment that grows. |
| The segment reached its size | It seals the segment. It writes the index chunk that grows again, now with this segment. It creates the next segment. |
| An index chunk begins: the first one, or the one after a full one | It writes the new index chunk, with its first segment. Then it replaces the manifest, which lists the new one and sums up the full one before it. |
| The recording ends | A commit. The end, appended to the segment. Then it seals the segment, writes the index chunk again, and replaces the manifest, which now holds the end. |

Two rules make every crash harmless:

- **An object is written before the object that describes it.**
  Whatever an index chunk or the manifest names is there, whole and for good.
  After a crash, the most that is missing is a description, and a description can be rebuilt from below.
- **Every step leaves a recording.**
  The backing gives each write [what it promises](#the-segment-backing), so no step leaves a description half replaced.

A write that fails ends the recording.
The layout says so and takes nothing more.
Nobody opens a recording to write to it again: a new recording has a new [ID](identity.md#recording-id).

The layout holds at most one block in memory: `recordings.block_size`, and one call more.

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
   It walks each of them: the segment header, then block header after block header, each with its summary.
   It checks every mark and every checksum, and that the whole block is there, and steps over the block by its length.
4. **The end of the recording.**
   The end in the last segment means that the recording ended in order.
   Otherwise the last block that is whole is where the recording stops: it still runs, or it was cut short.

A recording that was cut short opens like any other.
The reader says that it was cut short, and how many bytes behind the last whole block it left unread.
It changes nothing in the backing.

The walk follows lengths that a checksum covers, and never searches for a mark,
so the bytes of a frame can never pass for the start of a block.

| What is missing or damaged | The reader |
|---|---|
| The manifest | Lists the index chunks and the segments itself, and takes the description from the header of a segment. |
| An index chunk, or an item of one | Walks the segments that it described, as in step 3. |
| A block in the middle of a sealed segment | Steps over it with the length that the index chunk has, and reports its positions as lost. |
| The end of the segment that grew | Stops at the last whole block. |

Recovery is a function: it takes what the backing lists and the bytes it reads, and returns what the recording holds.
It runs in a test without a file.

### How a query walks the levels

A query hands in what it wants, and reads from the top down:

1. It reads the manifest, and keeps the index chunks whose summary can hold what it wants.
2. It reads those index chunks, and keeps the segments, and then the blocks, whose summary can hold it.
3. It reads the entries of each block that is left, with one read from the start of its summary to the end of its entries, and decides every call with the selector.
4. It reads a frame only when someone asks for the frames of a call.

| A reader that wants | Reads |
|---|---|
| The latest calls | The manifest, the last index chunk, and the last blocks. |
| A position, a moment or a call ID | The manifest, one index chunk, one block. |
| The calls a selector matches | The manifest, the index chunks that can match, and the entries of the blocks that can match. |
| Everything, as the index of the engine does when a recording opens | The entries of every block, in order, and no frame. |

A summary gets coarser with every level, so the levels prune different things.
A range of time, of positions or of call IDs prunes at every level, because each level covers one stretch.
A value that is rare prunes at every level too.
A value that half of the calls have prunes nowhere, and such a query reads most of the recording whatever the layout is.

### Sizes

The numbers below are estimates for a metadata-only recording: about 1 KiB for a call with its frames, and about 2 KiB of index for a block.
Nothing is measured yet.

| | With the defaults |
|---|---|
| A block | 4 MiB: about 4000 calls. |
| A segment | 256 MiB: 64 blocks, and about 130 KiB of index. |
| An index chunk | 8 MiB: about 64 segments, which is 16 GiB of traffic. |
| An item of the manifest | About 4 KiB for an index chunk. |

| A recording of | Segments | Index chunks | All index chunks | The manifest |
|---|---|---|---|---|
| 1 GiB | 4 | 1 | 0.5 MiB | 5 KiB |
| 100 GiB | 400 | 7 | 50 MiB | 30 KiB |
| 10 TiB | 40 960 | 640 | 5 GiB | 2.5 MiB |
| 1 PiB | 4.2 million | 65 536 | 512 GiB | 256 MiB |

- The index is about 0.05% of the traffic it describes.
- The index chunk that grows is written again for every segment: 4 MiB on average for 256 MiB of traffic, which is 1.6% more to write.
- The manifest is replaced once for every 16 GiB of traffic.
- A recording of 10 TiB opens with one read of 2.5 MiB, and its latest calls take three reads.
- At a petabyte the manifest is the one object that got large, and a recording of that size wants larger segments and larger index chunks, which the properties give it.
  A second level of index chunks is a later [version](#versions) of the layout, and no change of either contract.

### Compression

Version 1 compresses nothing.

The byte for it is in the header of a block, and it covers the entries alone.
The entries are where compression pays: they are text, and their keys repeat in every entry.
A reader decodes them anyway, so it loses nothing when they are compressed.
The frames stay plain in every version: a frame that is compressed in a segment cannot be handed out without a copy,
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
  The layout always knows what it wants next: 48 bytes of a block header, the entries of a block, the frames of one call.
  It asks for that range, and no read is larger than a block.
  A reader that wants a segment from an offset on pulls it piece by piece.
- **A read hands out a shared buffer.**
  The backing on files maps a segment into memory and hands out a range of the map: the system reads from the disk only the pages that someone looks at, and no byte is copied.
  A backing that fetches hands out what it fetched.
  The layout cuts entries and frames out of that buffer, and every frame it hands on shares it.
- **An append takes pieces.**
  The layout hands over the header, the summary and the entries of a block as one buffer, and every frame as the buffer it already is.
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
  a new kind of entry, a new state of a slot, a compression, a new level, a new key or a new value in a form.
- **A form only grows.**
  A later version adds a key that a reader of an old recording does without, or a value that an old recording never has.
  When a form must change in another way, the reader lifts an old entry to the new form before it builds the type.
  It can, because an entry describes itself: a reader reads an old entry as maps, lists and values without the types that wrote it.
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

What the UI shares is the form of an entry.
A call record and a journal entry are the types of `call` and `journal`, which the UI has already.
The call that the control plane sends from a recording is the call that the recording holds, in another encoding.

## Who reads a recording

| Reader | Reads | The storage gives it |
|---|---|---|
| The [index](../domains/observation.md#index), which the engine rebuilds when it opens a recording | Every call record, once. | The entries of every block in order, and no frame. The index of the engine is not stored. |
| A [query with a selector](control-plane.md#drafts), saved or draft, from the UI, the CLI or a test | The call records that the selector matches. | `Wanted`, which skips at every level. The manifest and the index chunks that a query read stay in memory, so a draft that changes with every key reads them once. |
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
  block_size: 4MiB        # the entries and frames that one block holds
  segment_size: 256MiB    # the size at which a data segment is sealed
  index_size: 8MiB        # the size at which an index chunk is full
  summary_values: 64      # the values of one field that a summary lists
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
| Does a reader copy? | No frame. A frame is a buffer that is shared, from the socket to the storage and from the storage to the codec. Records are decoded. | Slices that borrow from the storage. The trait would need lending types, and every storage would have to hold its bytes in one place. |
| Which encoding? | Framing of our own around the frames, and JSON for what has a structure: the forms that the types have, in the encoding that the control plane sends and that their tests check. | A binary encoding that describes itself, such as CBOR: a second encoding of the same forms, for fewer bytes, which compressing the entries gets later anyway. A format with a schema and zero-copy access: a second set of every type of a call, kept in step by hand, to borrow a few hundred bytes. An encoding that copies the layout of the types in memory: every change of a type would end every old recording. |
| What closes each level? | A size. An interval commits. | A count of entries, which bounds neither memory nor loss. An age for segments, which only a log with retention needs. |
| What is in a summary? | The values of every field, as lists with a limit. | Bloom filters, which answer `is` and cannot answer `matches`. Ranges of values, which no operator asks for. |
| What is left after a crash? | Everything that was committed. The segments describe themselves, so a missing index chunk or manifest is rebuilt from them. | A manifest that must be right. A crash between two writes would then lose what the segments still hold. |
| How does metadata change? | An object is written before what describes it, and a description is replaced in one step. | Changing an index or a manifest in place, which a crash can leave half done. |
| Where is the recording mode applied? | In `recording`, where an entry is made. A storage never sees what must not be kept. | In the engine, where every caller would have to remember it. In a layout, where every layout would have to. |
| What does metadata-only keep of a frame? | The frame, without the keys, values and headers of its records. | No frame at all. The usual recording of a real cluster could then seed no World and run no replay. |
| Is a recording one file? | No. It is a directory of segments, and whoever reads it reads it through the storage contract. Whether it exports to one file is not decided. | One file for each recording. A recording can be many gigabytes, and one file of that size has to be walked to be queried. |
| Is a recording compressed? | Not in version 1. A later version compresses the entries of a block, and never the frames. | Compression of whole blocks, which would end the frames without a copy. |
