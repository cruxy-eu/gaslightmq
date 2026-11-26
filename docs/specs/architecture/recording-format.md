# Recording format

This document gives the bytes of a [recording](storage.md#recordings) file,
and says how a writer makes one and how a reader opens one.
The crate [`recording`](crates.md#observation) holds both.

A recording is one file that only grows.
The format follows from four things that a recording must do:

- **A session never pays for it.**
  A session hands over what it already has in memory, and encodes nothing.
- **A reader copies no frame.**
  A frame can be many megabytes, and most readers never look at it.
- **A file that was cut short still opens.**
  A crash, a full disk or a full queue leaves a recording that is complete up to a known point.
- **An old file still opens.**
  A recording that someone attached to a bug report a year ago opens in the build of today.

## The shape of a file

```text
file   = header  chunk …  trailer
chunk  = chunk header  summary  entries  frames  chunk tail
```

| Part | Holds | Size |
|---|---|---|
| [Header](#header) | What the recording is: its environment, when it started, what it keeps. | Small, written once. |
| [Chunk](#chunks) | A stretch of the recording: its entries, and the frames of its calls. | About `recordings.chunk_size`. |
| Summary | What the chunk holds, so a reader knows whether it must read it. | Small. |
| [Entries](#entries) | Session events, call records and journal entries, in order. | A few hundred bytes each. |
| [Frames](#frames) | The Kafka frames of the calls of the chunk, back to back. | Whatever the frames are. |
| Chunk tail | The length of the chunk, so a reader walks from the end. | 16 bytes. |
| [Trailer](#trailer) | That the recording ended in order, and how. A file that was cut short has none. | 48 bytes. |

The file has no index and no table of its chunks.
Every chunk says how long it is, at its start and at its end, so a reader finds every chunk from either side.

## Bytes

- **A number has a fixed size, and its lowest byte comes first.**
  A reader reads a number from its bytes, wherever they stand.
  Nothing in a file is aligned, and nothing is padded.
- **A length counts bytes.**
  A reader checks every length against the bytes that are there before it uses it.
  A recording can come from anywhere, so a reader trusts nothing that a file says.
- **A checksum is a CRC-32C**, the checksum that Kafka gives its record batches.
- **What has a structure is CBOR.**
  The description in the header, the summary of a chunk and the body of every entry are CBOR ([RFC 8949](https://www.rfc-editor.org/rfc/rfc8949)), a binary encoding that describes itself.
  A map has text keys, and every map, list and text says its length at its start.
- **What is a frame is the frame.**
  The bytes of a Kafka frame stand in the file as they were on the wire, without the four bytes of their length.

The format therefore has two layers.
The frame of the file is fixed binary: marks, lengths and checksums, which a reader finds without decoding anything.
What the file says is in CBOR, in the forms that the types already have.

A call record and a journal entry have one form each: the form that their crate gives them.
The control plane writes that form as JSON, and a recording writes the same form as CBOR.
Their crates say that the forms need an encoding that describes itself, and CBOR is one.

## Header

| Offset | Size | Holds |
|---|---|---|
| 0 | 8 | The mark of a recording: the bytes `89 47 4D 51 52 45 43 0A`, which read `GMQREC` between a byte that no text holds and a line feed. |
| 8 | 2 | The [version](#versions) of the format: `1`. |
| 10 | 2 | Zero. |
| 12 | 4 | The length of the description. |
| 16 | … | The description, a CBOR map. |
| … | 4 | The checksum of every byte of the header before it. |

The description says what the recording is.

| Key | Holds |
|---|---|
| `environment` | The name of the environment. |
| `started` | The instant the recording started, in UTC and to the millisecond. |
| `clock` | The instant of the clock of the environment at that moment. Every entry tells its time on that clock, so this pair says when an entry was. |
| `privacy` | `full` or `metadata-only`. See [what a recording must not keep](#what-a-recording-must-not-keep). |
| `backend` | `upstream` or `simulated`: what stood behind the proxy. |
| `written-by` | The version of GaslightMQ that wrote the file. |
| `armed` | The names of the rules that were armed when the recording started, the highest priority first. |
| `scenario` | The scenario that was active when the recording started, and its step, when one was. |
| `journal-after` | The ID of the last journal entry before the recording started, when there was one. |

`environment` and `started` are the [ID of the recording](identity.md#recording-id).
The name of the file holds the same two, and a file that someone renamed still knows which recording it is.

The journal entries of a recording begin where the recording begins.
`armed` and `scenario` hold what the earlier entries would have said,
so a recording shows which rules were armed when its first call arrived.
They hold names, as an entry of the journal does.

## Chunks

A chunk is a stretch of the recording that the writer writes in one piece.

```text
chunk header   40 bytes
summary        CBOR
entries        one after the other
frames         one after the other
chunk tail     16 bytes
```

The chunk header:

| Offset | Size | Holds |
|---|---|---|
| 0 | 4 | The mark of a chunk: `GMQC`. |
| 4 | 4 | The checksum of the bytes 8 to 39 of this header. |
| 8 | 8 | The length of the chunk, from this mark to the end of its tail. |
| 16 | 4 | The length of the summary. |
| 20 | 4 | The length of the entries. |
| 24 | 4 | The number of entries. |
| 28 | 4 | The checksum of the summary and the entries. |
| 32 | 1 | The compression of the entries: `0`, none. See [compression](#compression). |
| 33 | 7 | Zero. |

The frames take the rest of the chunk, up to the tail.

The chunk tail:

| Offset | Size | Holds |
|---|---|---|
| 0 | 8 | The length of the chunk, as its header has it. |
| 8 | 4 | The checksum of the chunk header, as the header has it. |
| 12 | 4 | The mark of a tail: `GMQE`. |

The entries and the frames stand apart on purpose.
The entries are small and dense, and almost every reader wants all of them:
the index, a query, a replay.
The frames are large, and most readers want none of them.
A reader that walks the entries of a recording never touches a byte of a frame.

### What closes a chunk

The writer keeps one chunk open, in memory.
It writes the chunk to the file when it closes, and then starts the next one.

| A chunk closes when | Property | Default |
|---|---|---|
| Its entries and frames together reach a size. The call that reaches the size still belongs to the chunk. | `recordings.chunk_size` | `4MiB` |
| It has been open for a time, counted from its first entry. | `recordings.chunk_age` | `1s` |
| Someone wants to read the recording while it runs. | — | — |
| The recording stops. | — | — |

The size keeps a chunk small enough to skip and to hold in memory.
The age says how much a crash can lose, and how far the file is behind the traffic.

The writer has no clock.
It closes a chunk at the size by itself, and when its owner says so.
The engine says so when the age has passed, on the time of the system: a virtual clock can stand still, and a crash does not.

No count of entries closes a chunk.
A thousand heartbeats and a thousand fetches differ a thousandfold in bytes,
so a count would bound neither the memory nor what a crash loses.

### The summary

The summary says what a chunk holds, so a reader knows whether to read its entries.

| Key | Holds |
|---|---|
| `from`, `to` | The earliest and the latest instant that an entry of the chunk holds. |
| `calls` | The number of call records, and the lowest and the highest call ID among them. |
| `journal` | The number of journal entries, and the first and the last of their IDs. |
| `sessions` | The number of session events. |
| `fields` | For every [field](../domains/targeting.md#fields) that a call of the chunk has: its values. |

`fields` is a map from the name of a field to one of two things:

- **The list of its values**: every value that a call of the chunk has for the field, for the call as a whole or for one of its parts, each value once.
  A value has the form that a call record gives it.
- **The text `any`**: the chunk has more values than `recordings.summary_values`, which is `128` until someone sets it.

A field that no call of the chunk has is not in the map.

A list of values answers every operator of a [condition](../domains/targeting.md#conditions).
A query with a selector reads the summary of every chunk, and skips a chunk when one condition proves that no call in it can match:

| Condition | The chunk is skipped when |
|---|---|
| `is`, `is any of` | The field is not in the map, or its list holds none of the values. |
| `matches` | The field is not in the map, or the pattern matches no value of its list. |
| `is not` | Never. It also matches a call without the field. |
| Any, on a field that says `any` | Never. |

A summary only proves that a chunk cannot match.
The reader decides every call of a chunk it keeps with the selector itself, as a rule would.
Whether a selector can match a set of values is matching, so that check belongs to `selector`, and `recording` hands out the values.

`topic` and `partition` are summed up apart, although a selector tests them on the same partition.
A chunk with `orders.eu/1` and `payments.settled/3` is therefore read for `orders.eu/3`, and yields nothing.

### Compression

Version 1 compresses nothing.

The byte for it is there, and it covers the entries alone.
The entries are where compression pays: their keys repeat in every entry.
A reader decodes them anyway, so it loses nothing when they are compressed.
The summary stays plain, because a reader looks at it to skip the rest.
The frames stay plain in every version: a frame that is compressed in the file cannot be handed out without a copy,
and the record batches in it are often compressed already.

## Entries

The entries of a chunk stand one after the other.

| Offset | Size | Holds |
|---|---|---|
| 0 | 4 | The length of the body. |
| 4 | 1 | The kind: `1` a session event, `2` a call, `3` a journal entry. |
| 5 | … | The body. |

An entry stands where the recorder took it, so the entries of a file are in the order in which things ended:

- **A journal entry** stands where its change happened. The IDs of the journal entries of a file rise, without a gap.
- **A call** stands where it ended, because its record exists from then on.
  A Fetch that waited for records stands behind the heartbeats that came and went in the meantime.
  The IDs of the calls of a file therefore do not always rise.
  The call ID gives the order in which the requests arrived, and the timing of a record gives every moment of its call.
- **A session event** stands where it happened.

A recording holds the calls whose request arrived after it started and that ended before it stopped.

### Session event

The body is a CBOR map.

| `event` | Holds |
|---|---|
| `opened` | `connection`, `at`, `address` and `endpoint`. |
| `identified` | `connection`, `at`, and `client.id` when the first request carried one. |
| `software-named` | `connection`, `at` and `client.software`. |
| `closed` | `connection`, `at`, and `by`: `client`, `backend`, `lie` or `proxy`. |

A recording starts with an `opened` event for every session that is open at that moment.
Its `at` is the instant the session really opened, which is before the recording started,
and it holds what the session has learned by then: its `client.id` and its `client.software`.
Every connection number in a recording therefore has its session in the same file.

### Call

The body is a block of 72 bytes, and then the [call record](../domains/observation.md#call) in CBOR.

| Offset | Size | Holds |
|---|---|---|
| 0 | 8 | Where the frames of the call start, in bytes from the start of the frames of the chunk. |
| 8 | 16 | The slot of the request: what the client sent. |
| 24 | 16 | The slot of the upstream response: what the real cluster answered, when the overlay changed it. |
| 40 | 16 | The slot of the truth: what the backend answered. |
| 56 | 16 | The slot of the delivered response: what the client received. |
| 72 | … | The call record. |

A slot:

| Offset | Size | Holds |
|---|---|---|
| 0 | 1 | The state of the frame. |
| 1 | 3 | Zero. |
| 4 | 4 | The length the frame had on the wire. |
| 8 | 4 | The length of the frame in the file. |
| 12 | 4 | The checksum of the frame in the file. |

| State | Means |
|---|---|
| `0` none | The call has no such frame: a lie ended it before forwarding, or the client received nothing. |
| `1` whole | The file holds the frame as it was on the wire. |
| `2` redacted | The file holds the frame without [what a recording must not keep](#what-a-recording-must-not-keep). It still decodes. |
| `3` withheld | The frame existed, and the file holds none of it. |
| `4` same as the truth | Only for the delivered response: the client received the truth, byte for byte, and the file holds it once. |

The frames of a call stand in the order of their slots, back to back.
A reader finds a frame from the start of the call's frames and the lengths of the slots before it.

The call record says what the call was, and the frames are the proof.
A reader that wants the fields, the verdict and the explanation of a call reads its record and no frame.

The block is fixed binary, and not a part of the record, for two readers:
one that wants a frame decodes no record, and one that wants a record skips 72 bytes.

### Journal entry

The body is the [journal entry](../domains/observation.md#journal) in CBOR, in the form that its crate gives it.

## Frames

### Handing frames to the recorder

A call record holds no frame.
A session already has every frame of a call in memory, each as one buffer that several owners can share:
the request it read, the answer of the backend, and the bytes it sends to the client.

- **While a recording runs**, the session keeps a handle on each of those buffers until the call ends.
  It hands the handles over with the record: the request, the truth, the delivered response, and the upstream response when the overlay changed it.
  A handle shares the bytes, so the session copies nothing and encodes nothing.
- **While nothing records**, the session keeps no handle, and a call costs what it costs today.

The recorder takes the record and the handles from its queue, on its own thread.
Everything that costs time happens there: encoding the record, the checksums, and [redaction](#what-a-recording-must-not-keep).
The bytes of a frame are copied once, into the file.

A frame that waits in the queue keeps its bytes in memory.
A count of calls does not bound that: ten thousand fetches can wait with ten gigabytes.
The queue is therefore full when it holds `recordings.backlog` calls and journal entries,
or when the frames that wait hold `recordings.backlog_size` bytes, which is `256MiB` until someone sets it.

The delivered response is the truth whenever no lie changed the bytes, which is the usual case.
The writer compares the two, and stores them once.

### What a recording must not keep

The writer applies [privacy](storage.md#privacy) and blanks [credentials](storage.md#credentials-on-the-wire).
Nobody else does, so nobody can forget it:

- The writer learns whether it writes a `full` or a `metadata-only` recording when it is made, and writes that into the header.
- The one way to write a call takes its frames as they were on the wire.
  The writer has no way to write a frame as it is handed in.

For every frame, the writer reads the API of its call from the header of the request, and then does one of three things.

| The frame | In a full recording | In a metadata-only recording |
|---|---|---|
| A `SaslAuthenticate` request, or a response to one | Redacted: its authentication bytes are empty. | The same. |
| A frame that holds records, such as a Produce request and a Fetch response | Whole. | Redacted: every record is there without its key, its value and its headers. |
| A frame of any other API that the codec reads | Whole. | Whole. |
| A frame of a known API that the writer cannot read or cannot rewrite | Whole, and withheld when the API is `SaslAuthenticate`. | Withheld. |
| A frame of a call whose API the writer cannot tell | Withheld. | Withheld. |

- **A redacted frame is a frame.**
  It decodes at the version it had, and it says everything else that it said.
  A record batch keeps its base offset, its number of records, its timestamps, and the ID, the epoch and the sequence of its producer.
  A record keeps its offset and its timestamp.
  The writer stores such a batch without compression and with a new checksum.
- **The slot keeps the length that the frame had on the wire**, whatever the file holds of it.
  The size of the traffic is metadata, and a metadata-only recording keeps it.
- **The response to `SaslAuthenticate` is blanked too.**
  With SCRAM, what the server answers is enough to test guessed passwords against, next to what an attacker can guess.
- **Whatever the writer cannot prove harmless, it withholds.**
  An API that a later Kafka adds can carry records, so a metadata-only recording never stores a frame that the writer could not read.

The writer needs the Kafka codec for this, and for nothing else.

## Trailer

The writer ends a recording with a trailer.

| Offset | Size | Holds |
|---|---|---|
| 0 | 8 | The number of chunks. |
| 8 | 8 | The number of calls. |
| 16 | 8 | The number of journal entries. |
| 24 | 8 | The instant the recording ended, on the clock of the environment. |
| 32 | 1 | How it ended: `1` someone stopped it, or its environment stopped, `2` the recorder fell behind. |
| 33 | 3 | Zero. |
| 36 | 4 | The checksum of the bytes 0 to 35 of the trailer. |
| 40 | 8 | The mark of the end: the mark of a recording backwards, `0A 43 45 52 51 4D 47 89`. |

A recording that [stops by itself](storage.md#recordings) still ends in order.
The recorder writes everything it took from its queue, closes the chunk, and writes the trailer.
The last entry of the file is the journal entry that says why, and the trailer says it again,
so whoever lists the recordings reads no chunk.

A recording that the writer could not write has no trailer, because a file cannot say that it could not be written.
The journal in memory and the control plane say it.

## Writing

The writer writes to anything that takes bytes in order: a `std::io::Write`.
It never goes back in the file, and it never reads it.

- It encodes every entry when it takes it, into the open chunk.
- It keeps the handles of the frames of the open chunk, and no copy of their bytes.
  A frame that it redacts is new bytes, and small.
- When the chunk closes, it writes the chunk header, the summary, the entries, the frames and the tail, and flushes.
- When the recording stops, it closes the chunk and writes the trailer.
  The engine then makes sure that the file is on the disk.

The writer holds at most one chunk in memory: `recordings.chunk_size`, and one call more.

A write that fails ends the recording.
The writer says so and takes nothing more, and the file is as far as it got.

Nobody opens a recording to write to it again.
A new recording is a new file, with a new [ID](identity.md#recording-id).

## Reading

### A reader works on the bytes of the file

The reader takes the bytes of the whole file as one buffer that it shares with its caller.
It reads no file itself, because `recording` does no IO.
Whoever opens the file decides how the bytes come into memory:

- **The engine maps the file into memory.**
  The system then reads from the disk only the pages that someone looks at.
  A recording of many gigabytes opens at once, and the frames that nobody asks for never leave the disk.
- **A test reads the file into memory**, or builds the bytes itself.

The reader hands out:

| What | How |
|---|---|
| The description | Decoded. |
| The chunks, from the start or from the end | Each with its summary, decoded. A chunk whose entries nobody asks for is not read. |
| An entry: a session event, a call record, a journal entry | Decoded into its type, through the checks of that type. A file can therefore never hand out a call that could not have been one. |
| A frame of a call | A range of the buffer, as a Kafka frame that shares it. No byte is copied. The reader checks its checksum when it hands the frame out. |

A frame from a recording goes into the codec and into a pipeline as a frame from a socket does, without a copy.

Nothing in a file is aligned, and the reader reinterprets no bytes: it reads a number from its bytes, and takes a frame as the bytes it is.
The reader therefore needs no unsafe code.
Mapping a file does, and that stays with whoever opens it.

A call record is decoded, and not borrowed.
A record is a few hundred bytes of small values, and its types check what they hold.
Types that borrow from a file would be a second set of the same vocabulary.
The summaries keep a query from decoding the chunks that cannot match.

### Opening

1. The reader checks the header: the mark, the version and the checksum.
   A file without them is no recording, and opening fails.
2. It looks at the last 48 bytes.
   A trailer with its mark and its checksum means that the recording **ended in order**.
3. Without a trailer, the recording was **cut short**: it still runs, or its process crashed, or its disk was full.
   The reader walks the chunks from the header on: it reads a chunk header, checks its mark, its checksum and that the whole chunk is there, and steps over the chunk by its length.
   The last chunk that is whole is the end of the recording.

A recording that was cut short opens like any other.
The reader says that it was cut short, and how many bytes after the last whole chunk it left unread.
It never changes the file.

The walk from the header reads 40 bytes of every chunk and no entry, so it is short even for a long file.
It follows lengths that a checksum covers, and never searches for a mark,
so the bytes of a frame can never pass for the start of a chunk.

What a crash loses is the chunk that was open: at most `recordings.chunk_age` of traffic.

### From the end

A reader that wants the latest calls first starts at the trailer, or at the end of the last whole chunk.
The 16 bytes before that point are the tail of the last chunk.
The tail says how long its chunk is, so the reader steps back to the chunk header, and from there to the tail of the chunk before it.

Each chunk carries this pointer itself.
A table of all chunks at the end of the file would be written last, so a crash would never write it.

### A recording that runs

The engine reads a recording that is still running as it reads one that was cut short: up to the last whole chunk.
It tells the writer to close the open chunk first, so a query sees every call that the recorder has taken.
The file only grows, and a chunk that is written never changes.

### A damaged file

A chunk whose checksum does not fit is damaged.
The reader hands out none of it and says which chunk it is.
When the chunk header is whole, the reader steps over the chunk and goes on.
When the chunk header itself is damaged, the reader reaches the chunks behind it from the end.

## Versions

The header holds one version number.

- **A reader opens every version up to its own.**
  It keeps the reading code of every version that was ever released.
- **A reader refuses a version that is newer than itself**, and the message names both versions.
- **Every change of the format takes a new number**:
  a new kind of entry, a new state of a slot, a compression, a new key or a new value in a form.
- **A form only grows.**
  A later version adds a key that a reader of an old file does without, or a value that an old file never has.
  When a form must change in another way, the reader lifts an old entry to the new form before it builds the type.
  It can, because an entry describes itself: a reader reads an old entry as maps, lists and values without the types that wrote it.
- **The bytes that are zero today are free for a later version.**
- **Version 1 is fixed with the first release.**
  Until then it changes with the types it holds, and no file of an earlier build has to open.

The tests of `recording` keep one small recording of every released version, and open each of them.
That makes "an old file still opens" a check, and not a promise.

## The browser

Nothing of `recording` runs in the browser.
The crate needs the Kafka codec, which never [compiles to WebAssembly](crates.md#what-compiles-to-webassembly),
and a browser opens no file: the UI asks the control plane, and the engine answers from the recording.

What the UI shares is the form of an entry.
A call record and a journal entry are the types of `call` and `journal`, which the UI has already.
The call that the control plane sends from a recording is the call that the file holds, in another encoding.

## Who reads a recording

| Reader | Reads | The format gives it |
|---|---|---|
| The [index](../domains/observation.md#index), which the engine rebuilds when it opens a recording | Every call record, once. | The entries of every chunk in one pass, and no frame. The index is not in the file. |
| A [query with a selector](control-plane.md#drafts), saved or draft, from the UI, the CLI or a test | The summaries, then the call records of the chunks that can match. | The summaries of an open recording are small enough to keep in memory, so a draft that changes with every key reads no chunk that it can skip. |
| A [test](control-plane.md#tests) that asserts on verdicts and explanations | The same. | The explanation is a part of every call record. |
| The UI that shows the latest calls | The last chunks. | The walk [from the end](#from-the-end). |
| [Replay](crates.md#the-pipeline-is-sans-io) | The description, every entry, and the request and the truth of every call. | The rules that were armed at the start, the journal entries that changed them, the sessions, and the frames, which go into a pipeline without a copy. The record of every call holds the verdict and the explanation to compare with. |
| [Seeding a World](../domains/world.md#seeding) | The index, and the frames of the calls that describe the cluster or carry records. | The summaries name the chunks with `Metadata`, Produce and Fetch calls, and only their frames leave the disk. |
| Someone with a bug report | One file. | The header says what wrote it, and a file that was cut short opens. |

A metadata-only recording serves all of them.
Its frames still decode, so a replay runs and a World gets its topics, its partitions and its offsets.
It gives no record contents: a seeded World has no records from it, and a lie that changes records cannot be replayed on it.

A call whose truth is withheld cannot be replayed, and the reader says which calls those are.

### What a replay still needs

A recording holds names, and no definitions.
A replay takes the definitions of the rules from a workspace.

A call record does not say which snapshot of the armed rules its call saw.
The instants of the calls and of the journal entries relate the two, and two things at one instant have no order between them.
The design of replay settles what it needs.
A key in the call record or in the description carries it, in a new version and without a new layout.

## What a recording does not hold

| Not in the file | Because |
|---|---|
| The index, or any table of its chunks | The engine rebuilds the index when it opens the file. Whatever is written last is what a crash never writes. |
| The definitions of rules, selectors and presets | The journal names a definition, and the workspace holds it. |
| The request as the backend saw it | No reader of today needs it: a replay gets it from the pipeline it drives. A later version adds a slot when a reader does. |
| A client's credentials | The writer [blanks](#what-a-recording-must-not-keep) them in every recording. |
| A run ID or a revision of its own | No file holds a run ID. A journal entry holds the revision that its change left. |

## Properties

```yaml
# gaslight.yaml
recordings:
  directory: recordings
  backlog: 10000          # the calls and journal entries that the recorder may be behind
  backlog_size: 256MiB    # the bytes of the frames that may wait with them
  chunk_size: 4MiB        # the size at which a chunk closes
  chunk_age: 1s           # how long a chunk stays open at most
  summary_values: 128     # the values of one field that a summary lists
```

A size is a number of bytes with its unit.

## Decisions

| Question | Decision | What it replaced, and why |
|---|---|---|
| How are the bytes laid out? | A header, chunks that each hold a summary, the entries and the frames, and a trailer. Fixed binary for marks, lengths and checksums, and CBOR for what the file says. | Entries and frames in one stream. A reader of the entries would cross every frame, and a chunk could say its length only at its end. |
| Does a reader copy? | No frame. The reader works on the bytes of the file, which the engine maps, and a frame is a range of them. Records are decoded. | A reader over `std::io::Read`. It would copy every frame into a buffer, and read a long file from its start. The writer stays on `std::io::Write`. |
| Which encoding? | A layout of our own around the frames, and an encoding that describes itself for the records. | A format with a schema and zero-copy access: a second set of every type of a call, kept in step by hand, to borrow a few hundred bytes. An encoding that copies the layout of the types in memory: every change of a type would end every old file. |
| What closes a chunk? | A size and an age. | A count of entries, which bounds neither memory nor loss. |
| What is in a summary? | The values of every field, as lists with a limit. | Bloom filters, which answer `is` and cannot answer `matches`. Ranges, which no operator asks for. |
| How does a reader find chunks from the end? | The tail of every chunk holds its length. | A table of chunks at the end of the file, which a crash never writes. |
| What is left after a crash? | Every chunk that was closed. A reader walks the lengths from the header and stops at the last whole chunk. | Entries that are written one by one. They lose less, and a reader could then find the end of a file only by reading every entry or by searching for a mark. |
| Where is privacy applied? | In the writer, which takes frames as they were on the wire and has no way to write one as it is. | In the engine, before the writer. Every caller would have to remember it. |
| What does metadata-only keep of a frame? | The frame, without the keys, values and headers of its records. | No frame at all. The usual recording of a real cluster could then seed no World and run no replay. |
| Is a recording compressed? | Not in version 1. A later version compresses the entries of a chunk, and never the frames. | Compression of whole chunks, which would end the frames without a copy. |
