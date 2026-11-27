//! What the storage contract asks of every storage, whichever layout stands behind it.

use bytes::Bytes;
use gaslightmq_call::{Call, CallId};
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_journal::JournalEntry;
use gaslightmq_recording::prelude::*;

use crate::samples::{self, Sample};

/// The bytes of entries and frames at which the storage of the check closes a batch.
///
/// The recording of the check is a few hundred kibibytes, so it is many batches of this size.
/// In the segment layout this is `recordings.block_size`.
pub const BATCH_SIZE: usize = 8 * 1024;

/// The bytes that one reader of the storage of the check holds at most.
///
/// It is a few batches, so a consumer that keeps every batch of the recording of the check is told so.
/// In the segment layout this is `recordings.reader_memory`.
pub const READER_MEMORY: usize = 32 * 1024;

/// The calls of the recording of the check: more than a reader may hold, and more than a few batches.
const CALLS: u64 = 100;

/// Fails when a storage does not do what the [contract](RecordingStorage) asks of every storage.
///
/// `empty` makes a storage that holds no recording, and the check asks for a new one for each behaviour.
/// A storage on files hands out a new directory each time.
/// The storage closes a batch at [`BATCH_SIZE`], and lets one reader hold [`READER_MEMORY`]:
/// with the sizes that a storage has by itself, the small recording of the check would be one batch,
/// and nothing would show what a reader holds.
///
/// | Behaviour | The check |
/// |---|---|
/// | A recording that started is listed, and opens. | It is empty, it has no end, and its description is the one that went in. |
/// | A storage refuses an ID that a recording has. | Also once the writer of that recording is gone. |
/// | A recording that nobody started does not open. | [`OpenError::NotFound`]. |
/// | Recordings are listed in the order of their IDs. | In whichever order they started. |
/// | Every entry gets the next position. | From 0, without a gap. |
/// | Entries come back as they went in. | Each at its position, through its view, as its type. |
/// | A reader sees what a commit covers, while the recording runs. | A stream that reached the end hands out what a later commit added. |
/// | A recording that finished has its end. | The extent says the end that went in. |
/// | A recording whose writer went away opens all the same. | It has no end, and it holds what was committed. |
/// | A stream starts at the start, at the end or at a position, and reads forwards or backwards. | The entries come in the order of the reading, each once. |
/// | A reader gets frames only when it asks for them. | Without them, every call still says the states and the lengths of its frames. |
/// | A consumer that keeps every batch is told so, and loses no entry. | [`NextBatchError::ReaderBudgetSpent`] names [`READER_MEMORY`], and the stream goes on when the consumer lets go. |
/// | A stream hands out no less than what is wanted. | Every call of a range of call IDs comes, whatever the storage skipped. |
///
/// What the check cannot see from outside, it does not check: that a commit outlives a crash,
/// that a storage reads no stretch that a summary rules out, and what a storage says about a stretch that it lost.
/// The tests of a layout check those.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_recording_conformance::{BATCH_SIZE, MemoryStorage, READER_MEMORY, assert_recording_storage};
///
/// // The test of a storage hands the check a way to make an empty one, and drives it with the executor it has.
/// let empty = || MemoryStorage::new().with_batch_size(BATCH_SIZE).with_reader_memory(READER_MEMORY);
/// block_on(assert_recording_storage(empty));
/// ```
///
/// # Panics
///
/// When the storage breaks the contract. The message says which behaviour it broke.
pub async fn assert_recording_storage<S: RecordingStorage>(empty: impl Fn() -> S) {
    a_recording_that_started_is_listed_and_opens(&empty()).await;
    a_storage_refuses_an_id_that_a_recording_has(&empty()).await;
    a_recording_that_nobody_started_does_not_open(&empty()).await;
    recordings_are_listed_in_the_order_of_their_ids(&empty()).await;
    every_entry_gets_the_next_position(&empty()).await;
    entries_come_back_as_they_went_in(&empty()).await;
    a_reader_sees_what_a_commit_covers_while_the_recording_runs(&empty()).await;
    a_recording_that_finished_has_its_end(&empty()).await;
    a_recording_whose_writer_went_away_opens_all_the_same(&empty()).await;
    a_stream_starts_anywhere_and_reads_in_either_direction(&empty()).await;
    a_reader_gets_frames_only_when_it_asks_for_them(&empty()).await;
    a_consumer_that_keeps_every_batch_is_told_so_and_loses_no_entry(&empty()).await;
    a_stream_hands_out_no_less_than_what_is_wanted(&empty()).await;
}

/// What a consumer that keeps nothing takes out of a stream: every entry as its type, copied out of its batch.
#[derive(Debug, Clone, PartialEq)]
enum Read {
    Session(SessionEvent),
    Call {
        call: Box<Call>,
        /// What the recording holds of each of the four frames: its state, its length on the wire, and its bytes.
        frames: [(FrameState, u64, Option<Bytes>); 4],
    },
    Journal(JournalEntry),
}

impl Read {
    /// What a reader that asked for frames gets back for `sample`, from a full recording:
    /// every frame of the sample whole, because the codec reads a Produce and it holds no credentials.
    fn of(sample: &Sample) -> Read {
        match sample {
            Sample::Session(event) => Read::Session(event.clone()),
            Sample::Journal(entry) => Read::Journal(entry.clone()),
            Sample::Call { call, frames } => Read::Call {
                call: call.clone(),
                frames: FrameSlot::ALL.map(|slot| match frames.get(slot) {
                    Some(frame) => (
                        FrameState::Whole,
                        frame.payload().len() as u64,
                        Some(frame.payload().clone()),
                    ),
                    None => (FrameState::None, 0, None),
                }),
            },
        }
    }

    /// The same without the bytes of the frames: what a reader that asked for no frames gets back.
    fn without_frames(self) -> Read {
        match self {
            Read::Call { call, frames } => Read::Call {
                call,
                frames: frames.map(|(state, length, _)| (state, length, None)),
            },
            other @ (Read::Session(_) | Read::Journal(_)) => other,
        }
    }

    /// Copies an entry out of its batch.
    fn copied_out(view: &EntryView<'_>) -> Read {
        match view {
            EntryView::Session(event) => Read::Session(
                event
                    .to_session_event()
                    .expect("a session event that a storage kept builds its type"),
            ),
            EntryView::Journal(entry) => Read::Journal(
                entry
                    .to_journal_entry()
                    .expect("a journal entry that a storage kept builds its type"),
            ),
            EntryView::Call(call) => Read::Call {
                call: Box::new(call.to_call().expect("a call that a storage kept builds its type")),
                frames: FrameSlot::ALL.map(|slot| {
                    let frame = call.frame(slot).expect("a frame lies inside the frames of its batch");
                    let bytes = frame.bytes().map(|bytes| Bytes::copy_from_slice(bytes));
                    (frame.state(), frame.length_on_the_wire(), bytes)
                }),
            },
        }
    }
}

/// Every entry of `batch` with its position, copied out.
fn copied_out(batch: &Batch) -> Vec<(Position, Read)> {
    let entries = batch.entries().map(|(position, entry)| {
        let view = entry.expect("an entry that a storage kept passes the check");
        (position, Read::copied_out(&view))
    });
    entries.collect()
}

/// Reads a stream to its end with a consumer that keeps nothing.
async fn read_to_the_end(entries: &mut impl RecordedEntries) -> Vec<(Position, Read)> {
    let mut read = Vec::new();
    while let Some(batch) = entries
        .next()
        .await
        .expect("a stream whose consumer keeps nothing hands out its next batch")
    {
        assert!(
            batch.entries().next().is_some(),
            "a stream hands out no batch without an entry that the reader asked for"
        );
        read.extend(copied_out(&batch));
    }
    read
}

/// Reads the entries of a recording from `from` in `direction`, with their frames, and keeps nothing.
async fn read<R: RecordingReader>(reader: &R, from: Place, direction: Direction) -> Vec<(Position, Read)> {
    let mut entries = reader
        .read(from, direction, Everything, Frames::With)
        .await
        .expect("a stream starts while no other reader holds anything");
    read_to_the_end(&mut entries).await
}

/// Starts the recording of the check, writes `samples` into it with a commit behind every tenth entry,
/// and hands back the writer, which did not finish.
async fn recorded<S: RecordingStorage>(storage: &S, samples: &[Sample]) -> S::Writer {
    const ENTRIES_OF_A_COMMIT: usize = 10;

    let mut writer = storage
        .start(samples::orders_sim())
        .await
        .expect("a storage starts a recording whose ID is free");
    for commit in samples.chunks(ENTRIES_OF_A_COMMIT) {
        for sample in commit {
            writer.append(sample.entry()).await.expect("a writer takes an entry");
        }
        writer.commit().await.expect("a writer commits");
    }
    writer
}

/// What a reader with frames gets back for `samples`: each at the position of its place.
fn at_their_positions(samples: &[Sample]) -> Vec<(Position, Read)> {
    let positions = (0..).map(Position::new);
    positions.zip(samples.iter().map(Read::of)).collect()
}

fn end_of(samples: &[Sample]) -> End {
    End {
        ending: Ending::Stopped,
        at: Instant::ZERO + Duration::from_secs(90),
        entries: samples.len() as u64,
        calls: samples.iter().filter_map(Sample::call_id).count() as u64,
    }
}

async fn a_recording_that_started_is_listed_and_opens<S: RecordingStorage>(storage: &S) {
    let description = samples::orders_sim();
    assert_eq!(
        storage.recordings().await,
        Ok(Vec::new()),
        "a new storage lists no recording"
    );

    let _writer = storage
        .start(description.clone())
        .await
        .expect("a storage starts a recording whose ID is free");

    assert_eq!(
        storage.recordings().await,
        Ok(vec![description.id()]),
        "a recording that started is listed"
    );
    let reader = storage
        .open(&description.id())
        .await
        .expect("a recording that started opens");
    assert_eq!(
        reader.description(),
        &description,
        "a recording says the description that it started with"
    );
    assert_eq!(
        reader.extent().await,
        Ok(Extent { entries: 0, end: None }),
        "a recording that started is empty, and has no end"
    );
    assert_eq!(
        read(&reader, Place::Start, Direction::Forwards).await,
        Vec::new(),
        "a recording that started hands out no entry"
    );
}

async fn a_storage_refuses_an_id_that_a_recording_has<S: RecordingStorage>(storage: &S) {
    let description = samples::orders_sim();
    let writer = storage
        .start(description.clone())
        .await
        .expect("a storage starts a recording whose ID is free");

    assert_eq!(
        storage.start(description.clone()).await.err(),
        Some(StartError::AlreadyThere),
        "a storage refuses the ID of a recording that runs"
    );

    // Nobody writes to a recording again once its writer is gone.
    drop(writer);
    assert_eq!(
        storage.start(description.clone()).await.err(),
        Some(StartError::AlreadyThere),
        "a storage refuses the ID of a recording whose writer is gone"
    );
    assert_eq!(
        storage.recordings().await,
        Ok(vec![description.id()]),
        "a refused start leaves the recording that was there"
    );
}

async fn a_recording_that_nobody_started_does_not_open<S: RecordingStorage>(storage: &S) {
    let _writer = storage
        .start(samples::orders_sim())
        .await
        .expect("a storage starts a recording whose ID is free");
    let another = samples::description("staging-proxy", "2026-11-21T14:02:11.527Z").id();

    assert_eq!(
        storage.open(&another).await.err(),
        Some(OpenError::NotFound),
        "a recording that nobody started does not open"
    );
}

async fn recordings_are_listed_in_the_order_of_their_ids<S: RecordingStorage>(storage: &S) {
    let later = samples::description("orders-sim", "2026-11-22T09:00:00.000Z");
    let earlier = samples::description("orders-sim", "2026-11-21T14:02:11.527Z");
    let of_another_environment = samples::description("staging-proxy", "2026-11-20T08:30:00.000Z");
    let started = [&later, &of_another_environment, &earlier];

    let mut writers = Vec::new();
    for description in started {
        let writer = storage.start(description.clone()).await;
        writers.push(writer.expect("a storage starts a recording whose ID is free"));
    }

    assert_eq!(
        storage.recordings().await,
        Ok(vec![earlier.id(), later.id(), of_another_environment.id()]),
        "recordings are listed by their environment, and then by the instant they started"
    );
}

async fn every_entry_gets_the_next_position<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    let mut writer = storage
        .start(samples::orders_sim())
        .await
        .expect("a storage starts a recording whose ID is free");

    let mut positions = Vec::new();
    for sample in &samples {
        let position = writer.append(sample.entry()).await;
        positions.push(position.expect("a writer takes an entry"));
    }

    let counted: Vec<Position> = (0..samples.len() as u64).map(Position::new).collect();
    assert_eq!(positions, counted, "every entry gets the next position, from 0");
}

async fn entries_come_back_as_they_went_in<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    let writer = recorded(storage, &samples).await;
    writer
        .finish(end_of(&samples))
        .await
        .expect("a writer ends its recording");

    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording that ended opens");

    assert_eq!(
        read(&reader, Place::Start, Direction::Forwards).await,
        at_their_positions(&samples),
        "every entry comes back at its position, as the type that went in, with the frames that went in"
    );
}

async fn a_reader_sees_what_a_commit_covers_while_the_recording_runs<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    let (first, second) = samples.split_at(samples.len() / 2);
    let mut writer = recorded(storage, first).await;
    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording that runs opens");
    let mut entries = reader
        .read(Place::Start, Direction::Forwards, Everything, Frames::With)
        .await
        .expect("a stream starts on a recording that runs");

    let expected = at_their_positions(&samples);
    let (committed, later) = expected.split_at(first.len());
    assert_eq!(
        read_to_the_end(&mut entries).await,
        committed,
        "a reader sees every entry that a commit covers, while the recording runs"
    );
    assert_eq!(
        reader.extent().await,
        Ok(Extent {
            entries: first.len() as u64,
            end: None
        }),
        "a recording that runs goes as far as its last commit, and has no end"
    );

    for sample in second {
        writer.append(sample.entry()).await.expect("a writer takes an entry");
    }
    writer.commit().await.expect("a writer commits");

    assert_eq!(
        read_to_the_end(&mut entries).await,
        later,
        "a stream that reached the end hands out what a later commit added, and nothing twice"
    );
}

async fn a_recording_that_finished_has_its_end<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    let end = end_of(&samples);
    let mut writer = recorded(storage, &samples[..1]).await;
    // Finishing commits what the writer took and no commit covers yet.
    for sample in &samples[1..] {
        writer.append(sample.entry()).await.expect("a writer takes an entry");
    }

    writer.finish(end).await.expect("a writer ends its recording");

    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording that ended opens");
    assert_eq!(
        reader.extent().await,
        Ok(Extent {
            entries: samples.len() as u64,
            end: Some(end)
        }),
        "a recording that finished holds every entry that its writer took, and the end that went in"
    );
}

async fn a_recording_whose_writer_went_away_opens_all_the_same<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    let writer = recorded(storage, &samples).await;

    drop(writer);

    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording that was cut short opens");
    assert_eq!(
        reader.extent().await,
        Ok(Extent {
            entries: samples.len() as u64,
            end: None
        }),
        "a recording that was cut short has no end"
    );
    assert_eq!(
        read(&reader, Place::Start, Direction::Forwards).await,
        at_their_positions(&samples),
        "a recording that was cut short holds what was committed"
    );
}

async fn a_stream_starts_anywhere_and_reads_in_either_direction<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    drop(recorded(storage, &samples).await);
    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording opens");

    let forwards = at_their_positions(&samples);
    let backwards: Vec<_> = forwards.iter().rev().cloned().collect();
    let middle = forwards.len() / 2;
    let at_the_middle = Place::At(Position::new(middle as u64));
    let last = forwards.len() - 1;

    assert_eq!(
        read(&reader, Place::Start, Direction::Forwards).await,
        forwards,
        "from the start forwards, every entry comes in the order of the positions"
    );
    assert_eq!(
        read(&reader, Place::End, Direction::Backwards).await,
        backwards,
        "from the end backwards, every entry comes from the last to the first"
    );
    assert_eq!(
        read(&reader, at_the_middle, Direction::Forwards).await,
        forwards[middle..],
        "from a position forwards, the entry at the position comes first"
    );
    assert_eq!(
        read(&reader, at_the_middle, Direction::Backwards).await,
        backwards[last - middle..],
        "from a position backwards, the entry at the position comes first"
    );
    assert_eq!(
        read(&reader, Place::End, Direction::Forwards).await,
        forwards[last..],
        "from the end forwards, the last entry comes, and nothing else"
    );
    assert_eq!(
        read(&reader, Place::Start, Direction::Backwards).await,
        forwards[..1],
        "from the start backwards, the first entry comes, and nothing else"
    );
}

async fn a_reader_gets_frames_only_when_it_asks_for_them<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    drop(recorded(storage, &samples).await);
    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording opens");
    let mut entries = reader
        .read(Place::Start, Direction::Forwards, Everything, Frames::Without)
        .await
        .expect("a stream starts");

    let expected: Vec<_> = at_their_positions(&samples)
        .into_iter()
        .map(|(position, read)| (position, read.without_frames()))
        .collect();
    assert_eq!(
        read_to_the_end(&mut entries).await,
        expected,
        "a reader that asks for no frames gets every call with the states and the lengths of its frames"
    );
}

async fn a_consumer_that_keeps_every_batch_is_told_so_and_loses_no_entry<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    drop(recorded(storage, &samples).await);
    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording opens");
    let mut entries = reader
        .read(Place::Start, Direction::Forwards, Everything, Frames::With)
        .await
        .expect("a stream starts");

    let mut kept = Vec::new();
    let mut read = Vec::new();
    let mut told = 0;
    loop {
        match entries.next().await {
            Ok(Some(batch)) => {
                read.extend(copied_out(&batch));
                kept.push(batch);
            }
            Ok(None) => break,
            Err(refused) => {
                assert_eq!(
                    spent_budget(&refused),
                    Some(READER_MEMORY as u64),
                    "a stream over a recording that is whole fails for what its consumer holds alone, \
                     and the error names what one reader may hold: {refused}"
                );
                assert!(
                    !kept.is_empty(),
                    "a reader may always hold one batch, so a consumer that holds none is never refused"
                );
                told += 1;
                // The consumer lets go, and reads on from where it was.
                kept.clear();
            }
        }
    }

    assert!(
        told > 0,
        "a consumer that keeps every batch of a recording that is larger than its reader may hold is told so"
    );
    assert_eq!(
        read,
        at_their_positions(&samples),
        "a consumer that let go when it was told so loses no entry, and gets none twice"
    );
}

/// The budget that the error names, when it says that the consumer holds all that its reader may hold.
fn spent_budget(error: &NextBatchError) -> Option<u64> {
    match error {
        NextBatchError::ReaderBudgetSpent { held, budget } if *held > 0 => Some(*budget),
        NextBatchError::ReaderBudgetSpent { .. } | NextBatchError::Damaged { .. } | NextBatchError::NotRead { .. } => {
            None
        }
    }
}

async fn a_stream_hands_out_no_less_than_what_is_wanted<S: RecordingStorage>(storage: &S) {
    let samples = samples::traffic(CALLS);
    drop(recorded(storage, &samples).await);
    let reader = storage
        .open(&samples::orders_sim().id())
        .await
        .expect("a recording opens");
    let (first, last) = (CallId::new(40).expect("a call ID"), CallId::new(60).expect("a call ID"));
    // A stretch can hold one of the calls 40 to 60 when the range of its call IDs touches theirs.
    let wanted = move |summary: &Summary| {
        summary
            .call_ids()
            .is_some_and(|(lowest, highest)| lowest <= last && first <= highest)
    };
    let mut entries = reader
        .read(Place::Start, Direction::Forwards, wanted, Frames::With)
        .await
        .expect("a stream starts");

    let handed_out = read_to_the_end(&mut entries).await;

    let is_wanted = |sample: &&(Position, Read)| match &sample.1 {
        Read::Call { call, .. } => (first..=last).contains(&call.id()),
        Read::Session(_) | Read::Journal(_) => false,
    };
    let expected = at_their_positions(&samples);
    let wanted_calls: Vec<_> = expected.iter().filter(is_wanted).collect();
    let handed_out_calls: Vec<_> = handed_out.iter().filter(is_wanted).collect();
    assert_eq!(
        handed_out_calls, wanted_calls,
        "a stream hands out every entry that is wanted, in order, whatever it skipped"
    );
    assert!(
        handed_out.iter().all(|read| expected.contains(read)),
        "a stream hands out entries of its recording, each at its position"
    );
}
