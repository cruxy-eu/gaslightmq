//! Where the recordings of an engine are kept: a storage of recordings, whichever layout it is.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::future::BoxFuture;
use gaslightmq_call::Call;
use gaslightmq_index::{CallIndex, Facets};
use gaslightmq_recording::{
    Batch, Description, Direction, End, Entry, EntryView, Everything, Extent, ExtentError, Frames, ListError,
    NextBatchError, OpenError, Place, Position, ReadError, RecordedEntries, RecordingId, RecordingReader,
    RecordingStorage, RecordingWriter, StartError, Summary, SummaryValues, Wanted, WriteError,
};
use gaslightmq_selector::{EffectiveSelector, StretchValues};

use crate::recorder::Queue;
use crate::{ListRecordingsError, OpenRecordingError, QueryRecordingError, ReadingConfiguration};

/// Where the recordings of a process are kept: a [storage of recordings](RecordingStorage), whichever layout it is.
///
/// The engine holds a storage and names no layout.
/// Whoever puts a process together picks one, makes this handle of it once, and hands a clone to every engine:
/// `gmq` picks the segment layout on files, and a test picks a storage in memory.
/// A clone is a handle to the same storage.
///
/// A recording that runs is in the storage from its start, and every reader sees what its last commit covers.
/// [`open`](Self::open), [`query`](Self::query) and [`index`](Self::index) commit a recording that an engine of the
/// handle writes before they read it, so whoever reads a recording that runs sees every entry that its engine handed
/// over until then.
///
/// # Reading a recording
///
/// Every reading is a stream over the storage, and nothing waits for memory.
///
/// | To | Call | It streams |
/// |---|---|---|
/// | Say what a recording is, and how far it goes. | [`open`](Self::open) | Nothing: the storage says it. |
/// | Find the calls that a selector matches, and count them. | [`query`](Self::query) | The stretches whose summaries can match, and no frame. |
/// | Group the calls into facets, under a filter. | [`index`](Self::index) | Every entry once, and no frame. |
///
/// - **A recording stays open.** The handle keeps the last [`open_recordings`](ReadingConfiguration::open_recordings)
///   recordings that someone read open, with what the storage holds of each in memory, such as the manifest of the
///   segment layout. A query that someone sends with every key they type streams the index of the storage again,
///   and reads no description again.
/// - **The index is built from the recording.** No storage keeps it. The first [`index`](Self::index) of an open
///   recording reads every entry of it, and every later one reads what later commits added, from where the one
///   before stopped.
/// - **A query sends the type.** It keeps the calls that it answers with beyond its stream, so it copies each out
///   as a [`Call`], and holds no block of the storage once it answered.
/// - **A reading that finds no room is refused.** When the readers of the process hold as much as their budget
///   allows, the storage refuses the stream, and the query is [`Busy`](QueryRecordingError::Busy): whoever asked
///   asks again.
/// - **A damaged stretch is left out.** The storage says which entries it lost, and the reading goes on behind
///   them. The answer says how many entries it could not read.
#[derive(Clone)]
pub struct Recordings {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Box<dyn Storage>,
    configuration: ReadingConfiguration,
    /// The recordings that the engines of this handle write, each with the queue of its recorder.
    running: Mutex<BTreeMap<RecordingId, Arc<Queue>>>,
    /// The recordings that stay open, the one that someone read last at the back.
    open: Mutex<VecDeque<(RecordingId, Arc<OpenRecording>)>>,
}

/// A recording that stays open: the reader of its storage, and its index as far as it was built.
struct OpenRecording {
    reader: Box<dyn Reader>,
    index: tokio::sync::Mutex<Indexed>,
}

/// The index of an open recording, and how far it read.
struct Indexed {
    index: CallIndex,
    /// How many entries the index read: the position of the next entry that it reads.
    read: u64,
    /// How many entries the storage could not hand out.
    lost: u64,
}

/// The calls of a recording that a selector matches: how many there are, and a page of them.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let nothing = QueriedCalls { matched: 0, calls: Vec::new(), lost: 0 };
/// assert!(nothing.calls.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueriedCalls {
    /// How many calls of the whole recording the selector matches.
    pub matched: u64,
    /// The matched calls, in the order of the recording, from the first that the query did not skip on, and at most
    /// [`query_calls`](ReadingConfiguration::query_calls) of them.
    pub calls: Vec<Call>,
    /// How many entries of the recording the storage could not hand out, because they are damaged.
    /// The count and the calls leave them out.
    pub lost: u64,
}

/// The facets of the calls of a recording under a filter, from its index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingFacets {
    /// The facets, and how many calls the filter leaves.
    pub facets: Facets,
    /// How many entries of the recording the storage could not hand out, because they are damaged.
    /// The facets leave them out.
    pub lost: u64,
}

/// A recording as its storage holds it: what it is, how far it goes, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecording {
    /// What the recording is, as it was written when it started.
    pub description: Description,
    /// How many entries a commit covers, and the end of the recording when it ended in order.
    pub extent: Extent,
}

impl Recordings {
    /// The recordings of `storage`, read with the defaults of a [`ReadingConfiguration`].
    pub fn new<S>(storage: S) -> Self
    where
        S: RecordingStorage + 'static,
        S::Writer: 'static,
        S::Reader: 'static,
        <S::Reader as RecordingReader>::Entries: 'static,
    {
        Self::with_configuration(storage, ReadingConfiguration::new())
    }

    /// The recordings of `storage`, read as `configuration` says.
    pub fn with_configuration<S>(storage: S, configuration: ReadingConfiguration) -> Self
    where
        S: RecordingStorage + 'static,
        S::Writer: 'static,
        S::Reader: 'static,
        <S::Reader as RecordingReader>::Entries: 'static,
    {
        Self {
            inner: Arc::new(Inner {
                storage: Box::new(storage),
                configuration,
                running: Mutex::new(BTreeMap::new()),
                open: Mutex::new(VecDeque::new()),
            }),
        }
    }

    /// The ID of every recording of the storage, in their order: by the environment, and then by the instant it
    /// started. A recording that runs is among them.
    ///
    /// # Errors
    ///
    /// [`ListRecordingsError::StorageUnavailable`] when the storage did not answer.
    pub async fn list(&self) -> Result<Vec<RecordingId>, ListRecordingsError> {
        self.inner.storage.recordings().await.map_err(|error| match error {
            ListError::NotListed { reason } => ListRecordingsError::StorageUnavailable { reason },
        })
    }

    /// Opens a recording, and says what it is and how far it goes.
    ///
    /// A recording that an engine of this handle writes is committed first, so what it holds is every entry that its
    /// engine handed over until the call.
    ///
    /// # Errors
    ///
    /// - [`OpenRecordingError::NotFound`] when the storage has no recording with that ID.
    /// - [`OpenRecordingError::Unreadable`] when this build cannot read what is there.
    /// - [`OpenRecordingError::StorageUnavailable`] when the storage did not answer, and the recording can be opened again.
    pub async fn open(&self, recording: &RecordingId) -> Result<StoredRecording, OpenRecordingError> {
        self.commit_if_running(recording).await;
        let open = self.opened(recording).await.map_err(|error| match error {
            Unopened::NotFound => OpenRecordingError::NotFound {
                recording: recording.clone(),
            },
            Unopened::Unreadable { reason } => OpenRecordingError::Unreadable {
                recording: recording.clone(),
                reason,
            },
        })?;
        let extent = open.reader.extent().await.map_err(|error| match error {
            ExtentError::NotRead { reason } => OpenRecordingError::StorageUnavailable {
                recording: recording.clone(),
                reason,
            },
        })?;
        Ok(StoredRecording {
            description: open.reader.description().clone(),
            extent,
        })
    }

    /// The calls of a recording that `selector` matches: how many the whole recording holds, and those behind the
    /// first `skip` of them, at most [`query_calls`](ReadingConfiguration::query_calls).
    ///
    /// The storage streams only the stretches whose summaries can hold a call that the selector matches, and no
    /// frame, and the selector decides every call that it reads, as a rule would.
    /// A recording that an engine of this handle writes is committed first.
    ///
    /// # Errors
    ///
    /// - [`QueryRecordingError::NotFound`] when the storage has no recording with that ID.
    /// - [`QueryRecordingError::Unreadable`] when this build cannot read what is there.
    /// - [`QueryRecordingError::StorageUnavailable`] when the storage did not answer, and the query can be sent again.
    /// - [`QueryRecordingError::Busy`] when the readers of the process hold as much memory as they may.
    pub async fn query(
        &self,
        recording: &RecordingId,
        selector: &EffectiveSelector,
        skip: u64,
    ) -> Result<QueriedCalls, QueryRecordingError> {
        let unread = Unread::of(recording);
        self.commit_if_running(recording).await;
        let open = self.opened(recording).await.map_err(|error| unread.unopened(error))?;
        let wanted = MayMatch(selector.clone());
        let mut entries = open
            .reader
            .read(Place::Start, Direction::Forwards, Box::new(wanted), Frames::Without)
            .await
            .map_err(|error| unread.not_streamed(error))?;
        let page = self.inner.configuration.query_calls;
        let mut answer = QueriedCalls {
            matched: 0,
            calls: Vec::new(),
            lost: 0,
        };
        loop {
            let batch = match entries.next().await {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(NextBatchError::Damaged { from, to }) => {
                    answer.lost += lost_between(from, to);
                    continue;
                }
                Err(error) => return Err(unread.not_continued(error)),
            };
            for (_, entry) in batch.entries() {
                let call = match entry {
                    Ok(EntryView::Call(call)) => call,
                    Ok(EntryView::Session(_) | EntryView::Journal(_)) => continue,
                    Err(_) => {
                        answer.lost += 1;
                        continue;
                    }
                };
                let Ok(fields) = call.fields().to_fields() else {
                    answer.lost += 1;
                    continue;
                };
                if !selector.evaluate(&fields).is_matched() {
                    continue;
                }
                let in_the_page = answer.matched >= skip && answer.calls.len() < page;
                if in_the_page {
                    // The answer outlives the stream, so it holds the type, and no block of the storage.
                    let Ok(call) = call.to_call() else {
                        answer.lost += 1;
                        continue;
                    };
                    answer.calls.push(call);
                }
                answer.matched += 1;
            }
        }
        Ok(answer)
    }

    /// The facets of the calls of a recording under `filter`, from its index.
    ///
    /// The first index of an open recording reads every entry of it, without frames, and a later one reads only
    /// what later commits added. A recording that an engine of this handle writes is committed first.
    /// See [what a count means](CallIndex#what-a-count-means).
    ///
    /// # Errors
    ///
    /// The same as [`query`](Self::query).
    pub async fn index(
        &self,
        recording: &RecordingId,
        filter: &EffectiveSelector,
    ) -> Result<RecordingFacets, QueryRecordingError> {
        let unread = Unread::of(recording);
        self.commit_if_running(recording).await;
        let open = self.opened(recording).await.map_err(|error| unread.unopened(error))?;
        let mut indexed = open.index.lock().await;
        let extent = open.reader.extent().await.map_err(|error| match error {
            ExtentError::NotRead { reason } => unread.unavailable(reason),
        })?;
        if extent.entries > indexed.read {
            let from = Place::At(Position::new(indexed.read));
            let mut entries = open
                .reader
                .read(from, Direction::Forwards, Box::new(Everything), Frames::Without)
                .await
                .map_err(|error| unread.not_streamed(error))?;
            loop {
                let batch: Batch = match entries.next().await {
                    Ok(Some(batch)) => batch,
                    Ok(None) => break,
                    Err(NextBatchError::Damaged { from, to }) => {
                        indexed.lost += lost_between(from, to);
                        indexed.read = indexed.read.max(to.next().number());
                        continue;
                    }
                    Err(error) => return Err(unread.not_continued(error)),
                };
                for (position, entry) in batch.entries() {
                    indexed.read = position.next().number();
                    match entry.map(|entry| match entry {
                        EntryView::Call(call) => Some(call.fields().to_fields()),
                        EntryView::Session(_) | EntryView::Journal(_) => None,
                    }) {
                        Ok(Some(Ok(fields))) => indexed.index.add_fields(fields),
                        Ok(None) => {}
                        Ok(Some(Err(_))) | Err(_) => indexed.lost += 1,
                    }
                }
            }
        }
        Ok(RecordingFacets {
            facets: indexed.index.facets(filter),
            lost: indexed.lost,
        })
    }

    /// Commits `recording` when an engine of this handle writes it, so a reader sees every entry that its engine
    /// handed over until now.
    async fn commit_if_running(&self, recording: &RecordingId) {
        let running = self.running().get(recording).cloned();
        if let Some(queue) = running
            && let Some(committed) = queue.commit()
        {
            // A recorder that ended before it took the request has committed what it wrote, or written nothing more.
            let _ = committed.await;
        }
    }

    /// The open recording of that ID: the one that stays open, or one that the storage opens now and that stays open
    /// from here on, in place of the one that someone read longest ago.
    async fn opened(&self, recording: &RecordingId) -> Result<Arc<OpenRecording>, Unopened> {
        if let Some(open) = self.kept_open(recording) {
            return Ok(open);
        }
        let reader = self.inner.storage.open(recording).await.map_err(|error| match error {
            OpenError::NotFound => Unopened::NotFound,
            OpenError::EncodingNotRead { .. } | OpenError::LayoutNotRead { .. } | OpenError::NotReadable { .. } => {
                Unopened::Unreadable {
                    reason: error.to_string(),
                }
            }
        })?;
        let open = Arc::new(OpenRecording {
            reader,
            index: tokio::sync::Mutex::new(Indexed {
                index: CallIndex::new(self.inner.configuration.index.clone()),
                read: 0,
                lost: 0,
            }),
        });
        let mut kept = self.kept();
        // Someone else opened it in the meantime: theirs stays open, and this one closes.
        if let Some((_, theirs)) = kept.iter().find(|(id, _)| id == recording) {
            return Ok(Arc::clone(theirs));
        }
        kept.push_back((recording.clone(), Arc::clone(&open)));
        while kept.len() > self.inner.configuration.open_recordings {
            kept.pop_front();
        }
        Ok(open)
    }

    /// The open recording of that ID when it stays open, which is now the one that someone read last.
    fn kept_open(&self, recording: &RecordingId) -> Option<Arc<OpenRecording>> {
        let mut kept = self.kept();
        let at = kept.iter().position(|(id, _)| id == recording)?;
        let entry = kept.remove(at)?;
        let open = Arc::clone(&entry.1);
        kept.push_back(entry);
        Some(open)
    }

    /// The recordings that stay open. The list is whole whenever the lock is free.
    fn kept(&self) -> MutexGuard<'_, VecDeque<(RecordingId, Arc<OpenRecording>)>> {
        self.inner.open.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a recording in the storage: it exists from here on, and it is empty.
    pub(crate) async fn start(&self, description: Description) -> Result<Box<dyn Writer>, StartError> {
        self.inner.storage.start(description).await
    }

    /// An engine of this handle writes `recording` from now on, through the recorder behind `queue`.
    pub(crate) fn runs(&self, recording: RecordingId, queue: Arc<Queue>) {
        self.running().insert(recording, queue);
    }

    /// Nobody writes `recording` any more.
    pub(crate) fn ended(&self, recording: &RecordingId) {
        self.running().remove(recording);
    }

    /// The recordings that the engines of this handle write. The map is whole whenever the lock is free.
    fn running(&self) -> MutexGuard<'_, BTreeMap<RecordingId, Arc<Queue>>> {
        self.inner.running.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Writes the recordings that run.
impl fmt::Debug for Recordings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let running: Vec<String> = self.running().keys().map(ToString::to_string).collect();
        f.debug_struct("Recordings")
            .field("running", &running)
            .finish_non_exhaustive()
    }
}

/// A storage of recordings, whichever it is: the contract, with futures that the engine can hold as one type.
pub(crate) trait Storage: Send + Sync {
    fn start(&self, description: Description) -> BoxFuture<'_, Result<Box<dyn Writer>, StartError>>;
    fn recordings(&self) -> BoxFuture<'_, Result<Vec<RecordingId>, ListError>>;
    fn open<'a>(&'a self, recording: &'a RecordingId) -> BoxFuture<'a, Result<Box<dyn Reader>, OpenError>>;
}

/// What writes one recording, whichever storage it is.
pub(crate) trait Writer: Send {
    fn append(&mut self, entry: Entry) -> BoxFuture<'_, Result<Position, WriteError>>;
    fn commit(&mut self) -> BoxFuture<'_, Result<(), WriteError>>;
    fn finish(self: Box<Self>, end: End) -> BoxFuture<'static, Result<(), WriteError>>;
}

/// What reads one recording, whichever storage it is.
pub(crate) trait Reader: Send + Sync {
    fn description(&self) -> &Description;
    fn extent(&self) -> BoxFuture<'_, Result<Extent, ExtentError>>;
    fn read(
        &self,
        from: Place,
        direction: Direction,
        wanted: Box<dyn Wanted>,
        frames: Frames,
    ) -> BoxFuture<'_, Result<Box<dyn Entries>, ReadError>>;
}

/// A stream of the entries of a recording, whichever storage it is.
pub(crate) trait Entries: Send {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Batch>, NextBatchError>>;
}

/// What a query wants of a storage: the stretches that hold a call, which its selector can match.
struct MayMatch(EffectiveSelector);

impl Wanted for MayMatch {
    fn may_be_in(&self, summary: &Summary) -> bool {
        summary.calls() > 0
            && self.0.may_match_in(|field| match summary.values(field) {
                None => StretchValues::Absent,
                Some(SummaryValues::Listed(values)) => StretchValues::Listed(values),
                Some(SummaryValues::Any) => StretchValues::Any,
            })
    }
}

/// Why the storage did not open a recording, for whoever opens it to say in its own words.
enum Unopened {
    NotFound,
    Unreadable { reason: String },
}

/// The errors of a reading of one recording, in the words of a query.
struct Unread<'a> {
    recording: &'a RecordingId,
}

impl<'a> Unread<'a> {
    const fn of(recording: &'a RecordingId) -> Self {
        Self { recording }
    }

    fn unopened(&self, error: Unopened) -> QueryRecordingError {
        let recording = self.recording.clone();
        match error {
            Unopened::NotFound => QueryRecordingError::NotFound { recording },
            Unopened::Unreadable { reason } => QueryRecordingError::Unreadable { recording, reason },
        }
    }

    fn unavailable(&self, reason: String) -> QueryRecordingError {
        QueryRecordingError::StorageUnavailable {
            recording: self.recording.clone(),
            reason,
        }
    }

    fn busy(&self, reason: String) -> QueryRecordingError {
        QueryRecordingError::Busy {
            recording: self.recording.clone(),
            reason,
        }
    }

    fn not_streamed(&self, error: ReadError) -> QueryRecordingError {
        match error {
            ReadError::NotRead { reason } => self.unavailable(reason),
            ReadError::AllReadersBudgetSpent { .. } => self.busy(error.to_string()),
        }
    }

    /// A stream that did not go on. A reading holds no batch when it asks for the next, so a budget of its own that
    /// is spent says that the storage holds more than this reading.
    fn not_continued(&self, error: NextBatchError) -> QueryRecordingError {
        match error {
            NextBatchError::NotRead { reason } => self.unavailable(reason),
            NextBatchError::ReaderBudgetSpent { .. } => self.busy(error.to_string()),
            NextBatchError::Damaged { .. } => self.unavailable(error.to_string()),
        }
    }
}

/// How many entries lie from `from` to `to`, both of them included.
fn lost_between(from: Position, to: Position) -> u64 {
    to.number().saturating_sub(from.number()).saturating_add(1)
}

impl<S> Storage for S
where
    S: RecordingStorage + 'static,
    S::Writer: 'static,
    S::Reader: 'static,
    <S::Reader as RecordingReader>::Entries: 'static,
{
    fn start(&self, description: Description) -> BoxFuture<'_, Result<Box<dyn Writer>, StartError>> {
        Box::pin(async move {
            let writer = RecordingStorage::start(self, description).await?;
            Ok(Box::new(writer) as Box<dyn Writer>)
        })
    }

    fn recordings(&self) -> BoxFuture<'_, Result<Vec<RecordingId>, ListError>> {
        Box::pin(RecordingStorage::recordings(self))
    }

    fn open<'a>(&'a self, recording: &'a RecordingId) -> BoxFuture<'a, Result<Box<dyn Reader>, OpenError>> {
        Box::pin(async move {
            let reader = RecordingStorage::open(self, recording).await?;
            Ok(Box::new(reader) as Box<dyn Reader>)
        })
    }
}

impl<W: RecordingWriter + 'static> Writer for W {
    fn append(&mut self, entry: Entry) -> BoxFuture<'_, Result<Position, WriteError>> {
        Box::pin(RecordingWriter::append(self, entry))
    }

    fn commit(&mut self) -> BoxFuture<'_, Result<(), WriteError>> {
        Box::pin(RecordingWriter::commit(self))
    }

    fn finish(self: Box<Self>, end: End) -> BoxFuture<'static, Result<(), WriteError>> {
        Box::pin(RecordingWriter::finish(*self, end))
    }
}

impl<R> Reader for R
where
    R: RecordingReader + 'static,
    R::Entries: 'static,
{
    fn description(&self) -> &Description {
        RecordingReader::description(self)
    }

    fn extent(&self) -> BoxFuture<'_, Result<Extent, ExtentError>> {
        Box::pin(RecordingReader::extent(self))
    }

    fn read(
        &self,
        from: Place,
        direction: Direction,
        wanted: Box<dyn Wanted>,
        frames: Frames,
    ) -> BoxFuture<'_, Result<Box<dyn Entries>, ReadError>> {
        Box::pin(async move {
            let wanted = move |summary: &Summary| wanted.may_be_in(summary);
            let entries = RecordingReader::read(self, from, direction, wanted, frames).await?;
            Ok(Box::new(entries) as Box<dyn Entries>)
        })
    }
}

impl<E: RecordedEntries + 'static> Entries for E {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Batch>, NextBatchError>> {
        Box::pin(RecordedEntries::next(self))
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_call::{CallId, CallTiming, Delivered, Explanation};
    use gaslightmq_clock::Instant;
    use gaslightmq_environment::RecordingMode;
    use gaslightmq_field::{ConnectionNumber, Field, Fields, Outcome, PartFields};
    use gaslightmq_kernel::{ApiKey, TopicPartition};
    use gaslightmq_recording::CallFrames;
    use gaslightmq_selector::Condition;
    use gaslightmq_wire::Request;
    use gaslightmq_wire::messages::ProduceRequest;

    use super::*;

    /// How many values of one field a summary of these tests lists: `recordings.summary_values`.
    const LISTED: usize = 256;

    #[test]
    fn a_query_wants_the_stretches_whose_values_its_selector_can_match() {
        // A stretch that holds one Produce to `orders.eu/3`.
        let partition: TopicPartition = "orders.eu/3".parse().unwrap();
        let fields = Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::PRODUCE)
            .with_part(PartFields::new(partition))
            .with(Outcome::Ok);
        let call = Call::new(
            CallId::FIRST,
            fields,
            Delivered::Nothing,
            Explanation::none(),
            CallTiming::arrived(Instant::ZERO),
        )
        .unwrap();
        let (api_version, correlation_id) = (9, 1);
        let request = Request::new(
            api_version,
            correlation_id,
            Some("order-ingest"),
            &ProduceRequest::default(),
        )
        .unwrap();
        let entry = Entry::call(call, CallFrames::new(request.into_frame()), RecordingMode::Full).unwrap();
        let mut summary = Summary::empty();
        summary.add(&entry, LISTED);
        let wanted = |text: &str| MayMatch(Condition::parse(Field::Topic, text).unwrap().into());

        assert!(wanted("orders.*").may_be_in(&summary));
        assert!(!wanted("payments.*").may_be_in(&summary));
        assert!(MayMatch(EffectiveSelector::new()).may_be_in(&summary));
    }

    #[test]
    fn a_query_wants_no_stretch_without_a_call() {
        let every_call = MayMatch(EffectiveSelector::new());

        assert!(!every_call.may_be_in(&Summary::empty()));
    }
}
