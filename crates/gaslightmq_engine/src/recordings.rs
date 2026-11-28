//! Where the recordings of an engine are kept: a storage of recordings, whichever layout it is.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::future::BoxFuture;
use gaslightmq_recording::{
    Description, End, Entry, Extent, ExtentError, ListError, OpenError, Position, RecordingId, RecordingReader,
    RecordingStorage, RecordingWriter, StartError, WriteError,
};

use crate::recorder::Queue;
use crate::{ListRecordingsError, OpenRecordingError};

/// Where the recordings of a process are kept: a [storage of recordings](RecordingStorage), whichever layout it is.
///
/// The engine holds a storage and names no layout.
/// Whoever puts a process together picks one, makes this handle of it once, and hands a clone to every engine:
/// `gmq` picks the segment layout on files, and a test picks a storage in memory.
/// A clone is a handle to the same storage.
///
/// A recording that runs is in the storage from its start, and every reader sees what its last commit covers.
/// [`open`](Self::open) commits a recording that an engine of the handle writes before it reads it, so whoever opens
/// a recording that runs sees every entry that its engine handed over until then.
#[derive(Clone)]
pub struct Recordings {
    inner: Arc<Inner>,
}

struct Inner {
    storage: Box<dyn Storage>,
    /// The recordings that the engines of this handle write, each with the queue of its recorder.
    running: Mutex<BTreeMap<RecordingId, Arc<Queue>>>,
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
    /// The recordings of `storage`.
    pub fn new<S>(storage: S) -> Self
    where
        S: RecordingStorage + 'static,
        S::Writer: 'static,
        S::Reader: 'static,
    {
        Self {
            inner: Arc::new(Inner {
                storage: Box::new(storage),
                running: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// The ID of every recording of the storage, in their order: by the environment, and then by the instant it
    /// started. A recording that runs is among them.
    ///
    /// # Errors
    ///
    /// [`ListRecordingsError::NotListed`] when the storage did not answer.
    pub async fn list(&self) -> Result<Vec<RecordingId>, ListRecordingsError> {
        self.inner.storage.recordings().await.map_err(|error| match error {
            ListError::NotListed { reason } => ListRecordingsError::NotListed { reason },
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
    /// - [`OpenRecordingError::NotReadable`] when this build cannot read what is there.
    /// - [`OpenRecordingError::NotRead`] when the storage did not answer, and the recording can be opened again.
    pub async fn open(&self, recording: &RecordingId) -> Result<StoredRecording, OpenRecordingError> {
        let running = self.running().get(recording).cloned();
        if let Some(queue) = running
            && let Some(committed) = queue.commit()
        {
            // A recorder that ended before it took the request has committed what it wrote, or written nothing more.
            let _ = committed.await;
        }
        let reader = self.inner.storage.open(recording).await.map_err(|error| match error {
            OpenError::NotFound => OpenRecordingError::NotFound {
                recording: recording.clone(),
            },
            OpenError::EncodingNotRead { error } => OpenRecordingError::NotReadable {
                recording: recording.clone(),
                reason: error.to_string(),
            },
            OpenError::LayoutNotRead { .. } | OpenError::NotReadable { .. } => OpenRecordingError::NotReadable {
                recording: recording.clone(),
                reason: error.to_string(),
            },
        })?;
        let extent = reader.extent().await.map_err(|error| match error {
            ExtentError::NotRead { reason } => OpenRecordingError::NotRead {
                recording: recording.clone(),
                reason,
            },
        })?;
        Ok(StoredRecording {
            description: reader.description().clone(),
            extent,
        })
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
}

impl<S> Storage for S
where
    S: RecordingStorage + 'static,
    S::Writer: 'static,
    S::Reader: 'static,
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

impl<R: RecordingReader + 'static> Reader for R {
    fn description(&self) -> &Description {
        RecordingReader::description(self)
    }

    fn extent(&self) -> BoxFuture<'_, Result<Extent, ExtentError>> {
        Box::pin(RecordingReader::extent(self))
    }
}
