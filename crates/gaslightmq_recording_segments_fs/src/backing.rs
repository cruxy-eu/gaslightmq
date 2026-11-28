//! The backing on files: the backing contract of the segment layout, over a directory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use bytes::Bytes;
use gaslightmq_recording::{Position, RecordingId};
use gaslightmq_recording_segments::{FetchError, Listed, ListingError, Object, ObjectKind, SegmentBacking, StoreError};

use crate::configuration::FileConfiguration;
use crate::disk::{Found, Recordings};
use crate::map::Map;
use crate::stream::FileStream;

/// A backing that keeps the objects of recordings as files: one directory for each recording, below
/// `recordings.directory`.
///
/// ```text
/// recordings/
/// └── orders-sim.2026-11-21T14-02-11.527Z/
///     ├── manifest
///     ├── 00000000000000000000.index.open
///     ├── 00000000000000000000.segment
///     └── 00000000000008912345.segment.open
/// ```
///
/// | Operation | On files |
/// |---|---|
/// | Create | Creates `….segment.open` or `….index.open` with the call that refuses a file that is there, and syncs the directory. |
/// | Append | Writes the pieces with one write, and syncs the data of the file. |
/// | Seal | Syncs the file, renames it to the name without `.open`, makes it read-only, and syncs the directory. |
/// | Put the manifest | Writes a file beside the old one, syncs it, renames it over the old one, and syncs the directory. |
/// | Read | Maps a sealed file once, and hands out ranges of the map. Reads from a file that grows into buffers. |
/// | List | Reads the names of the directory. |
///
/// A file with another name is no object of the recording, and the backing leaves it alone.
/// Every touch of a file runs on the blocking pool of the Tokio runtime that awaits it, so it never holds up a task.
/// A clone is the same backing.
///
/// The segment layout over it is a storage of recordings on files:
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_field::ConnectionNumber;
/// use gaslightmq_recording::prelude::*;
/// use gaslightmq_recording_segments::prelude::*;
/// use gaslightmq_recording_segments_fs::prelude::*;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> anyhow::Result<()> {
/// let directory = tempfile::tempdir()?;
/// let backing = FileBacking::new(directory.path(), FileConfiguration::new());
/// let storage = SegmentStorage::new(backing, SegmentConfiguration::new());
/// let description = Description {
///     environment: "orders-sim".parse()?,
///     started: "2026-11-21T14:02:11.527Z".parse()?,
///     clock: Instant::ZERO,
///     mode: RecordingMode::Full,
///     backend: RecordedBackend::Simulated,
///     written_by: "0.1.0".to_owned(),
///     armed: Vec::new(),
///     scenario: None,
///     journal_after: None,
/// };
///
/// let mut writer = storage.start(description.clone()).await?;
/// writer
///     .append(Entry::session(&RecordedSessionEvent {
///         connection: ConnectionNumber::new(7),
///         at: Instant::ZERO,
///         change: SessionChange::Closed { by: Closer::Client },
///     }))
///     .await?;
/// writer.commit().await?;
///
/// let recording = directory.path().join("orders-sim.2026-11-21T14-02-11.527Z");
/// assert!(recording.join("00000000000000000000.segment.open").is_file());
/// assert!(recording.join("manifest").is_file());
///
/// let reader = storage.open(&description.id()).await?;
/// assert_eq!(reader.extent().await?, Extent { entries: 1, end: None });
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct FileBacking {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    recordings: Recordings,
    configuration: FileConfiguration,
    /// The sealed files that a stream holds mapped, so that every stream of one file reads the one map.
    maps: Mutex<HashMap<PathBuf, Weak<Map>>>,
    /// The number of the next put of a manifest, which names the file beside it.
    puts: AtomicU64,
}

impl FileBacking {
    /// The backing over `directory`, the directory of every recording: `recordings.directory`.
    ///
    /// It touches no file yet. The directory is made with the first recording.
    pub fn new(directory: impl Into<PathBuf>, configuration: FileConfiguration) -> FileBacking {
        FileBacking {
            shared: Arc::new(Shared {
                recordings: Recordings::new(directory.into()),
                configuration,
                maps: Mutex::new(HashMap::new()),
                puts: AtomicU64::new(0),
            }),
        }
    }

    /// The directory of every recording.
    pub fn directory(&self) -> &Path {
        self.shared.recordings.directory()
    }

    /// Runs `work` over the files on the blocking pool, and turns a thread that stopped into `stopped`.
    async fn blocking<T: Send + 'static, E>(
        &self,
        work: impl FnOnce(&Shared) -> Result<T, E> + Send + 'static,
        stopped: impl FnOnce(String) -> E,
    ) -> Result<T, E>
    where
        E: Send + 'static,
    {
        let shared = Arc::clone(&self.shared);
        tokio::task::spawn_blocking(move || work(&shared))
            .await
            .unwrap_or_else(|error| {
                Err(stopped(format!(
                    "the work on the files stopped before it ended: {error}"
                )))
            })
    }
}

impl Shared {
    /// A stream over the file of `object`, from `from` on.
    fn stream(&self, recording: &RecordingId, object: Object, from: u64) -> Result<FileStream, FetchError> {
        let path = self.recordings.sealed(recording, object);
        if let Some(map) = self.mapped(&path) {
            return Ok(FileStream::mapped(map, from, self.configuration.read_ahead));
        }
        match self.recordings.find(recording, object)? {
            Found::Sealed(file) => {
                let map = Map::sealed(&file).map_err(|error| FetchError::NotFetched {
                    reason: format!("{}: {error}", path.display()),
                })?;
                let mut maps = self.maps.lock().unwrap_or_else(PoisonError::into_inner);
                maps.retain(|_, map| map.strong_count() > 0);
                maps.insert(path, Arc::downgrade(&map));
                Ok(FileStream::mapped(map, from, self.configuration.read_ahead))
            }
            Found::Growing(file, path) => Ok(FileStream::growing(file, path, from, self.configuration.read_size)),
        }
    }

    /// The map of a sealed file that a stream still holds.
    fn mapped(&self, path: &Path) -> Option<Arc<Map>> {
        let maps = self.maps.lock().unwrap_or_else(PoisonError::into_inner);
        maps.get(path).and_then(Weak::upgrade)
    }
}

impl SegmentBacking for FileBacking {
    type Stream = FileStream;

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListingError> {
        self.blocking(
            |shared| shared.recordings.list(),
            |reason| ListingError::NotListed { reason },
        )
        .await
    }

    async fn create(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        let recording = recording.clone();
        self.blocking(
            move |shared| shared.recordings.create(&recording, object),
            |reason| StoreError::NotStored { reason },
        )
        .await
    }

    async fn append(&self, recording: &RecordingId, object: Object, pieces: &[Bytes]) -> Result<(), StoreError> {
        let recording = recording.clone();
        // The pieces are shared buffers: this hands them over, and copies none.
        let pieces = pieces.to_vec();
        self.blocking(
            move |shared| shared.recordings.append(&recording, object, &pieces),
            |reason| StoreError::NotStored { reason },
        )
        .await
    }

    async fn seal(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        let recording = recording.clone();
        self.blocking(
            move |shared| shared.recordings.seal(&recording, object),
            |reason| StoreError::NotStored { reason },
        )
        .await
    }

    async fn list(
        &self,
        recording: &RecordingId,
        kind: ObjectKind,
        from: Position,
    ) -> Result<Vec<Listed>, ListingError> {
        let recording = recording.clone();
        self.blocking(
            move |shared| shared.recordings.objects(&recording, kind, from),
            |reason| ListingError::NotListed { reason },
        )
        .await
    }

    async fn read(&self, recording: &RecordingId, object: Object, from: u64) -> Result<FileStream, FetchError> {
        let recording = recording.clone();
        self.blocking(
            move |shared| shared.stream(&recording, object, from),
            |reason| FetchError::NotFetched { reason },
        )
        .await
    }

    async fn put_manifest(&self, recording: &RecordingId, manifest: Bytes) -> Result<(), StoreError> {
        let recording = recording.clone();
        let beside = self.shared.puts.fetch_add(1, Ordering::Relaxed);
        self.blocking(
            move |shared| shared.recordings.put_manifest(&recording, &manifest, beside),
            |reason| StoreError::NotStored { reason },
        )
        .await
    }

    async fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError> {
        let recording = recording.clone();
        self.blocking(
            move |shared| shared.recordings.manifest(&recording),
            |reason| FetchError::NotFetched { reason },
        )
        .await
    }
}
