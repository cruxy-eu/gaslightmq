//! The backing in memory: for tests, and for an engine whose recordings need not outlive it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};
use gaslightmq_recording::{Position, RecordingId};

use crate::backing::{ByteStream, FetchError, Listed, ListingError, Object, ObjectKind, SegmentBacking, StoreError};

/// A backing that keeps the objects of recordings in memory.
///
/// Every write is whole, and nothing in it outlives its process. A clone is the same backing.
///
/// An object keeps each append as one buffer, so a stream hands out a header or a chunk as a slice of it,
/// without a copy.
///
/// The segment layout over it is a storage of recordings in memory:
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_field::ConnectionNumber;
/// use gaslightmq_recording::prelude::*;
/// use gaslightmq_recording_segments::prelude::*;
///
/// let storage = SegmentStorage::new(MemoryBacking::new(), SegmentConfiguration::new());
/// let description = Description {
///     environment: "orders-sim".parse().expect("a name"),
///     started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
///     clock: Instant::ZERO,
///     mode: RecordingMode::Full,
///     backend: RecordedBackend::Simulated,
///     written_by: "0.1.0".to_owned(),
///     armed: Vec::new(),
///     scenario: None,
///     journal_after: None,
/// };
/// let closed = SessionEvent {
///     connection: ConnectionNumber::new(7),
///     at: Instant::ZERO,
///     change: SessionChange::Closed { by: Closer::Client },
/// };
///
/// block_on(async {
///     let mut writer = storage.start(description.clone()).await.expect("a new recording");
///     writer.append(Entry::session(&closed)).await.expect("the entry is taken");
///     writer.commit().await.expect("the entry is in the recording");
///
///     let reader = storage.open(&description.id()).await.expect("the recording opens");
///     assert_eq!(reader.extent().await, Ok(Extent { entries: 1, end: None }));
/// });
/// ```
///
/// The backing itself keeps bytes under names:
///
/// ```
/// use bytes::Bytes;
/// use futures::executor::block_on;
/// use gaslightmq_recording::{Position, RecordingId};
/// use gaslightmq_recording_segments::prelude::*;
///
/// let backing = MemoryBacking::new();
/// let recording: RecordingId = "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("a recording ID");
/// let segment = Object::segment(Position::FIRST);
///
/// block_on(async {
///     backing.create(&recording, segment).await.expect("a new segment");
///     backing.append(&recording, segment, &[Bytes::from_static(b"GMQ")]).await.expect("an append");
///
///     let mut bytes = backing.read(&recording, segment, 0).await.expect("the segment is there");
///     assert_eq!(bytes.next(16).await, Ok(Some(Bytes::from_static(b"GMQ"))), "fewer at the end of the object");
///     assert_eq!(bytes.next(16).await, Ok(None), "and nothing behind it");
/// });
/// ```
#[derive(Debug, Clone, Default)]
pub struct MemoryBacking {
    recordings: Arc<Mutex<BTreeMap<RecordingId, Objects>>>,
}

/// The objects of one recording.
#[derive(Debug, Default)]
struct Objects {
    growing: BTreeMap<Object, Kept>,
    manifest: Option<Bytes>,
}

/// One object that grows: every append as one buffer, in order.
#[derive(Debug, Clone, Default)]
struct Kept {
    appends: Vec<Bytes>,
    length: u64,
    sealed: bool,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MemoryBacking {
    /// A backing that holds no recording.
    pub fn new() -> MemoryBacking {
        MemoryBacking::default()
    }

    /// Leaves `object` as a crash leaves it when only its first `length` bytes were on the disk:
    /// the last append torn, or appends lost. It is how a test of recovery makes a recording that was cut short.
    ///
    /// The object takes appends again afterwards, as a file that a crash cut short does.
    pub fn cut_short(&self, recording: &RecordingId, object: Object, length: u64) {
        let mut recordings = locked(&self.recordings);
        let Some(kept) = recordings
            .get_mut(recording)
            .and_then(|objects| objects.growing.get_mut(&object))
        else {
            return;
        };
        let mut left = length;
        let mut appends = Vec::new();
        for append in &kept.appends {
            let taken = left.min(append.len() as u64);
            if taken > 0 {
                appends.push(append.slice(..usize::try_from(taken).unwrap_or(append.len())));
            }
            left -= taken;
        }
        kept.length = length.min(kept.length);
        kept.appends = appends;
        kept.sealed = false;
    }

    /// Turns every bit of the byte at `at` of `object`, as a disk that went bad there does.
    /// It is how a test makes a recording whose bytes fail their checksum.
    pub fn damage(&self, recording: &RecordingId, object: Object, at: u64) {
        self.with(recording, object, |kept| {
            let mut before = 0;
            for append in &mut kept.appends {
                let len = append.len() as u64;
                if (before..before + len).contains(&at) {
                    let mut damaged = append.to_vec();
                    let index = usize::try_from(at - before).unwrap_or_default();
                    damaged[index] = !damaged[index];
                    *append = Bytes::from(damaged);
                    return;
                }
                before += len;
            }
        });
    }

    /// Takes the manifest of the recording away, as a crash before its first put leaves it.
    pub fn drop_manifest(&self, recording: &RecordingId) {
        if let Some(objects) = locked(&self.recordings).get_mut(recording) {
            objects.manifest = None;
        }
    }

    /// Takes `object` away, as a damaged disk or someone who deleted a file leaves the recording.
    pub fn drop_object(&self, recording: &RecordingId, object: Object) {
        if let Some(objects) = locked(&self.recordings).get_mut(recording) {
            objects.growing.remove(&object);
        }
    }

    fn with<T>(&self, recording: &RecordingId, object: Object, change: impl FnOnce(&mut Kept) -> T) -> Option<T> {
        let mut recordings = locked(&self.recordings);
        let kept = recordings.get_mut(recording)?.growing.get_mut(&object)?;
        Some(change(kept))
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a backing in memory has nothing to wait for"
)]
impl SegmentBacking for MemoryBacking {
    type Stream = MemoryStream;

    async fn recordings(&self) -> Result<Vec<RecordingId>, ListingError> {
        Ok(locked(&self.recordings).keys().cloned().collect())
    }

    async fn create(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        let mut recordings = locked(&self.recordings);
        let objects = recordings.entry(recording.clone()).or_default();
        if objects.growing.contains_key(&object) {
            return Err(StoreError::AlreadyThere);
        }
        objects.growing.insert(object, Kept::default());
        Ok(())
    }

    async fn append(&self, recording: &RecordingId, object: Object, pieces: &[Bytes]) -> Result<(), StoreError> {
        let appended = self.with(recording, object, |kept| {
            if kept.sealed {
                return Err(StoreError::NotStored {
                    reason: format!("the {object} is sealed"),
                });
            }
            // This is the write to the disk: the one copy of the bytes on the way in.
            let mut joined = BytesMut::with_capacity(pieces.iter().map(Bytes::len).sum());
            for piece in pieces {
                joined.extend_from_slice(piece);
            }
            kept.length += joined.len() as u64;
            if !joined.is_empty() {
                kept.appends.push(joined.freeze());
            }
            Ok(())
        });
        appended.unwrap_or_else(|| {
            Err(StoreError::NotStored {
                reason: format!("there is no {object}"),
            })
        })
    }

    async fn seal(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        self.with(recording, object, |kept| kept.sealed = true)
            .ok_or_else(|| StoreError::NotStored {
                reason: format!("there is no {object}"),
            })
    }

    async fn list(
        &self,
        recording: &RecordingId,
        kind: ObjectKind,
        from: Position,
    ) -> Result<Vec<Listed>, ListingError> {
        let recordings = locked(&self.recordings);
        let Some(objects) = recordings.get(recording) else {
            return Ok(Vec::new());
        };
        let listed = objects.growing.range(Object { kind, first: from }..);
        let listed = listed.take_while(|(object, _)| object.kind == kind);
        Ok(listed
            .map(|(object, kept)| Listed {
                first: object.first,
                length: kept.length,
                sealed: kept.sealed,
            })
            .collect())
    }

    async fn read(&self, recording: &RecordingId, object: Object, from: u64) -> Result<MemoryStream, FetchError> {
        let appends = self
            .with(recording, object, |kept| kept.appends.clone())
            .ok_or(FetchError::Missing)?;
        let mut stream = MemoryStream {
            backing: self.clone(),
            recording: recording.clone(),
            object,
            appends,
            at: 0,
        };
        stream.skip(from);
        Ok(stream)
    }

    async fn put_manifest(&self, recording: &RecordingId, manifest: Bytes) -> Result<(), StoreError> {
        locked(&self.recordings).entry(recording.clone()).or_default().manifest = Some(manifest);
        Ok(())
    }

    async fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError> {
        locked(&self.recordings)
            .get(recording)
            .and_then(|objects| objects.manifest.clone())
            .ok_or(FetchError::Missing)
    }
}

/// The bytes of one object of a [`MemoryBacking`], from an offset on.
///
/// It sees what later appends add to the object.
#[derive(Debug)]
pub struct MemoryStream {
    backing: MemoryBacking,
    recording: RecordingId,
    object: Object,
    /// The appends that are left to hand out, the first of them without what was handed out of it.
    appends: Vec<Bytes>,
    /// How many bytes of the object stand before the first of `appends`.
    at: u64,
}

impl MemoryStream {
    /// Steps over `bytes` bytes.
    fn skip(&mut self, mut bytes: u64) {
        while bytes > 0 && !self.appends.is_empty() {
            let first = &mut self.appends[0];
            let taken = bytes.min(first.len() as u64);
            let _ = first.split_to(usize::try_from(taken).unwrap_or(first.len()));
            self.at += taken;
            bytes -= taken;
            if first.is_empty() {
                self.appends.remove(0);
            }
        }
    }

    /// Takes what later appends added to the object.
    fn catch_up(&mut self) {
        let left: u64 = self.appends.iter().map(|append| append.len() as u64).sum();
        let seen = self.at + left;
        let later = self.backing.with(&self.recording, self.object, |kept| {
            let mut skipped = 0;
            let mut later = Vec::new();
            for append in &kept.appends {
                let len = append.len() as u64;
                if skipped + len <= seen {
                    skipped += len;
                } else {
                    let from = usize::try_from(seen.saturating_sub(skipped)).unwrap_or(append.len());
                    later.push(append.slice(from..));
                    skipped += len;
                }
            }
            later
        });
        self.appends.extend(later.into_iter().flatten());
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a backing in memory has nothing to wait for"
)]
impl ByteStream for MemoryStream {
    async fn next(&mut self, wanted: usize) -> Result<Option<Bytes>, FetchError> {
        let left: usize = self.appends.iter().map(Bytes::len).sum();
        if left < wanted {
            self.catch_up();
        }
        let Some(first) = self.appends.first_mut() else {
            return Ok(None);
        };
        let bytes = if first.len() >= wanted {
            first.split_to(wanted)
        } else {
            // The bytes stand in two appends or more, so they are joined.
            let mut joined = BytesMut::with_capacity(wanted);
            while joined.len() < wanted
                && let Some(first) = self.appends.first_mut()
            {
                let taken = first.split_to((wanted - joined.len()).min(first.len()));
                joined.extend_from_slice(&taken);
                if first.is_empty() {
                    self.appends.remove(0);
                }
            }
            joined.freeze()
        };
        self.appends.retain(|append| !append.is_empty());
        self.at += bytes.len() as u64;
        Ok(Some(bytes))
    }
}
