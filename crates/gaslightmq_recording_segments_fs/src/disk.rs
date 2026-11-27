//! The files themselves: how each operation of the backing contract is made of calls to the system.
//!
//! Everything here blocks, and runs on a thread of the blocking pool of the runtime.
//! Each function turns what the system said into an error of the contract, with the file it said it of.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IoSlice, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use gaslightmq_recording::{Position, RecordingId};
use gaslightmq_recording_segments::{FetchError, Listed, ListingError, Object, ObjectKind, StoreError};

use crate::name::{MANIFEST, ObjectFile};

/// The directory of every recording: `recordings.directory`.
#[derive(Debug, Clone)]
pub(crate) struct Recordings {
    directory: PathBuf,
}

/// Where an object that [`Recordings::find`] looked for stands.
pub(crate) enum Found {
    /// Its sealed file, open to read.
    Sealed(File),
    /// Its file that grows, open to read, and the path of it.
    Growing(File, PathBuf),
}

impl Recordings {
    pub(crate) fn new(directory: PathBuf) -> Recordings {
        Recordings { directory }
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    /// Every directory whose name is the ID of a recording, in the order of the IDs.
    pub(crate) fn list(&self) -> Result<Vec<RecordingId>, ListingError> {
        let not_listed = |error: io::Error| ListingError::NotListed {
            reason: format!("{}: {error}", self.directory.display()),
        };
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(not_listed(error)),
        };
        let mut recordings = Vec::new();
        for entry in entries {
            let entry = entry.map_err(not_listed)?;
            if !entry.file_type().map_err(not_listed)?.is_dir() {
                continue;
            }
            if let Some(recording) = entry.file_name().to_str().and_then(|name| name.parse().ok()) {
                recordings.push(recording);
            }
        }
        recordings.sort();
        Ok(recordings)
    }

    /// Creates the empty file of `object`, as one that grows.
    ///
    /// The file is made with the one call of the system that refuses a file that is there, so of two that create one
    /// object, one gets [`StoreError::AlreadyThere`]. The directory of the recording is made when it is not there.
    pub(crate) fn create(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        let directory = self.make(recording)?;
        let open = directory.join(ObjectFile::open(object).name());
        let sealed = directory.join(ObjectFile::sealed(object).name());
        if is_there(&sealed).map_err(|error| not_stored(&sealed, &error))? {
            return Err(StoreError::AlreadyThere);
        }
        match OpenOptions::new().write(true).create_new(true).open(&open) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Err(StoreError::AlreadyThere),
            Err(error) => return Err(not_stored(&open, &error)),
        }
        // A seal of the object that was there may have renamed its file between the look and the create.
        if is_there(&sealed).map_err(|error| not_stored(&sealed, &error))? {
            let _ = fs::remove_file(&open);
            return Err(StoreError::AlreadyThere);
        }
        sync_directory(&directory).map_err(|error| not_stored(&directory, &error))
    }

    /// Writes `pieces` behind the bytes of the file of `object`, and waits until they are on the disk.
    ///
    /// The file is open to append, so every write goes behind what is there and changes no byte of it.
    pub(crate) fn append(&self, recording: &RecordingId, object: Object, pieces: &[Bytes]) -> Result<(), StoreError> {
        let directory = self.of(recording);
        let open = directory.join(ObjectFile::open(object).name());
        let mut file = match OpenOptions::new().append(true).open(&open) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let sealed = directory.join(ObjectFile::sealed(object).name());
                let reason = if is_there(&sealed).unwrap_or_default() {
                    format!("the {object} is sealed")
                } else {
                    format!("there is no {object}")
                };
                return Err(StoreError::NotStored { reason });
            }
            Err(error) => return Err(not_stored(&open, &error)),
        };
        write_all(&mut file, pieces).map_err(|error| not_stored(&open, &error))?;
        file.sync_data().map_err(|error| not_stored(&open, &error))
    }

    /// Seals `object`: syncs its file, renames it to its sealed name, and syncs the directory.
    ///
    /// The rename is the one step: before it the object grows, after it the object is sealed. The sealed file is
    /// made read-only, so nobody who runs as its owner writes it again by mistake.
    /// An object that is sealed already stays sealed.
    pub(crate) fn seal(&self, recording: &RecordingId, object: Object) -> Result<(), StoreError> {
        let directory = self.of(recording);
        let open = directory.join(ObjectFile::open(object).name());
        let sealed = directory.join(ObjectFile::sealed(object).name());
        let file = match File::open(&open) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return if is_there(&sealed).map_err(|error| not_stored(&sealed, &error))? {
                    Ok(())
                } else {
                    Err(StoreError::NotStored {
                        reason: format!("there is no {object}"),
                    })
                };
            }
            Err(error) => return Err(not_stored(&open, &error)),
        };
        file.sync_all().map_err(|error| not_stored(&open, &error))?;
        fs::rename(&open, &sealed).map_err(|error| not_stored(&open, &error))?;
        let mut permissions = file
            .metadata()
            .map_err(|error| not_stored(&sealed, &error))?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&sealed, permissions).map_err(|error| not_stored(&sealed, &error))?;
        sync_directory(&directory).map_err(|error| not_stored(&directory, &error))
    }

    /// Every object of `kind` from `from` on, in the order of their positions, with the length of its file.
    pub(crate) fn objects(
        &self,
        recording: &RecordingId,
        kind: ObjectKind,
        from: Position,
    ) -> Result<Vec<Listed>, ListingError> {
        let directory = self.of(recording);
        let not_listed = |error: io::Error| ListingError::NotListed {
            reason: format!("{}: {error}", directory.display()),
        };
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(not_listed(error)),
        };
        let mut listed = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(not_listed)?;
            let Some(file) = entry.file_name().to_str().and_then(ObjectFile::parse) else {
                continue;
            };
            if file.object.kind != kind || file.object.first < from {
                continue;
            }
            let length = entry.metadata().map_err(not_listed)?.len();
            let object = Listed {
                first: file.object.first,
                length,
                sealed: file.sealed,
            };
            // A listing that runs while a seal renames the file can see both names, and the sealed one counts.
            listed
                .entry(object.first)
                .and_modify(|seen: &mut Listed| {
                    if object.sealed {
                        *seen = object;
                    }
                })
                .or_insert(object);
        }
        Ok(listed.into_values().collect())
    }

    /// Opens the file of `object` to read: the sealed one when it is sealed, and the one that grows otherwise.
    pub(crate) fn find(&self, recording: &RecordingId, object: Object) -> Result<Found, FetchError> {
        let sealed = self.sealed(recording, object);
        if let Some(file) = open_to_read(&sealed)? {
            return Ok(Found::Sealed(file));
        }
        let open = self.of(recording).join(ObjectFile::open(object).name());
        if let Some(file) = open_to_read(&open)? {
            return Ok(Found::Growing(file, open));
        }
        // A seal may have renamed the file between the two looks.
        match open_to_read(&sealed)? {
            Some(file) => Ok(Found::Sealed(file)),
            None => Err(FetchError::Missing),
        }
    }

    /// The path of the sealed file of `object`.
    pub(crate) fn sealed(&self, recording: &RecordingId, object: Object) -> PathBuf {
        self.of(recording).join(ObjectFile::sealed(object).name())
    }

    /// Replaces the manifest in one step: writes a file beside it, syncs it, renames it over the manifest, and syncs
    /// the directory. A reader gets the old manifest or the new one, and so does a crash.
    ///
    /// `beside` tells the file beside the manifest from that of another put that runs at the same time.
    /// Its name is no object of the recording, so a crash before the rename leaves a file that nobody reads.
    pub(crate) fn put_manifest(&self, recording: &RecordingId, manifest: &[u8], beside: u64) -> Result<(), StoreError> {
        let directory = self.make(recording)?;
        let temporary = directory.join(format!(".{MANIFEST}.{}.{beside}.tmp", std::process::id()));
        let target = directory.join(MANIFEST);
        let written = write_through(&temporary, manifest).and_then(|()| fs::rename(&temporary, &target));
        if let Err(error) = written {
            // What is left of the file beside the manifest is of no use to anyone.
            let _ = fs::remove_file(&temporary);
            return Err(not_stored(&target, &error));
        }
        sync_directory(&directory).map_err(|error| not_stored(&directory, &error))
    }

    /// The manifest of the recording, whole.
    pub(crate) fn manifest(&self, recording: &RecordingId) -> Result<Bytes, FetchError> {
        let path = self.of(recording).join(MANIFEST);
        match fs::read(&path) {
            Ok(manifest) => Ok(Bytes::from(manifest)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(FetchError::Missing),
            Err(error) => Err(not_fetched(&path, &error)),
        }
    }

    /// The directory of a recording.
    fn of(&self, recording: &RecordingId) -> PathBuf {
        self.directory.join(recording.to_string())
    }

    /// The directory of a recording, made when it is not there, and kept through a crash.
    fn make(&self, recording: &RecordingId) -> Result<PathBuf, StoreError> {
        let directory = self.of(recording);
        fs::create_dir_all(&self.directory).map_err(|error| not_stored(&self.directory, &error))?;
        match fs::create_dir(&directory) {
            Ok(()) => sync_directory(&self.directory).map_err(|error| not_stored(&self.directory, &error))?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(not_stored(&directory, &error)),
        }
        Ok(directory)
    }
}

/// Opens a file to read, or `None` when it is not there.
fn open_to_read(path: &Path) -> Result<Option<File>, FetchError> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(not_fetched(path, &error)),
    }
}

/// Whether a file is there.
fn is_there(path: &Path) -> io::Result<bool> {
    path.try_exists()
}

/// Writes every piece, in order, with as few calls of the system as it takes: one, when it takes them all.
fn write_all(file: &mut File, pieces: &[Bytes]) -> io::Result<()> {
    let mut slices: Vec<IoSlice<'_>> = pieces
        .iter()
        .filter(|piece| !piece.is_empty())
        .map(|piece| IoSlice::new(piece))
        .collect();
    let mut slices = &mut slices[..];
    while !slices.is_empty() {
        match file.write_vectored(slices) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => IoSlice::advance_slices(&mut slices, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Writes a new file, and waits until its bytes are on the disk.
fn write_through(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Asks the system to put what a directory lists on the disk, so that a create or a rename in it survives a crash.
///
/// A system that cannot open a directory as a file, as Windows, is asked nothing.
fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn not_stored(path: &Path, error: &io::Error) -> StoreError {
    StoreError::NotStored {
        reason: format!("{}: {error}", path.display()),
    }
}

fn not_fetched(path: &Path, error: &io::Error) -> FetchError {
    FetchError::NotFetched {
        reason: format!("{}: {error}", path.display()),
    }
}
