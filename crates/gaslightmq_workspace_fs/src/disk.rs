//! The files themselves: what a store finds in the directory of a workspace, and how it replaces a file.
//!
//! Everything here blocks, and runs on a thread of the blocking pool of the runtime.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::FileError;
use crate::held::Found;
use crate::kind::Place;

/// Every file of a kind, with its bytes, in the order of their paths below the directory of the workspace.
///
/// A file of its own, as `selectors.yaml`, comes with `None` when it is not there.
/// A directory, as `rules/`, gives its files that end in `.yaml` and do not start with a `.`.
/// A directory that is not there holds no file.
///
/// # Errors
///
/// Every file and directory that could not be read, with what the system said.
pub(crate) fn find(directory: &Path, place: Place) -> Result<Vec<Found>, Vec<FileError>> {
    let files = match place {
        Place::File(file) => vec![PathBuf::from(file)],
        Place::Lists(below) | Place::Each(below) => listed(directory, Path::new(below))?,
    };
    let mut found = Vec::with_capacity(files.len());
    let mut errors = Vec::new();
    for file in files {
        match read(directory, &file) {
            Ok(bytes) => found.push((file, bytes)),
            Err(reason) => errors.push(FileError { file, reason }),
        }
    }
    if errors.is_empty() { Ok(found) } else { Err(errors) }
}

/// The bytes of one file below the directory of the workspace, or `None` when it is not there.
///
/// # Errors
///
/// What the system said, when the file is there and could not be read.
pub(crate) fn read(directory: &Path, file: &Path) -> Result<Option<Vec<u8>>, String> {
    match fs::read(directory.join(file)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Replaces a file by `text` in one step: the text goes to a temporary file next to it, which is renamed over it.
///
/// A reader never sees half a file, and a crash leaves the old file or the new one.
/// The directory of the file is made when it is not there.
///
/// # Errors
///
/// What the system said, when the file could not be written. The file is then as it was.
pub(crate) fn replace(directory: &Path, file: &Path, text: &str) -> Result<(), String> {
    let path = directory.join(file);
    let parent = path.parent().unwrap_or(directory);
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let name = path
        .file_name()
        .map_or_else(Default::default, |name| name.to_string_lossy());
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let written = write_through(&temporary, text).and_then(|()| fs::rename(&temporary, &path));
    written.map_err(|error| {
        // What is left of the temporary file is of no use to anyone, whatever failed.
        let _ = fs::remove_file(&temporary);
        error.to_string()
    })
}

/// Takes a file away.
///
/// # Errors
///
/// What the system said, when the file could not be removed.
pub(crate) fn remove(directory: &Path, file: &Path) -> Result<(), String> {
    fs::remove_file(directory.join(file)).map_err(|error| error.to_string())
}

/// Writes a new file, and waits until its bytes are on the disk.
fn write_through(path: &Path, text: &str) -> io::Result<()> {
    let mut file = fs::File::create(path)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()
}

/// The files of a directory below the directory of the workspace, as paths below the directory of the workspace.
fn listed(directory: &Path, below: &Path) -> Result<Vec<PathBuf>, Vec<FileError>> {
    let failed = |error: io::Error| {
        vec![FileError {
            file: below.to_path_buf(),
            reason: error.to_string(),
        }]
    };
    let entries = match fs::read_dir(directory.join(below)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(failed(error)),
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(failed)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || !name.ends_with(".yaml") {
            continue;
        }
        let is_file = fs::metadata(entry.path()).map_err(failed)?.is_file();
        if is_file {
            files.push(below.join(name.as_ref()));
        }
    }
    files.sort();
    Ok(files)
}
