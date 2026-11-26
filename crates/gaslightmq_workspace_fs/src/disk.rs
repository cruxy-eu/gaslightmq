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
    let temporary = write_beside(directory, file, text)?;
    put_in_place(directory, &temporary, file)
}

/// Writes `text` to a temporary file next to `file`, and waits until its bytes are on the disk.
/// Returns the temporary file, which nobody reads as a definition: its name starts with a `.`.
///
/// The directory of the file is made when it is not there. The file itself stays as it is.
///
/// # Errors
///
/// What the system said, when the temporary file could not be written. Nothing of it is left then.
pub(crate) fn write_beside(directory: &Path, file: &Path, text: &str) -> Result<PathBuf, String> {
    let path = directory.join(file);
    let parent = path.parent().unwrap_or(directory);
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let name = path
        .file_name()
        .map_or_else(Default::default, |name| name.to_string_lossy());
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    write_through(&temporary, text).map_err(|error| {
        // What is left of the temporary file is of no use to anyone.
        let _ = fs::remove_file(&temporary);
        error.to_string()
    })?;
    Ok(temporary)
}

/// Renames a temporary file that [`write_beside`] wrote over `file`, in one step, and asks the system to keep the
/// rename through a crash.
///
/// # Errors
///
/// What the system said, when the rename failed. The file is then as it was, and the temporary file is gone.
pub(crate) fn put_in_place(directory: &Path, temporary: &Path, file: &Path) -> Result<(), String> {
    let path = directory.join(file);
    fs::rename(temporary, &path).map_err(|error| {
        let _ = fs::remove_file(temporary);
        error.to_string()
    })?;
    keep_entries_of(path.parent().unwrap_or(directory));
    Ok(())
}

/// Takes away a temporary file that [`write_beside`] wrote, and that nobody puts in place.
pub(crate) fn throw_away(temporary: &Path) {
    // A temporary file that is gone already is as good as one taken away.
    let _ = fs::remove_file(temporary);
}

/// Takes a file away, and asks the system to keep that through a crash.
///
/// # Errors
///
/// What the system said, when the file could not be removed.
pub(crate) fn remove(directory: &Path, file: &Path) -> Result<(), String> {
    let path = directory.join(file);
    fs::remove_file(&path).map_err(|error| error.to_string())?;
    keep_entries_of(path.parent().unwrap_or(directory));
    Ok(())
}

/// Asks the system to put what a directory lists on the disk, so that a rename or a remove in it survives a crash.
///
/// The file is in its place, or gone, before this runs, so a system that refuses changes nothing that a reader
/// sees: the entry reaches the disk with the next sync of the system instead. A system that cannot open a directory
/// as a file, as Windows, is asked nothing.
fn keep_entries_of(directory: &Path) {
    #[cfg(unix)]
    if let Ok(entries) = fs::File::open(directory) {
        let _ = entries.sync_all();
    }
    #[cfg(not(unix))]
    let _ = directory;
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
