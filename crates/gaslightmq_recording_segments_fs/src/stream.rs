//! The bytes of one file of a recording, as a stream that the segment layout pulls.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use gaslightmq_recording_segments::{ByteStream, FetchError};

use crate::map::Map;

/// The bytes of one object of a [`FileBacking`](crate::FileBacking), from an offset on.
///
/// Over a sealed file it hands out ranges of the map of the file, and copies nothing. It tells the system which
/// bytes come next, `recordings.read_ahead` of them, so they are read from the disk before it gets there.
///
/// Over a file that grows it reads into buffers, `recordings.read_size` at least, and hands out slices of them.
/// It sees what later appends add to the file, also once the file is sealed.
#[derive(Debug)]
pub struct FileStream {
    over: Over,
}

#[derive(Debug)]
enum Over {
    Sealed(Sealed),
    Growing(Growing),
}

/// A stream over the map of a sealed file.
#[derive(Debug)]
struct Sealed {
    map: Arc<Map>,
    /// The bytes that are left to hand out.
    left: Bytes,
    /// The offset in the file of the first of `left`.
    at: u64,
    /// The end of what the system was told comes next.
    advised: u64,
    read_ahead: u64,
}

/// A stream over a file that grows.
#[derive(Debug)]
struct Growing {
    file: Arc<File>,
    /// The path that the file had when the stream opened it, for what a failed read says.
    path: PathBuf,
    /// What was read and not handed out yet.
    read: BytesMut,
    /// The offset in the file of the first byte of `read`.
    at: u64,
    read_size: usize,
}

impl FileStream {
    pub(crate) fn mapped(map: Arc<Map>, from: u64, read_ahead: u64) -> FileStream {
        let whole = map.bytes();
        let start = usize::try_from(from).map_or(whole.len(), |from| from.min(whole.len()));
        FileStream {
            over: Over::Sealed(Sealed {
                map,
                left: whole.slice(start..),
                at: start as u64,
                advised: start as u64,
                read_ahead,
            }),
        }
    }

    pub(crate) fn growing(file: File, path: PathBuf, from: u64, read_size: usize) -> FileStream {
        FileStream {
            over: Over::Growing(Growing {
                file: Arc::new(file),
                path,
                read: BytesMut::new(),
                at: from,
                read_size,
            }),
        }
    }
}

impl ByteStream for FileStream {
    async fn next(&mut self, wanted: usize) -> Result<Option<Bytes>, FetchError> {
        match &mut self.over {
            Over::Sealed(sealed) => Ok(sealed.next(wanted)),
            Over::Growing(growing) => growing.next(wanted).await,
        }
    }
}

impl Sealed {
    fn next(&mut self, wanted: usize) -> Option<Bytes> {
        if self.left.is_empty() {
            return None;
        }
        let bytes = self.left.split_to(wanted.min(self.left.len()));
        let end = self.at + bytes.len() as u64;
        self.advise(end);
        self.at = end;
        Some(bytes)
    }

    /// Keeps the system told of the `read_ahead` bytes behind `end`, in steps of half of them, so it tells the
    /// system once for every half window and not once for every chunk.
    fn advise(&mut self, end: u64) {
        if self.read_ahead == 0 || end + self.read_ahead / 2 < self.advised {
            return;
        }
        let from = self.advised.max(self.at);
        let until = end + self.read_ahead;
        self.map.will_need(from, until - from);
        self.advised = until;
    }
}

impl Growing {
    async fn next(&mut self, wanted: usize) -> Result<Option<Bytes>, FetchError> {
        if self.read.len() < wanted {
            self.read_more(wanted).await?;
        }
        if self.read.is_empty() {
            return Ok(None);
        }
        let bytes = self.read.split_to(wanted.min(self.read.len())).freeze();
        self.at += bytes.len() as u64;
        Ok(Some(bytes))
    }

    /// Reads behind what was read until it holds `wanted` bytes, or the file ends.
    /// It asks for `read_size` bytes at least, so the next chunks come from the same read.
    async fn read_more(&mut self, wanted: usize) -> Result<(), FetchError> {
        let file = Arc::clone(&self.file);
        let mut read = std::mem::take(&mut self.read);
        let held = read.len();
        let more = (wanted - held).max(self.read_size);
        let offset = self.at + held as u64;
        let read_of = format!("a read of {}", self.path.display());
        let filled = tokio::task::spawn_blocking(move || {
            read.resize(held + more, 0);
            let filled = read_at(&file, &mut read[held..], offset);
            let length = held + *filled.as_ref().unwrap_or(&0);
            read.truncate(length);
            (read, filled)
        })
        .await;
        let (read, filled) = filled.map_err(|error| FetchError::NotFetched {
            reason: format!("{read_of} stopped before it ended: {error}"),
        })?;
        self.read = read;
        filled.map(|_| ()).map_err(|error| FetchError::NotFetched {
            reason: format!("{read_of}: {error}"),
        })
    }
}

/// Fills `buffer` with the bytes of `file` from `offset` on, and says how many there were: fewer at its end.
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match read_once(file, &mut buffer[filled..], offset + filled as u64) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

#[cfg(unix)]
fn read_once(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
fn read_once(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}
