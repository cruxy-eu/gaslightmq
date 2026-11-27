//! A sealed file, mapped into memory: the one unsafe code of this crate.
//!
//! Its bytes go out as [`Bytes`] ranges of the map, so the compressed bytes of a block go from the cache of the
//! system into the codec without a copy. The map lives as long as one of those ranges does.

use std::fs::File;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::Mmap;

/// The bytes of one sealed file, mapped into memory once.
#[derive(Debug)]
pub(crate) struct Map {
    /// `None` for an empty file, which has nothing to map.
    mapped: Option<Mmap>,
}

impl Map {
    /// Maps the whole of `file`, as long as it is now.
    ///
    /// `file` is a sealed file of a recording: one that [`seal`](crate::disk::seal) renamed to its sealed name.
    ///
    /// # Errors
    ///
    /// What the system said, when it did not map the file.
    pub(crate) fn sealed(file: &File) -> io::Result<Arc<Map>> {
        let length = file.metadata()?.len();
        if length == 0 {
            return Ok(Arc::new(Map { mapped: None }));
        }
        // SAFETY: a map is sound as long as nobody changes or shortens the file underneath it.
        // The backing assumes that nobody but itself writes in the directory of a recording.
        // Under that assumption the file never changes: the backing reaches it by its sealed name alone, which only a
        // seal gives, after the last append and after the file was synced, and nothing of this crate opens a sealed
        // file to write it again. The length is that of this open file, taken just before, so the map covers bytes
        // that are there.
        // The seal also makes the file read-only, which lowers the risk and does not remove it: root, or a process
        // that makes the file writable first, can still write it.
        // When the assumption breaks: a byte that someone changed fails its checksum in the layout, as a damaged disk
        // does. A file that someone shortened ends the process with SIGBUS on the first read of the map past its new
        // end, which is no error that the backing can return. Nothing catches the signal.
        let mapped = unsafe { Mmap::map(file)? };
        Ok(Arc::new(Map { mapped: Some(mapped) }))
    }

    /// Every byte of the file, as one buffer that keeps the map.
    pub(crate) fn bytes(self: &Arc<Map>) -> Bytes {
        Bytes::from_owner(Mapped(Arc::clone(self)))
    }

    /// Tells the system that the `length` bytes from `offset` on come next, so that it reads them from the disk now.
    /// A range outside the map is cut to the map. Advice that the system does not take changes nothing.
    pub(crate) fn will_need(&self, offset: u64, length: u64) {
        let Some(mapped) = &self.mapped else {
            return;
        };
        let (Ok(offset), Ok(length)) = (usize::try_from(offset), usize::try_from(length)) else {
            return;
        };
        let length = length.min(mapped.len().saturating_sub(offset));
        if length == 0 {
            return;
        }
        // Advice is all it is: a system that does not take it reads the bytes when the codec touches them.
        #[cfg(unix)]
        let _ = mapped.advise_range(memmap2::Advice::WillNeed, offset, length);
        #[cfg(not(unix))]
        let _ = (offset, length);
    }

    fn as_slice(&self) -> &[u8] {
        self.mapped.as_deref().unwrap_or_default()
    }
}

/// The owner of the buffer that [`Map::bytes`] hands out.
struct Mapped(Arc<Map>);

impl AsRef<[u8]> for Mapped {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
