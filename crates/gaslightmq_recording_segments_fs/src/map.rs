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
        // The file is a sealed one: the backing reaches it by its sealed name alone, which only a seal gives, after
        // the last append and after the file was synced. Nothing of this crate opens a sealed file to write it again,
        // and the seal made it read-only, so no process of its owner opens it to write either.
        // The length is that of this open file, taken just before, so the map covers bytes that are there.
        // A process of another owner, or someone who makes the file writable first and then shortens it, breaks the
        // backing as one who deletes its directory does; a read of the map past the new end then stops the process.
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
