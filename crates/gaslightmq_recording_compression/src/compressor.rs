//! The compressor: it packs what it is handed, one block at a time.

use crate::codec::{Compression, PackEnd, ZstandardLevel, ZstandardPacker};
use crate::error::CompressError;

/// Packs the bytes that it is handed, one block at a time, and puts what it packed behind what `out` holds.
///
/// [`finish_block`](Self::finish_block) ends a block, so that a reader unpacks it by itself,
/// and the next byte starts the next block. The caller keeps the bytes that come out, and says where a block starts.
///
/// | Call | A reader unpacks, from what came out so far | The block |
/// |---|---|---|
/// | [`write`](Self::write) | What the compressor let go of, which may be less than it was handed. | Goes on. |
/// | [`flush`](Self::flush) | Every byte that it was handed. It is a commit. | Goes on, and the compressor remembers it. |
/// | [`finish_block`](Self::finish_block) | Every byte that it was handed. | Ends. |
///
/// It does no IO and starts no thread: it packs on the thread that calls it.
///
/// ```
/// use gaslightmq_recording_compression::{CompressError, Compression, Compressor};
///
/// # fn main() -> Result<(), CompressError> {
/// let mut compressor = Compressor::new(Compression::default())?;
/// let mut segment = Vec::new();
///
/// compressor.write(b"the bytes of the first block", &mut segment)?;
/// compressor.finish_block(&mut segment)?;
/// let second_block_starts = segment.len();
/// compressor.write(b"the bytes of the second block", &mut segment)?;
/// compressor.finish_block(&mut segment)?;
/// # assert!(segment.len() > second_block_starts);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Compressor {
    inner: Inner,
    /// Bytes were handed over since the last flush, and a flush has to let go of them.
    unflushed: bool,
    /// Bytes were handed over since the last block ended, so a block is open.
    open_block: bool,
}

#[derive(Debug)]
enum Inner {
    None,
    Zstandard {
        level: ZstandardLevel,
        packer: ZstandardPacker,
    },
}

impl Compressor {
    /// A compressor that packs as `compression` says. It knows nothing yet: what it is handed first starts a block.
    ///
    /// # Errors
    ///
    /// [`CompressError`] when the codec does not start.
    pub fn new(compression: Compression) -> Result<Compressor, CompressError> {
        let inner = match compression {
            Compression::None => Inner::None,
            Compression::Zstandard { level } => Inner::Zstandard {
                level,
                packer: ZstandardPacker::new(level).map_err(|reason| CompressError { reason })?,
            },
        };
        Ok(Compressor {
            inner,
            unflushed: false,
            open_block: false,
        })
    }

    /// The compression that it packs with. Its [`codec`](Compression::codec) is the one that the header of a block names.
    pub const fn compression(&self) -> Compression {
        match &self.inner {
            Inner::None => Compression::None,
            Inner::Zstandard { level, .. } => Compression::Zstandard { level: *level },
        }
    }

    /// Takes `bytes` into the block that is open, or starts one, and puts what it has ready behind what `out` holds.
    ///
    /// # Errors
    ///
    /// [`CompressError`] when the codec fails. The block is then lost, and so is the compressor.
    pub fn write(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.unflushed = true;
        self.open_block = true;
        self.pack(bytes, out, PackEnd::Nothing)
    }

    /// Lets go of every byte that it was handed, behind what `out` holds, so that a reader unpacks all of them.
    /// The block goes on, and the compressor remembers it, so a small commit still compresses well.
    /// It writes nothing when nothing was handed over since the last flush.
    ///
    /// # Errors
    ///
    /// [`CompressError`] when the codec fails. The block is then lost, and so is the compressor.
    pub fn flush(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        if !self.unflushed {
            return Ok(());
        }
        self.unflushed = false;
        self.pack(&[], out, PackEnd::Flush)
    }

    /// Ends the block, and puts its last bytes behind what `out` holds.
    /// A reader unpacks the block by itself, and what the compressor is handed next starts a new one.
    /// A block that nothing was handed to is no block, and writes nothing.
    ///
    /// # Errors
    ///
    /// [`CompressError`] when the codec fails. The block is then lost, and so is the compressor.
    pub fn finish_block(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        if !self.open_block {
            return Ok(());
        }
        self.unflushed = false;
        self.open_block = false;
        self.pack(&[], out, PackEnd::Block)
    }

    fn pack(&mut self, bytes: &[u8], out: &mut Vec<u8>, end: PackEnd) -> Result<(), CompressError> {
        match &mut self.inner {
            Inner::None => {
                out.extend_from_slice(bytes);
                Ok(())
            }
            Inner::Zstandard { packer, .. } => packer.pack(bytes, out, end).map_err(|reason| CompressError { reason }),
        }
    }
}
