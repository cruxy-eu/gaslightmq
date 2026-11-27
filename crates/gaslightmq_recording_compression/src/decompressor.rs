//! The decompressor: the bytes of a block in, the block out, into the buffer of its batch.

use gaslightmq_recording::BatchBuffer;

use crate::codec::{Codec, ZstandardUnpacker};
use crate::error::DecompressError;

/// How the bytes of a block ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockEnd {
    /// Where its writer finished it. With [`Codec::None`], where the buffer was full.
    Finished,
    /// The bytes ran out before the frame ended. The buffer holds every byte up to the last flush whose bytes were
    /// all there, and perhaps a little more.
    ///
    /// A block whose last chunk ends at a flush ends so, and it is whole: the layout closed it after a commit had
    /// already cut its last chunk. A block that was cut short ends so too. The codec cannot tell the two apart, and
    /// the layout can, from the lengths and the checksums of the chunks.
    Unfinished,
}

/// What [`Decompressor::read_block`] unpacked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnpackedBlock {
    /// How the block ended.
    pub end: BlockEnd,
    /// The bytes of the chunks, one after the other, that the block took.
    /// What follows them is the start of the next block.
    pub taken: usize,
}

/// Unpacks blocks with the codec that a header names, each into a [`BatchBuffer`].
///
/// It starts wherever a block starts, so a reader starts at any block that the index names.
/// The buffer is the one place where the bytes of a block are written:
/// Zstandard looks back into it for what a later byte repeats, and keeps no window and no copy of the block.
///
/// It does no IO: it unpacks the bytes that it is handed, on the thread that calls it.
///
/// ```
/// use gaslightmq_recording::BatchBuffer;
/// use gaslightmq_recording_compression::{BlockEnd, Codec, DecompressError, Decompressor};
///
/// # fn main() -> Result<(), DecompressError> {
/// let stored: &[u8] = b"a block that was stored without compression";
/// let mut decompressor = Decompressor::new(Codec::None);
/// let mut buffer = BatchBuffer::with_room(stored.len());
///
/// let unpacked = decompressor.read_block([stored], &mut buffer)?;
///
/// assert_eq!(unpacked.end, BlockEnd::Finished);
/// assert_eq!(buffer.written(), stored);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Decompressor {
    inner: Inner,
}

#[derive(Debug)]
enum Inner {
    None,
    Zstandard { unpacker: ZstandardUnpacker },
}

impl Decompressor {
    /// A decompressor of `codec`, which the header of a block names.
    pub fn new(codec: Codec) -> Decompressor {
        let inner = match codec {
            Codec::None => Inner::None,
            Codec::Zstandard => Inner::Zstandard {
                unpacker: ZstandardUnpacker::new(),
            },
        };
        Decompressor { inner }
    }

    /// The codec that it unpacks with.
    pub const fn codec(&self) -> Codec {
        match &self.inner {
            Inner::None => Codec::None,
            Inner::Zstandard { .. } => Codec::Zstandard,
        }
    }

    /// Unpacks one block from `chunks` into the room of `out`, behind the bytes that `out` already holds.
    /// So several blocks unpack one after the other into one buffer, as the frames of the chunks of a block do.
    ///
    /// `chunks` are the bytes of the chunks of the block, one after the other, without their headers,
    /// and the first byte of the first chunk is the first byte of the block.
    /// The whole block comes in one call: the next call starts a block of its own.
    /// `out` has the room for the whole block, which the layout knows before it unpacks.
    ///
    /// With Zstandard a block ends where its writer [finished](crate::Compressor::finish_block) it,
    /// and with [`Codec::None`] where `out` is full.
    /// The block takes no byte of the chunks behind its end, and [`UnpackedBlock::taken`] says how many it took.
    ///
    /// # Errors
    ///
    /// [`DecompressError`] when the bytes are not what the codec wrote, or when the block is larger than the room
    /// of `out`.
    pub fn read_block<'a>(
        &mut self,
        chunks: impl IntoIterator<Item = &'a [u8]>,
        out: &mut BatchBuffer,
    ) -> Result<UnpackedBlock, DecompressError> {
        match &mut self.inner {
            Inner::None => Ok(read_stored(chunks, out)),
            Inner::Zstandard { unpacker } => read_zstandard(unpacker, chunks, out),
        }
    }
}

/// A block of [`Codec::None`]: the bytes as they are, until `out` is full.
fn read_stored<'a>(chunks: impl IntoIterator<Item = &'a [u8]>, out: &mut BatchBuffer) -> UnpackedBlock {
    let mut taken = 0;
    for chunk in chunks {
        if out.written().len() == out.room() {
            break;
        }
        taken += out.write(chunk);
    }
    let end = if out.written().len() == out.room() {
        BlockEnd::Finished
    } else {
        BlockEnd::Unfinished
    };
    UnpackedBlock { end, taken }
}

/// A block of Zstandard: one frame, unpacked into the room of `out`.
fn read_zstandard<'a>(
    unpacker: &mut ZstandardUnpacker,
    chunks: impl IntoIterator<Item = &'a [u8]>,
    out: &mut BatchBuffer,
) -> Result<UnpackedBlock, DecompressError> {
    let not_unpacked = |reason| DecompressError { reason };
    unpacker.start().map_err(not_unpacked)?;
    let room = out.unwritten();
    let mut written = 0;
    let mut taken = 0;
    let mut end = BlockEnd::Unfinished;
    'chunks: for chunk in chunks {
        let mut rest = chunk;
        while !rest.is_empty() {
            let before = written;
            let step = unpacker.unpack(rest, room, &mut written).map_err(not_unpacked)?;
            rest = &rest[step.taken..];
            taken += step.taken;
            if step.finished {
                end = BlockEnd::Finished;
                break 'chunks;
            }
            if step.taken == 0 && written == before {
                return Err(not_unpacked(format!(
                    "the block unpacks into more than its room of {} bytes",
                    room.len()
                )));
            }
        }
    }
    out.advance(written);
    Ok(UnpackedBlock { end, taken })
}
