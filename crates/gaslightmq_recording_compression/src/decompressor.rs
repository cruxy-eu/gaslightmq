//! The decompressor: a stream of bytes in, one block at a time out, into the buffer of its batch.

use futures::{AsyncBufRead, AsyncBufReadExt};
use gaslightmq_recording::BatchBuffer;

use crate::codec::{Codec, ZstandardUnpacker};
use crate::error::DecompressError;

/// How the bytes of a block ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockEnd {
    /// Where its writer finished it. With [`Codec::None`], where the buffer was full.
    Finished,
    /// Before its writer finished it: the stream had no more bytes.
    /// The buffer holds every byte up to the last flush that reached the stream.
    /// That is the block that was open when a recording was cut short.
    Unfinished,
}

/// Unpacks the blocks of a stream of bytes `R`, one after the other, with the codec that a header names.
///
/// It starts wherever a block starts, so a reader starts at any block that the index names.
/// Each block unpacks into a [`BatchBuffer`], which is the one place where its bytes are written:
/// Zstandard looks back into that buffer for what a later byte repeats, and keeps no copy of the block.
///
/// ```
/// use gaslightmq_recording::BatchBuffer;
/// use gaslightmq_recording_compression::{BlockEnd, Codec, DecompressError, Decompressor};
///
/// # fn main() -> Result<(), DecompressError> {
/// let stored: &[u8] = b"a block that was stored without compression";
/// let mut decompressor = Decompressor::new(Codec::None, stored);
/// let mut buffer = BatchBuffer::with_room(stored.len());
///
/// let end = futures::executor::block_on(decompressor.read_block(&mut buffer))?;
///
/// assert_eq!(end, Some(BlockEnd::Finished));
/// assert_eq!(buffer.written(), stored);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Decompressor<R> {
    inner: Inner<R>,
}

#[derive(Debug)]
enum Inner<R> {
    None { reader: R },
    Zstandard { reader: R, unpacker: ZstandardUnpacker },
}

impl<R: AsyncBufRead + Unpin> Decompressor<R> {
    /// A decompressor that unpacks the stream `reader` with `codec`, which the header of a block names.
    /// The first byte of `reader` is the first byte of a block.
    pub fn new(codec: Codec, reader: R) -> Decompressor<R> {
        let inner = match codec {
            Codec::None => Inner::None { reader },
            Codec::Zstandard => Inner::Zstandard {
                reader,
                unpacker: ZstandardUnpacker::new(),
            },
        };
        Decompressor { inner }
    }

    /// The codec that it unpacks with.
    pub fn codec(&self) -> Codec {
        match &self.inner {
            Inner::None { .. } => Codec::None,
            Inner::Zstandard { .. } => Codec::Zstandard,
        }
    }

    /// The stream.
    pub fn get_ref(&self) -> &R {
        match &self.inner {
            Inner::None { reader } | Inner::Zstandard { reader, .. } => reader,
        }
    }

    /// The stream, from the first byte that the decompressor did not take.
    pub fn into_inner(self) -> R {
        match self.inner {
            Inner::None { reader } | Inner::Zstandard { reader, .. } => reader,
        }
    }

    /// Unpacks the next block of the stream behind what `out` holds, and takes no byte of the block after it.
    ///
    /// `out` has the room for the whole block, which the layout knows before it unpacks.
    /// With Zstandard a block ends where its writer [finished](crate::Compressor::finish_block) it,
    /// and with [`Codec::None`] where `out` is full.
    /// It is `None` when the stream has no more bytes, before a block starts.
    ///
    /// # Errors
    ///
    /// - [`DecompressError::NotUnpacked`] when the bytes are not what the codec wrote,
    ///   or when the block is larger than the room of `out`.
    /// - [`DecompressError::Unreadable`] when the stream fails.
    pub async fn read_block(&mut self, out: &mut BatchBuffer) -> Result<Option<BlockEnd>, DecompressError> {
        match &mut self.inner {
            Inner::None { reader } => read_stored(reader, out).await,
            Inner::Zstandard { reader, unpacker } => read_zstandard(reader, unpacker, out).await,
        }
    }
}

/// A block of [`Codec::None`]: the bytes as they are, until `out` is full.
async fn read_stored<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    out: &mut BatchBuffer,
) -> Result<Option<BlockEnd>, DecompressError> {
    let mut started = false;
    while out.written().len() < out.room() {
        let input = reader.fill_buf().await.map_err(|error| DecompressError::Unreadable {
            reason: error.to_string(),
        })?;
        if input.is_empty() {
            return Ok(started.then_some(BlockEnd::Unfinished));
        }
        started = true;
        let taken = out.write(input);
        reader.consume_unpin(taken);
    }
    Ok(Some(BlockEnd::Finished))
}

/// A block of Zstandard: one frame, unpacked into the room of `out`.
async fn read_zstandard<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    unpacker: &mut ZstandardUnpacker,
    out: &mut BatchBuffer,
) -> Result<Option<BlockEnd>, DecompressError> {
    let not_unpacked = |reason| DecompressError::NotUnpacked { reason };
    unpacker.start().map_err(not_unpacked)?;
    let room = out.unwritten();
    let mut written = 0;
    let mut started = false;
    let end = loop {
        let input = reader.fill_buf().await.map_err(|error| DecompressError::Unreadable {
            reason: error.to_string(),
        })?;
        if input.is_empty() {
            break started.then_some(BlockEnd::Unfinished);
        }
        started = true;
        let before = written;
        let step = unpacker.unpack(input, room, &mut written).map_err(not_unpacked)?;
        reader.consume_unpin(step.taken);
        if step.finished {
            break Some(BlockEnd::Finished);
        }
        if step.taken == 0 && written == before {
            return Err(not_unpacked(format!(
                "the block unpacks into more than its room of {} bytes",
                room.len()
            )));
        }
    };
    out.advance(written);
    Ok(end)
}
