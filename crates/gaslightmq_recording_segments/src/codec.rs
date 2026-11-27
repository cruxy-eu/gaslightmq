//! The one place where the layout meets the codec: it compresses the entries of a block as one stream, and the frames
//! of each chunk by themselves, and unpacks the chunks of a block into the buffer of the block.
//!
//! Nothing else in the crate names a codec. The codec that a block names is a number in its header,
//! and a reader takes the decompressor from that number.
//!
//! The seam has the shape of `recording_compression`, which is synchronous and does no IO: a compressor takes bytes,
//! flushes at a commit and finishes at the end of a block, and a decompressor unpacks every chunk of a block in one
//! call, into the one buffer of the block.

use gaslightmq_recording::BatchBuffer;

/// The codec that a new recording takes, and that the header of a block names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    /// The bytes of a block as they are.
    None,
}

impl Codec {
    /// The number that the header of a block names the codec by.
    pub(crate) const fn id(self) -> u8 {
        match self {
            Codec::None => 0,
        }
    }

    /// The codec that the header of a block names by `id`.
    pub(crate) fn from_id(id: u8) -> Result<Codec, String> {
        match id {
            0 => Ok(Codec::None),
            unknown => Err(format!("this build has no codec with the number {unknown}")),
        }
    }
}

/// Compresses one stream of bytes: the entries of one block, or the frames of one chunk.
#[derive(Debug)]
pub(crate) struct Compressor {
    codec: Codec,
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "the shape of recording_compression, whose compressor can fail where none cannot"
)]
impl Compressor {
    pub(crate) const fn new(codec: Codec) -> Result<Compressor, String> {
        Ok(Compressor { codec })
    }

    /// Takes bytes. What the codec has ready goes behind `out`.
    pub(crate) fn write(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
        match self.codec {
            // The one copy of the bytes on the way in.
            Codec::None => out.extend_from_slice(bytes),
        }
        Ok(())
    }

    /// Lets go of everything it took, so a reader can unpack all of it. The stream goes on: this is a commit.
    pub(crate) fn flush(&mut self, _out: &mut Vec<u8>) -> Result<(), String> {
        match self.codec {
            Codec::None => Ok(()),
        }
    }

    /// Ends the stream: the block, or the frames of a chunk, is whole. The compressor is ready for the next one.
    pub(crate) fn finish_block(&mut self, _out: &mut Vec<u8>) -> Result<(), String> {
        match self.codec {
            Codec::None => Ok(()),
        }
    }
}

/// How the stream of a block ends: finished, or at a flush. A block whose last chunk a commit cut ends at a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "the shape of recording_compression, whose codecs can end a block at a flush"
)]
pub(crate) enum BlockEnd {
    Finished,
    Unfinished,
}

/// What one call unpacked: how its stream ends, and how many bytes of the chunks it took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnpackedBlock {
    pub(crate) end: BlockEnd,
    pub(crate) taken: usize,
}

/// Unpacks the chunks of one block, or the frames of one chunk, with the codec that the header of the block names.
#[derive(Debug)]
pub(crate) struct Decompressor {
    codec: Codec,
}

impl Decompressor {
    /// A decompressor at the start of a block, or of the frames of a chunk.
    pub(crate) const fn new(codec: Codec) -> Decompressor {
        Decompressor { codec }
    }

    /// Unpacks every chunk of one stream behind what `out` holds, in one call: the codec writes the whole stream
    /// into one buffer.
    pub(crate) fn read_block<'a>(
        &mut self,
        chunks: impl IntoIterator<Item = &'a [u8]>,
        out: &mut BatchBuffer,
    ) -> Result<UnpackedBlock, String> {
        let mut taken = 0;
        match self.codec {
            Codec::None => {
                for chunk in chunks {
                    if out.write(chunk) != chunk.len() {
                        return Err("the chunks of a block hold more than its room".to_owned());
                    }
                    taken += chunk.len();
                }
            }
        }
        Ok(UnpackedBlock {
            end: BlockEnd::Finished,
            taken,
        })
    }
}
