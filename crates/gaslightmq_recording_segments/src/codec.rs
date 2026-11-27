//! The one place where the layout meets the codec: it compresses the entries of a block as one stream, and the frames
//! of each chunk by themselves, and unpacks a chunk into the buffer of its block.
//!
//! Nothing else in the crate names a codec. The codec that a block names is a number in its header,
//! and a reader takes the decompressor from that number.

use gaslightmq_recording::BatchBuffer;

/// The codec that a new recording takes.
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
}

/// Compresses one stream of bytes: the entries of one block, or the frames of one chunk.
#[derive(Debug)]
pub(crate) struct Compressor {
    codec: Codec,
}

impl Compressor {
    pub(crate) const fn new(codec: Codec) -> Compressor {
        Compressor { codec }
    }

    /// Takes bytes. What the codec has ready goes behind `out`.
    pub(crate) fn write(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        match self.codec {
            // The one copy of the bytes on the way in.
            Codec::None => out.extend_from_slice(bytes),
        }
    }

    /// Lets go of everything it took, so a reader can unpack all of it. The stream goes on.
    pub(crate) fn flush(&mut self, _out: &mut Vec<u8>) {
        match self.codec {
            Codec::None => {}
        }
    }
}

/// Unpacks the chunks of one block, or the frames of one chunk, with the codec that the header of the block names.
#[derive(Debug)]
pub(crate) struct Decompressor {
    codec: Codec,
}

impl Decompressor {
    /// The decompressor of the codec with that number.
    pub(crate) fn of(id: u8) -> Result<Decompressor, String> {
        match id {
            0 => Ok(Decompressor { codec: Codec::None }),
            unknown => Err(format!("this build has no codec with the number {unknown}")),
        }
    }

    /// Unpacks the next chunk behind what `out` holds, and checks that it unpacked to `unpacked` bytes.
    pub(crate) fn read(&mut self, chunk: &[u8], unpacked: usize, out: &mut BatchBuffer) -> Result<(), String> {
        let before = out.written().len();
        match self.codec {
            Codec::None => {
                out.write(chunk);
            }
        }
        let written = out.written().len() - before;
        if written != unpacked {
            return Err(format!(
                "a chunk unpacked to {written} bytes, and its header says {unpacked}"
            ));
        }
        Ok(())
    }
}
