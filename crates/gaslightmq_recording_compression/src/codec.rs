//! The codecs themselves: what packs the bytes of a block and unpacks them again, with no IO.
//!
//! [`Compressor`](crate::Compressor) and [`Decompressor`](crate::Decompressor) are the streams around them.

use std::fmt;

use zstd_safe::zstd_sys::ZSTD_EndDirective;
use zstd_safe::{CCtx, CParameter, DCtx, DParameter, InBuffer, OutBuffer, ResetDirective};

use crate::error::{CodecError, ZstandardLevelError};

/// The ways to compress a recording, with what each needs to know: a closed set.
///
/// It is the properties `recordings.compression` and `recordings.compression_level` together,
/// and Zstandard at level 3 until someone sets them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// The bytes as they are.
    None,
    /// Zstandard, which works as hard as its level says.
    Zstandard {
        /// How hard it works: `recordings.compression_level`.
        level: ZstandardLevel,
    },
}

impl Default for Compression {
    fn default() -> Compression {
        Compression::Zstandard {
            level: ZstandardLevel::DEFAULT,
        }
    }
}

impl Compression {
    /// The codec that unpacks what this compression packed. A reader needs no level.
    pub const fn codec(&self) -> Codec {
        match self {
            Compression::None => Codec::None,
            Compression::Zstandard { .. } => Codec::Zstandard,
        }
    }
}

/// A codec of this build: a closed set, which the header of a block names by a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    /// The bytes as they are. Its number is `0`.
    None,
    /// Zstandard. Its number is `1`.
    Zstandard,
}

impl Codec {
    /// The number that the header of a block names the codec by.
    pub const fn id(self) -> u8 {
        match self {
            Codec::None => 0,
            Codec::Zstandard => 1,
        }
    }

    /// The codec that the header of a block names by its number.
    ///
    /// # Errors
    ///
    /// [`CodecError`] when this build has no codec with that number.
    pub const fn from_id(id: u8) -> Result<Codec, CodecError> {
        match id {
            0 => Ok(Codec::None),
            1 => Ok(Codec::Zstandard),
            _ => Err(CodecError { id }),
        }
    }
}

/// How hard Zstandard works: `recordings.compression_level`, which is `3` until someone sets it.
///
/// A higher level packs smaller and takes longer, and a negative one is faster than level 1.
/// A level is one that Zstandard has, and not `0`, which Zstandard reads as its own default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ZstandardLevel(i32);

impl ZstandardLevel {
    /// The level until someone sets one.
    pub const DEFAULT: ZstandardLevel = ZstandardLevel(3);

    /// The level `level`.
    ///
    /// # Errors
    ///
    /// [`ZstandardLevelError`] when Zstandard has no such level, or when it is `0`.
    pub fn new(level: i32) -> Result<ZstandardLevel, ZstandardLevelError> {
        let (fastest, hardest) = (zstd_safe::min_c_level(), zstd_safe::max_c_level());
        if level == 0 || !(fastest..=hardest).contains(&level) {
            return Err(ZstandardLevelError {
                level,
                fastest,
                hardest,
            });
        }
        Ok(ZstandardLevel(level))
    }

    /// The level as a number.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl Default for ZstandardLevel {
    fn default() -> ZstandardLevel {
        ZstandardLevel::DEFAULT
    }
}

/// What a call to [`ZstandardPacker::pack`] ends with, after the bytes it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PackEnd {
    /// Nothing: the packer keeps what it likes, and the block goes on.
    Nothing,
    /// A flush: a reader unpacks every byte that the packer took so far. The block goes on.
    Flush,
    /// The end of the block: its frame is finished, and the next byte starts a frame of its own.
    Block,
}

/// Zstandard as it packs: one frame for each block.
///
/// The frame of a block holds every byte that was packed since the block before it ended,
/// so a reader starts at any block and unpacks it by itself.
pub(crate) struct ZstandardPacker {
    context: CCtx<'static>,
}

impl ZstandardPacker {
    /// A packer at `level`, which knows nothing yet.
    pub(crate) fn new(level: ZstandardLevel) -> Result<ZstandardPacker, String> {
        let mut context = CCtx::create();
        context
            .set_parameter(CParameter::CompressionLevel(level.get()))
            .map_err(reason)?;
        Ok(ZstandardPacker { context })
    }

    /// Packs `bytes`, puts what it has ready behind what `out` holds, and ends as `end` says.
    pub(crate) fn pack(&mut self, bytes: &[u8], out: &mut Vec<u8>, end: PackEnd) -> Result<(), String> {
        let directive = match end {
            PackEnd::Nothing => ZSTD_EndDirective::ZSTD_e_continue,
            PackEnd::Flush => ZSTD_EndDirective::ZSTD_e_flush,
            PackEnd::Block => ZSTD_EndDirective::ZSTD_e_end,
        };
        let mut input = InBuffer::around(bytes);
        loop {
            out.reserve(CCtx::out_size());
            let written = out.len();
            let mut output = OutBuffer::around_pos(out, written);
            let left = self
                .context
                .compress_stream2(&mut output, &mut input, directive)
                .map_err(reason)?;
            let done = match end {
                PackEnd::Nothing => input.pos() == bytes.len(),
                PackEnd::Flush | PackEnd::Block => left == 0,
            };
            if done {
                return Ok(());
            }
        }
    }
}

/// What one call of [`ZstandardUnpacker::unpack`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Unpacked {
    /// The bytes of the input that it took.
    pub(crate) taken: usize,
    /// Whether the frame of the block ended.
    pub(crate) finished: bool,
}

/// Zstandard as it unpacks: one frame, which is one block, into the room of its buffer.
///
/// The room is where the decoder looks back for what a later byte repeats, so it keeps no window of its own,
/// and every byte of the block is written once.
pub(crate) struct ZstandardUnpacker {
    context: DCtx<'static>,
}

impl ZstandardUnpacker {
    /// An unpacker that knows no block yet.
    pub(crate) fn new() -> ZstandardUnpacker {
        ZstandardUnpacker {
            context: DCtx::create(),
        }
    }

    /// Forgets the block before, if any, so that the next byte is the start of a block.
    pub(crate) fn start(&mut self) -> Result<(), String> {
        self.context.reset(ResetDirective::SessionOnly).map_err(reason)?;
        self.context
            .set_parameter(DParameter::StableOutBuffer(true))
            .map_err(reason)?;
        Ok(())
    }

    /// Unpacks what it can of `input` into `room`, behind the first `written` bytes, and moves `written` on.
    ///
    /// Every call of one block hands over the same `room`, and `written` as the call before left it.
    pub(crate) fn unpack(&mut self, input: &[u8], room: &mut [u8], written: &mut usize) -> Result<Unpacked, String> {
        let mut input = InBuffer::around(input);
        let mut output = OutBuffer::around_pos(room, *written);
        let left = self
            .context
            .decompress_stream(&mut output, &mut input)
            .map_err(reason)?;
        *written = output.pos();
        Ok(Unpacked {
            taken: input.pos(),
            finished: left == 0,
        })
    }
}

impl fmt::Debug for ZstandardPacker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstandardPacker").finish_non_exhaustive()
    }
}

impl fmt::Debug for ZstandardUnpacker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstandardUnpacker").finish_non_exhaustive()
    }
}

/// What Zstandard says about a failure, as text.
fn reason(code: usize) -> String {
    zstd_safe::get_error_name(code).to_owned()
}
