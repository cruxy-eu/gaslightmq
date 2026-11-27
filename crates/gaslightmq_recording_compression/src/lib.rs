//! The codecs of a recording: a compressor and a decompressor over a closed set of codecs.
//!
//! A codec takes bytes and gives bytes. It knows no entry and no segment, and it does no IO:
//! the layout hands it the bytes of a block, says where a block ends, and keeps what comes out.
//!
//! | Type | Is |
//! |---|---|
//! | [`Compression`], [`ZstandardLevel`] | How a new recording is compressed: none, or Zstandard at a level. |
//! | [`Codec`] | A codec of this build, which the header of a block names by its number. A reader takes it from there. |
//! | [`Compressor`] | Packs what it is handed, one block at a time. |
//! | [`Decompressor`], [`UnpackedBlock`], [`BlockEnd`] | Unpacks a block into the buffer of its batch, and says how it ended and how many bytes it took. |
//!
//! # Blocks
//!
//! Each block is compressed by itself. With Zstandard a block is a frame of its own:
//!
//! - **A reader starts at any block.** It needs nothing of the block before.
//! - **A torn tail loses one block at most.** The frame of the block that was open is not finished,
//!   and a reader unpacks it up to the last flush whose bytes are all there: [`BlockEnd::Unfinished`].
//! - **A flush is a commit.** After it a reader unpacks every byte that was handed over, and the block goes on,
//!   so a small commit still compresses well: the codec remembers the block so far.
//!
//! # Where the work is done
//!
//! On the thread that calls. The recorder writes on a thread of its own, so no session waits for the codec,
//! and a reader on an async runtime hands the unpacking to a blocking task where it does its IO.
//!
//! # Errors
//!
//! | Error | When |
//! |---|---|
//! | [`CodecError`] | A header names a codec that this build does not have. |
//! | [`ZstandardLevelError`] | A level that Zstandard does not have. |
//! | [`CompressError`] | The codec failed to pack. |
//! | [`DecompressError`] | A block was not unpacked. |

mod codec;
mod compressor;
mod decompressor;
mod error;

pub use codec::{Codec, Compression, ZstandardLevel};
pub use compressor::Compressor;
pub use decompressor::{BlockEnd, Decompressor, UnpackedBlock};
pub use error::{CodecError, CompressError, DecompressError, ZstandardLevelError};
