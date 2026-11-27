//! The codecs of a recording: a compressor and a decompressor as streams of bytes, over a closed set of codecs.
//!
//! A codec takes bytes and gives bytes. It knows no entry and no segment:
//! the layout hands it the bytes of a block, and says where a block ends.
//!
//! | Type | Is |
//! |---|---|
//! | [`Compression`], [`ZstandardLevel`] | How a new recording is compressed: none, or Zstandard at a level. |
//! | [`Codec`] | A codec of this build, which the header of a block names by its number. A reader takes it from there. |
//! | [`Compressor`] | A stream that packs what it is written into a sink, one block at a time. |
//! | [`Decompressor`], [`BlockEnd`] | A stream that unpacks blocks into the buffer of their batch, and how a block ended. |
//! | [`CompressionConfig`], [`CompressionPool`] | The properties of a compressor, and the threads that compress. |
//!
//! # Blocks
//!
//! Each block is compressed by itself. With Zstandard a block is a frame of its own:
//!
//! - **A reader starts at any block.** It needs nothing of the block before.
//! - **A torn tail loses one block at most.** The frame of the block that was open is not finished,
//!   and a reader unpacks it up to the last flush that reached the stream: [`BlockEnd::Unfinished`].
//! - **A flush is a commit.** After it a reader unpacks every byte that was written, and the block goes on,
//!   so a small commit still compresses well: the codec remembers the block so far.
//!
//! # Where the work is done
//!
//! The task that writes never runs the codec. A [`Compressor`] gathers what it is written, and a thread of the
//! [`CompressionPool`] packs it. A [`Decompressor`] unpacks on the task that reads: a block of 4 MiB unpacks in about
//! 1.5 ms.
//!
//! # Errors
//!
//! | Error | When |
//! |---|---|
//! | [`CodecError`] | A header names a codec that this build does not have. |
//! | [`ZstandardLevelError`] | A level that Zstandard does not have. |
//! | [`DecompressError`] | A block was not unpacked, or the stream it comes from failed. |
//! | [`CompressionPoolError`] | The threads that compress did not start. |
//!
//! A [`Compressor`] is an [`AsyncWrite`](futures::AsyncWrite), so it fails with what its sink returns,
//! or with an error that says that the codec failed.

mod codec;
mod compressor;
mod config;
mod decompressor;
mod error;

pub use codec::{Codec, Compression, ZstandardLevel};
pub use compressor::Compressor;
pub use config::{CompressionConfig, CompressionPool};
pub use decompressor::{BlockEnd, Decompressor};
pub use error::{CodecError, CompressionPoolError, DecompressError, ZstandardLevelError};
