//! The errors of the codecs.

/// The header of a block names a codec that this build does not have.
///
/// A reader reads nothing of that block, and the layout reports its positions as lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("this build has no codec with the number {id}")]
pub struct CodecError {
    /// The number that the header names.
    pub id: u8,
}

/// A level that Zstandard does not have.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{level} is no level of Zstandard: a level is from {fastest} to {hardest}, and not 0")]
pub struct ZstandardLevelError {
    /// The level that someone asked for.
    pub level: i32,
    /// The fastest level that Zstandard has.
    pub fastest: i32,
    /// The hardest level that Zstandard has.
    pub hardest: i32,
}

/// Why a [`Decompressor`](crate::Decompressor) read no block.
///
/// Either way the layout reports the positions of the block as lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecompressError {
    /// The bytes are not what the codec wrote, or they unpack into more than the room of the buffer.
    #[error("the block was not unpacked: {reason}")]
    NotUnpacked {
        /// What the codec said.
        reason: String,
    },
    /// The stream of bytes that the block comes from failed.
    #[error("the bytes of the block could not be read: {reason}")]
    Unreadable {
        /// What the stream said.
        reason: String,
    },
}

/// The threads that compress could not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the threads that compress recordings could not start: {reason}")]
pub struct CompressionPoolError {
    /// What the system said.
    pub reason: String,
}
