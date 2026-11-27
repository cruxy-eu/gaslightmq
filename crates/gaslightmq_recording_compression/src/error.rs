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

/// The compressor failed. Zstandard says why, and nothing that it packed after the last flush is safe.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the block was not compressed: {reason}")]
pub struct CompressError {
    /// What the codec said.
    pub reason: String,
}

/// A block was not unpacked: the bytes are not what the codec wrote, or they unpack into more than the room of the
/// buffer.
///
/// The layout reports the positions of the block as lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the block was not unpacked: {reason}")]
pub struct DecompressError {
    /// What the codec said.
    pub reason: String,
}
