//! What the segment layout is configured with.
//!
//! A property is a public field, and each one has a default.

/// What a [`SegmentStorage`](crate::SegmentStorage) is configured with: the properties `recordings.*` of the
/// segment layout.
///
/// A writer closes each level at its size. A reader needs two of them, its budgets:
/// a recording says its codec and its versions itself, and its objects say their sizes.
///
/// ```
/// use gaslightmq_recording_segments::prelude::*;
///
/// let mut configuration = SegmentConfiguration::new();
/// assert_eq!(configuration.block_size, 4 * 1024 * 1024);
/// assert_eq!(configuration.segment_size, 1024 * 1024 * 1024);
///
/// // A test that wants a small recording to be many segments.
/// configuration.segment_size = 64 * 1024;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SegmentConfiguration {
    /// `recordings.block_size`. Default: 4 MiB.
    ///
    /// The bytes of the entries and frames of one block, before they are compressed: what a reader holds at a time,
    /// and where a stream can start. An entry is never cut in two, so an entry that is larger is a block of its own.
    pub block_size: u64,
    /// `recordings.span_size`. Default: 64 MiB.
    ///
    /// The bytes of the entries and frames of the blocks that one summary covers, before they are compressed.
    /// A block is never cut in two.
    pub span_size: u64,
    /// `recordings.segment_size`. Default: 1 GiB.
    ///
    /// The size at which a data segment is sealed, as it is stored. A block is never cut in two.
    pub segment_size: u64,
    /// `recordings.index_size`. Default: 256 MiB.
    ///
    /// The size at which an index chunk is full: the next item would take it over this size.
    pub index_size: u64,
    /// `recordings.summary_values`. Default: 256.
    ///
    /// The values of one field that a summary lists. A field with more says that it can hold any value.
    pub summary_values: usize,
    /// `recordings.reader_memory`. Default: 16 MiB.
    ///
    /// The bytes of unpacked blocks that one reader holds at most: the batches that it handed out and that someone
    /// still holds. A reader may always hold one block, however large it is.
    pub reader_memory: u64,
    /// `recordings.all_readers_memory`. Default: 256 MiB.
    ///
    /// The bytes of unpacked blocks that all readers of the storage hold together at most.
    /// A stream that finds no room for one block, of [`block_size`](Self::block_size), is refused.
    pub all_readers_memory: u64,
}

impl SegmentConfiguration {
    const MIB: u64 = 1024 * 1024;

    /// The defaults.
    pub const fn new() -> SegmentConfiguration {
        SegmentConfiguration {
            block_size: 4 * Self::MIB,
            span_size: 64 * Self::MIB,
            segment_size: 1024 * Self::MIB,
            index_size: 256 * Self::MIB,
            summary_values: 256,
            reader_memory: 16 * Self::MIB,
            all_readers_memory: 256 * Self::MIB,
        }
    }
}

/// The defaults.
impl Default for SegmentConfiguration {
    fn default() -> Self {
        Self::new()
    }
}
