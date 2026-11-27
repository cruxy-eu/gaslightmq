//! The segment layout: a storage of recordings over any backing.
//!
//! The layout keeps a recording as three kinds of object, and each kind describes the one below it.
//!
//! ```text
//! manifest                      one, small, replaced as a whole
//!   └─ index chunk              up to `recordings.index_size` each
//!        └─ data segment        up to `recordings.segment_size` each, as it is stored
//!             └─ span           up to `recordings.span_size` of entries and frames: what one summary covers
//!                  └─ block     up to `recordings.block_size` of entries and frames: what a reader holds
//!                       └─ chunk  what one commit wrote of a block, compressed
//! ```
//!
//! It keeps the entries that [`gaslightmq_recording`] wrote, and the version of the encoding that they have,
//! and never looks into one. What it says itself is fixed binary: headers, and records that carry the summaries of
//! a span of blocks, of a segment and of an index chunk. It has no message and no stored type of its own.
//!
//! | Type | Is |
//! |---|---|
//! | [`SegmentStorage`], [`SegmentWriter`], [`SegmentReader`], [`SegmentEntries`] | The layout, as the storage contract of `recording` asks for it. |
//! | [`SegmentConfiguration`] | The properties of the layout: the size of each level, and the budgets of the readers. |
//! | [`SegmentBacking`], [`ByteStream`] | The backing contract: the place where the objects of the layout live, which hands out their bytes as streams. |
//! | [`Object`], [`ObjectKind`], [`Listed`] | The names of the objects that grow, and what a backing lists of them. |
//! | [`StoreError`], [`ListingError`], [`FetchError`] | The errors of the backing contract, which the layout turns into those of a recording. |
//! | [`MemoryBacking`], [`MemoryStream`] | The backing in memory, behind the feature `memory`. |
//!
//! # Writing
//!
//! An object is written before the object that describes it, so whatever an index chunk or the manifest names is
//! there, whole and for good. Every step leaves a recording: the backing gives each write what it promises, so no
//! step leaves the manifest half replaced, and what a crash leaves half appended fails a checksum.
//!
//! # Opening
//!
//! A reader works from the top down, and trusts each level only as far as it reaches: the manifest, the index
//! chunks behind it, and the segments that no index chunk describes, which it walks block by block and chunk by
//! chunk. A recording that was cut short opens like any other, and stops at its last whole chunk.
//! A missing manifest or index chunk is rebuilt from the segments.
//!
//! # Memory
//!
//! A stream holds the blocks that it handed out and that someone still holds, up to `recordings.reader_memory`,
//! and all streams of a storage hold up to `recordings.all_readers_memory` together.
//! Nothing waits for memory: a stream that finds no room is refused, and a consumer that holds all its stream may
//! hold is told so.
//!
//! # No IO
//!
//! The rules of the layout are plain functions over bytes: the framing, the order of writing, the walks that
//! recover a recording, and the budgets. The one part that is async hands bytes to the backing and takes bytes from
//! it: the crate awaits the backing it is handed, and nothing else.

mod backing;
mod budget;
mod configuration;
mod framing;
#[cfg(feature = "memory")]
mod memory;
mod storage;
mod walk;
mod writing;

pub use backing::{ByteStream, FetchError, Listed, ListingError, Object, ObjectKind, SegmentBacking, StoreError};
pub use configuration::SegmentConfiguration;
#[cfg(feature = "memory")]
pub use memory::{MemoryBacking, MemoryStream};
pub use storage::{SegmentEntries, SegmentReader, SegmentStorage, SegmentWriter};

/// Every type of this crate, with its errors.
///
/// Import it next to the prelude of `recording`: no name in it is a name of that prelude or of the standard one.
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ByteStream, FetchError, Listed, ListingError, Object, ObjectKind, SegmentBacking, SegmentConfiguration,
        SegmentEntries, SegmentReader, SegmentStorage, SegmentWriter, StoreError,
    };
    #[cfg(feature = "memory")]
    #[doc(no_inline)]
    pub use crate::{MemoryBacking, MemoryStream};
}
