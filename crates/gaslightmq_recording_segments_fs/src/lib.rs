//! The backing on files of the segment layout: one directory for each recording, with its segments, its index
//! chunks and its manifest as files.
//!
//! | Type | Is |
//! |---|---|
//! | [`FileBacking`] | The backing over the directory of every recording, `recordings.directory`. It keeps the [backing contract](gaslightmq_recording_segments::SegmentBacking). |
//! | [`FileStream`] | The bytes of one file, as a stream: ranges of the map of a sealed file, or reads of one that grows. |
//! | [`FileConfiguration`] | How the backing reads its files: `recordings.read_size` and `recordings.read_ahead`. |
//!
//! The backing returns the errors of the contract, and has none of its own: what the system said of a file is the
//! `reason` of one, with the path of the file.
//!
//! # The files
//!
//! ```text
//! recordings/
//! └── orders-sim.2026-11-21T14-02-11.527Z/
//!     ├── manifest
//!     ├── 00000000000000000000.index.open
//!     ├── 00000000000000000000.segment
//!     ├── 00000000000008912345.segment
//!     └── 00000000000017840022.segment.open
//! ```
//!
//! The name of a directory is the ID of its recording. The name of a file is the position of the first entry that
//! its object holds or describes, with 20 digits so that the names sort as the positions do, then its kind, and
//! `.open` while it grows. A file with another name is no object of the recording, and the backing leaves it alone.
//!
//! # A crash
//!
//! What a write returned is on the disk. An append is one write, and then a sync of the data of the file: a crash
//! while it runs leaves the first part of its pieces. A create, a seal and a put of the manifest each happen in one
//! step, which is a call of the system that either happened or did not, followed by a sync of the directory:
//! the create of a file that refuses one that is there, and a rename.
//!
//! # A sealed file is mapped
//!
//! A sealed file never changes again, which is what makes a map of it sound. Mapping it is the one unsafe code of
//! the crate. The backing makes a sealed file read-only, so no process of its owner writes it by mistake.
//!
//! # IO
//!
//! Every touch of a file blocks, so it runs on the blocking pool of the Tokio runtime that awaits the backing.
//! Only the names of the files are plain functions over text.

mod backing;
mod configuration;
mod disk;
mod map;
mod name;
mod stream;

pub use backing::FileBacking;
pub use configuration::FileConfiguration;
pub use stream::FileStream;

/// Every type of this crate.
///
/// Import it next to the preludes of `recording` and `recording_segments`: no name in it is a name of those or of
/// the standard one.
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{FileBacking, FileConfiguration, FileStream};
}
