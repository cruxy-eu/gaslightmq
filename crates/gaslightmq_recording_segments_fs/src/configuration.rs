//! What the backing on files is configured with.
//!
//! A property is a public field, and each one has a default.

/// What a [`FileBacking`](crate::FileBacking) is configured with: how it reads its files.
///
/// Where the files are, `recordings.directory`, is what the backing is made with.
///
/// ```
/// use gaslightmq_recording_segments_fs::prelude::*;
///
/// let mut configuration = FileConfiguration::new();
/// assert_eq!(configuration.read_size, 256 * 1024);
/// assert_eq!(configuration.read_ahead, 8 * 1024 * 1024);
///
/// // A disk that gains nothing from reading ahead.
/// configuration.read_ahead = 0;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileConfiguration {
    /// `recordings.read_size`. Default: 256 KiB.
    ///
    /// The bytes that a stream over a file that grows asks the system for at least, with one read.
    /// It hands out the 48 bytes of a chunk header and the chunk behind it from that one read.
    /// A stream asks for more when it is asked for more.
    pub read_size: usize,
    /// `recordings.read_ahead`. Default: 8 MiB, two blocks of the default size.
    ///
    /// How far ahead of a stream over a sealed file the system is told the bytes come next, so it reads them from
    /// the disk before the stream gets there. `0` tells it nothing.
    pub read_ahead: u64,
}

impl FileConfiguration {
    /// The defaults.
    pub const fn new() -> FileConfiguration {
        FileConfiguration {
            read_size: 256 * 1024,
            read_ahead: 8 * 1024 * 1024,
        }
    }
}

/// The defaults.
impl Default for FileConfiguration {
    fn default() -> Self {
        Self::new()
    }
}
