//! What a compressor is set to, and the threads that compress.

use std::num::NonZeroUsize;

use futures::executor::ThreadPool;

use crate::codec::Compression;
use crate::error::CompressionPoolError;

/// What a [`Compressor`](crate::Compressor) and the [`CompressionPool`] are set to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionConfig {
    /// The codec that a new recording takes, with its level:
    /// `recordings.compression` and `recordings.compression_level`. Zstandard at level 3 until someone sets them.
    pub compression: Compression,
    /// The bytes that a compressor gathers before it hands them to a thread of the pool:
    /// `recordings.compression_buffer_size`, `256KiB` until someone sets it.
    ///
    /// A thread takes a job for every buffer, and a job that is too small costs more than it compresses.
    /// A flush and the end of a block hand over what was gathered, however little it is.
    pub buffer_size: NonZeroUsize,
    /// The threads of the [`CompressionPool`]: `recordings.compression_threads`, `4` until someone sets it.
    ///
    /// A compressor gives one job at a time to the pool, so this is how many compressors work at once.
    pub threads: NonZeroUsize,
}

impl CompressionConfig {
    /// `recordings.compression_buffer_size` until someone sets it.
    pub const DEFAULT_BUFFER_SIZE: NonZeroUsize = NonZeroUsize::new(256 * 1024).unwrap();
    /// `recordings.compression_threads` until someone sets it.
    pub const DEFAULT_THREADS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
}

impl Default for CompressionConfig {
    fn default() -> CompressionConfig {
        CompressionConfig {
            compression: Compression::default(),
            buffer_size: CompressionConfig::DEFAULT_BUFFER_SIZE,
            threads: CompressionConfig::DEFAULT_THREADS,
        }
    }
}

/// The threads that compress, so that whoever writes a recording never runs the codec itself.
///
/// A process makes one and hands it to every [`Compressor`](crate::Compressor). A clone shares the same threads,
/// and they stop when the last clone and the last compressor are gone.
#[derive(Debug, Clone)]
pub struct CompressionPool {
    threads: ThreadPool,
}

impl CompressionPool {
    /// Starts `config.threads` threads.
    ///
    /// # Errors
    ///
    /// [`CompressionPoolError`] when the system starts no thread.
    pub fn new(config: &CompressionConfig) -> Result<CompressionPool, CompressionPoolError> {
        let threads = ThreadPool::builder()
            .pool_size(config.threads.get())
            .name_prefix("recording-compression-")
            .create()
            .map_err(|error| CompressionPoolError {
                reason: error.to_string(),
            })?;
        Ok(CompressionPool { threads })
    }

    pub(crate) fn threads(&self) -> &ThreadPool {
        &self.threads
    }
}
