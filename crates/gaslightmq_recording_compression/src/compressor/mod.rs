//! The compressor: a stream that packs what it is written, one block at a time.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::AsyncWrite;

use crate::codec::Compression;
use crate::config::{CompressionConfig, CompressionPool};

mod background;
use background::BackgroundCompressor;

/// Packs the bytes that it is written, and writes what it packed to the sink `W`.
///
/// It packs one block at a time. [`finish_block`](Self::finish_block) ends a block,
/// so that a reader unpacks it by itself, and the next byte starts the next block.
/// The sink stays open, and every block goes into it, one behind the other.
///
/// | Call | A reader unpacks | The block |
/// |---|---|---|
/// | `write` | What the compressor handed out so far, which may be less than what it was written. | Goes on. |
/// | `flush` | Every byte that was written. | Goes on, and the compressor remembers it. |
/// | [`finish_block`](Self::finish_block) | Every byte that was written. | Ends. |
/// | `close` | Every byte that was written. | Ends, and the sink closes. |
///
/// Zstandard packs on the threads of the [`CompressionPool`], so the task that writes never runs the codec:
/// the compressor gathers what it is written, up to [`CompressionConfig::buffer_size`], and hands it over.
/// While a thread packs, a write waits.
/// [`Compression::None`] writes the bytes to the sink as they are.
///
/// ```
/// use futures::AsyncWriteExt;
/// use gaslightmq_recording_compression::{CompressionConfig, CompressionPool, Compressor};
///
/// # fn main() -> Result<(), gaslightmq_recording_compression::CompressionPoolError> {
/// let config = CompressionConfig::default();
/// let pool = CompressionPool::new(&config)?;
/// let mut compressor = Compressor::new(&config, &pool, Vec::new());
///
/// futures::executor::block_on(async {
///     compressor.write_all(b"the bytes of the first block").await?;
///     compressor.finish_block().await?;
///     compressor.write_all(b"the bytes of the second block").await?;
///     compressor.close().await
/// })
/// .expect("a Vec takes every byte");
///
/// let segment: Vec<u8> = compressor.into_inner();
/// # assert!(!segment.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Compressor<W> {
    inner: Inner<W>,
}

#[derive(Debug)]
enum Inner<W> {
    None { writer: W },
    Zstandard { writer: BackgroundCompressor<W> },
}

impl<W: AsyncWrite + Unpin> Compressor<W> {
    /// A compressor that packs as `config` says, on the threads of `pool`, into `writer`.
    ///
    /// It knows nothing yet: what it writes first is the start of a block.
    pub fn new(config: &CompressionConfig, pool: &CompressionPool, writer: W) -> Compressor<W> {
        let inner = match config.compression {
            Compression::None => Inner::None { writer },
            Compression::Zstandard { level } => Inner::Zstandard {
                writer: BackgroundCompressor::new(level, config.buffer_size, pool, writer),
            },
        };
        Compressor { inner }
    }

    /// The compression that it packs with. Its [`codec`](Compression::codec) is the one that the header of a block names.
    pub fn compression(&self) -> Compression {
        match &self.inner {
            Inner::None { .. } => Compression::None,
            Inner::Zstandard { writer } => Compression::Zstandard { level: writer.level() },
        }
    }

    /// The sink.
    pub fn get_ref(&self) -> &W {
        match &self.inner {
            Inner::None { writer } => writer,
            Inner::Zstandard { writer } => writer.get_ref(),
        }
    }

    /// The sink, to change. What the compressor still holds goes behind what is in it now.
    pub fn get_mut(&mut self) -> &mut W {
        match &mut self.inner {
            Inner::None { writer } => writer,
            Inner::Zstandard { writer } => writer.get_mut(),
        }
    }

    /// The sink, and what the compressor did not hand out yet is gone. Close it first to keep that.
    pub fn into_inner(self) -> W {
        match self.inner {
            Inner::None { writer } => writer,
            Inner::Zstandard { writer } => writer.into_inner(),
        }
    }

    /// Ends the block, as [`finish_block`](Self::finish_block) does.
    ///
    /// # Errors
    ///
    /// What the sink returns, or an error that says that the codec failed.
    pub fn poll_finish_block(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::None { writer } => Pin::new(writer).poll_flush(cx),
            Inner::Zstandard { writer } => writer.poll_finish_block(cx),
        }
    }

    /// Ends the block: its last bytes go to the sink, and the sink is flushed.
    /// A reader unpacks the block by itself, and the next byte that the compressor is written starts a new one.
    /// A block that nothing was written to since the last one ended is no block, and writes nothing.
    ///
    /// # Errors
    ///
    /// What the sink returns, or an error that says that the codec failed.
    pub async fn finish_block(&mut self) -> io::Result<()> {
        futures::future::poll_fn(|cx| Pin::new(&mut *self).poll_finish_block(cx)).await
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Compressor<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            Inner::None { writer } => Pin::new(writer).poll_write(cx, buf),
            Inner::Zstandard { writer } => Pin::new(writer).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::None { writer } => Pin::new(writer).poll_flush(cx),
            Inner::Zstandard { writer } => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::None { writer } => Pin::new(writer).poll_close(cx),
            Inner::Zstandard { writer } => Pin::new(writer).poll_close(cx),
        }
    }
}
