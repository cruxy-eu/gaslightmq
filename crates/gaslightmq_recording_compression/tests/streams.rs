//! Whatever the compressor packs unpacks to the same bytes, however slowly the sink takes them and the stream hands
//! them out.

use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt};
use gaslightmq_recording::BatchBuffer;
use gaslightmq_recording_compression::{
    BlockEnd, Compression, CompressionConfig, CompressionPool, Compressor, Decompressor, ZstandardLevel,
};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;

/// The most bytes that a block of a case holds.
const LARGEST_BLOCK: usize = 64 * 1024;
/// The most blocks that a case writes.
const MOST_BLOCKS: usize = 8;
/// A buffer this small hands many jobs to the pool for every block.
const SMALL_BUFFER: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

/// A sink that is slow in the ways that `pattern` says, one step for each call, over and over.
/// A step that is `None` returns `Pending`, and a step `Some(share)` takes a `share` in 256ths of what it is handed,
/// and at least one byte. A call after `Pending` goes on, as a sink that was waiting for room does.
struct SlowSink {
    bytes: Vec<u8>,
    pattern: Vec<Option<u8>>,
    step: usize,
    waited: bool,
}

impl SlowSink {
    fn next_step(&mut self, cx: &mut Context<'_>) -> Option<u8> {
        let step = self.pattern[self.step % self.pattern.len()];
        self.step += 1;
        if std::mem::take(&mut self.waited) {
            return Some(step.unwrap_or(u8::MAX));
        }
        if step.is_none() {
            self.waited = true;
            cx.waker().wake_by_ref();
        }
        step
    }
}

impl AsyncWrite for SlowSink {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(share) = this.next_step(cx) else {
            return Poll::Pending;
        };
        let taken = (buf.len() * usize::from(share) / 256).max(1).min(buf.len());
        this.bytes.extend_from_slice(&buf[..taken]);
        Poll::Ready(Ok(taken))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().next_step(cx) {
            Some(_) => Poll::Ready(Ok(())),
            None => Poll::Pending,
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

/// A stream that hands out its bytes in pieces of `piece` bytes at most, and is `Pending` before every piece.
struct SlowStream<'a> {
    bytes: &'a [u8],
    piece: usize,
    pending: bool,
}

impl AsyncRead for SlowStream<'_> {
    fn poll_read(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let available = this.bytes.len().min(this.piece).min(buf.len());
        buf[..available].copy_from_slice(&this.bytes[..available]);
        this.bytes = &this.bytes[available..];
        Poll::Ready(Ok(available))
    }
}

impl AsyncBufRead for SlowStream<'_> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        this.pending = !this.pending;
        if this.pending {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let available = this.bytes.len().min(this.piece);
        Poll::Ready(Ok(&this.bytes[..available]))
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.bytes = &this.bytes[amount..];
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn every_block_unpacks_to_what_was_written(
        blocks in vec(vec(any::<u8>(), 0..LARGEST_BLOCK), 1..MOST_BLOCKS),
        flushes in vec(any::<Index>(), MOST_BLOCKS),
        pattern in vec(proptest::option::of(any::<u8>()), 1..16),
        piece in 1..4096_usize,
        level in prop_oneof![Just(-5), Just(1), Just(3), Just(9)],
    ) {
        let config = CompressionConfig {
            compression: Compression::Zstandard { level: ZstandardLevel::new(level)? },
            buffer_size: SMALL_BUFFER,
            ..CompressionConfig::default()
        };
        let pool = CompressionPool::new(&config)?;
        let sink = SlowSink { bytes: Vec::new(), pattern, step: 0, waited: false };
        let mut compressor = Compressor::new(&config, &pool, sink);

        block_on(async {
            for (block, flush) in blocks.iter().zip(&flushes) {
                let (before, after) = block.split_at(flush.index(block.len() + 1));
                compressor.write_all(before).await?;
                compressor.flush().await?;
                compressor.write_all(after).await?;
                compressor.finish_block().await?;
            }
            compressor.close().await
        })?;
        let segment = compressor.into_inner().bytes;

        let stream = SlowStream { bytes: &segment, piece, pending: false };
        let mut decompressor = Decompressor::new(config.compression.codec(), stream);
        for block in blocks.iter().filter(|block| !block.is_empty()) {
            let mut buffer = BatchBuffer::with_room(block.len());
            prop_assert_eq!(block_on(decompressor.read_block(&mut buffer))?, Some(BlockEnd::Finished));
            prop_assert_eq!(buffer.written(), block.as_slice());
        }
        prop_assert_eq!(block_on(decompressor.read_block(&mut BatchBuffer::with_room(LARGEST_BLOCK)))?, None);
    }

    #[test]
    fn none_hands_the_bytes_to_the_sink_as_they_are(
        bytes in vec(any::<u8>(), 0..LARGEST_BLOCK),
        pattern in vec(proptest::option::of(any::<u8>()), 1..16),
        piece in 1..4096_usize,
    ) {
        let config = CompressionConfig { compression: Compression::None, ..CompressionConfig::default() };
        let pool = CompressionPool::new(&config)?;
        let sink = SlowSink { bytes: Vec::new(), pattern, step: 0, waited: false };
        let mut compressor = Compressor::new(&config, &pool, sink);

        block_on(async {
            compressor.write_all(&bytes).await?;
            compressor.close().await
        })?;
        let segment = compressor.into_inner().bytes;
        prop_assert_eq!(&segment, &bytes);

        let stream = SlowStream { bytes: &segment, piece, pending: false };
        let mut decompressor = Decompressor::new(config.compression.codec(), stream);
        let mut buffer = BatchBuffer::with_room(bytes.len());
        prop_assert_eq!(block_on(decompressor.read_block(&mut buffer))?, Some(BlockEnd::Finished));
        prop_assert_eq!(buffer.written(), bytes.as_slice());
    }
}
