//! Zstandard on the threads of the pool, behind a buffer, so that the writer never runs the codec itself.

use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::future::RemoteHandle;
use futures::task::SpawnExt;
use futures::{AsyncWrite, FutureExt};

use crate::codec::{PackEnd, ZstandardLevel, ZstandardPacker};
use crate::config::CompressionPool;

/// What goes to a thread of the pool and comes back: the packer, the bytes it packs and the bytes it made.
#[derive(Debug)]
struct Job {
    packer: ZstandardPacker,
    /// The bytes that wait to be packed. The writer gathers them here, up to the size of the buffer.
    input: Vec<u8>,
    /// The bytes that the packer made, which go to the sink.
    output: Vec<u8>,
}

impl Job {
    /// Packs the bytes that wait, and ends as `end` says. It runs on a thread of the pool.
    fn pack(mut self, end: PackEnd) -> Result<Job, String> {
        self.packer.pack(&self.input, &mut self.output, end)?;
        self.input.clear();
        Ok(self)
    }
}

/// Where the compressor stands.
#[derive(Debug, Default)]
enum State {
    /// It gathers bytes for the next job.
    Ready { job: Job },
    /// A thread of the pool packs.
    Packing { handle: RemoteHandle<Result<Job, String>> },
    /// The bytes that the pool made go to the sink, from `written` on.
    Writing { job: Job, written: usize },
    /// The packer failed, and the compressor packs nothing more.
    Failed { reason: String },
    /// A call stopped in the middle, which only a panic does.
    #[default]
    Interrupted,
}

/// Zstandard as a stream: it gathers what it is handed, and a thread of the pool packs it.
///
/// One job at a time is out. While it is, the writer waits for it before it hands over more.
#[derive(Debug)]
pub(crate) struct BackgroundCompressor<W> {
    state: State,
    sink: W,
    pool: CompressionPool,
    buffer_size: NonZeroUsize,
    level: ZstandardLevel,
    /// Bytes were packed since the last flush, and a flush has to hand them out.
    unflushed: bool,
    /// Bytes were handed over since the last block ended, so the frame of a block is open.
    open_block: bool,
}

impl<W: AsyncWrite + Unpin> BackgroundCompressor<W> {
    pub(crate) fn new(level: ZstandardLevel, buffer_size: NonZeroUsize, pool: &CompressionPool, sink: W) -> Self {
        let state = match ZstandardPacker::new(level) {
            Ok(packer) => State::Ready {
                job: Job {
                    packer,
                    input: Vec::with_capacity(buffer_size.get()),
                    output: Vec::new(),
                },
            },
            Err(reason) => State::Failed { reason },
        };
        BackgroundCompressor {
            state,
            sink,
            pool: pool.clone(),
            buffer_size,
            level,
            unflushed: false,
            open_block: false,
        }
    }

    pub(crate) const fn level(&self) -> ZstandardLevel {
        self.level
    }

    pub(crate) const fn get_ref(&self) -> &W {
        &self.sink
    }

    pub(crate) const fn get_mut(&mut self) -> &mut W {
        &mut self.sink
    }

    pub(crate) fn into_inner(self) -> W {
        self.sink
    }

    /// Moves on until the compressor gathers bytes again: the job that is out comes back, and its bytes reach the sink.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match std::mem::take(&mut self.state) {
                State::Ready { job } => {
                    self.state = State::Ready { job };
                    return Poll::Ready(Ok(()));
                }
                State::Packing { mut handle } => match handle.poll_unpin(cx) {
                    Poll::Pending => {
                        self.state = State::Packing { handle };
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(job)) => self.state = State::Writing { job, written: 0 },
                    Poll::Ready(Err(reason)) => self.state = State::Failed { reason },
                },
                State::Writing { mut job, written } => {
                    let pending = &job.output[written..];
                    if pending.is_empty() {
                        job.output.clear();
                        self.state = State::Ready { job };
                        continue;
                    }
                    let outcome = Pin::new(&mut self.sink).poll_write(cx, pending);
                    match outcome {
                        Poll::Ready(Ok(0)) => {
                            self.state = State::Writing { job, written };
                            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                        }
                        Poll::Ready(Ok(size)) => {
                            self.state = State::Writing {
                                job,
                                written: written + size,
                            };
                        }
                        Poll::Ready(Err(error)) => {
                            self.state = State::Writing { job, written };
                            return Poll::Ready(Err(error));
                        }
                        Poll::Pending => {
                            self.state = State::Writing { job, written };
                            return Poll::Pending;
                        }
                    }
                }
                State::Failed { reason } => {
                    let error = io::Error::other(format!("the compressor failed: {reason}"));
                    self.state = State::Failed { reason };
                    return Poll::Ready(Err(error));
                }
                State::Interrupted => {
                    return Poll::Ready(Err(io::Error::other("the compressor stopped in the middle of a call")));
                }
            }
        }
    }

    /// Hands the bytes that were gathered to a thread of the pool, to pack and end as `end` says.
    /// The compressor gathers bytes, as [`poll_ready`](Self::poll_ready) left it.
    fn start(&mut self, end: PackEnd) {
        let State::Ready { job } = std::mem::take(&mut self.state) else {
            return;
        };
        match end {
            PackEnd::Nothing => {
                self.unflushed = true;
                self.open_block = true;
            }
            PackEnd::Flush => {
                self.unflushed = false;
                self.open_block |= !job.input.is_empty();
            }
            PackEnd::Block => {
                self.unflushed = false;
                self.open_block = false;
            }
        }
        self.state = match self.pool.threads().spawn_with_handle(async move { job.pack(end) }) {
            Ok(handle) => State::Packing { handle },
            Err(error) => State::Failed {
                reason: error.to_string(),
            },
        };
    }

    /// Hands over what was gathered, ends as `end` says, and waits until its bytes reached the sink.
    /// It hands over nothing when there is nothing to end.
    fn poll_end(&mut self, cx: &mut Context<'_>, end: PackEnd) -> Poll<io::Result<()>> {
        loop {
            ready!(self.poll_ready(cx))?;
            let gathered = matches!(&self.state, State::Ready { job } if !job.input.is_empty());
            let needed = match end {
                PackEnd::Nothing => gathered,
                PackEnd::Flush => gathered || self.unflushed,
                PackEnd::Block => gathered || self.open_block,
            };
            if !needed {
                return Poll::Ready(Ok(()));
            }
            self.start(end);
        }
    }

    /// Finishes the frame of the block, hands its last bytes to the sink and flushes the sink.
    /// The next byte starts a block of its own.
    pub(crate) fn poll_finish_block(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_end(cx, PackEnd::Block))?;
        Pin::new(&mut self.sink).poll_flush(cx)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BackgroundCompressor<W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_ready(cx))?;
        let State::Ready { job } = &mut this.state else {
            return Poll::Ready(Err(io::Error::other("the compressor was not ready after it said so")));
        };
        let room = this.buffer_size.get().saturating_sub(job.input.len());
        let taken = room.min(buf.len());
        job.input.extend_from_slice(&buf[..taken]);
        if job.input.len() >= this.buffer_size.get() {
            this.start(PackEnd::Nothing);
        }
        Poll::Ready(Ok(taken))
    }

    /// Hands out every byte that was written so far, so that a reader unpacks all of it, and flushes the sink.
    /// The block goes on.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_end(cx, PackEnd::Flush))?;
        Pin::new(&mut this.sink).poll_flush(cx)
    }

    /// Finishes the block that is open, and closes the sink.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_end(cx, PackEnd::Block))?;
        Pin::new(&mut this.sink).poll_close(cx)
    }
}
