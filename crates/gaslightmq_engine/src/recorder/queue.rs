//! The queue in front of a recorder: what the engine handed over and the recorder did not write yet, with its bounds.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_call::Call;
use gaslightmq_journal::JournalEntry;
use gaslightmq_pipeline as pipeline;
use gaslightmq_recording::{Ending, RecordedSessionEvent};
use tokio::sync::{Notify, mpsc, oneshot};

/// One thing that waits for the recorder.
#[derive(Debug)]
pub(crate) enum Waiting {
    /// An event of a session, with the instant it happened.
    Session(RecordedSessionEvent),
    /// A call that ended, with its frames as they were on the wire.
    Call(Arc<Call>, pipeline::KeptFrames),
    /// An entry of the journal.
    Journal(JournalEntry),
    /// Someone reads the recording: the recorder commits what it took before, and says so.
    Commit(oneshot::Sender<()>),
    /// The recording ends here, in this way. Nothing comes behind it.
    End(Ending),
}

/// One thing in the queue, with the bytes of frames that it holds.
#[derive(Debug)]
pub(crate) struct Queued {
    pub waiting: Waiting,
    frame_bytes: u64,
}

/// How much may wait for a recorder: `recordings.queue_entries` and `recordings.queue_size`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueBounds {
    pub entries: u64,
    pub frame_bytes: u64,
}

/// The queue of one recording: the engine hands over, the recorder takes, and nobody waits for the other.
///
/// The queue is full when it holds as many entries as its bounds say, or when the frames that wait hold as many bytes.
/// A session never waits for the disk, so an entry that comes to a full queue is not taken, and the recording falls
/// behind: from then on the queue takes the entries of the journal alone, so the journal entry that says why the
/// recording stops still follows the last one before it, and then its end. Whoever takes from the queue learns that it
/// fell behind at once, without taking what waits before it.
#[derive(Debug)]
pub(crate) struct Queue {
    state: Mutex<State>,
    bounds: QueueBounds,
    fell_behind: Notify,
}

#[derive(Debug)]
struct State {
    /// Where the queue hands over, until it ended.
    to_the_recorder: Option<mpsc::UnboundedSender<Queued>>,
    entries: u64,
    frame_bytes: u64,
    falling_behind: bool,
}

impl Queue {
    /// An empty queue with `bounds`, and the end that the recorder takes from.
    pub fn new(bounds: QueueBounds) -> (Arc<Self>, mpsc::UnboundedReceiver<Queued>) {
        let (to_the_recorder, from_the_engine) = mpsc::unbounded_channel();
        let queue = Self {
            state: Mutex::new(State {
                to_the_recorder: Some(to_the_recorder),
                entries: 0,
                frame_bytes: 0,
                falling_behind: false,
            }),
            bounds,
            fell_behind: Notify::new(),
        };
        (Arc::new(queue), from_the_engine)
    }

    /// Hands an entry to the recorder, unless the queue is full or the recording fell behind.
    ///
    /// The entry that finds the queue full is not taken, and the recording falls behind.
    /// An entry of the journal is taken whatever the queue holds once the recording fell behind.
    pub fn offer(&self, waiting: Waiting) {
        let frame_bytes = match &waiting {
            Waiting::Call(_, frames) => frame_bytes(frames),
            _ => 0,
        };
        let mut state = self.state();
        let journal = matches!(waiting, Waiting::Journal(_));
        if state.falling_behind && !journal {
            return;
        }
        let full = state.entries.saturating_add(1) > self.bounds.entries
            || state.frame_bytes.saturating_add(frame_bytes) > self.bounds.frame_bytes;
        if full && !state.falling_behind {
            state.falling_behind = true;
            self.fell_behind.notify_one();
            if !journal {
                return;
            }
        }
        state.entries += 1;
        state.frame_bytes += frame_bytes;
        state.hand_over(Queued { waiting, frame_bytes });
    }

    /// Asks the recorder to commit what it took before, and returns the word that it did.
    /// `None` when the queue ended, and no recorder takes anything any more.
    pub fn commit(&self) -> Option<oneshot::Receiver<()>> {
        let (done, committed) = oneshot::channel();
        let mut state = self.state();
        state.to_the_recorder.as_ref()?;
        state.hand_over(Queued {
            waiting: Waiting::Commit(done),
            frame_bytes: 0,
        });
        Some(committed)
    }

    /// The recording ends here: the recorder takes what waits, and then `ending`. The queue takes nothing more.
    /// Returns whether the queue had not ended before.
    pub fn end(&self, ending: Ending) -> bool {
        let mut state = self.state();
        if state.to_the_recorder.is_none() {
            return false;
        }
        state.hand_over(Queued {
            waiting: Waiting::End(ending),
            frame_bytes: 0,
        });
        state.to_the_recorder = None;
        true
    }

    /// The recorder took `queued` from the queue, which has room for it again.
    pub fn took(&self, queued: &Queued) {
        if matches!(queued.waiting, Waiting::Commit(_) | Waiting::End(_)) {
            return;
        }
        let mut state = self.state();
        state.entries = state.entries.saturating_sub(1);
        state.frame_bytes = state.frame_bytes.saturating_sub(queued.frame_bytes);
    }

    /// Waits until an entry found the queue full. Returns at once when one did before.
    pub async fn fell_behind(&self) {
        if self.state().falling_behind {
            return;
        }
        self.fell_behind.notified().await;
    }

    /// The state. It is whole whenever the lock is free, also after a panic of the thread that held it.
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    fn hand_over(&mut self, queued: Queued) {
        if let Some(to_the_recorder) = &self.to_the_recorder
            && to_the_recorder.send(queued).is_err()
        {
            // The recorder is gone, and takes nothing more.
            self.to_the_recorder = None;
        }
    }
}

/// The bytes of the frames of a call, which wait in memory with it.
fn frame_bytes(frames: &pipeline::KeptFrames) -> u64 {
    let length = |frame: &gaslightmq_wire::Frame| frame.payload().len() as u64;
    length(&frames.request) + frames.truth.as_ref().map_or(0, length) + frames.delivered.as_ref().map_or(0, length)
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Instant;
    use gaslightmq_field::ConnectionNumber;
    use gaslightmq_journal::{Change, ChangeSurface, RecentJournal, RecentJournalSize, RecordingStopCause};
    use gaslightmq_recording::{Closer, SessionChange};

    use super::*;

    fn closed(connection: u64) -> Waiting {
        Waiting::Session(RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Client },
        })
    }

    fn stopped() -> Waiting {
        let mut journal = RecentJournal::new(RecentJournalSize::default());
        let change = Change::RecordingStopped {
            why: RecordingStopCause::FellBehind,
        };
        Waiting::Journal(journal.record(Instant::ZERO, change, ChangeSurface::Test, None))
    }

    fn drain(taken: &mut mpsc::UnboundedReceiver<Queued>) -> Vec<Queued> {
        let mut drained = Vec::new();
        while let Ok(queued) = taken.try_recv() {
            drained.push(queued);
        }
        drained
    }

    const TWO_ENTRIES: QueueBounds = QueueBounds {
        entries: 2,
        frame_bytes: 1024,
    };

    #[tokio::test]
    async fn a_full_queue_takes_no_entry_and_the_recording_falls_behind() {
        let (queue, mut taken) = Queue::new(TWO_ENTRIES);

        for connection in 1..=3 {
            queue.offer(closed(connection));
        }

        queue.fell_behind().await;
        assert_eq!(drain(&mut taken).len(), 2);
    }

    #[tokio::test]
    async fn the_recording_that_fell_behind_still_takes_the_entry_of_the_journal_and_then_its_end() {
        let (queue, mut taken) = Queue::new(TWO_ENTRIES);
        for connection in 1..=3 {
            queue.offer(closed(connection));
        }

        queue.offer(stopped());
        queue.offer(closed(4));
        assert!(queue.end(Ending::FellBehind));

        let taken = drain(&mut taken);
        assert_eq!(taken.len(), 4);
        assert!(matches!(taken[2].waiting, Waiting::Journal(_)));
        assert!(matches!(taken[3].waiting, Waiting::End(Ending::FellBehind)));
    }

    #[tokio::test]
    async fn what_the_recorder_took_makes_room_again() {
        let (queue, mut taken) = Queue::new(TWO_ENTRIES);
        queue.offer(closed(1));
        queue.offer(closed(2));

        for queued in drain(&mut taken) {
            queue.took(&queued);
        }
        queue.offer(closed(3));
        queue.offer(closed(4));

        assert_eq!(drain(&mut taken).len(), 2);
        assert!(!queue.state().falling_behind);
    }

    #[test]
    fn a_queue_that_ended_takes_nothing_more() {
        let (queue, mut taken) = Queue::new(TWO_ENTRIES);

        assert!(queue.end(Ending::Stopped));
        queue.offer(closed(1));

        assert!(!queue.end(Ending::Stopped));
        assert!(queue.commit().is_none());
        assert_eq!(drain(&mut taken).len(), 1, "the end, and nothing behind it");
    }
}
