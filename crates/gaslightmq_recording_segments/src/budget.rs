//! The memory budgets of the readers: of one stream, and of all streams of a storage together.
//!
//! Nothing waits for memory. A stream that finds no room for one block when it starts is refused,
//! and a consumer that holds all that its stream may hold is told so. Each refusal names its budget.
//!
//! A stream sets the room for one block aside when it starts, so it never stops half way:
//! all streams together count what each holds, and at least what each set aside.
//! A buffer that a stream handed out tells the budgets when its last holder lets go of it.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_recording::BatchBuffer;

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What all streams of a storage hold together: `recordings.all_readers_memory`.
#[derive(Debug, Clone)]
pub(crate) struct ReadersMemory {
    budget: u64,
    held: Arc<Mutex<u64>>,
}

/// Why a budget left no room: the bytes that are held, and the bytes that the budget allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Spent {
    pub(crate) held: u64,
    pub(crate) budget: u64,
}

impl ReadersMemory {
    pub(crate) fn new(budget: u64) -> ReadersMemory {
        ReadersMemory {
            budget,
            held: Arc::default(),
        }
    }

    /// What all streams hold together now.
    #[cfg(test)]
    pub(crate) fn held(&self) -> u64 {
        *locked(&self.held)
    }

    /// The memory of a new stream, which sets `room` aside for one block, and holds at most `budget` itself.
    ///
    /// A stream that the others leave no room for is refused. When nobody holds anything, a stream is never
    /// refused, however large its room is: a budget never makes a recording unreadable.
    pub(crate) fn stream(&self, room: u64, budget: u64) -> Result<StreamMemory, Spent> {
        let mut held = locked(&self.held);
        if *held > 0 && *held + room > self.budget {
            return Err(Spent {
                held: *held,
                budget: self.budget,
            });
        }
        *held += room;
        Ok(StreamMemory {
            counts: Arc::new(Mutex::new(Counts {
                held: 0,
                set_aside: room,
            })),
            all: Arc::clone(&self.held),
            budget,
        })
    }
}

/// What one stream holds, and what it set aside.
#[derive(Debug)]
struct Counts {
    held: u64,
    set_aside: u64,
}

impl Counts {
    /// What the stream counts against all streams together.
    fn counted(&self) -> u64 {
        self.held.max(self.set_aside)
    }
}

/// What one stream holds: `recordings.reader_memory`.
#[derive(Debug)]
pub(crate) struct StreamMemory {
    counts: Arc<Mutex<Counts>>,
    all: Arc<Mutex<u64>>,
    budget: u64,
}

impl StreamMemory {
    /// Whether the stream may hand out a block of `room` bytes more.
    /// It may always hold one block, however large it is.
    pub(crate) fn has_room_for(&self, room: u64) -> Result<(), Spent> {
        let held = locked(&self.counts).held;
        if held > 0 && held + room > self.budget {
            return Err(Spent {
                held,
                budget: self.budget,
            });
        }
        Ok(())
    }

    /// A buffer of `room` bytes that counts against the stream until its last holder lets go of it.
    pub(crate) fn buffer(&self, room: u64) -> BatchBuffer {
        change(&self.counts, &self.all, |counts| counts.held += room);
        let (counts, all) = (Arc::clone(&self.counts), Arc::clone(&self.all));
        let bytes = usize::try_from(room).expect("a block that fits into memory");
        BatchBuffer::with_room(bytes).when_released(move || {
            change(&counts, &all, |counts| counts.held -= room);
        })
    }
}

impl Drop for StreamMemory {
    /// A stream that ends gives back what it set aside. What its consumer still holds stays counted.
    fn drop(&mut self) {
        change(&self.counts, &self.all, |counts| counts.set_aside = 0);
    }
}

/// Changes what a stream counts, and what all streams count with it.
fn change(counts: &Mutex<Counts>, all: &Mutex<u64>, update: impl FnOnce(&mut Counts)) {
    let mut counts = locked(counts);
    let before = counts.counted();
    update(&mut counts);
    let after = counts.counted();
    let mut all = locked(all);
    *all = *all + after - before;
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK: u64 = 4096;
    const ONE_READER: u64 = 4 * BLOCK;
    const ALL_READERS: u64 = 2 * BLOCK;

    #[test]
    fn a_stream_sets_the_room_for_one_block_aside_and_counts_what_it_holds_beyond_it() {
        let readers = ReadersMemory::new(ALL_READERS);
        let stream = readers.stream(BLOCK, ONE_READER).expect("room for one stream");

        assert_eq!(
            readers.held(),
            BLOCK,
            "a stream that holds nothing counts what it set aside"
        );
        let first = stream.buffer(BLOCK);
        assert_eq!(readers.held(), BLOCK, "the first block takes what was set aside");
        let second = stream.buffer(BLOCK);
        assert_eq!(readers.held(), 2 * BLOCK);

        drop((first, second));
        assert_eq!(readers.held(), BLOCK);
        drop(stream);
        assert_eq!(readers.held(), 0, "a stream that ended gives back what it set aside");
    }

    #[test]
    fn a_stream_that_finds_no_room_for_one_block_is_refused() {
        let readers = ReadersMemory::new(ALL_READERS);
        let _first = readers.stream(BLOCK, ONE_READER).expect("room for one stream");
        let _second = readers.stream(BLOCK, ONE_READER).expect("room for two streams");

        let refused = readers.stream(BLOCK, ONE_READER).err();

        assert_eq!(
            refused,
            Some(Spent {
                held: ALL_READERS,
                budget: ALL_READERS
            })
        );
    }

    #[test]
    fn a_consumer_that_holds_what_its_stream_may_hold_is_told_so_and_may_always_hold_one_block() {
        let readers = ReadersMemory::new(ALL_READERS);
        let stream = readers.stream(BLOCK, ONE_READER).expect("room for one stream");
        let larger_than_the_budget = 2 * ONE_READER;

        assert_eq!(stream.has_room_for(larger_than_the_budget), Ok(()));
        let held = stream.buffer(larger_than_the_budget);
        assert_eq!(
            stream.has_room_for(BLOCK),
            Err(Spent {
                held: larger_than_the_budget,
                budget: ONE_READER
            })
        );
        drop(held);
        assert_eq!(stream.has_room_for(BLOCK), Ok(()));
    }

    #[test]
    fn what_a_consumer_still_holds_counts_after_its_stream_ended() {
        let readers = ReadersMemory::new(ALL_READERS);
        let stream = readers.stream(BLOCK, ONE_READER).expect("room for one stream");
        let kept = stream.buffer(2 * BLOCK);

        drop(stream);

        assert_eq!(readers.held(), 2 * BLOCK);
        drop(kept);
        assert_eq!(readers.held(), 0);
    }
}
