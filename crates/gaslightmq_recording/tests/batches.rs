//! A batch: the entries that a reader asked for, in the order of the reading, and what a damaged buffer leaves.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_field::ConnectionNumber;
use gaslightmq_recording::prelude::*;

/// The bytes of a head: every entry stands behind one.
const HEAD: usize = 16;

#[test]
fn a_batch_hands_out_the_entries_that_the_reader_asked_for_in_the_order_of_the_reading() -> anyhow::Result<()> {
    // Five sessions closed, a millisecond apart, and their entries are the positions 10 to 14 of a recording.
    let mut batch_encoder = BatchEncoder::new();
    let mut encoded = Vec::new();
    for connection in 0..5 {
        let closed = RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO + Duration::from_millis(connection),
            change: SessionChange::Closed { by: Closer::Client },
        };
        encoded.push(batch_encoder.encode(Entry::session(&closed)));
    }
    let stored: Vec<u8> = encoded.iter().flat_map(|entry| entry.entry().to_vec()).collect();
    let first = Position::new(10);
    let connections = |batch: &Batch| -> Result<Vec<(u64, u64)>, EntryError> {
        let read = batch.entries().map(|(position, entry)| match entry? {
            EntryView::Session(event) => Ok((position.number(), event.connection().value())),
            EntryView::Call(_) | EntryView::Journal(_) => Err(EntryError::Damaged {
                reason: "the entry is no session event".to_owned(),
            }),
        });
        read.collect()
    };

    let mut whole = BatchBuffer::with_room(stored.len());
    whole.write(&stored);
    let batch = Batch::checked(EncodingVersion::CURRENT, first, whole, None);
    assert_eq!(batch.len(), 5);
    assert_eq!(connections(&batch)?, [(10, 0), (11, 1), (12, 2), (13, 3), (14, 4)]);

    // A stream that starts at position 12 gets the block that holds it, and the entries from that one on.
    let from_the_middle = batch.clone().starting_at(Position::new(12));
    assert_eq!(connections(&from_the_middle)?, [(12, 2), (13, 3), (14, 4)]);

    // A stream backwards from position 12 gets the entries from that one back.
    let backwards = batch.clone().ending_at(Position::new(12)).towards(Direction::Backwards);
    assert_eq!(connections(&backwards)?, [(12, 2), (11, 1), (10, 0)]);

    // A position outside the batch leaves the whole batch, or nothing of it.
    assert_eq!(connections(&batch.clone().starting_at(Position::new(3)))?.len(), 5);
    assert_eq!(connections(&batch.clone().starting_at(Position::new(15)))?, []);
    assert_eq!(connections(&batch.ending_at(Position::new(9)))?, []);
    Ok(())
}

#[test]
fn an_entry_that_fails_the_check_is_left_out_and_the_others_are_read() -> anyhow::Result<()> {
    let mut batch_encoder = BatchEncoder::new();
    let mut encoded = Vec::new();
    for connection in 0..3 {
        let closed = RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Proxy },
        };
        encoded.push(batch_encoder.encode(Entry::session(&closed)));
    }
    let mut stored: Vec<u8> = encoded.iter().flat_map(|entry| entry.entry().to_vec()).collect();

    // Someone wrote over the message of the second entry, and left its head alone.
    let second = encoded[0].entry().len();
    let message = second + HEAD..second + encoded[1].entry().len();
    stored[message].fill(0xFF);
    let mut entries = BatchBuffer::with_room(stored.len());
    entries.write(&stored);
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, None);

    let read: Vec<(Position, bool)> = batch
        .entries()
        .map(|(position, entry)| (position, entry.is_ok()))
        .collect();
    assert_eq!(
        read,
        [
            (Position::new(0), true),
            (Position::new(1), false),
            (Position::new(2), true)
        ]
    );
    let Some((_, Err(error))) = batch.entries().nth(1) else {
        anyhow::bail!("the second entry comes as its error");
    };
    assert!(error.to_string().starts_with("the entry is damaged: "), "{error}");
    assert_eq!(
        batch.copy_of(Position::new(1)).err(),
        Some(error),
        "a damaged entry is not copied out"
    );
    Ok(())
}

#[test]
fn a_head_that_fails_ends_the_walk_and_the_batch_holds_the_entries_before_it() {
    let mut batch_encoder = BatchEncoder::new();
    let mut encoded = Vec::new();
    for connection in 0..3 {
        let closed = RecordedSessionEvent {
            connection: ConnectionNumber::new(connection),
            at: Instant::ZERO,
            change: SessionChange::Closed { by: Closer::Lie },
        };
        encoded.push(batch_encoder.encode(Entry::session(&closed)));
    }
    let mut stored: Vec<u8> = encoded.iter().flat_map(|entry| entry.entry().to_vec()).collect();

    // The head of the second entry says a kind that no message has, so nothing says where the third one starts.
    let kind_of_the_second = encoded[0].entry().len() + 4;
    let no_kind = 99;
    stored[kind_of_the_second] = no_kind;
    let mut entries = BatchBuffer::with_room(stored.len());
    entries.write(&stored);
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, None);

    // A layout knows that it wrote three entries, and reports the two that are missing as lost.
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.entries().count(), 1);
}

#[test]
fn a_copy_of_an_entry_keeps_no_other_buffer_in_memory() -> anyhow::Result<()> {
    let closed = RecordedSessionEvent {
        connection: ConnectionNumber::new(7),
        at: Instant::ZERO + Duration::from_secs(90),
        change: SessionChange::Closed { by: Closer::Client },
    };
    let encoded = BatchEncoder::new().encode(Entry::session(&closed));

    // A storage counts what its readers hold: the buffer of a batch tells it when the last holder lets go.
    let released = Arc::new(AtomicBool::new(false));
    let told = Arc::clone(&released);
    let mut entries =
        BatchBuffer::with_room(encoded.entry().len()).when_released(move || told.store(true, Ordering::Release));
    entries.write(encoded.entry());
    let position = Position::new(900);
    let batch = Batch::checked(EncodingVersion::CURRENT, position, entries, Some(Bytes::new()));

    // Whoever keeps the batch, or a clone of it, keeps its buffer.
    let kept = batch.clone();
    let copy = batch.copy_of(position)?;
    drop(batch);
    assert!(!released.load(Ordering::Acquire));

    // The copy is a batch of one in a buffer of its own, so the buffer of the storage goes when the batch goes.
    drop(kept);
    assert!(released.load(Ordering::Acquire));
    let Some((copied, Ok(EntryView::Session(view)))) = copy.entries().next() else {
        anyhow::bail!("the copy holds the entry");
    };
    assert_eq!(copied, position);
    assert_eq!(view.to_session_event()?, closed);
    Ok(())
}
