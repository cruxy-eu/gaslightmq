//! What every backing of the segment layout does, as one check that the tests of any backing run.
//!
//! The [backing contract](gaslightmq_recording_segments::SegmentBacking) says what the segment layout asks of the
//! place where its bytes live. This crate holds that behaviour once: [`assert_segment_backing`] takes a way to make an
//! empty backing, and fails when the backing does not do what the contract asks.
//! The tests of the backing in memory run it, and so do the tests of the backing on files, so files are held to what
//! memory does.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_recording_segments::prelude::*;
//! use gaslightmq_recording_segments_conformance::assert_segment_backing;
//!
//! // The check is a future, and the test of a backing drives it with the executor it has.
//! block_on(assert_segment_backing(MemoryBacking::new));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate awaits the backing it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

use bytes::Bytes;
use gaslightmq_recording::{Position, RecordingId};
use gaslightmq_recording_segments::prelude::*;

/// Fails when a backing does not do what the [contract](SegmentBacking) asks of every backing.
///
/// `empty` makes a backing that holds no recording, and the check asks for a new one for each behaviour.
/// A backing on files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | A new backing holds no recording. | It lists none, and has no manifest and no object of one. |
/// | A recording that has an object is listed. | In the order of the IDs, whichever order they came in. |
/// | Create makes an empty object, and refuses one that is there. | It lists with length 0, not sealed. [`StoreError::AlreadyThere`] for the second, sealed or not. |
/// | Append puts the pieces behind the bytes, in their order, as one stretch. | A read from 0 gives every byte in order. |
/// | Read gives the bytes from an offset on, as many as asked for. | As one buffer, fewer at the end of the object, and `None` behind it. |
/// | Seal makes an object take no append. | It lists as sealed, and keeps its bytes. |
/// | List gives every object of a kind from a position on. | In the order of their positions, with their lengths. A segment and an index chunk at one position are two objects. |
/// | Put the manifest makes it, or replaces it whole. | A read gives the last one put. |
/// | What is not there is missing. | [`FetchError::Missing`] for an object and for a manifest. |
///
/// What the check cannot see from outside, it does not check: that a write that returned outlives a crash,
/// and that a crash tears nothing but an append.
///
/// # Panics
///
/// When the backing breaks the contract. The message says which behaviour it broke.
pub async fn assert_segment_backing<B: SegmentBacking>(empty: impl Fn() -> B) {
    a_new_backing_holds_no_recording(&empty()).await;
    a_recording_that_has_an_object_is_listed(&empty()).await;
    create_makes_an_empty_object_and_refuses_one_that_is_there(&empty()).await;
    append_puts_the_pieces_behind_the_bytes_in_their_order(&empty()).await;
    read_gives_the_bytes_from_an_offset_on_as_many_as_asked_for(&empty()).await;
    seal_makes_an_object_take_no_append(&empty()).await;
    list_gives_every_object_of_a_kind_from_a_position_on(&empty()).await;
    put_manifest_makes_it_or_replaces_it_whole(&empty()).await;
    what_is_not_there_is_missing(&empty()).await;
}

fn orders_sim() -> RecordingId {
    "orders-sim.2026-11-21T14-02-11.527Z".parse().expect("a recording ID")
}

fn staging_proxy() -> RecordingId {
    "staging-proxy.2026-11-20T08-30-00.000Z"
        .parse()
        .expect("a recording ID")
}

/// Every byte of `object` from `from` on, read as one stream that asks for `wanted` bytes at a time.
async fn read_all<B: SegmentBacking>(
    backing: &B,
    recording: &RecordingId,
    object: Object,
    from: u64,
    wanted: usize,
) -> Vec<u8> {
    let mut stream = backing
        .read(recording, object, from)
        .await
        .expect("a backing reads an object that is there");
    let mut read = Vec::new();
    while let Some(bytes) = stream
        .next(wanted)
        .await
        .expect("a backing hands out the bytes of an object")
    {
        assert!(
            bytes.len() <= wanted,
            "a stream hands out no more than it was asked for"
        );
        read.extend_from_slice(&bytes);
    }
    read
}

async fn a_new_backing_holds_no_recording<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();

    assert_eq!(
        backing.recordings().await,
        Ok(Vec::new()),
        "a new backing lists no recording"
    );
    assert_eq!(
        backing.manifest(&recording).await,
        Err(FetchError::Missing),
        "a new backing has no manifest"
    );
    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, Position::FIRST).await,
        Ok(Vec::new()),
        "a new backing lists no segment"
    );
}

async fn a_recording_that_has_an_object_is_listed<B: SegmentBacking>(backing: &B) {
    let (later, earlier) = (staging_proxy(), orders_sim());

    backing
        .create(&later, Object::segment(Position::FIRST))
        .await
        .expect("a backing creates a segment");
    backing
        .put_manifest(&earlier, Bytes::from_static(b"manifest"))
        .await
        .expect("a backing puts a manifest");

    assert_eq!(
        backing.recordings().await,
        Ok(vec![earlier, later]),
        "a recording with an object or a manifest is listed, in the order of the IDs"
    );
}

async fn create_makes_an_empty_object_and_refuses_one_that_is_there<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let segment = Object::segment(Position::FIRST);

    backing
        .create(&recording, segment)
        .await
        .expect("a backing creates a segment");

    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, Position::FIRST).await,
        Ok(vec![Listed {
            first: Position::FIRST,
            length: 0,
            sealed: false
        }]),
        "a segment that was created is there, empty, and grows"
    );
    assert_eq!(
        backing.create(&recording, segment).await,
        Err(StoreError::AlreadyThere),
        "a backing refuses to create an object that is there"
    );
    backing
        .seal(&recording, segment)
        .await
        .expect("a backing seals a segment");
    assert_eq!(
        backing.create(&recording, segment).await,
        Err(StoreError::AlreadyThere),
        "a backing refuses to create an object that is there and sealed"
    );
}

async fn append_puts_the_pieces_behind_the_bytes_in_their_order<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let segment = Object::segment(Position::FIRST);
    backing
        .create(&recording, segment)
        .await
        .expect("a backing creates a segment");

    let first = [Bytes::from_static(b"header "), Bytes::from_static(b"chunk ")];
    let second = [
        Bytes::from_static(b"chunk "),
        Bytes::new(),
        Bytes::from_static(b"record"),
    ];
    backing.append(&recording, segment, &first).await.expect("an append");
    backing.append(&recording, segment, &second).await.expect("an append");

    let whole = b"header chunk chunk record";
    assert_eq!(
        read_all(backing, &recording, segment, 0, whole.len()).await,
        whole,
        "the pieces of every append stand behind the bytes before them, in their order"
    );
    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, Position::FIRST).await,
        Ok(vec![Listed {
            first: Position::FIRST,
            length: whole.len() as u64,
            sealed: false
        }]),
        "an object is as long as what was appended"
    );
}

async fn read_gives_the_bytes_from_an_offset_on_as_many_as_asked_for<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let chunk = Object::index_chunk(Position::FIRST);
    backing
        .create(&recording, chunk)
        .await
        .expect("a backing creates an index chunk");
    let first = [Bytes::from_static(b"0123456789")];
    let second = [Bytes::from_static(b"abcdef")];
    backing.append(&recording, chunk, &first).await.expect("an append");
    backing.append(&recording, chunk, &second).await.expect("an append");

    let mut stream = backing.read(&recording, chunk, 4).await.expect("a read from an offset");

    assert_eq!(
        stream.next(8).await,
        Ok(Some(Bytes::from_static(b"456789ab"))),
        "a stream hands out as many bytes as it is asked for, as one buffer, across two appends"
    );
    assert_eq!(
        stream.next(8).await,
        Ok(Some(Bytes::from_static(b"cdef"))),
        "fewer at the end of the object"
    );
    assert_eq!(stream.next(8).await, Ok(None), "and nothing behind it");
    assert_eq!(
        read_all(backing, &recording, chunk, 16, 8).await,
        Vec::<u8>::new(),
        "a read from the end of an object hands out nothing"
    );
}

async fn seal_makes_an_object_take_no_append<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let segment = Object::segment(Position::FIRST);
    backing
        .create(&recording, segment)
        .await
        .expect("a backing creates a segment");
    let sealed = [Bytes::from_static(b"sealed")];
    backing.append(&recording, segment, &sealed).await.expect("an append");

    backing
        .seal(&recording, segment)
        .await
        .expect("a backing seals a segment");

    assert!(
        matches!(
            backing
                .append(&recording, segment, &[Bytes::from_static(b"more")])
                .await,
            Err(StoreError::NotStored { .. })
        ),
        "a sealed object takes no append"
    );
    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, Position::FIRST).await,
        Ok(vec![Listed {
            first: Position::FIRST,
            length: sealed[0].len() as u64,
            sealed: true
        }]),
        "a sealed object lists as sealed, with the bytes it had"
    );
    assert_eq!(
        read_all(backing, &recording, segment, 0, 64).await,
        b"sealed",
        "a sealed object keeps its bytes"
    );
}

async fn list_gives_every_object_of_a_kind_from_a_position_on<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let (first, second, third) = (Position::FIRST, Position::new(8_912_345), Position::new(17_840_022));
    for position in [third, first, second] {
        backing
            .create(&recording, Object::segment(position))
            .await
            .expect("a backing creates a segment");
    }
    backing
        .create(&recording, Object::index_chunk(first))
        .await
        .expect("a segment and an index chunk at one position are two objects");
    let grown = [Bytes::from_static(b"grown")];
    backing
        .append(&recording, Object::segment(second), &grown)
        .await
        .expect("an append");

    let listed = |first: Position, length: usize| Listed {
        first,
        length: length as u64,
        sealed: false,
    };
    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, first).await,
        Ok(vec![listed(first, 0), listed(second, grown[0].len()), listed(third, 0)]),
        "every segment, in the order of the positions, with its length"
    );
    assert_eq!(
        backing.list(&recording, ObjectKind::Segment, second).await,
        Ok(vec![listed(second, grown[0].len()), listed(third, 0)]),
        "the segments from a position on"
    );
    assert_eq!(
        backing.list(&recording, ObjectKind::IndexChunk, first).await,
        Ok(vec![listed(first, 0)]),
        "the index chunks, apart from the segments"
    );
    assert_eq!(
        backing.list(&staging_proxy(), ObjectKind::Segment, first).await,
        Ok(Vec::new()),
        "the objects of one recording are not those of another"
    );
}

async fn put_manifest_makes_it_or_replaces_it_whole<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    let first = Bytes::from_static(b"the manifest with no index chunk");
    let second = Bytes::from_static(b"the manifest with one");

    backing
        .put_manifest(&recording, first.clone())
        .await
        .expect("a backing puts a manifest");
    assert_eq!(
        backing.manifest(&recording).await,
        Ok(first),
        "a read gives the manifest that was put"
    );

    backing
        .put_manifest(&recording, second.clone())
        .await
        .expect("a backing replaces a manifest");
    assert_eq!(
        backing.manifest(&recording).await,
        Ok(second),
        "a read gives the manifest that replaced it, whole"
    );
}

async fn what_is_not_there_is_missing<B: SegmentBacking>(backing: &B) {
    let recording = orders_sim();
    backing
        .create(&recording, Object::segment(Position::FIRST))
        .await
        .expect("a backing creates a segment");

    assert!(
        matches!(
            backing.read(&recording, Object::segment(Position::new(1)), 0).await,
            Err(FetchError::Missing)
        ),
        "a segment that was never created is missing"
    );
    assert!(
        matches!(
            backing.read(&recording, Object::index_chunk(Position::FIRST), 0).await,
            Err(FetchError::Missing)
        ),
        "an index chunk is not the segment at its position"
    );
    assert_eq!(
        backing.manifest(&recording).await,
        Err(FetchError::Missing),
        "a recording whose manifest was never put has none"
    );
}
