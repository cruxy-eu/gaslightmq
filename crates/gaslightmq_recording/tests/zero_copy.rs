//! Nothing is copied twice: a frame goes into a recording as the buffer that the session read,
//! and comes out as a slice of the buffer that its batch was unpacked into.

use bytes::Bytes;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_wire::Request;
use gaslightmq_wire::messages::MetadataRequest;

#[test]
fn a_call_comes_back_through_a_view_and_its_frame_is_a_slice_of_its_batch() -> anyhow::Result<()> {
    // A Metadata call on connection 7, which the backend answered.
    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ClientId::new("order-ingest")?)
        .with(ApiKey::METADATA)
        .with(Outcome::Ok);
    let arrived = Instant::ZERO + Duration::from_millis(250);
    let call = Call::new(
        CallId::new(42)?,
        fields,
        Delivered::Response(ResponseSummary::new(Answer::Ok)),
        Explanation::none(),
        CallTiming::arrived(arrived).delivered(arrived + Duration::from_millis(3))?,
    )?;
    let api_version = 12;
    let correlation_id = 5;
    let request = Request::new(
        api_version,
        correlation_id,
        Some("order-ingest"),
        &MetadataRequest::default(),
    )?;
    let read_from_the_socket = request.into_frame();

    // The recorder makes the entry, and the storage gets the bytes of the entry and the frame.
    let frames = CallFrames::new(read_from_the_socket.clone());
    let entry = Entry::call(call.clone(), frames, RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);

    // The frame that the storage writes is the buffer that the session read, and no copy of it.
    let [kept] = encoded.frames() else {
        anyhow::bail!("the recording keeps the request, which is the one frame of the call");
    };
    assert_eq!(kept.as_ptr(), read_from_the_socket.payload().as_ptr());

    // A storage unpacks the entries of a batch into one buffer, and its frames into another.
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    // This copy stands in for the decompressor: it is the one place where the bytes of a frame are written.
    let written_by_the_decompressor = Bytes::copy_from_slice(kept);
    let first = Position::new(900);
    let batch = Batch::checked(
        EncodingVersion::CURRENT,
        first,
        entries,
        Some(written_by_the_decompressor.clone()),
    );

    // A reader reads the call where it lies.
    let Some((position, entry)) = batch.entries().next() else {
        anyhow::bail!("the batch holds one entry");
    };
    let EntryView::Call(view) = entry? else {
        anyhow::bail!("the entry is a call");
    };
    assert_eq!(position, first);
    assert_eq!(view.id()?, call.id());
    assert_eq!(view.connection(), ConnectionNumber::new(7));
    assert_eq!(view.arrived_at(), arrived);
    let client = view.fields().values().find(|value| value.field() == Field::ClientId);
    assert_eq!(client.and_then(|value| value.text()), Some("order-ingest"));

    // The frame is a slice of the buffer that the frames of the batch were unpacked into.
    let recorded = view.frame(FrameSlot::Request)?;
    assert_eq!(recorded.state(), FrameState::Whole);
    assert_eq!(
        recorded.bytes().map(|bytes| bytes.as_ptr()),
        Some(written_by_the_decompressor.as_ptr())
    );
    assert_eq!(recorded.frame(), Some(read_from_the_socket));
    assert_eq!(view.frame(FrameSlot::Truth)?.state(), FrameState::None);

    // Whoever asks for the type gets it built, and it is the call that went in.
    assert_eq!(view.to_call()?, call);
    Ok(())
}
