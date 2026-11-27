//! What no recording keeps: the credentials of a client in any recording,
//! and the keys, values and headers of records in a metadata-only one.

use bytes::{Bytes, BytesMut};
use gaslightmq_call::prelude::*;
use gaslightmq_clock::Instant;
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{
    FetchRequest, FetchResponse, ProduceRequest, SaslAuthenticateRequest, SaslAuthenticateResponse,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Frame, Record, RecordBatch, Request, Response};

#[test]
fn no_recording_keeps_the_credentials_of_a_client() -> anyhow::Result<()> {
    // A client authenticates with SASL/PLAIN, and the broker answers with a message of its own.
    let password = b"s3cret-of-alice";
    let token = [b"\0alice\0".as_slice(), password].concat();
    let asked = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(token));
    let api_version = 2;
    let correlation_id = 4;
    let request = Request::new(api_version, correlation_id, Some("order-ingest"), &asked)?;
    let server_says = b"server-final-of-alice";
    let truth =
        request.response(&SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from_static(server_says)))?;
    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ApiKey::SASL_AUTHENTICATE)
        .with(Outcome::Ok);
    let call = Call::new(
        CallId::FIRST,
        fields,
        ResponseSummary::new(Answer::Ok).into(),
        Explanation::none(),
        CallTiming::arrived(Instant::ZERO),
    )?;
    let frames = CallFrames::new(request.frame().clone())
        .with_truth(truth.frame().clone())
        .with_delivered(truth.frame().clone());

    // A full recording keeps everything that a recording may keep.
    let entry = Entry::call(call, frames, RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);

    // Neither the password nor the answer of the server is anywhere in what a storage is handed.
    let kept_frames = encoded.frames().concat();
    for stored in [encoded.entry().as_ref(), kept_frames.as_slice()] {
        assert!(!stored.windows(password.len()).any(|bytes| bytes == password));
        assert!(!stored.windows(server_says.len()).any(|bytes| bytes == server_says));
    }

    // The rest of the call is there: the API, the client and the correlation ID.
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    let batch = Batch::checked(
        EncodingVersion::CURRENT,
        Position::FIRST,
        entries,
        Some(Bytes::from(kept_frames)),
    );
    let Some((_, Ok(EntryView::Call(view)))) = batch.entries().next() else {
        anyhow::bail!("the batch holds the call");
    };
    let asked_again = view.frame(FrameSlot::Request)?;
    assert_eq!(asked_again.state(), FrameState::Redacted);
    assert_eq!(asked_again.length_on_the_wire(), request.frame().payload().len() as u64);
    let Some(kept) = asked_again.frame() else {
        anyhow::bail!("a redacted frame is a frame");
    };
    let kept = Request::decode(kept)?;
    assert_eq!(
        (kept.api(), kept.correlation_id(), kept.client_id()),
        (request.api(), correlation_id, Some("order-ingest"))
    );
    assert!(kept.body::<SaslAuthenticateRequest>()?.auth_bytes.is_empty());

    let answered = view.frame(FrameSlot::Truth)?;
    assert_eq!(answered.state(), FrameState::Redacted);
    let Some(answer) = answered.frame() else {
        anyhow::bail!("a redacted frame is a frame");
    };
    let answer = Response::decode(request.api(), answer)?;
    assert!(answer.body::<SaslAuthenticateResponse>()?.auth_bytes.is_empty());
    assert_eq!(view.frame(FrameSlot::Delivered)?.bytes(), answered.bytes());
    assert_eq!(view.frame(FrameSlot::Upstream)?.state(), FrameState::None);
    Ok(())
}

#[test]
fn a_metadata_only_recording_keeps_a_produce_without_the_keys_values_and_headers_of_its_records() -> anyhow::Result<()>
{
    // A producer writes a card number to `orders.eu/3`, under a key and with a header.
    let card = b"4111 1111 1111 1111";
    let (key, trace) = (b"customer-42", b"trace-of-the-order");
    let offset = 120u64;
    let records = RecordBatch::new(vec![
        Record::new(Bytes::from_static(card))
            .with_key(Bytes::from_static(key))
            .with_header("trace", Bytes::from_static(trace))
            .with_offset(offset),
    ])
    .encode()?;
    let partition = PartitionProduceData::default()
        .with_index(3)
        .with_records(Some(records));
    let topic = TopicProduceData::default()
        .with_name(StrBytes::from_static_str("orders.eu").into())
        .with_partition_data(vec![partition]);
    let body = ProduceRequest::default().with_acks(-1).with_topic_data(vec![topic]);
    let api_version = 9;
    let correlation_id = 1;
    let produce = Request::new(api_version, correlation_id, Some("order-ingest"), &body)?;
    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ApiKey::PRODUCE)
        .with_part("orders.eu/3".parse::<TopicPartition>()?)
        .with(Outcome::Ok);
    let call = Call::new(
        CallId::FIRST,
        fields,
        ResponseSummary::new(Answer::Ok).into(),
        Explanation::none(),
        CallTiming::arrived(Instant::ZERO),
    )?;

    let entry = Entry::call(
        call,
        CallFrames::new(produce.frame().clone()),
        RecordingMode::MetadataOnly,
    )?;
    let encoded = BatchEncoder::new().encode(entry);

    let kept_frames = encoded.frames().concat();
    for secret in [card.as_slice(), key, trace] {
        let on_the_wire = produce.frame().payload();
        assert!(on_the_wire.windows(secret.len()).any(|bytes| bytes == secret));
        assert!(!kept_frames.windows(secret.len()).any(|bytes| bytes == secret));
    }

    // Everything else is there: the acks, the partition, and the record at its offset.
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    let batch = Batch::checked(
        EncodingVersion::CURRENT,
        Position::FIRST,
        entries,
        Some(Bytes::from(kept_frames)),
    );
    let Some((_, Ok(EntryView::Call(view)))) = batch.entries().next() else {
        anyhow::bail!("the batch holds the Produce");
    };
    let produced = view.frame(FrameSlot::Request)?;
    assert_eq!(produced.state(), FrameState::Redacted);
    // The size of the traffic is metadata, and a metadata-only recording keeps it.
    assert_eq!(produced.length_on_the_wire(), produce.frame().payload().len() as u64);
    let Some(kept) = produced.frame() else {
        anyhow::bail!("a redacted frame is a frame");
    };
    let kept: ProduceRequest = Request::decode(kept)?.body()?;
    assert_eq!(kept.acks, -1);
    let partition = &kept.topic_data[0].partition_data[0];
    assert_eq!(partition.index, 3);
    let batches = RecordBatch::decode_all(partition.records.as_ref().expect("the records stay"))?;
    assert_eq!(
        batches[0].records,
        [Record::empty().with_offset(offset).with_timestamp(0)]
    );
    Ok(())
}

#[test]
fn a_metadata_only_recording_keeps_a_fetch_response_without_the_payloads_and_a_full_one_keeps_it_whole()
-> anyhow::Result<()> {
    // A consumer fetches a card number from `orders.eu/3`.
    let card = b"4111 1111 1111 1111";
    let offset = 120u64;
    let records = RecordBatch::new(vec![Record::new(Bytes::from_static(card)).with_offset(offset)]).encode()?;
    let api_version = 12;
    let correlation_id = 2;
    let fetch = Request::new(api_version, correlation_id, Some("analytics"), &FetchRequest::default())?;
    let fetched = PartitionData::default()
        .with_partition_index(3)
        .with_records(Some(records));
    let topic = FetchableTopicResponse::default()
        .with_topic(StrBytes::from_static_str("orders.eu").into())
        .with_partitions(vec![fetched]);
    let truth = fetch.response(&FetchResponse::default().with_responses(vec![topic]))?;
    let fields = Fields::new()
        .with(ConnectionNumber::new(8))
        .with(ApiKey::FETCH)
        .with_part("orders.eu/3".parse::<TopicPartition>()?)
        .with(Outcome::Ok);
    let call = Call::new(
        CallId::FIRST,
        fields,
        ResponseSummary::new(Answer::Ok).into(),
        Explanation::none(),
        CallTiming::arrived(Instant::ZERO),
    )?;
    let frames = CallFrames::new(fetch.frame().clone()).with_truth(truth.frame().clone());

    // A metadata-only recording keeps the request whole, because it holds no record, and the response without the card.
    let entry = Entry::call(call.clone(), frames.clone(), RecordingMode::MetadataOnly)?;
    let encoded = BatchEncoder::new().encode(entry);
    let [request, response] = encoded.frames() else {
        anyhow::bail!("the recording keeps the request and the truth");
    };
    assert_eq!(request, fetch.frame().payload());
    assert!(!response.windows(card.len()).any(|bytes| bytes == card));
    let kept: FetchResponse = Response::decode(fetch.api(), Frame::new(response.clone()))?.body()?;
    let partition = &kept.responses[0].partitions[0];
    assert_eq!(partition.partition_index, 3);
    let batches = RecordBatch::decode_all(partition.records.as_ref().expect("the records stay"))?;
    assert_eq!(
        batches[0].records,
        [Record::empty().with_offset(offset).with_timestamp(0)]
    );

    // A full recording keeps both frames as they were on the wire: the buffers that it was handed.
    let entry = Entry::call(call, frames, RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);
    let [request, response] = encoded.frames() else {
        anyhow::bail!("the recording keeps the request and the truth");
    };
    assert_eq!(request.as_ptr(), fetch.frame().payload().as_ptr());
    assert_eq!(response.as_ptr(), truth.frame().payload().as_ptr());
    Ok(())
}

#[test]
fn a_frame_that_nobody_can_read_is_withheld_and_keeps_its_length() -> anyhow::Result<()> {
    // A request of an API that the codec does not know could carry anything, even in a full recording.
    let unknown_api = 9999i16;
    let api_version = 0i16;
    let correlation_id = 5i32;
    let no_client_id = -1i16;
    let mut payload = BytesMut::new();
    payload.extend_from_slice(&unknown_api.to_be_bytes());
    payload.extend_from_slice(&api_version.to_be_bytes());
    payload.extend_from_slice(&correlation_id.to_be_bytes());
    payload.extend_from_slice(&no_client_id.to_be_bytes());
    payload.extend_from_slice(b"whatever it carries");
    let request = Frame::new(payload.freeze());
    let answer = Frame::new(Bytes::from_static(b"\0\0\0\x05and its answer"));
    let call = Call::new(
        CallId::FIRST,
        Fields::new().with(ConnectionNumber::new(7)).without_truth(),
        Delivered::UnreadResponse,
        Explanation::none(),
        CallTiming::arrived(Instant::ZERO),
    )?;
    let frames = CallFrames::new(request.clone()).with_delivered(answer.clone());

    let entry = Entry::call(call, frames, RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);

    // The storage gets no byte of either frame.
    assert_eq!(encoded.frames(), &[] as &[Bytes]);

    // The call still says that the frames existed, and how long they were.
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, Some(Bytes::new()));
    let Some((_, Ok(EntryView::Call(view)))) = batch.entries().next() else {
        anyhow::bail!("the batch holds the call");
    };
    let asked = view.frame(FrameSlot::Request)?;
    assert_eq!(
        (asked.state(), asked.length_on_the_wire(), asked.bytes()),
        (FrameState::Withheld, request.payload().len() as u64, None)
    );
    let answered = view.frame(FrameSlot::Delivered)?;
    assert_eq!(
        (answered.state(), answered.length_on_the_wire(), answered.frame()),
        (FrameState::Withheld, answer.payload().len() as u64, None)
    );
    assert_eq!(view.frame(FrameSlot::Truth)?.state(), FrameState::None);
    Ok(())
}

#[test]
fn a_frame_that_has_to_change_and_does_not_decode_is_withheld() -> anyhow::Result<()> {
    // A SaslAuthenticate request whose body is cut short: its header reads, and its body does not.
    let token = b"\0alice\0s3cret-of-alice";
    let asked = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(token));
    let api_version = 2;
    let correlation_id = 4;
    let whole = Request::new(api_version, correlation_id, Some("order-ingest"), &asked)?;
    let cut_in_the_token = whole.frame().payload().len() - 4;
    let request = Frame::new(whole.frame().payload().slice(..cut_in_the_token));
    let call = Call::new(
        CallId::FIRST,
        Fields::new().with(ConnectionNumber::new(7)).without_truth(),
        Delivered::ConnectionClosed,
        Explanation::none(),
        CallTiming::arrived(Instant::ZERO),
    )?;

    let entry = Entry::call(call, CallFrames::new(request), RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);

    assert_eq!(encoded.frames(), &[] as &[Bytes]);
    let secret = b"s3cret";
    assert!(!encoded.entry().windows(secret.len()).any(|bytes| bytes == secret));
    Ok(())
}
