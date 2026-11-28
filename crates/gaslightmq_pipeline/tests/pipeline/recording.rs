//! While a recording runs, a call hands its frames out with its record, and shares their bytes.

use gaslightmq_kernel::prelude::*;

use crate::driver::*;

#[test]
fn a_call_that_arrives_while_a_recording_runs_hands_out_its_frames() {
    let mut driver = Driver::with([]);
    driver.pipeline.record_frames(true);
    let request = produce(41, &["orders.eu/3"]);

    let (call, forwarded) = driver.send(&request).forwarded();
    let truth = truth(&forwarded);
    let answered = driver.answer_with(call, truth.clone());

    let frames = answered.emitted_frames();
    assert_eq!(frames.request, *request.frame());
    assert_eq!(frames.truth.as_ref(), Some(truth.frame()));
    assert_eq!(frames.delivered, Some(answered.sent(&request).frame().clone()));
}

#[test]
fn a_frame_shares_the_buffer_that_the_call_holds() {
    let mut driver = Driver::with([]);
    driver.pipeline.record_frames(true);
    let request = produce(41, &["orders.eu/3"]);

    let (call, forwarded) = driver.send(&request).forwarded();
    let truth = truth(&forwarded);
    let answered = driver.answer_with(call, truth.clone());

    // The truth went through no lie, so the frame of the recording is the buffer that the backend answered with.
    let frames = answered.emitted_frames();
    let kept = frames.truth.expect("the backend answered");
    assert_eq!(kept.payload().as_ptr(), truth.frame().payload().as_ptr());
}

#[test]
fn a_call_hands_out_no_frame_while_nothing_records() {
    let mut driver = Driver::with([]);
    let request = heartbeat(1);

    let arrived = driver.send(&request).forwarded();
    let answered = driver.answer(&arrived);

    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(answered.frames(), [None]);
}

#[test]
fn a_call_that_arrived_before_the_recording_started_hands_out_no_frame() {
    let mut driver = Driver::with([]);
    let arrived = driver.send(&heartbeat(1)).forwarded();

    driver.pipeline.record_frames(true);
    let answered = driver.answer(&arrived);

    assert_eq!(answered.frames(), [None]);
}

#[test]
fn a_call_that_a_lie_answers_before_forwarding_has_no_truth() {
    let mut driver = Driver::with([on_api("not-leader", ApiKey::PRODUCE, refusal(NOT_LEADER))]);
    driver.pipeline.record_frames(true);
    let request = produce(41, &["orders.eu/3"]);

    let answered = driver.send(&request);

    assert_eq!(answered.kinds(), ["send", "emit"]);
    let frames = answered.emitted_frames();
    assert_eq!(frames.request, *request.frame());
    assert_eq!(frames.truth, None, "the backend never saw the request");
    assert_eq!(frames.delivered, Some(answered.sent(&request).frame().clone()));
}
