//! What a frame loses before a recording keeps it.
//!
//! A recording keeps a frame whole only when it knows what the frame holds.
//! The API of a call is in the header of its request, and the request and every response of the call follow from it:
//!
//! - `SaslAuthenticate` carries the credentials of the client, and what the server answers is enough to test
//!   guessed passwords against. The authentication bytes of the request and of the response are blanked,
//!   in every recording.
//! - In a metadata-only recording, a Produce request and a Fetch or `ShareFetch` response carry records.
//!   Their keys, values and headers are taken out, and every other field stays. A control batch stays whole.
//! - A request whose header does not decode, or whose API the codec does not know, could hold either.
//!   Nobody can tell, so every frame of its call is withheld, and so is a frame whose body does not decode
//!   where it has to change.
//!
//! This is the only code of a recording that needs the Kafka codec.

use bytes::Bytes;
use gaslightmq_environment::RecordingMode;
use gaslightmq_wire::messages::{
    ApiKey, FetchResponse, ProduceRequest, SaslAuthenticateRequest, SaslAuthenticateResponse, ShareFetchResponse,
};
use gaslightmq_wire::protocol::{Decodable, Encodable};
use gaslightmq_wire::{Api, Frame, RecordBatches, Request, Response};

use crate::{CallFrames, FrameState};

/// What a recording holds of one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeptFrame {
    pub(crate) state: FrameState,
    /// The bytes that the frame had on the wire, without the four bytes of its length.
    pub(crate) on_the_wire: u64,
    /// The bytes that the recording holds of it.
    pub(crate) bytes: Option<Bytes>,
}

impl KeptFrame {
    const NONE: KeptFrame = KeptFrame {
        state: FrameState::None,
        on_the_wire: 0,
        bytes: None,
    };

    /// The frame as it was on the wire: the buffer that the recorder was handed, and no copy of it.
    fn whole(frame: Frame) -> KeptFrame {
        KeptFrame {
            state: FrameState::Whole,
            on_the_wire: frame.payload().len() as u64,
            bytes: Some(frame.into_payload()),
        }
    }

    fn withheld(frame: &Frame) -> KeptFrame {
        KeptFrame {
            state: FrameState::Withheld,
            on_the_wire: frame.payload().len() as u64,
            bytes: None,
        }
    }

    /// The frame that `rewritten` stands for, or nothing of it when it could not be rewritten.
    fn redacted(frame: &Frame, rewritten: Option<Frame>) -> KeptFrame {
        match rewritten {
            Some(rewritten) => KeptFrame {
                state: FrameState::Redacted,
                on_the_wire: frame.payload().len() as u64,
                bytes: Some(rewritten.into_payload()),
            },
            None => KeptFrame::withheld(frame),
        }
    }
}

/// The frames of a call as a recording in `mode` keeps them, in the order of the slots.
pub(crate) fn frames(frames: CallFrames, mode: RecordingMode) -> [KeptFrame; 4] {
    let [request, upstream, truth, delivered] = frames.frames;
    // The API of a call is in the header of its request. Without it, nobody can tell what a frame of the call holds.
    let decoded = request.as_ref().and_then(|frame| Request::decode(frame.clone()).ok());
    let decoded = decoded.filter(|request| request.api().known().is_some());
    let Some(decoded) = decoded else {
        return [request, upstream, truth, delivered]
            .map(|frame| frame.map_or(KeptFrame::NONE, |frame| KeptFrame::withheld(&frame)));
    };
    let api = decoded.api();
    let response = |frame: Option<Frame>| frame.map_or(KeptFrame::NONE, |frame| redact_response(api, frame, mode));
    [
        request.map_or(KeptFrame::NONE, |frame| redact_request(frame, &decoded, mode)),
        response(upstream),
        response(truth),
        response(delivered),
    ]
}

/// What a frame of an API holds that a recording in a mode must not keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Secret {
    Nothing,
    Credentials,
    Records,
}

fn request_secret(api: Api, mode: RecordingMode) -> Secret {
    match api.known() {
        Some(ApiKey::SaslAuthenticate) => Secret::Credentials,
        Some(ApiKey::Produce) if !mode.keeps_records() => Secret::Records,
        _ => Secret::Nothing,
    }
}

fn response_secret(api: Api, mode: RecordingMode) -> Secret {
    match api.known() {
        Some(ApiKey::SaslAuthenticate) => Secret::Credentials,
        Some(ApiKey::Fetch | ApiKey::ShareFetch) if !mode.keeps_records() => Secret::Records,
        _ => Secret::Nothing,
    }
}

fn redact_request(frame: Frame, request: &Request, mode: RecordingMode) -> KeptFrame {
    let rewritten = match request_secret(request.api(), mode) {
        Secret::Nothing => return KeptFrame::whole(frame),
        Secret::Credentials => rewrite_request(request, |body: &mut SaslAuthenticateRequest| {
            body.auth_bytes = Bytes::new();
            Some(())
        }),
        Secret::Records => rewrite_request(request, |body: &mut ProduceRequest| {
            let mut partitions = body
                .topic_data
                .iter_mut()
                .flat_map(|topic| topic.partition_data.iter_mut());
            partitions.try_for_each(|partition| without_payloads(&mut partition.records))
        }),
    };
    KeptFrame::redacted(&frame, rewritten)
}

fn redact_response(api: Api, frame: Frame, mode: RecordingMode) -> KeptFrame {
    let secret = response_secret(api, mode);
    if secret == Secret::Nothing {
        return KeptFrame::whole(frame);
    }
    let Ok(response) = Response::decode(api, frame.clone()) else {
        return KeptFrame::withheld(&frame);
    };
    let rewritten = match (secret, api.known()) {
        (Secret::Credentials, _) => rewrite_response(&response, |body: &mut SaslAuthenticateResponse| {
            body.auth_bytes = Bytes::new();
            Some(())
        }),
        (Secret::Records, Some(ApiKey::Fetch)) => rewrite_response(&response, |body: &mut FetchResponse| {
            let mut partitions = body.responses.iter_mut().flat_map(|topic| topic.partitions.iter_mut());
            partitions.try_for_each(|partition| without_payloads(&mut partition.records))
        }),
        (Secret::Records, _) => rewrite_response(&response, |body: &mut ShareFetchResponse| {
            let mut partitions = body.responses.iter_mut().flat_map(|topic| topic.partitions.iter_mut());
            partitions.try_for_each(|partition| without_payloads(&mut partition.records))
        }),
        (Secret::Nothing, _) => return KeptFrame::whole(frame),
    };
    KeptFrame::redacted(&frame, rewritten)
}

/// The records of one partition without their keys, values and headers, or `None` when they do not decode.
fn without_payloads(records: &mut Option<Bytes>) -> Option<()> {
    if let Some(bytes) = records.as_mut() {
        *bytes = RecordBatches::decode(bytes).ok()?.without_payloads().encode().ok()?;
    }
    Some(())
}

/// The request with its body changed by `change`, or `None` when the body does not decode or encode again.
fn rewrite_request<B>(request: &Request, change: impl FnOnce(&mut B) -> Option<()>) -> Option<Frame>
where
    B: gaslightmq_wire::protocol::Request,
{
    let mut body: B = request.body().ok()?;
    change(&mut body)?;
    let rewritten = Request::new(
        request.api().version,
        request.correlation_id(),
        request.client_id(),
        &body,
    )
    .ok()?;
    Some(rewritten.into_frame())
}

/// The response with its body changed by `change`, or `None` when the body does not decode or encode again.
fn rewrite_response<B>(response: &Response, change: impl FnOnce(&mut B) -> Option<()>) -> Option<Frame>
where
    B: Decodable + Encodable,
{
    let mut body: B = response.body().ok()?;
    change(&mut body)?;
    let rewritten = Response::new(response.api(), response.correlation_id(), &body).ok()?;
    Some(rewritten.into_frame())
}
