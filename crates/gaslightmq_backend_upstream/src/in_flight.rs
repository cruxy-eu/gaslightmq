//! The requests of one connection that the cluster has and has not answered yet.

use std::collections::VecDeque;

use gaslightmq_field::Acks;
use gaslightmq_wire::{ApiKey, Frame, Request, Response, WireError};

/// Whether the cluster answers a request.
///
/// Only a Produce with `acks=0` has no response.
/// The backend has to know before it sends the request: a response that never comes must not hold up
/// the answers behind it, and a response that does come belongs to this request and to no other.
/// The pipeline reads the same from the request, at the same place, so the two agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reply {
    /// The cluster answers the request.
    Comes,
    /// The request has no response: a Produce with `acks=0`.
    Never,
}

impl Reply {
    /// The first version of Produce that carries a `transactional.id` in front of its `acks`.
    const FIRST_PRODUCE_WITH_TRANSACTIONS: i16 = 3;
    /// The first version of Produce that writes its texts with a compact length.
    const FIRST_FLEXIBLE_PRODUCE: i16 = 9;

    /// Whether the cluster answers `request`.
    ///
    /// The `acks` of a Produce stand at the start of its body: first up to version 2,
    /// and behind the `transactional.id` since version 3.
    /// Nothing else of the body is read, so a Produce whose records are broken still says whether it is answered.
    ///
    /// # Errors
    ///
    /// [`WireError::Truncated`] when the body of a Produce ends before its `acks`.
    /// Nothing says then whether a response comes.
    pub(crate) fn to(request: &Request) -> Result<Self, WireError> {
        let api = request.api();
        if api.known() != Some(ApiKey::Produce) {
            return Ok(Self::Comes);
        }
        let body = request.body_bytes();
        let acks = Self::acks_at_the_start_of(&body, api.version).ok_or(WireError::Truncated {
            what: "Produce request body",
            len: body.len(),
        })?;
        Ok(if acks == Acks::NONE { Self::Never } else { Self::Comes })
    }

    fn acks_at_the_start_of(body: &[u8], version: i16) -> Option<Acks> {
        let mut rest = body;
        if version >= Self::FIRST_FLEXIBLE_PRODUCE {
            // A compact text: its length and one as a varint, and zero for no text.
            let length = usize::try_from(Self::unsigned_varint(&mut rest)?).ok()?;
            rest = rest.get(length.saturating_sub(1)..)?;
        } else if version >= Self::FIRST_PRODUCE_WITH_TRANSACTIONS {
            // A text: its length in two bytes, and a negative length for no text.
            let length = i16::from_be_bytes(*rest.first_chunk()?);
            rest = rest.get(size_of::<i16>() + usize::try_from(length).unwrap_or(0)..)?;
        }
        rest.first_chunk().map(|acks| Acks::new(i16::from_be_bytes(*acks)))
    }

    /// Reads an unsigned varint from the start of `bytes`, and moves `bytes` past it.
    fn unsigned_varint(bytes: &mut &[u8]) -> Option<u32> {
        let mut value = 0_u32;
        for shift in (0..u32::BITS).step_by(7) {
            let (&byte, rest) = bytes.split_first()?;
            *bytes = rest;
            value |= u32::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }
}

/// The requests of one connection that the cluster has and has not answered yet, the oldest first.
///
/// A broker answers the requests of one connection in the order in which it got them.
/// The next frame of the connection therefore answers the oldest request, whatever it holds,
/// and its correlation ID only confirms that.
///
/// `W` is whoever waits for the answer.
/// The queue does not look at it, so the code that owns the socket keeps there what it answers through.
#[derive(Debug)]
pub(crate) struct InFlight<W> {
    unanswered: VecDeque<(Request, W)>,
}

impl<W> InFlight<W> {
    /// No request is in flight.
    pub(crate) const fn new() -> Self {
        Self {
            unanswered: VecDeque::new(),
        }
    }

    /// The cluster has `request`, and `waiter` waits for its answer.
    pub(crate) fn sent(&mut self, request: Request, waiter: W) {
        self.unanswered.push_back((request, waiter));
    }

    /// The cluster sent `frame`. It answers the oldest request.
    pub(crate) fn answered(&mut self, frame: Frame) -> Answered<W> {
        let Some((request, waiter)) = self.unanswered.pop_front() else {
            return Answered::OutOfStep {
                error: WireError::Decode {
                    what: "response",
                    reason: "no request waits for it".to_owned(),
                },
                waiter: None,
            };
        };
        let correlation_id = match Response::peek_correlation_id(&frame) {
            Ok(correlation_id) => correlation_id,
            Err(error) => return Answered::Unreadable { error, waiter },
        };
        if correlation_id != request.correlation_id() {
            return Answered::OutOfStep {
                error: WireError::Decode {
                    what: "response header",
                    reason: format!(
                        "correlation ID {correlation_id} does not answer the {} request {}",
                        request.api(),
                        request.correlation_id()
                    ),
                },
                waiter: Some(waiter),
            };
        }
        match Response::decode(request.api(), frame) {
            Ok(response) => Answered::Response {
                request,
                response,
                waiter,
            },
            Err(error) => Answered::Unreadable { error, waiter },
        }
    }

    /// Everyone who still waits for an answer, the oldest first. Nothing is in flight afterwards.
    pub(crate) fn give_up(&mut self) -> impl Iterator<Item = W> + '_ {
        self.unanswered.drain(..).map(|(_, waiter)| waiter)
    }

    /// How many requests the cluster has not answered yet.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.unanswered.len()
    }
}

/// What a frame of the cluster is to the requests in flight.
#[derive(Debug)]
pub(crate) enum Answered<W> {
    /// The frame is the response to `request`, and `waiter` waits for it.
    Response {
        request: Request,
        response: Response,
        waiter: W,
    },
    /// The frame answers the request of `waiter`, and it does not read as a response.
    /// The frames behind it still answer the requests behind that one.
    Unreadable { error: WireError, waiter: W },
    /// The frame answers no request in flight, or another one than the oldest.
    /// From here on, nothing says which frame answers which request.
    /// `waiter` waits for the oldest request, if there is one.
    OutOfStep { error: WireError, waiter: Option<W> },
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::Api;
    use gaslightmq_wire::messages::produce_request::ProduceRequest;
    use gaslightmq_wire::messages::{
        FetchRequest, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse, TransactionalId,
    };
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    fn heartbeat(correlation_id: i32) -> Request {
        Request::new(4, correlation_id, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
    }

    fn produce(version: i16, acks: i16, transactional: Option<&'static str>) -> Request {
        let transactional = transactional.map(|id| TransactionalId(StrBytes::from_static_str(id)));
        let body = ProduceRequest::default()
            .with_acks(acks)
            .with_transactional_id(transactional);
        Request::new(version, 7, Some("order-ingest"), &body).unwrap()
    }

    #[test]
    fn every_request_but_a_produce_without_acks_is_answered() {
        assert_eq!(Reply::to(&heartbeat(1)), Ok(Reply::Comes));
        let fetch = Request::new(12, 1, None, &FetchRequest::default()).unwrap();
        assert_eq!(Reply::to(&fetch), Ok(Reply::Comes));

        for version in 3..=12 {
            for transactional in [None, Some("payments-tx"), Some("")] {
                // A version without transactions cannot write the ID of one.
                assert_eq!(
                    Reply::to(&produce(version, 0, transactional)),
                    Ok(Reply::Never),
                    "v{version}"
                );
                assert_eq!(
                    Reply::to(&produce(version, 1, transactional)),
                    Ok(Reply::Comes),
                    "v{version}"
                );
                assert_eq!(
                    Reply::to(&produce(version, -1, transactional)),
                    Ok(Reply::Comes),
                    "v{version}"
                );
            }
        }
    }

    #[test]
    fn a_produce_that_ends_before_its_acks_says_nothing() {
        let whole = produce(9, 0, Some("payments-tx"));
        let header = whole.frame().payload().len() - whole.body_bytes().len();
        let cut = Request::decode(Frame::new(whole.frame().payload().slice(..header + 3))).unwrap();

        assert_eq!(
            Reply::to(&cut),
            Err(WireError::Truncated {
                what: "Produce request body",
                len: 3
            })
        );
    }

    #[test]
    fn a_frame_answers_the_oldest_request() {
        let mut in_flight = InFlight::new();
        in_flight.sent(heartbeat(1), "first");
        in_flight.sent(heartbeat(2), "second");
        let answer = |request: &Request| request.response(&HeartbeatResponse::default()).unwrap().into_frame();

        let Answered::Response {
            request,
            response,
            waiter,
        } = in_flight.answered(answer(&heartbeat(1)))
        else {
            panic!("the frame answers the first request");
        };

        assert_eq!((request.correlation_id(), waiter), (1, "first"));
        assert_eq!(response.api(), Api::of::<HeartbeatRequest>(4));
        assert_eq!(in_flight.len(), 1);
        assert!(matches!(
            in_flight.answered(answer(&heartbeat(2))),
            Answered::Response { waiter: "second", .. }
        ));
    }

    #[test]
    fn a_frame_that_answers_another_request_puts_the_connection_out_of_step() {
        let mut in_flight = InFlight::new();
        in_flight.sent(heartbeat(1), "first");
        in_flight.sent(heartbeat(2), "second");
        let of_the_second = heartbeat(2).response(&HeartbeatResponse::default()).unwrap();

        let Answered::OutOfStep { error, waiter } = in_flight.answered(of_the_second.into_frame()) else {
            panic!("the frame answers the second request first");
        };

        assert_eq!(waiter, Some("first"));
        assert_eq!(
            error.to_string(),
            "cannot decode response header: correlation ID 2 does not answer the Heartbeat v4 request 1"
        );
        assert_eq!(in_flight.give_up().collect::<Vec<_>>(), ["second"]);
        assert_eq!(in_flight.len(), 0);
    }

    #[test]
    fn a_frame_that_nobody_waits_for_puts_the_connection_out_of_step() {
        let mut in_flight: InFlight<&str> = InFlight::new();
        let unasked = heartbeat(1).response(&HeartbeatResponse::default()).unwrap();

        let Answered::OutOfStep { error, waiter } = in_flight.answered(unasked.into_frame()) else {
            panic!("no request is in flight");
        };

        assert_eq!(waiter, None);
        assert_eq!(error.to_string(), "cannot decode response: no request waits for it");
    }

    #[test]
    fn a_frame_that_is_no_response_leaves_the_requests_behind_it_in_step() {
        let mut in_flight = InFlight::new();
        let metadata = Request::new(12, 1, None, &MetadataRequest::default()).unwrap();
        in_flight.sent(metadata.clone(), "first");
        in_flight.sent(heartbeat(2), "second");

        // Three bytes hold no correlation ID.
        let short = in_flight.answered(Frame::new(&[0_u8, 0, 0][..]));
        assert!(matches!(short, Answered::Unreadable { waiter: "first", .. }));
        let answer = heartbeat(2).response(&HeartbeatResponse::default()).unwrap();
        assert!(matches!(
            in_flight.answered(answer.into_frame()),
            Answered::Response { waiter: "second", .. }
        ));

        // A flexible header ends in tagged fields, and this one is cut off before them.
        in_flight.sent(metadata.clone(), "third");
        let whole = metadata.response(&MetadataResponse::default()).unwrap();
        let cut = Frame::new(whole.frame().payload().slice(..4));
        assert!(matches!(
            in_flight.answered(cut),
            Answered::Unreadable { waiter: "third", .. }
        ));
    }
}
