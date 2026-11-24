//! The calls in flight on one session, and the order in which their requests and responses go on.

use std::collections::VecDeque;
use std::fmt;

use gaslightmq_call::CallId;
use gaslightmq_clock::Instant;

use crate::{CallIds, SessionError};

/// How far a call in flight is.
///
/// ```text
/// RequestHeld ──▶ RequestQueued ──▶ AwaitingResponse ──▶ ResponseQueued ──▶ the call leaves
///      └───────────────┴──────────────────┴──▶ Silenced
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallPhase {
    /// The request arrived, and the pipeline holds it:
    /// it still decides about the request, or a delay before forwarding runs.
    RequestHeld,
    /// The pipeline let the request go, and it waits for the requests before it.
    RequestQueued,
    /// The request went on, and the call waits for its response:
    /// for the backend, for what the pipeline does with the truth, or for a delay after the response.
    AwaitingResponse,
    /// The response is ready, and it waits for the responses before it.
    ResponseQueued,
    /// A silence took the call. It never gets a response, and it never leaves.
    Silenced,
}

impl CallPhase {
    /// The phase in words.
    pub const fn name(self) -> &'static str {
        match self {
            CallPhase::RequestHeld => "request held",
            CallPhase::RequestQueued => "request queued",
            CallPhase::AwaitingResponse => "awaiting response",
            CallPhase::ResponseQueued => "response queued",
            CallPhase::Silenced => "silenced",
        }
    }
}

impl fmt::Display for CallPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One call in flight: its ID, when it arrived, how far it is, and what the pipeline keeps for it.
///
/// `C` is whatever the pipeline needs to finish the call, such as the request, the rules that fired and the truth.
/// The session never looks at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightCall<C> {
    id: CallId,
    arrived_at: Instant,
    phase: CallPhase,
    content: C,
}

impl<C> InFlightCall<C> {
    /// Which call of its environment the call is.
    pub const fn id(&self) -> CallId {
        self.id
    }

    /// When the request arrived.
    pub const fn arrived_at(&self) -> Instant {
        self.arrived_at
    }

    /// How far the call is.
    pub const fn phase(&self) -> CallPhase {
        self.phase
    }

    /// What the pipeline keeps for the call.
    pub const fn content(&self) -> &C {
        &self.content
    }

    /// What the pipeline keeps for the call, to change it.
    pub const fn content_mut(&mut self) -> &mut C {
        &mut self.content
    }

    /// What the pipeline kept for the call, once the call has left.
    pub fn into_content(self) -> C {
        self.content
    }
}

/// That a silence stopped a connection: which call it took, and when.
///
/// A stalled connection never answers that call or any call after it,
/// and no request after it reaches the backend.
/// The session stays open until the client gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stalled {
    by: CallId,
    since: Instant,
}

impl Stalled {
    /// The call that the silence took. With several silenced calls, it is the earliest one.
    pub const fn by(&self) -> CallId {
        self.by
    }

    /// When the silence took that call.
    pub const fn since(&self) -> Instant {
        self.since
    }
}

/// The calls in flight on one session, in the order the client sent their requests.
///
/// A call is in flight from the moment its request arrives until its response leaves.
/// The pipeline keeps what it needs for the call here, as `C`,
/// so a lie after the response applies to the call it belongs to.
///
/// # Call IDs
///
/// Every call that arrives gets its [`CallId`] from the [`CallIds`] of the environment,
/// which all of its sessions share.
/// The IDs of one session therefore rise in the order its requests arrived, and they need not follow each other:
/// the calls of other sessions lie between them.
///
/// # Ordering
///
/// A Kafka client expects its responses in the order it sent its requests, and so does a backend.
/// Calls therefore pass two gates, and each gate opens for one call at a time, in the order of arrival.
///
/// | Gate | The pipeline says | The gate opens with | Which holds back |
/// |---|---|---|---|
/// | Requests | [`release_request`](Self::release_request) | [`next_request`](Self::next_request) | A delay before forwarding holds back its request and every request behind it. |
/// | Responses | [`release_response`](Self::release_response) | [`next_response`](Self::next_response) | A delay after the response holds back its response and every response behind it. |
///
/// The pipeline lets go of a request or a response whenever it is done with it, in any order.
/// It then takes from the gate until the gate gives nothing, and acts on what it gets.
/// A request that passed its gate does not wait for the responses before it,
/// so the backend receives it on time while an earlier response is held back.
///
/// A call that the pipeline answers itself passes the request gate like any other, and is not forwarded.
/// A call without a response, such as a Produce with `acks: 0`, passes the response gate with nothing to send.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_session::prelude::*;
///
/// let now = Instant::ZERO;
/// let mut calls = InFlightCalls::new(CallIds::new());
/// let produce = calls.arrive("Produce", now);
/// let fetch = calls.arrive("Fetch", now);
///
/// // A delay before forwarding runs for the Produce, so the Fetch behind it waits too.
/// calls.release_request(fetch)?;
/// assert!(calls.next_request().is_none());
///
/// // The delay ends, and both requests go on in the order they arrived.
/// calls.release_request(produce)?;
/// assert_eq!(calls.next_request().map(|(call, _)| call), Some(produce));
/// assert_eq!(calls.next_request().map(|(call, _)| call), Some(fetch));
///
/// // The backend answers the Fetch first. Its response waits for the response of the Produce.
/// calls.release_response(fetch)?;
/// assert!(calls.next_response().is_none());
/// calls.release_response(produce)?;
/// assert_eq!(calls.next_response().map(InFlightCall::into_content), Some("Produce"));
/// assert_eq!(calls.next_response().map(InFlightCall::into_content), Some("Fetch"));
/// assert!(calls.is_empty());
/// # Ok::<(), SessionError>(())
/// ```
///
/// # Silence
///
/// [`silence`](Self::silence) takes a call for good, and the connection is [stalled](Self::stalled) from then on.
/// The silenced call never leaves, so no response behind it ever passes the response gate.
/// No request behind it passes the request gate either.
/// Calls before it still finish: their requests go on, and their responses leave.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_session::prelude::*;
///
/// let now = Instant::ZERO;
/// let mut calls = InFlightCalls::new(CallIds::new());
/// let metadata = calls.arrive("Metadata", now);
/// let produce = calls.arrive("Produce", now);
/// let fetch = calls.arrive("Fetch", now);
/// calls.release_request(metadata)?;
/// calls.release_request(fetch)?;
///
/// calls.silence(produce, now)?;
/// assert_eq!(calls.stalled().map(|stalled| stalled.by()), Some(produce));
///
/// // The Metadata before the silence still goes on, and the Fetch behind it never does.
/// assert_eq!(calls.next_request().map(|(call, _)| call), Some(metadata));
/// assert!(calls.next_request().is_none());
/// # Ok::<(), SessionError>(())
/// ```
#[derive(Debug, Clone)]
pub struct InFlightCalls<C> {
    /// The call IDs of the environment, which every call that arrives takes its ID from.
    ids: CallIds,
    /// The calls in flight, in the order they arrived, which is the order of their IDs.
    /// A call only leaves at the front.
    calls: VecDeque<InFlightCall<C>>,
    /// How many calls arrived, in flight or not.
    arrived: u64,
    /// The call that arrived last, or `None` before the first one.
    last_arrived: Option<CallId>,
    /// The call whose request passed the request gate last. Requests pass in order, so every earlier one passed too.
    last_passed: Option<CallId>,
    stalled: Option<Stalled>,
}

impl<C> InFlightCalls<C> {
    /// The calls of a session that received no request yet.
    ///
    /// `ids` is the handle to the call IDs of the environment of the session.
    pub const fn new(ids: CallIds) -> Self {
        Self {
            ids,
            calls: VecDeque::new(),
            arrived: 0,
            last_arrived: None,
            last_passed: None,
            stalled: None,
        }
    }

    /// A request arrived: the call is in flight from now on, and gets the next call ID of the environment.
    ///
    /// `content` is what the pipeline keeps for the call.
    /// The call starts with its [request held](CallPhase::RequestHeld).
    pub fn arrive(&mut self, content: C, now: Instant) -> CallId {
        let id = self.ids.assign();
        self.arrived = self.arrived.saturating_add(1);
        self.last_arrived = Some(id);
        self.calls.push_back(InFlightCall {
            id,
            arrived_at: now,
            phase: CallPhase::RequestHeld,
            content,
        });
        id
    }

    /// The pipeline lets the request of `call` go.
    /// The request passes [the request gate](Self::next_request) once every request before it has.
    ///
    /// # Errors
    ///
    /// [`SessionError::CallNotInFlight`] when the call is not in flight,
    /// and [`SessionError::RequestNotHeld`] when its request was let go before, or a silence took the call.
    pub fn release_request(&mut self, call: CallId) -> Result<(), SessionError> {
        let in_flight = self.in_flight_mut(call)?;
        if in_flight.phase != CallPhase::RequestHeld {
            return Err(SessionError::RequestNotHeld {
                call,
                phase: in_flight.phase,
            });
        }
        in_flight.phase = CallPhase::RequestQueued;
        Ok(())
    }

    /// Lets the next request pass the request gate, when the pipeline let it go.
    ///
    /// Requests pass in the order they arrived.
    /// The call that passes [awaits its response](CallPhase::AwaitingResponse) from then on:
    /// the pipeline forwards its request, or answers the call itself.
    ///
    /// `None` when the next request is still held, when no request waits,
    /// or when the next request arrived after a call that a silence took.
    pub fn next_request(&mut self) -> Option<(CallId, &mut C)> {
        let stalled = self.stalled;
        // The calls are in the order of their IDs, so the first one after the last that passed is next.
        let index = match self.last_passed {
            Some(last) => self.calls.partition_point(|call| call.id <= last),
            None => 0,
        };
        let in_flight = self.calls.get_mut(index)?;
        let next = in_flight.id;
        if stalled.is_some_and(|stalled| stalled.by <= next) || in_flight.phase != CallPhase::RequestQueued {
            return None;
        }
        in_flight.phase = CallPhase::AwaitingResponse;
        self.last_passed = Some(next);
        Some((next, &mut in_flight.content))
    }

    /// The pipeline is done with the response of `call`, or knows that the call has none.
    /// The call passes [the response gate](Self::next_response) once every call before it has.
    ///
    /// # Errors
    ///
    /// [`SessionError::CallNotInFlight`] when the call is not in flight,
    /// and [`SessionError::ResponseNotAwaited`] when its request did not pass the request gate yet,
    /// its response was let go before, or a silence took the call.
    pub fn release_response(&mut self, call: CallId) -> Result<(), SessionError> {
        let in_flight = self.in_flight_mut(call)?;
        if in_flight.phase != CallPhase::AwaitingResponse {
            return Err(SessionError::ResponseNotAwaited {
                call,
                phase: in_flight.phase,
            });
        }
        in_flight.phase = CallPhase::ResponseQueued;
        Ok(())
    }

    /// Lets the next call pass the response gate, when the pipeline is done with its response.
    ///
    /// Calls pass in the order they arrived.
    /// The call that passes leaves: the pipeline sends its response, and it is no longer in flight.
    ///
    /// `None` when the earliest call still waits for its response, when no call is in flight,
    /// or when a silence took the earliest call.
    pub fn next_response(&mut self) -> Option<InFlightCall<C>> {
        if self.calls.front()?.phase != CallPhase::ResponseQueued {
            return None;
        }
        self.calls.pop_front()
    }

    /// A silence takes `call`: it never gets a response, and the connection is stalled.
    ///
    /// Returns since when the connection is stalled, and by which call.
    /// That is the earliest silenced call, so a second silence on a later call changes nothing.
    ///
    /// # Errors
    ///
    /// [`SessionError::CallNotInFlight`] when the call is not in flight.
    pub fn silence(&mut self, call: CallId, now: Instant) -> Result<Stalled, SessionError> {
        self.in_flight_mut(call)?.phase = CallPhase::Silenced;
        let stalled = match self.stalled {
            Some(earlier) if earlier.by <= call => earlier,
            _ => Stalled { by: call, since: now },
        };
        self.stalled = Some(stalled);
        Ok(stalled)
    }

    /// Whether a silence stopped the connection, and by which call.
    pub const fn stalled(&self) -> Option<Stalled> {
        self.stalled
    }

    /// The call with that ID, when it is in flight.
    pub fn get(&self, call: CallId) -> Option<&InFlightCall<C>> {
        self.calls.get(self.index_of(call)?)
    }

    /// The call with that ID, when it is in flight, to change what the pipeline keeps for it.
    pub fn get_mut(&mut self, call: CallId) -> Option<&mut InFlightCall<C>> {
        let index = self.index_of(call)?;
        self.calls.get_mut(index)
    }

    /// The calls in flight, in the order their requests arrived.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &InFlightCall<C>> + DoubleEndedIterator {
        self.calls.iter()
    }

    /// How many calls are in flight.
    pub fn len(&self) -> usize {
        self.calls.len()
    }

    /// Whether no call is in flight.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// How many calls arrived on the session, in flight or not.
    pub const fn arrived(&self) -> u64 {
        self.arrived
    }

    /// Ends every call in flight, and returns them in the order their requests arrived.
    ///
    /// A session that closes does this, because none of its calls gets a response any more.
    /// The call IDs go on: a later call does not get the ID of a call that ended.
    pub fn end_all(&mut self) -> Vec<InFlightCall<C>> {
        self.last_passed = self.last_arrived;
        self.calls.drain(..).collect()
    }

    /// Where the call with that ID is in the queue, when it is in flight.
    ///
    /// The calls are in the order of their IDs, and the IDs of one session need not follow each other.
    fn index_of(&self, call: CallId) -> Option<usize> {
        self.calls.binary_search_by_key(&call, |in_flight| in_flight.id).ok()
    }

    fn in_flight_mut(&mut self, call: CallId) -> Result<&mut InFlightCall<C>, SessionError> {
        self.get_mut(call).ok_or(SessionError::CallNotInFlight { call })
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Duration;

    use super::*;

    const NOW: Instant = Instant::from_nanos(1_000_000_000);

    /// Calls with their API as content, and their requests held.
    fn arrived<const N: usize>(apis: [&'static str; N]) -> (InFlightCalls<&'static str>, [CallId; N]) {
        let mut calls = InFlightCalls::new(CallIds::new());
        let ids = apis.map(|api| calls.arrive(api, NOW));
        (calls, ids)
    }

    /// Takes from the request gate until it gives nothing.
    fn requests(calls: &mut InFlightCalls<&'static str>) -> Vec<&'static str> {
        std::iter::from_fn(|| calls.next_request().map(|(_, api)| *api)).collect()
    }

    /// Takes from the response gate until it gives nothing.
    fn responses(calls: &mut InFlightCalls<&'static str>) -> Vec<&'static str> {
        std::iter::from_fn(|| calls.next_response().map(InFlightCall::into_content)).collect()
    }

    #[test]
    fn calls_get_their_ids_in_the_order_they_arrive() {
        let mut calls = InFlightCalls::new(CallIds::new());
        assert!(calls.is_empty());

        let first = calls.arrive("Metadata", NOW);
        let second = calls.arrive("Produce", NOW + Duration::from_millis(5));

        assert_eq!([first, second], [CallId::FIRST, CallId::FIRST.next()]);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls.arrived(), 2);
        let produce = calls.get(second).unwrap();
        assert_eq!(produce.id(), second);
        assert_eq!(produce.arrived_at(), NOW + Duration::from_millis(5));
        assert_eq!(produce.phase(), CallPhase::RequestHeld);
        assert_eq!(*produce.content(), "Produce");
        assert_eq!(
            calls.iter().map(|call| *call.content()).collect::<Vec<_>>(),
            ["Metadata", "Produce"]
        );
    }

    #[test]
    fn the_sessions_of_an_environment_count_their_calls_together() {
        let call_ids = CallIds::new();
        let mut one = InFlightCalls::new(call_ids.clone());
        let mut other = InFlightCalls::new(call_ids.clone());

        let produce = one.arrive("Produce", NOW);
        let heartbeat = other.arrive("Heartbeat", NOW);
        let fetch = one.arrive("Fetch", NOW);

        // A later call has a higher ID, on whichever session it arrived.
        assert_eq!(
            [produce, heartbeat, fetch],
            [CallId::FIRST, produce.next(), heartbeat.next()]
        );
        assert_eq!([one.arrived(), other.arrived()], [2, 1]);
        assert_eq!(call_ids.assigned(), 3);

        // A session only knows its own calls, and keeps them in order although their IDs do not follow each other.
        assert_eq!(one.get(heartbeat), None);
        assert_eq!(other.get(produce), None);
        one.release_request(fetch).unwrap();
        assert_eq!(requests(&mut one), Vec::<&str>::new());
        one.release_request(produce).unwrap();
        assert_eq!(requests(&mut one), ["Produce", "Fetch"]);
        one.release_response(fetch).unwrap();
        one.release_response(produce).unwrap();
        assert_eq!(responses(&mut one), ["Produce", "Fetch"]);
    }

    #[test]
    fn a_silence_stalls_its_session_whatever_the_other_sessions_do() {
        let call_ids = CallIds::new();
        let mut one = InFlightCalls::new(call_ids.clone());
        let mut other = InFlightCalls::new(call_ids);

        let metadata = one.arrive("Metadata", NOW);
        let heartbeat = other.arrive("Heartbeat", NOW);
        let produce = one.arrive("Produce", NOW);
        other.arrive("Heartbeat", NOW);
        let fetch = one.arrive("Fetch", NOW);

        one.silence(produce, NOW).unwrap();
        one.release_request(metadata).unwrap();
        one.release_request(fetch).unwrap();
        other.release_request(heartbeat).unwrap();

        assert_eq!(requests(&mut one), ["Metadata"]);
        assert_eq!(requests(&mut other), ["Heartbeat"]);
        assert_eq!(other.stalled(), None);
    }

    #[test]
    fn the_pipeline_changes_what_it_keeps_for_a_call() {
        let mut calls = InFlightCalls::new(CallIds::new());
        let call = calls.arrive(vec!["request"], NOW);

        calls.get_mut(call).unwrap().content_mut().push("truth");

        assert_eq!(*calls.get(call).unwrap().content(), ["request", "truth"]);
    }

    #[test]
    fn a_call_goes_through_its_phases_and_leaves() {
        let (mut calls, [call]) = arrived(["Produce"]);
        let phase = |calls: &InFlightCalls<_>| calls.get(call).map(InFlightCall::phase);

        assert_eq!(phase(&calls), Some(CallPhase::RequestHeld));
        calls.release_request(call).unwrap();
        assert_eq!(phase(&calls), Some(CallPhase::RequestQueued));
        assert_eq!(calls.next_request(), Some((call, &mut "Produce")));
        assert_eq!(phase(&calls), Some(CallPhase::AwaitingResponse));
        calls.release_response(call).unwrap();
        assert_eq!(phase(&calls), Some(CallPhase::ResponseQueued));

        let left = calls.next_response().unwrap();
        assert_eq!(left.id(), call);
        assert_eq!(left.phase(), CallPhase::ResponseQueued);
        assert_eq!(phase(&calls), None);
        assert!(calls.is_empty());
        assert_eq!(calls.arrived(), 1);
    }

    #[test]
    fn a_held_request_holds_back_every_request_behind_it() {
        let (mut calls, [produce, fetch, metadata]) = arrived(["Produce", "Fetch", "Metadata"]);

        calls.release_request(fetch).unwrap();
        calls.release_request(metadata).unwrap();
        assert_eq!(requests(&mut calls), Vec::<&str>::new());

        calls.release_request(produce).unwrap();
        assert_eq!(requests(&mut calls), ["Produce", "Fetch", "Metadata"]);
        assert_eq!(requests(&mut calls), Vec::<&str>::new());
    }

    #[test]
    fn a_held_response_holds_back_every_response_behind_it() {
        let (mut calls, [produce, fetch, metadata]) = arrived(["Produce", "Fetch", "Metadata"]);
        for call in [produce, fetch, metadata] {
            calls.release_request(call).unwrap();
        }
        assert_eq!(requests(&mut calls).len(), 3);

        calls.release_response(metadata).unwrap();
        calls.release_response(fetch).unwrap();
        assert_eq!(responses(&mut calls), Vec::<&str>::new());

        calls.release_response(produce).unwrap();
        assert_eq!(responses(&mut calls), ["Produce", "Fetch", "Metadata"]);
    }

    #[test]
    fn a_held_response_does_not_hold_back_a_later_request() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);

        calls.release_request(produce).unwrap();
        assert_eq!(requests(&mut calls), ["Produce"]);

        // The response of the Produce is delayed, and the Fetch still reaches the backend.
        calls.release_request(fetch).unwrap();
        assert_eq!(requests(&mut calls), ["Fetch"]);
        assert_eq!(responses(&mut calls), Vec::<&str>::new());
    }

    #[test]
    fn a_request_arrives_while_earlier_calls_are_in_flight() {
        let (mut calls, [produce]) = arrived(["Produce"]);
        calls.release_request(produce).unwrap();
        assert_eq!(requests(&mut calls), ["Produce"]);
        calls.release_response(produce).unwrap();
        assert_eq!(responses(&mut calls), ["Produce"]);

        let fetch = calls.arrive("Fetch", NOW);
        assert_eq!(fetch, produce.next());
        calls.release_request(fetch).unwrap();
        assert_eq!(requests(&mut calls), ["Fetch"]);
        calls.release_response(fetch).unwrap();
        assert_eq!(responses(&mut calls), ["Fetch"]);
    }

    #[test]
    fn a_silence_stalls_the_connection() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);
        assert_eq!(calls.stalled(), None);

        let stalled = calls.silence(produce, NOW + Duration::from_millis(3)).unwrap();

        assert_eq!(stalled.by(), produce);
        assert_eq!(stalled.since(), NOW + Duration::from_millis(3));
        assert_eq!(calls.stalled(), Some(stalled));
        assert_eq!(calls.get(produce).unwrap().phase(), CallPhase::Silenced);

        // The request behind the silence never goes on, and nothing ever leaves.
        calls.release_request(fetch).unwrap();
        assert_eq!(requests(&mut calls), Vec::<&str>::new());
        assert_eq!(responses(&mut calls), Vec::<&str>::new());

        // A request that arrives later stays where it is too.
        let metadata = calls.arrive("Metadata", NOW);
        calls.release_request(metadata).unwrap();
        assert_eq!(requests(&mut calls), Vec::<&str>::new());
        assert_eq!(calls.len(), 3);
    }

    #[test]
    fn the_calls_before_a_silence_still_finish() {
        let (mut calls, [metadata, produce, fetch]) = arrived(["Metadata", "Produce", "Fetch"]);

        calls.silence(produce, NOW).unwrap();
        calls.release_request(fetch).unwrap();
        calls.release_request(metadata).unwrap();

        assert_eq!(requests(&mut calls), ["Metadata"]);
        calls.release_response(metadata).unwrap();
        assert_eq!(responses(&mut calls), ["Metadata"]);
        assert_eq!(requests(&mut calls), Vec::<&str>::new());
        assert_eq!(
            calls.iter().map(InFlightCall::phase).collect::<Vec<_>>(),
            [CallPhase::Silenced, CallPhase::RequestQueued]
        );
    }

    #[test]
    fn a_silence_after_the_request_went_on_stops_the_requests_behind_it() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);
        calls.release_request(produce).unwrap();
        assert_eq!(requests(&mut calls), ["Produce"]);

        calls.silence(produce, NOW).unwrap();
        calls.release_request(fetch).unwrap();

        assert_eq!(requests(&mut calls), Vec::<&str>::new());
        assert_eq!(responses(&mut calls), Vec::<&str>::new());
    }

    #[test]
    fn the_earliest_silenced_call_is_the_one_that_stalled_the_connection() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);

        let later = calls.silence(fetch, NOW).unwrap();
        assert_eq!(later.by(), fetch);

        let earlier = calls.silence(produce, NOW + Duration::from_millis(1)).unwrap();
        assert_eq!(earlier.by(), produce);

        let again = calls.silence(fetch, NOW + Duration::from_millis(2)).unwrap();
        assert_eq!(again, earlier);
        assert_eq!(calls.stalled(), Some(earlier));
    }

    #[test]
    fn ending_every_call_returns_them_in_order_and_keeps_the_ids_going() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);
        calls.release_request(produce).unwrap();
        assert_eq!(requests(&mut calls), ["Produce"]);

        let ended = calls.end_all();

        assert_eq!(
            ended.iter().map(|call| (call.id(), call.phase())).collect::<Vec<_>>(),
            [(produce, CallPhase::AwaitingResponse), (fetch, CallPhase::RequestHeld)]
        );
        assert!(calls.is_empty());
        assert_eq!(calls.get(fetch), None);

        let next = calls.arrive("Metadata", NOW);
        assert_eq!(next, fetch.next());
        calls.release_request(next).unwrap();
        assert_eq!(requests(&mut calls), ["Metadata"]);
    }

    #[test]
    fn a_call_that_is_not_in_flight_is_an_error() {
        let (mut calls, [produce]) = arrived(["Produce"]);
        let never = produce.next();
        let not_in_flight = |call| Err(SessionError::CallNotInFlight { call });

        assert_eq!(calls.release_request(never), not_in_flight(never));
        assert_eq!(calls.release_response(never), not_in_flight(never));
        assert_eq!(
            calls.silence(never, NOW),
            Err(SessionError::CallNotInFlight { call: never })
        );
        assert_eq!(calls.get(never), None);

        calls.release_request(produce).unwrap();
        calls.next_request().unwrap();
        calls.release_response(produce).unwrap();
        calls.next_response().unwrap();
        assert_eq!(calls.release_response(produce), not_in_flight(produce));
    }

    #[test]
    fn a_request_is_let_go_once() {
        let (mut calls, [produce, fetch]) = arrived(["Produce", "Fetch"]);

        calls.release_request(produce).unwrap();
        assert_eq!(
            calls.release_request(produce),
            Err(SessionError::RequestNotHeld {
                call: produce,
                phase: CallPhase::RequestQueued
            })
        );

        calls.silence(fetch, NOW).unwrap();
        assert_eq!(
            calls.release_request(fetch),
            Err(SessionError::RequestNotHeld {
                call: fetch,
                phase: CallPhase::Silenced
            })
        );
    }

    #[test]
    fn a_response_is_let_go_after_the_request_went_on_and_once() {
        let (mut calls, [produce]) = arrived(["Produce"]);
        let not_awaited = |phase| Err(SessionError::ResponseNotAwaited { call: produce, phase });

        assert_eq!(calls.release_response(produce), not_awaited(CallPhase::RequestHeld));
        calls.release_request(produce).unwrap();
        assert_eq!(calls.release_response(produce), not_awaited(CallPhase::RequestQueued));
        calls.next_request().unwrap();
        calls.release_response(produce).unwrap();
        assert_eq!(calls.release_response(produce), not_awaited(CallPhase::ResponseQueued));
    }

    #[test]
    fn every_phase_has_a_name() {
        let phases = [
            CallPhase::RequestHeld,
            CallPhase::RequestQueued,
            CallPhase::AwaitingResponse,
            CallPhase::ResponseQueued,
            CallPhase::Silenced,
        ];

        assert_eq!(
            phases.map(|phase| phase.to_string()),
            [
                "request held",
                "request queued",
                "awaiting response",
                "response queued",
                "silenced"
            ]
        );
    }
}
