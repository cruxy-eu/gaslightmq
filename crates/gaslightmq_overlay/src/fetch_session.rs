use gaslightmq_wire::messages::{FetchRequest, FetchResponse};
use gaslightmq_wire::{DecodeError, EncodeError, Request};

/// A Fetch request as the real cluster gets to see it: without a fetch session.
///
/// A client with a fetch session only names the partitions that changed since its last Fetch,
/// so most of its requests would not carry the topics and partitions that selectors test.
/// The real cluster therefore never sees a request for a session, and it never grants one.
/// A client that gets no session keeps sending full Fetch requests.
///
/// | The request | Is |
/// |---|---|
/// | Has no session fields: a version before 7 | [`AsItArrived`](Self::AsItArrived) |
/// | Asks for no session: session ID 0 and the final epoch | [`AsItArrived`](Self::AsItArrived) |
/// | Is a full request that asks for a session, or closes one | [`Rewritten`](Self::Rewritten) |
/// | Continues a session: any other epoch | [`ContinuesSession`](Self::ContinuesSession) |
#[derive(Debug, Clone)]
pub(crate) enum SessionlessFetch {
    /// The request asks for no session, and it goes on byte for byte.
    AsItArrived,
    /// The request asked for a session. This request asks for the same partitions, and for no session.
    ///
    /// It is encoded anew, for the same version, under the same correlation ID and client ID.
    Rewritten(Request),
    /// The request continues a session, and names only what changed in it.
    /// No broker granted that session, so nothing of the request can go on.
    ContinuesSession,
}

impl SessionlessFetch {
    /// The first version of Fetch that has fetch sessions.
    const FIRST_VERSION: i16 = 7;
    /// The session ID of a request and of a response outside any session.
    const NO_SESSION: i32 = 0;
    /// The epoch of a full request that asks for a new session.
    const INITIAL_EPOCH: i32 = 0;
    /// The epoch of a full request that asks for no session.
    const FINAL_EPOCH: i32 = -1;

    /// Whether Fetch has fetch sessions at `version`.
    pub(crate) const fn applies_to(version: i16) -> bool {
        version >= Self::FIRST_VERSION
    }

    /// Take the fetch session out of `request`, a Fetch request.
    pub(crate) fn of(request: &Request) -> Result<Self, SessionlessFetchFlaw> {
        let version = request.api().version;
        if !Self::applies_to(version) {
            return Ok(Self::AsItArrived);
        }
        let mut body: FetchRequest = request
            .body()
            .map_err(|error| SessionlessFetchFlaw::UnreadableRequest { error })?;
        match (body.session_id, body.session_epoch) {
            (Self::NO_SESSION, Self::FINAL_EPOCH) => Ok(Self::AsItArrived),
            (_, Self::INITIAL_EPOCH | Self::FINAL_EPOCH) => {
                body.session_id = Self::NO_SESSION;
                body.session_epoch = Self::FINAL_EPOCH;
                Request::new(version, request.correlation_id(), request.client_id(), &body)
                    .map(Self::Rewritten)
                    .map_err(|error| SessionlessFetchFlaw::UnwritableRequest { error })
            }
            _ => Ok(Self::ContinuesSession),
        }
    }

    /// Make `response`, a Fetch response of a version with fetch sessions, say that it is part of no session.
    /// It returns whether the response said otherwise.
    pub(crate) fn deny_session(response: &mut FetchResponse) -> bool {
        let granted = response.session_id != Self::NO_SESSION;
        response.session_id = Self::NO_SESSION;
        granted
    }
}

/// Why the fetch session could not leave a Fetch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionlessFetchFlaw {
    /// The body of the request does not decode.
    UnreadableRequest { error: DecodeError },
    /// The request without its session does not encode.
    UnwritableRequest { error: EncodeError },
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    /// A Fetch of `orders.eu/3` at offset 40.
    fn fetch(version: i16, session_id: i32, session_epoch: i32) -> Request {
        let partition = FetchPartition::default().with_partition(3).with_fetch_offset(40);
        let topic = FetchTopic::default()
            .with_topic(StrBytes::from_static_str("orders.eu").into())
            .with_partitions(vec![partition]);
        let body = FetchRequest::default()
            .with_max_wait_ms(500)
            .with_session_id(session_id)
            .with_session_epoch(session_epoch)
            .with_topics(vec![topic]);
        Request::new(version, 42, Some("order-audit"), &body).unwrap()
    }

    #[test]
    fn a_fetch_before_sessions_goes_on_as_it_arrived() {
        for version in [4, 5, 6] {
            let request = fetch(version, 0, -1);
            assert!(matches!(
                SessionlessFetch::of(&request),
                Ok(SessionlessFetch::AsItArrived)
            ));
        }
    }

    #[test]
    fn a_fetch_that_asks_for_no_session_goes_on_as_it_arrived() {
        for version in [7, 11, 12, 17] {
            let request = fetch(version, 0, -1);
            assert!(matches!(
                SessionlessFetch::of(&request),
                Ok(SessionlessFetch::AsItArrived)
            ));
        }
    }

    #[test]
    fn a_full_fetch_loses_its_wish_for_a_session_and_nothing_else() {
        // A new session, a new session in place of session 77, and no session in place of session 77.
        for (session_id, session_epoch) in [(0, 0), (77, 0), (77, -1)] {
            for version in [7, 11, 12, 17] {
                let request = fetch(version, session_id, session_epoch);
                let Ok(SessionlessFetch::Rewritten(sessionless)) = SessionlessFetch::of(&request) else {
                    panic!("a full Fetch v{version} of session {session_id} at epoch {session_epoch} is rewritten");
                };

                assert_eq!(sessionless.api(), request.api());
                assert_eq!(sessionless.correlation_id(), 42);
                assert_eq!(sessionless.client_id(), Some("order-audit"));
                let asked: FetchRequest = request.body().unwrap();
                let forwarded: FetchRequest = sessionless.body().unwrap();
                assert_eq!((forwarded.session_id, forwarded.session_epoch), (0, -1));
                assert_eq!(
                    forwarded.with_session_id(session_id).with_session_epoch(session_epoch),
                    asked
                );
            }
        }
    }

    #[test]
    fn a_fetch_that_continues_a_session_cannot_go_on() {
        for (session_id, session_epoch) in [(77, 1), (77, 2048), (0, 1), (77, -2)] {
            let request = fetch(12, session_id, session_epoch);
            assert!(matches!(
                SessionlessFetch::of(&request),
                Ok(SessionlessFetch::ContinuesSession)
            ));
        }
    }

    #[test]
    fn a_fetch_that_does_not_decode_says_so() {
        // The request ends before its body does.
        let whole = fetch(12, 0, 0);
        let cut = whole.frame().payload().slice(..whole.frame().payload().len() - 4);
        let request = Request::decode(gaslightmq_wire::Frame::new(cut)).unwrap();

        assert!(matches!(
            SessionlessFetch::of(&request),
            Err(SessionlessFetchFlaw::UnreadableRequest { .. })
        ));
    }

    #[test]
    fn a_response_says_that_it_is_part_of_no_session() {
        let mut granted = FetchResponse::default().with_session_id(77);
        assert!(SessionlessFetch::deny_session(&mut granted));
        assert_eq!(granted.session_id, 0);

        let mut sessionless = FetchResponse::default();
        assert!(!SessionlessFetch::deny_session(&mut sessionless));
        assert_eq!(sessionless.session_id, 0);
    }
}
