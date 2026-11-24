//! The state of one session.

use std::sync::Arc;

use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_trigger::SessionCounters;
use gaslightmq_wire::Request;

use crate::identity::{client_id_of, client_software_of};
use crate::{
    CallIds, ClientContext, ClientContexts, ClientInstance, Closed, ClosedBy, Connection, ConnectionOrdinal,
    Identification, Identity, IdentityChange, InFlightCall, InFlightCalls, LearnedFields, Lifecycle, SessionKey,
    Stalled,
};

/// The state of one session: everything that the pipeline of one connection keeps between two things that happen.
///
/// | Part | Is |
/// |---|---|
/// | [Lifecycle](Self::lifecycle) | Opened, identified or closed, and [who closed it](Self::closed). |
/// | [Identity](Self::identity) | The connection, the `client.id` and the client software. |
/// | [Session key](Self::key) | The host, the endpoint and the ordinal of the connection, which is the same in every run. |
/// | [Client context](Self::learned) | What the sessions of the same client instance learned together. |
/// | [Counters](Self::counters) | The match counts of the rules that count `per: session`. |
/// | [Calls in flight](Self::calls) | The requests that wait for a response, and the order in which they go on. |
/// | [Stalled](Self::stalled) | Whether a silence stopped the connection. |
///
/// `C` is what the pipeline keeps for [a call in flight](InFlightCalls).
///
/// The session does no IO and reads no clock.
/// Its pipeline tells it what happened and when, and reads what it knows.
/// None of the state is persisted: a session dies with its connection,
/// and a client that reconnects gets a new session, with no call in flight and every counter at zero.
/// Only the client context outlives it, because the sessions of a client share it.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::MetadataRequest;
///
/// // The engine shares the client contexts and the call IDs of its environment, and numbers the connections.
/// let contexts = ClientContexts::new(Duration::from_secs(60));
/// let call_ids = CallIds::new();
/// let mut ordinals = ConnectionOrdinals::new();
/// let now = Instant::ZERO;
///
/// // A client connects, and its session opens.
/// let connection = Connection::new(
///     ConnectionNumber::new(7),
///     "10.0.4.27:51459".parse().expect("a client address"),
///     Endpoint::Broker(BrokerId::from(2)),
/// );
/// let ordinal = ordinals.assign(connection.host(), connection.endpoint());
/// let mut session: Session<Request> = Session::open(connection, ordinal, contexts.clone(), call_ids.clone(), now);
/// assert_eq!(session.lifecycle(), Lifecycle::Opened);
/// assert_eq!(session.key().to_string(), "10.0.4.27 broker 2 #1");
///
/// // The first request identifies it.
/// let request = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default()).expect("a request");
/// let identification = session.identify(&request);
/// assert_eq!(session.lifecycle(), Lifecycle::Identified);
/// assert_eq!(identification.changes().len(), 1);
/// assert_eq!(session.identity().client_id().map(ClientId::as_str), Some("order-ingest"));
///
/// // The call is in flight until its response leaves.
/// let call = session.calls_mut().arrive(request, now);
/// session.calls_mut().release_request(call)?;
/// assert!(session.calls_mut().next_request().is_some());
/// session.calls_mut().release_response(call)?;
/// assert!(session.calls_mut().next_response().is_some());
///
/// // The client hangs up, and the session ends.
/// session.close(ClosedBy::Client, now + Duration::from_secs(1));
/// assert_eq!(session.lifecycle(), Lifecycle::Closed);
/// # Ok::<(), SessionError>(())
/// ```
#[derive(Debug)]
pub struct Session<C> {
    identity: Identity,
    key: SessionKey,
    opened_at: Instant,
    /// The session's share of the context of its client instance, from the first request until the session closes.
    context: Option<ClientContext>,
    closed: Option<Closed>,
    contexts: ClientContexts,
    counters: SessionCounters,
    calls: InFlightCalls<C>,
}

impl<C> Session<C> {
    /// A client connected at `now`: its session opens.
    ///
    /// `ordinal` says which connection from its host to its endpoint this one is,
    /// and it makes the [session key](Self::key) together with those two.
    /// `contexts` is the engine's handle to the client contexts.
    /// The session joins the context of its client instance when its first request arrives.
    /// `call_ids` is the engine's handle to the call IDs of its environment.
    /// Every request that arrives on the session takes the ID of its call from there.
    pub fn open(
        connection: Connection,
        ordinal: ConnectionOrdinal,
        contexts: ClientContexts,
        call_ids: CallIds,
        now: Instant,
    ) -> Self {
        let key = SessionKey::of(&connection, ordinal);
        Self {
            identity: Identity::of(connection),
            key,
            opened_at: now,
            context: None,
            closed: None,
            contexts,
            counters: SessionCounters::new(key.counter_key()),
            calls: InFlightCalls::new(call_ids),
        }
    }

    /// Where the session is in its life.
    pub const fn lifecycle(&self) -> Lifecycle {
        match (&self.closed, &self.context) {
            (Some(_), _) => Lifecycle::Closed,
            (None, Some(_)) => Lifecycle::Identified,
            (None, None) => Lifecycle::Opened,
        }
    }

    /// The identity fields of the session, as far as they are known.
    pub const fn identity(&self) -> &Identity {
        &self.identity
    }

    /// The connection of the session.
    pub const fn connection(&self) -> Connection {
        self.identity.connection()
    }

    /// The key that tells the session from every other one, and that is the same in every run.
    pub const fn key(&self) -> SessionKey {
        self.key
    }

    /// When the client connected.
    pub const fn opened_at(&self) -> Instant {
        self.opened_at
    }

    /// Who closed the session and when, or `None` while it is open.
    pub const fn closed(&self) -> Option<Closed> {
        self.closed
    }

    /// The client instance of the session, or `None` before the first request and after the session closed.
    pub fn client(&self) -> Option<&ClientInstance> {
        self.context.as_ref().map(ClientContext::client)
    }

    /// Takes what the header of `request` says about the client, and returns what that changed.
    ///
    /// - The first request identifies the session.
    ///   The session joins the client context of its `client.id` and its host.
    ///   Its `client.id` stays what the first request said, whatever a later request says.
    /// - An `ApiVersions` request names the client software, when no earlier request did.
    ///
    /// The pipeline calls this for every request, at the Identify stage.
    /// A closed session takes nothing.
    ///
    /// What the session cannot keep is an [error](Identification::errors) of the identification.
    /// The session then stays as it was for that field, and the next request can still set it.
    pub fn identify(&mut self, request: &Request) -> Identification {
        let mut identification = Identification::default();
        if self.closed.is_some() {
            return identification;
        }
        if self.context.is_none() {
            match client_id_of(request) {
                Ok(client_id) => {
                    let client = self.identity.identify(client_id);
                    self.context = Some(self.contexts.join(&client));
                    identification.changed(IdentityChange::Identified(client));
                }
                Err(error) => identification.failed(error),
            }
        }
        if self.identity.client_software().is_none() {
            match client_software_of(request) {
                Ok(Some(software)) => {
                    self.identity.name_software(software.clone());
                    identification.changed(IdentityChange::SoftwareNamed(software));
                }
                Ok(None) => {}
                Err(error) => identification.failed(error),
            }
        }
        identification
    }

    /// What the client context of the session knows at this moment.
    ///
    /// A call reads it once, at the Identify stage, and works with that snapshot from start to finish.
    /// A session without a context, before its first request or after it closed, knows nothing.
    pub fn learned(&self) -> Arc<LearnedFields> {
        self.context.as_ref().map_or_default(ClientContext::learned)
    }

    /// A call of the session carried `value`, and every session of the same client instance knows it from now on.
    ///
    /// Returns whether the client context did not know the value yet.
    /// A session without a context learns nothing.
    pub fn learn(&self, value: impl LearnedValue) -> bool {
        self.context.as_ref().is_some_and(|context| context.learn(value))
    }

    /// The fields that every call of the session has at this moment:
    /// the identity fields, and what the client context provides for the learned fields.
    ///
    /// The pipeline adds the fields of the request.
    /// A request that carries a learned field replaces what the context provides for it.
    pub fn fields(&self) -> Fields {
        self.learned().provide(self.identity.fields())
    }

    /// The match counts of the rules that count `per: session`, and the counter key of the session.
    pub const fn counters(&self) -> &SessionCounters {
        &self.counters
    }

    /// The counters, to evaluate a trigger with.
    pub const fn counters_mut(&mut self) -> &mut SessionCounters {
        &mut self.counters
    }

    /// The calls in flight.
    pub const fn calls(&self) -> &InFlightCalls<C> {
        &self.calls
    }

    /// The calls in flight, to move them on.
    pub const fn calls_mut(&mut self) -> &mut InFlightCalls<C> {
        &mut self.calls
    }

    /// The calls in flight and the counters at once,
    /// for an evaluation that reads what a call keeps while it counts a match.
    pub const fn calls_and_counters_mut(&mut self) -> (&mut InFlightCalls<C>, &mut SessionCounters) {
        (&mut self.calls, &mut self.counters)
    }

    /// Whether a silence stopped the connection, and by which call.
    pub const fn stalled(&self) -> Option<Stalled> {
        self.calls.stalled()
    }

    /// The session closes at `now`, because `by` closed the connection.
    ///
    /// Returns the calls that were in flight, in the order their requests arrived: none of them gets a response.
    /// The session leaves its client context, which lingers for the sessions that come after it.
    ///
    /// A session closes once.
    /// Closing it again changes nothing and returns no call, so [`closed`](Self::closed) keeps who closed it first.
    pub fn close(&mut self, by: ClosedBy, now: Instant) -> Vec<InFlightCall<C>> {
        if self.closed.is_some() {
            return Vec::new();
        }
        self.closed = Some(Closed::new(by, now));
        if let Some(context) = self.context.take() {
            context.leave(now);
        }
        self.calls.end_all()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Duration;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_trigger::Counter;
    use gaslightmq_wire::messages::{ApiVersionsRequest, MetadataRequest};
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;
    use crate::{CallPhase, SessionError};

    const OPENED_AT: Instant = Instant::from_nanos(2_000_000_000);

    fn contexts() -> ClientContexts {
        ClientContexts::new(Duration::from_secs(60))
    }

    fn open(number: u64, contexts: &ClientContexts) -> Session<&'static str> {
        let connection = Connection::new(
            ConnectionNumber::new(number),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Broker(BrokerId::from(2)),
        );
        let ordinal = ConnectionOrdinal::new(number).unwrap();
        Session::open(connection, ordinal, contexts.clone(), CallIds::new(), OPENED_AT)
    }

    fn metadata(client_id: Option<&str>) -> Request {
        Request::new(12, 1, client_id, &MetadataRequest::default()).unwrap()
    }

    fn api_versions(client_id: &str, name: &'static str, version: &'static str) -> Request {
        let body = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(name))
            .with_client_software_version(StrBytes::from_static_str(version));
        Request::new(3, 1, Some(client_id), &body).unwrap()
    }

    fn client(client_id: &str) -> ClientInstance {
        ClientInstance::new(Some(ClientId::new(client_id).unwrap()), "10.0.4.27".parse().unwrap())
    }

    fn text(reading: Reading<FieldValues>) -> String {
        match reading {
            Reading::Present(values) => values.to_string(),
            Reading::Absent => "(absent)".to_owned(),
            Reading::Pending => "(pending)".to_owned(),
        }
    }

    #[test]
    fn a_new_session_is_opened_and_anonymous() {
        let contexts = contexts();
        let session = open(7, &contexts);

        assert_eq!(session.lifecycle(), Lifecycle::Opened);
        assert_eq!(session.opened_at(), OPENED_AT);
        assert_eq!(session.connection().number(), ConnectionNumber::new(7));
        assert_eq!(session.key().to_string(), "10.0.4.27 broker 2 #7");
        assert_eq!(session.counters().key().as_str(), "10.0.4.27 broker 2 #7");
        assert_eq!(session.client(), None);
        assert_eq!(session.closed(), None);
        assert_eq!(session.stalled(), None);
        assert!(session.calls().is_empty());
        assert!(session.learned().is_empty());
        assert!(contexts.is_empty());

        let fields = session.fields();
        assert_eq!(text(fields.read(Field::Connection)), "#7");
        assert_eq!(text(fields.read(Field::Endpoint)), "broker 2");
        assert_eq!(fields.read(Field::ClientId), Reading::Absent);
    }

    #[test]
    fn the_first_request_identifies_the_session() {
        let contexts = contexts();
        let mut session = open(7, &contexts);

        let identification = session.identify(&metadata(Some("order-ingest")));

        assert_eq!(
            identification.changes(),
            [IdentityChange::Identified(client("order-ingest"))]
        );
        assert_eq!(identification.errors(), []);
        assert_eq!(session.lifecycle(), Lifecycle::Identified);
        assert_eq!(session.client(), Some(&client("order-ingest")));
        assert_eq!(text(session.fields().read(Field::ClientId)), "order-ingest");
        assert_eq!(contexts.sessions(&client("order-ingest")), 1);
    }

    #[test]
    fn a_later_request_changes_nothing() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.identify(&metadata(Some("order-ingest")));

        let same = session.identify(&metadata(Some("order-ingest")));
        let other = session.identify(&metadata(Some("kafka-ui")));

        assert!(same.is_uneventful());
        assert!(other.is_uneventful());
        assert_eq!(session.client(), Some(&client("order-ingest")));
        assert_eq!(contexts.len(), 1);
    }

    #[test]
    fn a_first_request_without_a_client_id_identifies_the_session_too() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        let anonymous = ClientInstance::new(None, "10.0.4.27".parse().unwrap());

        let identification = session.identify(&metadata(None));

        assert_eq!(
            identification.changes(),
            [IdentityChange::Identified(anonymous.clone())]
        );
        assert_eq!(session.lifecycle(), Lifecycle::Identified);
        assert_eq!(session.identity().client_id(), None);
        assert_eq!(session.fields().read(Field::ClientId), Reading::Absent);
        assert_eq!(contexts.sessions(&anonymous), 1);
    }

    #[test]
    fn an_api_versions_request_names_the_client_software() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        let java = ClientSoftware::new("apache-kafka-java", "3.8.0").unwrap();

        let first = session.identify(&api_versions("order-ingest", "apache-kafka-java", "3.8.0"));
        let second = session.identify(&api_versions("order-ingest", "librdkafka", "2.6.1"));

        assert_eq!(
            first.changes(),
            [
                IdentityChange::Identified(client("order-ingest")),
                IdentityChange::SoftwareNamed(java.clone())
            ]
        );
        assert!(second.is_uneventful());
        assert_eq!(session.identity().client_software(), Some(&java));
        assert_eq!(
            text(session.fields().read(Field::ClientSoftware)),
            "apache-kafka-java 3.8.0"
        );
    }

    #[test]
    fn the_client_software_can_come_after_the_first_request() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.identify(&metadata(Some("order-ingest")));

        let identification = session.identify(&api_versions("order-ingest", "librdkafka", "2.6.1"));

        assert_eq!(
            identification.changes(),
            [IdentityChange::SoftwareNamed(
                ClientSoftware::new("librdkafka", "2.6.1").unwrap()
            )]
        );
    }

    #[test]
    fn software_that_the_session_cannot_keep_still_identifies_it() {
        let contexts = contexts();
        let mut session = open(7, &contexts);

        let identification = session.identify(&api_versions("order-ingest", "", ""));

        assert_eq!(
            identification.changes(),
            [IdentityChange::Identified(client("order-ingest"))]
        );
        assert!(matches!(
            identification.errors(),
            [SessionError::InvalidClientSoftware { .. }]
        ));
        assert_eq!(session.lifecycle(), Lifecycle::Identified);
        assert_eq!(session.identity().client_software(), None);

        // A later request that names it well sets it.
        let later = session.identify(&api_versions("order-ingest", "librdkafka", "2.6.1"));
        assert_eq!(later.changes().len(), 1);
        assert!(session.identity().client_software().is_some());
    }

    #[test]
    fn sessions_of_one_client_share_what_they_learn() {
        let contexts = contexts();
        let mut coordinator = open(1, &contexts);
        let mut fetcher = open(2, &contexts);
        let mut other = open(3, &contexts);
        coordinator.identify(&metadata(Some("analytics")));
        fetcher.identify(&metadata(Some("analytics")));
        other.identify(&metadata(Some("order-ingest")));

        assert!(coordinator.learn(GroupId::new("analytics-etl").unwrap()));
        assert!(!fetcher.learn(GroupId::new("analytics-etl").unwrap()));

        let group = fetcher.fields().read(Field::Group).present().unwrap();
        assert_eq!(group.to_string(), "analytics-etl");
        assert_eq!(group.origin(), Origin::ClientContext);
        assert_eq!(other.fields().read(Field::Group), Reading::Absent);
        assert_eq!(contexts.sessions(&client("analytics")), 2);
    }

    #[test]
    fn a_session_without_a_context_learns_nothing() {
        let contexts = contexts();
        let session = open(7, &contexts);

        assert!(!session.learn(GroupId::new("analytics-etl").unwrap()));
        assert!(session.learned().is_empty());
    }

    #[test]
    fn a_session_counts_the_matches_of_a_rule_for_itself() {
        let contexts = contexts();
        let mut first = open(1, &contexts);
        let mut second = open(2, &contexts);
        let rule = Counter::new();

        assert_eq!(first.counters_mut().count(&rule).value(), 1);
        assert_eq!(first.counters_mut().count(&rule).value(), 2);
        assert_eq!(second.counters_mut().count(&rule).value(), 1);
        assert_eq!(first.counters().matches(&rule), 2);
    }

    #[test]
    fn an_evaluation_reads_a_call_while_it_counts_a_match() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        let rule = Counter::new();
        let call = session.calls_mut().arrive("Produce", OPENED_AT);

        let (calls, counters) = session.calls_and_counters_mut();
        let in_flight = calls.get_mut(call).unwrap();
        let number = counters.count(&rule);
        *in_flight.content_mut() = "Produce, first match";

        assert_eq!(number.value(), 1);
        assert_eq!(*session.calls().get(call).unwrap().content(), "Produce, first match");
    }

    #[test]
    fn a_silence_stalls_the_session_and_leaves_it_open() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.identify(&metadata(Some("order-ingest")));
        let call = session.calls_mut().arrive("Produce", OPENED_AT);

        let stalled = session.calls_mut().silence(call, OPENED_AT).unwrap();

        assert_eq!(session.stalled(), Some(stalled));
        assert_eq!(session.lifecycle(), Lifecycle::Identified);

        // The client gives up.
        let ended = session.close(ClosedBy::Client, OPENED_AT + Duration::from_secs(30));
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].phase(), CallPhase::Silenced);
    }

    #[test]
    fn closing_records_who_closed_the_session_and_ends_its_calls() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.identify(&metadata(Some("order-ingest")));
        session.calls_mut().arrive("Produce", OPENED_AT);
        session.calls_mut().arrive("Fetch", OPENED_AT);
        let closed_at = OPENED_AT + Duration::from_secs(5);

        let ended = session.close(ClosedBy::Lie, closed_at);

        assert_eq!(
            ended.iter().map(|call| *call.content()).collect::<Vec<_>>(),
            ["Produce", "Fetch"]
        );
        assert_eq!(session.lifecycle(), Lifecycle::Closed);
        let closed = session.closed().unwrap();
        assert_eq!((closed.by(), closed.at()), (ClosedBy::Lie, closed_at));
        assert!(session.calls().is_empty());
        assert_eq!(session.client(), None);
        assert_eq!(contexts.sessions(&client("order-ingest")), 0);
    }

    #[test]
    fn a_session_closes_once() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.calls_mut().arrive("Produce", OPENED_AT);

        assert_eq!(session.close(ClosedBy::Lie, OPENED_AT).len(), 1);
        // The transport reports the close that the hang-up caused.
        let again = session.close(ClosedBy::Client, OPENED_AT + Duration::from_secs(1));

        assert_eq!(again.len(), 0);
        assert_eq!(session.closed().map(|closed| closed.by()), Some(ClosedBy::Lie));
        assert_eq!(session.closed().map(|closed| closed.at()), Some(OPENED_AT));
    }

    #[test]
    fn a_session_can_close_before_its_first_request() {
        let contexts = contexts();
        let mut session = open(7, &contexts);

        assert_eq!(session.close(ClosedBy::Client, OPENED_AT).len(), 0);

        assert_eq!(session.lifecycle(), Lifecycle::Closed);
        assert!(contexts.is_empty());
    }

    #[test]
    fn a_closed_session_takes_no_identity() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.close(ClosedBy::Backend, OPENED_AT);

        let identification = session.identify(&metadata(Some("order-ingest")));

        assert!(identification.is_uneventful());
        assert_eq!(session.lifecycle(), Lifecycle::Closed);
        assert_eq!(session.identity().client_id(), None);
        assert!(contexts.is_empty());
    }

    #[test]
    fn a_session_that_is_dropped_leaves_its_context() {
        let contexts = contexts();
        let mut session = open(7, &contexts);
        session.identify(&metadata(Some("order-ingest")));
        assert_eq!(contexts.sessions(&client("order-ingest")), 1);

        drop(session);

        assert_eq!(contexts.sessions(&client("order-ingest")), 0);
    }
}
