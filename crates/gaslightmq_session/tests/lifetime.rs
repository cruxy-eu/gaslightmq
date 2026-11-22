//! A session from its first byte to its last, as the session spec tells it:
//! identity, the client context across connections, ordering, and what outlives a disconnect.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_session::prelude::*;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::Request;
use gaslightmq_wire::messages::{ApiVersionsRequest, FetchRequest, JoinGroupRequest, ProduceRequest};
use gaslightmq_wire::protocol::StrBytes;

const LINGER: Duration = Duration::from_secs(60);

/// What an engine keeps for all its sessions.
struct Proxy {
    contexts: ClientContexts,
    ordinals: ConnectionOrdinals,
    connections: u64,
}

impl Proxy {
    fn new() -> Self {
        Self {
            contexts: ClientContexts::new(LINGER),
            ordinals: ConnectionOrdinals::new(),
            connections: 0,
        }
    }

    /// A client on `10.0.4.27` connects to the endpoint of that broker.
    fn accept(&mut self, broker: i32, now: Instant) -> Session<Request> {
        self.connections += 1;
        let port = 51_000 + self.connections;
        let connection = Connection::new(
            ConnectionNumber::new(self.connections),
            format!("10.0.4.27:{port}").parse().unwrap(),
            Endpoint::Broker(BrokerId::try_from(broker).unwrap()),
        );
        let ordinal = self.ordinals.assign(connection.host(), connection.endpoint());
        Session::open(connection, ordinal, self.contexts.clone(), now)
    }
}

fn at(millis: u64) -> Instant {
    Instant::ZERO + Duration::from_millis(millis)
}

fn api_versions(client_id: &str) -> Request {
    let body = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("apache-kafka-java"))
        .with_client_software_version(StrBytes::from_static_str("3.8.0"));
    Request::new(3, 0, Some(client_id), &body).unwrap()
}

fn join_group(client_id: &str) -> Request {
    Request::new(5, 1, Some(client_id), &JoinGroupRequest::default()).unwrap()
}

fn fetch(client_id: &str, correlation_id: i32) -> Request {
    Request::new(12, correlation_id, Some(client_id), &FetchRequest::default()).unwrap()
}

fn produce(client_id: &str, correlation_id: i32) -> Request {
    Request::new(9, correlation_id, Some(client_id), &ProduceRequest::default()).unwrap()
}

fn text(reading: Reading<FieldValues>) -> String {
    match reading {
        Reading::Present(values) => values.to_string(),
        Reading::Absent => "(absent)".to_owned(),
        Reading::Pending => "(pending)".to_owned(),
    }
}

/// The correlation IDs of the requests that pass the request gate now.
fn forwarded(session: &mut Session<Request>) -> Vec<i32> {
    std::iter::from_fn(|| {
        session
            .calls_mut()
            .next_request()
            .map(|(_, request)| request.correlation_id())
    })
    .collect()
}

/// The correlation IDs of the calls whose responses leave now.
fn sent(session: &mut Session<Request>) -> Vec<i32> {
    std::iter::from_fn(|| {
        session
            .calls_mut()
            .next_response()
            .map(|call| call.content().correlation_id())
    })
    .collect()
}

#[test]
fn a_session_goes_from_opened_to_identified_to_closed() {
    let mut proxy = Proxy::new();
    let mut session = proxy.accept(2, at(0));

    // Opened: only the connection fields are known.
    assert_eq!(session.lifecycle(), Lifecycle::Opened);
    let fields = session.fields();
    assert_eq!(text(fields.read(Field::Connection)), "#1");
    assert_eq!(text(fields.read(Field::Address)), "10.0.4.27:51001");
    assert_eq!(text(fields.read(Field::Host)), "10.0.4.27");
    assert_eq!(text(fields.read(Field::Endpoint)), "broker 2");
    assert_eq!(fields.read(Field::ClientId), Reading::Absent);
    assert_eq!(fields.read(Field::ClientSoftware), Reading::Absent);

    // Identified: the first request carries `client.id`, and an ApiVersions request the client software.
    let identification = session.identify(&api_versions("order-ingest"));
    assert_eq!(session.lifecycle(), Lifecycle::Identified);
    assert_eq!(
        identification
            .changes()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [
            "identified as order-ingest from 10.0.4.27",
            "runs apache-kafka-java 3.8.0"
        ]
    );
    let fields = session.fields();
    assert_eq!(text(fields.read(Field::ClientId)), "order-ingest");
    assert_eq!(text(fields.read(Field::ClientSoftware)), "apache-kafka-java 3.8.0");
    for field in FieldGroup::Connection.fields() {
        assert!(fields.read(*field).is_present(), "{field}");
    }

    // Closed: the session records who closed it.
    session.close(ClosedBy::Backend, at(900));
    assert_eq!(session.lifecycle(), Lifecycle::Closed);
    let closed = session.closed().unwrap();
    assert_eq!((closed.by(), closed.at()), (ClosedBy::Backend, at(900)));
}

#[test]
fn a_fetch_gets_the_group_that_the_coordinator_connection_named() {
    let mut proxy = Proxy::new();

    // The consumer joins its group on the connection to its coordinator.
    let mut coordinator = proxy.accept(1, at(0));
    coordinator.identify(&join_group("analytics"));
    assert!(coordinator.learn(GroupId::new("analytics-etl").unwrap()));
    assert!(coordinator.learn(MemberId::new("consumer-1-4f2a").unwrap()));

    // It fetches on another connection, to another broker.
    let mut fetcher = proxy.accept(2, at(10));
    assert_eq!(
        fetcher.fields().read(Field::Group),
        Reading::Absent,
        "not identified yet"
    );
    fetcher.identify(&fetch("analytics", 1));

    let fields = fetcher.fields().with(ApiKey::FETCH);
    let group = fields.read(Field::Group).present().unwrap();
    assert_eq!(group.to_string(), "analytics-etl");
    assert_eq!(group.origin(), Origin::ClientContext);
    assert_eq!(text(fields.read(Field::MemberId)), "consumer-1-4f2a");

    // Another client on the same host has its own context.
    let mut ingest = proxy.accept(2, at(20));
    ingest.identify(&produce("order-ingest", 1));
    assert_eq!(ingest.fields().read(Field::Group), Reading::Absent);
}

#[test]
fn a_request_that_carries_a_learned_field_provides_it_alone() {
    let mut proxy = Proxy::new();
    let mut session = proxy.accept(1, at(0));
    session.identify(&join_group("analytics"));
    session.learn(GroupId::new("analytics-etl").unwrap());
    session.learn(GroupId::new("payments").unwrap());

    // The client context has two groups, and this request names one of them.
    let heartbeat = session
        .fields()
        .with(ApiKey::HEARTBEAT)
        .with(GroupId::new("payments").unwrap());

    let group = heartbeat.read(Field::Group).present().unwrap();
    assert_eq!(group.to_string(), "payments");
    assert_eq!(group.origin(), Origin::Call);
}

#[test]
fn a_call_keeps_the_learned_fields_it_started_with() {
    let mut proxy = Proxy::new();
    let mut coordinator = proxy.accept(1, at(0));
    let mut fetcher = proxy.accept(2, at(0));
    coordinator.identify(&join_group("analytics"));
    fetcher.identify(&fetch("analytics", 1));

    // A Fetch starts before the client names its group, and ends after.
    let in_flight = fetcher.fields().with(ApiKey::FETCH);
    coordinator.learn(GroupId::new("analytics-etl").unwrap());

    assert_eq!(in_flight.read(Field::Group), Reading::Absent);
    assert_eq!(text(fetcher.fields().read(Field::Group)), "analytics-etl");
}

#[test]
fn the_session_key_is_the_same_in_every_run() {
    // In one run the consumer connects first, in the other run another host does.
    let keys_of_a_run = |others_first: bool| {
        let mut proxy = Proxy::new();
        if others_first {
            let elsewhere: ClientHost = "10.0.4.99".parse().unwrap();
            proxy.connections += 3;
            proxy.ordinals.assign(elsewhere, Endpoint::Broker(BrokerId::from(2)));
        }
        let first = proxy.accept(2, at(0));
        let second = proxy.accept(2, at(0));
        (first.connection().number(), first.key(), second.key())
    };

    let (number, first, second) = keys_of_a_run(false);
    let (other_number, other_first, other_second) = keys_of_a_run(true);

    assert_ne!(
        number, other_number,
        "the connection number depends on who connected before"
    );
    assert_eq!((first, second), (other_first, other_second));
    assert_eq!(first.to_string(), "10.0.4.27 broker 2 #1");
    assert_eq!(second.to_string(), "10.0.4.27 broker 2 #2");
}

#[test]
fn a_rule_that_counts_per_session_counts_each_connection_apart() {
    let mut proxy = Proxy::new();
    let mut first = proxy.accept(2, at(0));
    let mut second = proxy.accept(2, at(0));

    // `trigger: { first: 2, per: session }`
    let trigger = TriggerParts::new()
        .with_first(Count::new(2).unwrap())
        .with_per(Per::Session);
    let die = Die::new(Seed::new(42), &DiceKey::new("Stale metadata").unwrap());
    let arming = Arming::new(trigger.into(), Lifetime::UntilDisarmed, die, at(0));
    let outcome = |session: &mut Session<Request>| {
        let evaluation = arming.evaluate(at(5), session.counters_mut());
        let outcome = evaluation.rolled().outcome();
        if let Some(firing) = evaluation.into_firing() {
            firing.confirm();
        }
        outcome
    };

    assert_eq!(outcome(&mut first), TriggerOutcome::Fired);
    assert_eq!(outcome(&mut first), TriggerOutcome::Fired);
    assert_eq!(outcome(&mut first), TriggerOutcome::Held);
    assert_eq!(
        outcome(&mut second),
        TriggerOutcome::Fired,
        "the second session counts from one"
    );
    assert_eq!(first.counters().key().as_str(), "10.0.4.27 broker 2 #1");
    assert_eq!(second.counters().key().as_str(), "10.0.4.27 broker 2 #2");
}

#[test]
fn a_delay_after_the_response_holds_back_the_responses_behind_it_and_no_request() {
    let mut proxy = Proxy::new();
    let mut session = proxy.accept(2, at(0));
    let delayed = session.calls_mut().arrive(produce("order-ingest", 1), at(0));
    let behind = session.calls_mut().arrive(produce("order-ingest", 2), at(1));

    // Both requests reach the backend on time.
    session.calls_mut().release_request(delayed).unwrap();
    session.calls_mut().release_request(behind).unwrap();
    assert_eq!(forwarded(&mut session), [1, 2]);

    // The backend answers both, and the delay holds the first response back.
    session.calls_mut().release_response(behind).unwrap();
    assert_eq!(sent(&mut session), Vec::<i32>::new());

    // The timer of the delay fires.
    session.calls_mut().release_response(delayed).unwrap();
    assert_eq!(sent(&mut session), [1, 2]);
    assert!(session.calls().is_empty());
}

#[test]
fn a_delay_before_forwarding_holds_back_the_requests_behind_it() {
    let mut proxy = Proxy::new();
    let mut session = proxy.accept(2, at(0));
    let delayed = session.calls_mut().arrive(produce("order-ingest", 1), at(0));
    let behind = session.calls_mut().arrive(produce("order-ingest", 2), at(1));

    session.calls_mut().release_request(behind).unwrap();
    assert_eq!(forwarded(&mut session), Vec::<i32>::new());

    // The timer of the delay fires.
    session.calls_mut().release_request(delayed).unwrap();
    assert_eq!(forwarded(&mut session), [1, 2]);
}

#[test]
fn a_silence_stalls_the_connection_until_the_client_gives_up() {
    let mut proxy = Proxy::new();
    let mut session = proxy.accept(2, at(0));
    session.identify(&produce("order-ingest", 1));
    let before = session.calls_mut().arrive(produce("order-ingest", 1), at(0));
    let silenced = session.calls_mut().arrive(produce("order-ingest", 2), at(1));
    let after = session.calls_mut().arrive(produce("order-ingest", 3), at(2));
    session.calls_mut().release_request(before).unwrap();
    assert_eq!(forwarded(&mut session), [1]);

    session.calls_mut().silence(silenced, at(3)).unwrap();
    session.calls_mut().release_request(after).unwrap();

    // The session stays open, and stops forwarding.
    assert_eq!(session.lifecycle(), Lifecycle::Identified);
    let stalled = session.stalled().unwrap();
    assert_eq!((stalled.by(), stalled.since()), (silenced, at(3)));
    assert_eq!(forwarded(&mut session), Vec::<i32>::new());

    // The call before the silence still gets its answer, and nothing after it does.
    session.calls_mut().release_response(before).unwrap();
    assert_eq!(sent(&mut session), [1]);
    assert_eq!(sent(&mut session), Vec::<i32>::new());

    // The client gives up.
    let ended = session.close(ClosedBy::Client, at(30_000));
    assert_eq!(
        ended
            .iter()
            .map(|call| (call.content().correlation_id(), call.phase()))
            .collect::<Vec<_>>(),
        [(2, CallPhase::Silenced), (3, CallPhase::RequestQueued)]
    );
}

#[test]
fn a_client_that_reconnects_gets_a_new_session_and_keeps_its_context() {
    let mut proxy = Proxy::new();
    let rule = Counter::new();

    let mut session = proxy.accept(1, at(0));
    session.identify(&join_group("analytics"));
    session.learn(GroupId::new("analytics-etl").unwrap());
    session.counters_mut().count(&rule);
    session.counters_mut().count(&rule);
    rule.count();
    session.calls_mut().arrive(join_group("analytics"), at(5));

    // A hang-up closes the real connection, and the session ends with it.
    let ended = session.close(ClosedBy::Lie, at(100));
    assert_eq!(ended.len(), 1);
    assert_eq!(session.closed().map(|closed| closed.by()), Some(ClosedBy::Lie));

    // The client reconnects: a new session, a new connection number and a new key.
    let mut reconnected = proxy.accept(1, at(600));
    assert_eq!(reconnected.lifecycle(), Lifecycle::Opened);
    assert_eq!(reconnected.connection().number(), ConnectionNumber::new(2));
    assert_eq!(reconnected.key().to_string(), "10.0.4.27 broker 1 #2");

    // It starts with no call in flight and its counters at zero.
    assert!(reconnected.calls().is_empty());
    assert_eq!(reconnected.counters().matches(&rule), 0);
    assert_eq!(reconnected.counters_mut().count(&rule).value(), 1);
    assert_eq!(
        reconnected.calls_mut().arrive(fetch("analytics", 1), at(600)).value(),
        1
    );

    // The client context and the rule's shared counter live in the engine, so they are still there.
    reconnected.identify(&fetch("analytics", 1));
    assert_eq!(text(reconnected.fields().read(Field::Group)), "analytics-etl");
    assert_eq!(rule.count().value(), 2);
}

#[test]
fn the_client_context_ends_when_the_last_session_has_been_closed_for_a_while() {
    let mut proxy = Proxy::new();
    let analytics = ClientInstance::new(Some(ClientId::new("analytics").unwrap()), "10.0.4.27".parse().unwrap());
    let linger = TimeSpan::from_duration(LINGER);

    let mut coordinator = proxy.accept(1, at(0));
    let mut fetcher = proxy.accept(2, at(0));
    coordinator.identify(&join_group("analytics"));
    fetcher.identify(&fetch("analytics", 1));
    coordinator.learn(GroupId::new("analytics-etl").unwrap());

    coordinator.close(ClosedBy::Client, at(1_000));
    assert_eq!(
        proxy.contexts.expire(at(1_000) + linger),
        [],
        "a session of the client is still open"
    );

    fetcher.close(ClosedBy::Client, at(5_000));
    assert_eq!(proxy.contexts.expire(at(4_999) + linger), []);
    assert_eq!(proxy.contexts.expire(at(5_000) + linger), [analytics]);

    // A client that comes back after that starts from nothing.
    let mut later = proxy.accept(1, at(900_000));
    later.identify(&fetch("analytics", 1));
    assert_eq!(later.fields().read(Field::Group), Reading::Absent);
}
