//! A silence stalls the session, and a hang-up closes it.

use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_pipeline::prelude::*;
use gaslightmq_session::prelude::*;
use gaslightmq_trigger::prelude::*;

use crate::driver::*;

#[test]
fn a_silence_never_answers_and_stalls_the_session() {
    let mut driver = Driver::with([on_api("blackhole", ApiKey::PRODUCE, LieDefinition::Silence)]);

    // The request is read and never answered: no forward, and no send.
    let silenced = driver.send(&produce(41, &["orders.eu/3"]));
    assert_eq!(silenced.kinds(), ["emit"]);
    let record = silenced.emitted();
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert_eq!(record.verdict().to_string(), "silence");
    assert_eq!(
        explained(&record),
        ["blackhole: matched orders.eu/3; match 1: fired; silence applied"]
    );
    assert_eq!(truth_of(&record, "orders.eu/3"), "none");
    assert_eq!(record.timing().forwarded_at(), None);

    // The session is stalled, and it stays open until the client gives up.
    let stalled = driver.pipeline.stalled().unwrap();
    assert_eq!(stalled.by(), CallNumber::FIRST);
    assert_eq!(driver.pipeline.closed(), None);

    // A request behind the silence never reaches the backend and never gets an answer.
    let behind = driver.send(&heartbeat(42));
    assert_eq!(behind.kinds(), ["emit"]);
    let record = behind.emitted();
    assert_eq!(record.id(), Driver::call(2));
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert!(record.verdict().is_pass());
    assert_eq!(driver.pipeline.in_flight(), 2);

    // The client gives up. Both calls have their record already.
    assert!(driver.handle(Input::ClientClosed).is_empty());
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Client)
    );
}

#[test]
fn a_call_before_a_silence_still_finishes() {
    let mut driver = Driver::with([on_api("blackhole", ApiKey::PRODUCE, LieDefinition::Silence)]);
    let first = heartbeat(41);

    let arrived = driver.send(&first);
    assert_eq!(driver.send(&produce(42, &["orders.eu/3"])).kinds(), ["emit"]);

    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(answered.emitted().id(), Driver::call(1));
}

#[test]
fn a_request_behind_a_silence_is_no_match_of_any_rule() {
    let blackhole = on_api("blackhole", ApiKey::PRODUCE, LieDefinition::Silence);
    let once =
        on_api("refuse-once", ApiKey::HEARTBEAT, refusal(REBALANCING)).with_lifetime(Lifetime::Firings(Count::ONE));
    let mut driver = Driver::with([blackhole, once]);

    driver.send(&produce(41, &["orders.eu/3"]));
    let behind = driver.send(&heartbeat(42)).emitted();

    // No rule was evaluated for it, so the rule counted no match and set no firing aside.
    assert!(behind.explanation().is_empty());
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"refuse-once".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().counter().matches(), 0);
    assert_eq!(rule.arming().budget().map(Budget::left), Some(1));
}

#[test]
fn a_hang_up_before_forwarding_closes_the_connection() {
    let mut driver = Driver::with([on_api("disconnect", ApiKey::HEARTBEAT, HangUpLie::new())]);

    let hung_up = driver.send(&heartbeat(41));

    // The request is dropped, and the connection closes.
    assert_eq!(hung_up.kinds(), ["emit", "close"]);
    let record = hung_up.emitted();
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);
    assert_eq!(record.verdict().to_string(), "hang-up");
    assert_eq!(record.timing().forwarded_at(), None);

    // The session ended, and a lie closed it.
    let closed = driver.pipeline.closed().unwrap();
    assert_eq!((closed.by(), closed.at()), (ClosedBy::Lie, driver.now()));
    assert_eq!(driver.pipeline.lifecycle(), Lifecycle::Closed);
    assert_eq!(driver.pipeline.in_flight(), 0);

    // A pipeline whose session ended takes no further input.
    assert!(driver.send(&heartbeat(42)).is_empty());
    assert!(driver.handle(Input::ClientClosed).is_empty());
}

#[test]
fn a_hang_up_after_the_response_closes_instead_of_sending() {
    let late = HangUpLie::new().at(Stage::AfterResponse);
    let mut driver = Driver::with([on_api("late-disconnect", ApiKey::PRODUCE, late)]);
    let request = produce(41, &["orders.eu/3"]);

    // The backend handles the request.
    let arrived = driver.send(&request);
    assert_eq!(arrived.forwarded().1.frame(), request.frame());

    // The connection closes before the client reads the answer.
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(answered.kinds(), ["emit", "close"]);
    let record = answered.emitted();
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);
    assert_eq!(record.verdict().to_string(), "hang-up after the response");
    // The records were written, and the record of the call shows that truth.
    assert_eq!(truth_of(&record, "orders.eu/3"), "ok");
    assert_eq!(driver.pipeline.closed().map(|closed| closed.by()), Some(ClosedBy::Lie));
}

#[test]
fn a_hang_up_ends_every_call_in_flight_and_their_firings_go_back() {
    let lost = on_api("lost-acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT))
        .with_lifetime(Lifetime::Firings(Count::ONE));
    let disconnect = on_api("disconnect", ApiKey::HEARTBEAT, HangUpLie::new());
    let mut driver = Driver::with([lost, disconnect]);

    // The Produce waits for the backend when the Heartbeat hangs up.
    assert_eq!(driver.send(&produce(41, &["orders.eu/3"])).kinds(), ["forward"]);
    let hung_up = driver.send(&heartbeat(42));

    // Both calls end, in the order they arrived, and the connection closes.
    assert_eq!(hung_up.kinds(), ["emit", "emit", "close"]);
    let [produce, heartbeat] = hung_up.calls().try_into().unwrap();
    assert_eq!(produce.delivered(), &Delivered::Nothing);
    assert_eq!(heartbeat.delivered(), &Delivered::ConnectionClosed);

    // The refusal of the Produce was never told, so the call passes and the firing is free again.
    assert!(produce.verdict().is_pass());
    assert_eq!(
        explained(&produce),
        [
            "lost-acknowledgement: matched orders.eu/3; match 1, 0 of 1 left: fired; \
          REQUEST_TIMED_OUT after the response not told: the call ended before the lie could be told"
        ]
    );
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"lost-acknowledgement".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().budget().map(Budget::left), Some(1));
}

#[test]
fn a_rule_that_waits_for_the_truth_is_pre_empted_when_a_silence_ends_the_call() {
    // The rule that waits has the higher priority, and it still loses: the truth never exists.
    let lost = on_success("lost-acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let blackhole = on_api("blackhole", ApiKey::PRODUCE, LieDefinition::Silence);
    let mut driver = Driver::with([lost, blackhole]);

    let record = driver.send(&produce(41, &["orders.eu/3"])).emitted();

    assert_eq!(
        explained(&record),
        [
            "lost-acknowledgement: pre-empted by blackhole, which ended the call before the truth",
            "blackhole: matched orders.eu/3; match 1: fired; silence applied",
        ]
    );
    // Nobody evaluated the rule, so the call is no match of it.
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"lost-acknowledgement".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().counter().matches(), 0);
}

#[test]
fn a_client_that_closes_ends_its_calls_without_an_answer() {
    let mut driver = Driver::with([]);
    let arrived = driver.send(&produce(41, &["orders.eu/3"]));

    let closed = driver.handle(Input::ClientClosed);

    // The call got no answer, and its record says so. The transport is closed already, so nothing asks to close it.
    assert_eq!(closed.kinds(), ["emit"]);
    assert_eq!(closed.emitted().delivered(), &Delivered::Nothing);
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Client)
    );

    // The backend answers a call whose session ended, and nothing happens.
    assert!(driver.answer(&arrived.forwarded()).is_empty());
}

#[test]
fn a_backend_without_an_answer_ends_the_session_and_every_record_says_why() {
    let mut driver = Driver::with([]);
    let first = driver.send(&heartbeat(41)).forwarded();
    let second = driver.send(&produce(42, &["orders.eu/3"])).forwarded();

    // The backend lost the cluster while the second call was there.
    let failure = "the connection to the cluster ended before the answer arrived: reset by the broker";
    let ended = driver.handle(Input::BackendFailed {
        call: second.0,
        reason: failure.to_owned(),
    });

    // No call gets an answer, and nothing is made up for the client: the connection closes.
    assert_eq!(ended.kinds(), ["emit", "emit", "close"]);
    assert_eq!(ended.sends().len(), 0, "a failure is no response");
    let [heartbeat, produce] = ended.calls().try_into().unwrap();
    assert_eq!(heartbeat.delivered(), &Delivered::Nothing);
    assert_eq!(produce.delivered(), &Delivered::ConnectionClosed);
    for record in [&heartbeat, &produce] {
        assert_eq!(
            record.explanation().failure().map(Failure::message),
            Some(format!("the backend has no answer for call #7.2: {failure}").as_str())
        );
        assert!(record.verdict().is_pass(), "a failure of the backend is no lie");
    }

    // The backend closed the session, and the pipeline takes no further input.
    let closed = driver.pipeline.closed().unwrap();
    assert_eq!((closed.by(), closed.at()), (ClosedBy::Backend, driver.now()));
    assert_eq!(driver.pipeline.in_flight(), 0);
    assert!(driver.answer(&first).is_empty());
}

#[test]
fn a_failure_ends_the_session_whichever_call_of_the_connection_it_names() {
    let mut driver = Driver::with([]);

    // A Produce without a response leaves at once, and a Heartbeat waits for the backend.
    let no_acks = driver.send(&produce_with_acks(41, &["orders.eu/3"], 0));
    assert_eq!(no_acks.kinds(), ["forward", "emit"]);
    driver.send(&heartbeat(42));

    // A failure for a call of another connection is not for this session.
    let elsewhere = driver.handle(Input::BackendFailed {
        call: "#8.1".parse().unwrap(),
        reason: "the backend cannot reach the cluster".to_owned(),
    });
    assert!(elsewhere.is_empty());
    assert_eq!(driver.pipeline.closed(), None);

    // The backend could not hand on the Produce. That call is gone, and the session ends all the same.
    let ended = driver.handle(Input::BackendFailed {
        call: no_acks.forwarded().0,
        reason: "the backend cannot reach the cluster".to_owned(),
    });
    assert_eq!(ended.kinds(), ["emit", "close"]);
    let record = ended.emitted();
    assert_eq!(record.id(), Driver::call(2));
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some("the backend has no answer for call #7.1: the backend cannot reach the cluster")
    );
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Backend)
    );
}

#[test]
fn a_backend_that_closes_the_session_ends_every_call_in_flight() {
    let mut driver = Driver::with([]);
    driver.send(&heartbeat(41));
    driver.send(&produce(42, &["orders.eu/3"]));

    // The broker of the endpoint went down.
    let ended = driver.handle(Input::BackendClosed {
        reason: "endpoint `broker 2` accepts no connections any more".to_owned(),
    });

    assert_eq!(ended.kinds(), ["emit", "emit", "close"]);
    for record in ended.calls() {
        assert_eq!(record.delivered(), &Delivered::Nothing);
        assert_eq!(
            record.explanation().failure().map(Failure::message),
            Some("the backend closed the session: endpoint `broker 2` accepts no connections any more")
        );
    }
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Backend)
    );

    // A session that ended ends once: the client that closes afterwards changes nothing.
    assert!(driver.handle(Input::ClientClosed).is_empty());
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Backend)
    );
}

#[test]
fn bytes_that_are_no_request_close_the_connection() {
    let mut driver = Driver::with([]);
    let arrived = driver.send(&heartbeat(41));
    assert_eq!(arrived.kinds(), ["forward"]);

    // A frame of two bytes has no request header.
    let closed = driver.handle(Input::ClientBytes(&[0, 0, 0, 2, 0xFF, 0xFF]));

    // The proxy closes the connection itself, as a broker does, and the call in flight ends.
    assert_eq!(closed.kinds(), ["emit", "close"]);
    assert_eq!(closed.emitted().delivered(), &Delivered::Nothing);
    assert_eq!(
        driver.pipeline.closed().map(|closed| closed.by()),
        Some(ClosedBy::Proxy)
    );
}

#[test]
fn a_lie_whose_turn_never_came_is_not_told_when_the_session_ends() {
    let rebalance = on_api("rebalance", ApiKey::HEARTBEAT, refusal(REBALANCING));
    let lost = on_topic("lost-acknowledgement", "orders.*", late_refusal(TIMED_OUT));
    let mut driver = Driver::with([rebalance, lost]);

    // The first Produce waits for the backend, so everything behind it waits for its answer.
    driver.send(&produce(41, &["payments.settled/0"]));
    // The second Produce is answered, and its refused answer waits for its turn.
    let second = driver.send(&produce(42, &["orders.eu/3"])).forwarded();
    assert!(driver.answer(&second).is_empty());
    // The Heartbeat is refused without the backend. That happened: the backend never saw it.
    assert!(driver.send(&heartbeat(43)).is_empty());

    // The client gives up before any answer left.
    let closed = driver.handle(Input::ClientClosed);
    let [first, second, third] = closed.calls().try_into().unwrap();

    assert!(first.verdict().is_pass());
    // The client never heard that its orders failed, so that lie was not told.
    assert!(second.verdict().is_pass());
    let fate = second
        .explanation()
        .rule("lost-acknowledgement")
        .unwrap()
        .lie()
        .unwrap();
    assert_eq!(
        fate.failure().map(Failure::message),
        Some("the call ended before the lie could be told")
    );
    // The refusal before forwarding kept the Heartbeat from the backend, so it applied.
    assert_eq!(third.verdict().to_string(), "REBALANCE_IN_PROGRESS");
    assert_eq!(third.delivered(), &Delivered::Nothing);
}

#[test]
fn a_request_behind_a_silence_teaches_the_client_context_nothing() {
    let mut driver = Driver::with([on_api("blackhole", ApiKey::PRODUCE, LieDefinition::Silence)]);
    driver.send(&produce(41, &["orders.eu/3"]));

    // The request names a group, and it never reaches the backend: nothing happens to it.
    let behind = driver.send(&join_group(42)).emitted();

    // Its record still shows what it asked.
    let group = behind.read(Field::Group).present().unwrap();
    assert_eq!(group.to_string(), "analytics-etl");
    // The other sessions of the client do not learn the group from it.
    let client = ClientInstance::new(
        Some(ClientId::new("order-ingest").unwrap()),
        driver.pipeline.connection().host(),
    );
    let learned = driver.shared.contexts().learned(&client).unwrap();
    assert_eq!(learned.groups().count(), 0);
}

#[test]
fn a_request_behind_a_silence_does_not_identify_the_session() {
    // The silence takes every call, also the first one of the connection.
    let everything = gaslightmq_rule::Rule::new(
        "blackhole".parse().unwrap(),
        gaslightmq_selector::Conditions::new(),
        LieDefinition::Silence,
    );
    let mut driver = Driver::with([everything]);
    let api_versions = |correlation_id, software: &'static str| {
        let body = gaslightmq_wire::messages::ApiVersionsRequest::default()
            .with_client_software_name(gaslightmq_wire::protocol::StrBytes::from_static_str(software))
            .with_client_software_version(gaslightmq_wire::protocol::StrBytes::from_static_str("2.6.1"));
        gaslightmq_wire::Request::new(3, correlation_id, Some("order-ingest"), &body).unwrap()
    };

    // The first request identifies the session, and then the silence takes it.
    driver.send(&heartbeat(41));
    assert_eq!(driver.pipeline.identity().client_software(), None);

    // A request behind the silence names the client software, and the session does not take it.
    driver.send(&api_versions(42, "librdkafka"));
    assert_eq!(driver.pipeline.identity().client_software(), None);
}
