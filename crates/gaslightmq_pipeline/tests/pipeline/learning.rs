//! What the pipeline learns from its calls, and shares with the other sessions of its proxy.

use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_session::prelude::*;

use crate::driver::*;

#[test]
fn a_topic_that_a_request_names_by_id_has_its_name_once_the_cluster_told_it() {
    let flap = || on_partition("leader-flap", "orders.eu", "3", refusal(NOT_LEADER));
    let mut driver = Driver::with([flap()]);
    let fetch = fetch_by_id(41, &["orders.eu/3", "orders.eu/4"]);

    // Nobody knows the topic of this ID yet, so the request names no part, and the rule has nothing to match.
    let unnamed = driver.send(&fetch);
    assert_eq!(unnamed.forwarded().1.frame(), fetch.frame());
    let record = driver.answer(&unnamed.forwarded()).emitted();
    assert!(record.explanation().is_empty());
    assert_eq!(record.parts(), []);

    // The cluster names the topic with its ID in a Metadata response.
    let metadata = driver.send(&metadata(42, &["orders.eu", "payments.settled"]));
    driver.answer(&metadata.forwarded());
    let known = driver.shared.topics().names();
    assert_eq!(
        known.name_of(topic_id("orders.eu")).map(TopicName::as_str),
        Some("orders.eu")
    );

    // The same Fetch now names its partitions, and the refusal takes one of them out.
    let fetch = fetch_by_id(43, &["orders.eu/3", "orders.eu/4"]);
    let named = driver.send(&fetch);
    assert_eq!(asked(&named.forwarded().1), ["orders.eu/4"]);
    let answered = driver.answer(&named.forwarded());
    assert_eq!(
        told(&fetch, &answered.sent(&fetch)),
        ["orders.eu/3 NOT_LEADER_OR_FOLLOWER", "orders.eu/4 ok"]
    );
    assert_eq!(
        answered.emitted().verdict().to_string(),
        "NOT_LEADER_OR_FOLLOWER on orders.eu/3"
    );

    // Another connection of the proxy knows the topic too: the names belong to the cluster.
    let mut other = Driver::sharing(driver.shared.clone(), 8, snapshot([flap()], Instant::ZERO));
    let fetch = fetch_by_id(1, &["orders.eu/3", "orders.eu/4"]);
    assert_eq!(asked(&other.send(&fetch).forwarded().1), ["orders.eu/4"]);
}

#[test]
fn a_group_that_one_connection_names_targets_the_calls_of_another() {
    // "delay-the-fetch-of-the-group-analytics-etl". A Fetch never names a group.
    let of_the_group = Conditions::new()
        .with(Condition::is(ApiKey::FETCH))
        .with(Condition::parse(Field::Group, "analytics-etl").unwrap());
    let slow = || {
        Rule::new(
            "slow-fetch".parse().unwrap(),
            of_the_group.clone(),
            LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_secs(1))),
        )
    };
    let mut coordinator = Driver::with([slow()]);
    let mut fetcher = Driver::sharing(coordinator.shared.clone(), 8, snapshot([slow()], Instant::ZERO));

    // Before the client named its group, the rule does not match its Fetch.
    let before = fetcher.send(&fetch_by_id(1, &["orders.eu/3"]));
    assert!(fetcher.answer(&before.forwarded()).emitted().explanation().is_empty());

    // The client joins its group on the connection to its coordinator, and gets its member ID.
    let join = coordinator.send(&join_group(41));
    let joined = coordinator.answer(&join.forwarded()).emitted();
    let group = joined.read(Field::Group).present().unwrap();
    assert_eq!(group.origin(), Origin::Call);

    // The Fetch on the other connection now has the group, from the client context.
    let after = fetcher.send(&fetch_by_id(2, &["orders.eu/3"]));
    let timer = fetcher.answer(&after.forwarded()).timer();
    let record = fetcher.fire(timer).emitted();
    assert_eq!(record.verdict().to_string(), "+1 s");
    let group = record.read(Field::Group).present().unwrap();
    assert_eq!(
        (group.to_string(), group.origin()),
        ("analytics-etl".to_owned(), Origin::ClientContext)
    );

    // The member ID came with the truth of the JoinGroup.
    let client = ClientInstance::new(
        Some(ClientId::new("order-ingest").unwrap()),
        coordinator.pipeline.connection().host(),
    );
    let learned = coordinator.shared.contexts().learned(&client).unwrap();
    let members: Vec<&str> = learned.member_ids().map(MemberId::as_str).collect();
    assert_eq!(members, ["consumer-1-4f2a"]);
}

#[test]
fn a_session_that_closes_leaves_its_client_context() {
    let mut driver = Driver::with([]);
    driver.send(&heartbeat(41));
    let client = ClientInstance::new(
        Some(ClientId::new("order-ingest").unwrap()),
        driver.pipeline.connection().host(),
    );
    assert_eq!(driver.shared.contexts().sessions(&client), 1);

    driver.handle(gaslightmq_pipeline::Input::ClientClosed);

    // The context lingers for the sessions that come after it, and the engine expires it.
    assert_eq!(driver.shared.contexts().sessions(&client), 0);
    assert_eq!(driver.shared.contexts().len(), 1);
}
