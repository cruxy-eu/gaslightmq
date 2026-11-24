//! The targeting spec, example by example: each rule of `docs/specs/domains/targeting.md` as a plain function call.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_selector::prelude::*;

fn name(text: &str) -> SelectorName {
    text.parse().unwrap()
}

/// The condition that a file writes as `field: text`.
fn on(field: Field, text: &str) -> Condition {
    Condition::parse(field, text).unwrap()
}

fn is_not(field: Field, text: &str) -> Condition {
    Condition::is_not(field.parse_value(text).unwrap())
}

fn is_any_of(field: Field, texts: &[&str]) -> Condition {
    Condition::is_any_of(texts.iter().map(|text| field.parse_value(text).unwrap())).unwrap()
}

fn all(conditions: impl IntoIterator<Item = Condition>) -> EffectiveSelector {
    conditions.into_iter().collect()
}

fn matched(parts: &[&str]) -> Match {
    Match::Matched(Target::of_parts(parts.iter().map(|part| part.parse().unwrap())).unwrap())
}

fn partition(text: &str) -> TopicPartition {
    text.parse().unwrap()
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn group(id: &str) -> GroupId {
    GroupId::new(id).unwrap()
}

/// A request of `order-ingest` on connection #7, which comes from 10.0.4.27 and reaches broker 2.
fn from_ingest(api: ApiKey) -> Fields {
    Fields::new()
        .with(ConnectionNumber::new(7))
        .with("10.0.4.27:51459".parse::<ClientAddress>().unwrap())
        .with("10.0.4.27".parse::<ClientHost>().unwrap())
        .with(Endpoint::Broker(BrokerId::from(2)))
        .with(client("order-ingest"))
        .with(api)
        .with(ApiVersion::from(11))
}

fn produce(partitions: &[&str]) -> Fields {
    partitions
        .iter()
        .fold(from_ingest(ApiKey::PRODUCE).with(Acks::ALL), |call, text| {
            call.with_part(partition(text))
        })
}

/// The selectors of the spec, and the scope-like ones a workspace has next to them.
fn workspace() -> SelectorSet {
    SelectorSet::new([
        Selector::new(
            name("order-producers"),
            Conditions::new()
                .with(on(Field::Api, "Produce"))
                .with(on(Field::Topic, "orders.*")),
        ),
        Selector::new(
            name("order-producers-from-ingest"),
            on(Field::ClientId, "order-ingest").into(),
        )
        .including(name("order-producers")),
    ])
    .unwrap()
}

// Multi-valued fields

#[test]
fn a_positive_condition_matches_when_any_value_matches() {
    let call = produce(&["orders.eu/1", "payments.settled/3"]);

    assert_eq!(
        all([on(Field::Topic, "payments.settled")]).evaluate(&call),
        matched(&["payments.settled/3"])
    );
    assert_eq!(all([on(Field::Topic, "orders.us")]).evaluate(&call), Match::NotMatched);
}

#[test]
fn is_not_matches_when_no_value_equals() {
    // `topic is not orders.eu` does not match a Produce that carries `orders.eu` and `payments.settled`.
    let not_eu = all([is_not(Field::Topic, "orders.eu")]);

    assert_eq!(
        not_eu.evaluate(&produce(&["orders.eu/1", "payments.settled/3"])),
        Match::NotMatched
    );
    assert_eq!(
        not_eu.evaluate(&produce(&["payments.settled/3"])),
        matched(&["payments.settled/3"])
    );
}

#[test]
fn conditions_on_topic_and_partition_apply_to_the_same_partition() {
    // `topic is orders.eu` with `partition is 3` matches `orders.eu/3`,
    // not a request that carries `orders.eu/1` and `payments.settled/3`.
    let eu_3 = all([on(Field::Topic, "orders.eu"), on(Field::Partition, "3")]);

    assert_eq!(eu_3.evaluate(&produce(&["orders.eu/3"])), matched(&["orders.eu/3"]));
    assert_eq!(
        eu_3.evaluate(&produce(&["orders.eu/1", "payments.settled/3"])),
        Match::NotMatched
    );
    assert_eq!(
        eu_3.evaluate(&produce(&["orders.eu/1", "orders.eu/3", "payments.settled/3"])),
        matched(&["orders.eu/3"])
    );
}

#[test]
fn the_partitions_that_satisfy_the_conditions_are_the_target() {
    // A selector on `orders.*` matches a Produce for `orders.eu` and `payments.settled`,
    // and the rule's lie applies to the `orders.eu` partitions only.
    let orders = all([on(Field::Topic, "orders.*")]);
    let call = produce(&["orders.eu/3", "payments.settled/0", "orders.eu/4", "orders.us/0"]);

    assert_eq!(
        orders.evaluate(&call),
        matched(&["orders.eu/3", "orders.eu/4", "orders.us/0"])
    );
}

#[test]
fn a_selector_that_tests_no_part_targets_every_part() {
    let call = produce(&["orders.eu/3", "payments.settled/0"]);

    assert_eq!(
        all([on(Field::Api, "Produce")]).evaluate(&call),
        matched(&["orders.eu/3", "payments.settled/0"])
    );
    assert_eq!(
        EffectiveSelector::new().evaluate(&call),
        Match::Matched(call.whole_target())
    );
}

#[test]
fn is_not_leaves_the_target_to_the_other_conditions() {
    let call = produce(&["orders.eu/3", "orders.us/0", "payments.settled/0"]);

    assert_eq!(
        all([on(Field::Topic, "orders.*"), is_not(Field::Topic, "orders.de")]).evaluate(&call),
        matched(&["orders.eu/3", "orders.us/0"])
    );
    assert_eq!(
        all([on(Field::Topic, "orders.*"), is_not(Field::Topic, "orders.us")]).evaluate(&call),
        Match::NotMatched
    );
    assert_eq!(
        all([on(Field::Topic, "orders.*"), is_not(Field::Partition, "3")]).evaluate(&call),
        Match::NotMatched
    );
}

#[test]
fn on_a_request_without_partitions_the_target_is_the_whole_call() {
    let heartbeat = from_ingest(ApiKey::HEARTBEAT).with(group("analytics-etl"));

    assert_eq!(
        all([on(Field::Group, "analytics-etl")]).evaluate(&heartbeat),
        Match::Matched(Target::Call)
    );
    assert_eq!(
        all([on(Field::Topic, "orders.eu")]).evaluate(&heartbeat),
        Match::NotMatched
    );
    assert_eq!(all([on(Field::Topic, "*")]).evaluate(&heartbeat), Match::NotMatched);
    assert_eq!(
        all([is_not(Field::Topic, "orders.eu")]).evaluate(&heartbeat),
        Match::Matched(Target::Call)
    );
}

#[test]
fn on_a_request_that_names_topics_only_the_target_is_a_set_of_topics() {
    let metadata = from_ingest(ApiKey::METADATA)
        .with_part(TopicName::new("orders.eu").unwrap())
        .with_part(TopicName::new("payments.settled").unwrap());

    assert_eq!(
        all([on(Field::Topic, "orders.*")]).evaluate(&metadata),
        matched(&["orders.eu"])
    );
    assert_eq!(all([on(Field::Partition, "3")]).evaluate(&metadata), Match::NotMatched);
}

#[test]
fn a_field_that_the_request_does_not_have_fails_every_positive_condition() {
    let fetch = from_ingest(ApiKey::FETCH).with_part(partition("orders.eu/3"));

    for condition in [
        on(Field::Acks, "-1"),
        on(Field::Acks, "*"),
        is_any_of(Field::Acks, &["-1", "1"]),
    ] {
        assert_eq!(
            all([condition.clone()]).evaluate(&fetch),
            Match::NotMatched,
            "{condition}"
        );
    }
    assert_eq!(
        all([is_not(Field::Acks, "-1")]).evaluate(&fetch),
        matched(&["orders.eu/3"])
    );
}

// Conditions

#[test]
fn is_equals_one_value() {
    // `api is Produce`
    let is_produce = all([on(Field::Api, "Produce")]);

    assert!(is_produce.evaluate(&from_ingest(ApiKey::PRODUCE)).is_matched());
    assert!(!is_produce.evaluate(&from_ingest(ApiKey::FETCH)).is_matched());
}

#[test]
fn is_any_of_equals_one_of_several_values() {
    // `api is any of JoinGroup, SyncGroup`
    let group_apis = all([is_any_of(Field::Api, &["JoinGroup", "SyncGroup"])]);

    assert!(group_apis.evaluate(&from_ingest(ApiKey::JOIN_GROUP)).is_matched());
    assert!(group_apis.evaluate(&from_ingest(ApiKey::SYNC_GROUP)).is_matched());
    assert!(!group_apis.evaluate(&from_ingest(ApiKey::HEARTBEAT)).is_matched());
}

#[test]
fn matches_is_a_glob_match() {
    // `topic matches orders.*`
    let orders = all([on(Field::Topic, "orders.*")]);

    assert_eq!(orders.conditions()[0].operator(), Operator::Matches);
    assert!(orders.evaluate(&produce(&["orders.eu/3"])).is_matched());
    assert!(orders.evaluate(&produce(&["orders.us/0"])).is_matched());
    assert!(!orders.evaluate(&produce(&["payments.settled/0"])).is_matched());
    assert!(!orders.evaluate(&produce(&["reorders.eu/0"])).is_matched());
}

#[test]
fn a_glob_works_on_every_field() {
    let call = produce(&["orders.eu/3"]).with(ClientSoftware::new("librdkafka", "2.6.1").unwrap());

    for (field, pattern) in [
        (Field::ClientId, "order-*"),
        (Field::ClientSoftware, "librdkafka 2.*"),
        (Field::Host, "10.0.4.*"),
        (Field::Endpoint, "broker ?"),
        (Field::Api, "Prod*"),
        (Field::Connection, "#*"),
    ] {
        assert!(
            all([on(field, pattern)]).evaluate(&call).is_matched(),
            "{field} matches {pattern}"
        );
    }
    assert!(
        !all([on(Field::ClientSoftware, "apache-kafka-java *")])
            .evaluate(&call)
            .is_matched()
    );
}

#[test]
fn is_not_does_not_equal() {
    // `client.id is not kafka-ui`
    let not_the_ui = all([is_not(Field::ClientId, "kafka-ui")]);
    let from_the_ui = Fields::new().with(client("kafka-ui")).with(ApiKey::METADATA);

    assert!(not_the_ui.evaluate(&from_ingest(ApiKey::METADATA)).is_matched());
    assert!(!not_the_ui.evaluate(&from_the_ui).is_matched());
}

#[test]
fn is_not_with_several_values_equals_none_of_them() {
    let tools = ValueSet::new([client("kafka-ui"), client("kafka-exporter")]).unwrap();
    let no_tool = all([Condition::IsNot(tools)]);
    let from = |id: &str| Fields::new().with(client(id)).with(ApiKey::METADATA);

    assert!(no_tool.evaluate(&from("order-ingest")).is_matched());
    assert!(!no_tool.evaluate(&from("kafka-ui")).is_matched());
    assert!(!no_tool.evaluate(&from("kafka-exporter")).is_matched());
}

#[test]
fn is_not_matches_a_session_that_is_not_identified() {
    let opened = Fields::new().with(ConnectionNumber::new(7)).with(Endpoint::Bootstrap);

    assert!(
        all([is_not(Field::ClientId, "kafka-ui")])
            .evaluate(&opened)
            .is_matched()
    );
    assert!(
        !all([on(Field::ClientId, "order-ingest")])
            .evaluate(&opened)
            .is_matched()
    );
    assert!(all([on(Field::Endpoint, "bootstrap")]).evaluate(&opened).is_matched());
}

#[test]
fn values_within_one_condition_combine_with_or_and_conditions_with_and() {
    let selector = all([
        is_any_of(Field::Api, &["Produce", "Fetch"]),
        on(Field::ClientId, "order-ingest"),
    ]);
    let other_client = |api: ApiKey| Fields::new().with(client("payments-svc")).with(api);

    assert!(selector.evaluate(&from_ingest(ApiKey::PRODUCE)).is_matched());
    assert!(selector.evaluate(&from_ingest(ApiKey::FETCH)).is_matched());
    assert!(!selector.evaluate(&from_ingest(ApiKey::METADATA)).is_matched());
    assert!(!selector.evaluate(&other_client(ApiKey::PRODUCE)).is_matched());
}

// Learned fields

#[test]
fn a_fetch_matches_its_group_once_the_client_named_the_group() {
    // `api is Fetch` with `group is analytics-etl` matches the fetches of that group's consumers,
    // once any connection of the same client named the group.
    let selector = all([on(Field::Api, "Fetch"), on(Field::Group, "analytics-etl")]);
    let fetch = from_ingest(ApiKey::FETCH).with_part(partition("orders.eu/3"));

    assert_eq!(selector.evaluate(&fetch), Match::NotMatched);
    assert_eq!(
        selector.evaluate(&fetch.with_learned(group("analytics-etl"))),
        matched(&["orders.eu/3"])
    );
}

#[test]
fn a_client_with_several_consumers_has_several_groups() {
    let fetch = from_ingest(ApiKey::FETCH)
        .with_learned(group("payments"))
        .with_learned(group("analytics-etl"));

    assert!(all([on(Field::Group, "payments")]).evaluate(&fetch).is_matched());
    assert!(all([on(Field::Group, "analytics-etl")]).evaluate(&fetch).is_matched());
    assert!(all([on(Field::Group, "*-etl")]).evaluate(&fetch).is_matched());
    assert!(!all([is_not(Field::Group, "payments")]).evaluate(&fetch).is_matched());
    // Two conditions on one field, as an include gives them: the client has both groups.
    assert!(
        all([on(Field::Group, "payments"), on(Field::Group, "analytics-etl")])
            .evaluate(&fetch)
            .is_matched()
    );
}

#[test]
fn a_request_that_carries_its_group_matches_that_group_only() {
    let commit = from_ingest(ApiKey::OFFSET_COMMIT)
        .with_learned(group("payments"))
        .with(group("analytics-etl"));

    assert!(all([on(Field::Group, "analytics-etl")]).evaluate(&commit).is_matched());
    assert!(!all([on(Field::Group, "payments")]).evaluate(&commit).is_matched());
}

// Selectors

#[test]
fn a_selector_is_a_named_set_of_conditions() {
    let selectors = workspace();
    let order_producers = selectors.effective(&name("order-producers")).unwrap();

    assert_eq!(order_producers.to_string(), "api is Produce and topic matches orders.*");
    assert_eq!(
        order_producers.evaluate(&produce(&["orders.eu/3", "payments.settled/0"])),
        matched(&["orders.eu/3"])
    );
    assert_eq!(
        order_producers.evaluate(&from_ingest(ApiKey::FETCH).with_part(partition("orders.eu/3"))),
        Match::NotMatched
    );
}

#[test]
fn the_included_conditions_combine_with_and() {
    let selectors = workspace();
    let from_ingest = selectors.effective(&name("order-producers-from-ingest")).unwrap();
    let from_payments = Fields::new()
        .with(client("payments-svc"))
        .with(ApiKey::PRODUCE)
        .with_part(partition("orders.eu/3"));

    assert_eq!(
        from_ingest.to_string(),
        "api is Produce and topic matches orders.* and client.id is order-ingest"
    );
    assert_eq!(
        from_ingest.evaluate(&produce(&["orders.eu/3"])),
        matched(&["orders.eu/3"])
    );
    assert_eq!(from_ingest.evaluate(&from_payments), Match::NotMatched);
    assert_eq!(
        from_ingest.evaluate(&produce(&["payments.settled/0"])),
        Match::NotMatched
    );
}

#[test]
fn an_include_and_its_selector_can_test_the_same_field() {
    let selectors = SelectorSet::new([
        Selector::new(name("orders"), on(Field::Topic, "orders.*").into()),
        Selector::new(name("european-orders"), on(Field::Topic, "*.eu").into()).including(name("orders")),
    ])
    .unwrap();
    let european_orders = selectors.effective(&name("european-orders")).unwrap();

    assert_eq!(
        european_orders.evaluate(&produce(&["orders.eu/3", "orders.us/0", "payments.eu/1"])),
        matched(&["orders.eu/3"])
    );
}

#[test]
fn things_reference_selectors_by_name_and_a_name_that_no_selector_has_does_not_resolve() {
    let selectors = workspace();

    assert_eq!(
        selectors
            .get(&name("order-producers-from-ingest"))
            .and_then(Selector::include),
        Some(&name("order-producers"))
    );
    assert_eq!(
        selectors.resolve(&SelectorRef::Named(name("order-writers"))),
        Err(SelectorSetError::UnknownSelector {
            name: name("order-writers")
        })
    );
}

#[test]
fn a_rule_names_a_selector_or_writes_its_conditions_inline() {
    let selectors = workspace();
    let by_name = SelectorRef::Named(name("order-producers"));
    let inline = SelectorRef::Inline(on(Field::Api, "Metadata").into());

    assert!(
        selectors
            .resolve(&by_name)
            .unwrap()
            .evaluate(&produce(&["orders.eu/3"]))
            .is_matched()
    );
    assert!(
        selectors
            .resolve(&inline)
            .unwrap()
            .evaluate(&from_ingest(ApiKey::METADATA))
            .is_matched()
    );
}

// Building selectors from traffic

#[test]
fn ticking_a_field_of_a_recorded_call_adds_a_condition_with_the_calls_value() {
    let recorded = produce(&["orders.eu/3", "orders.us/0"]);
    let ticked = [Field::ClientId, Field::Api, Field::Topic, Field::Acks];

    let conditions: Conditions = ticked
        .into_iter()
        .filter_map(|field| recorded.read(field).present())
        .map(|values| Condition::from(&values))
        .collect();

    assert_eq!(
        conditions.to_string(),
        "client.id is order-ingest and api is Produce and topic is any of orders.eu, orders.us and acks is -1"
    );
    assert_eq!(
        EffectiveSelector::from(conditions.clone()).evaluate(&recorded),
        Match::Matched(recorded.whole_target())
    );

    // A value can be generalised into a glob, such as `orders.eu` into `orders.*`.
    let generalised = conditions.with(on(Field::Topic, "orders.*"));
    let another = produce(&["orders.de/1", "payments.settled/0"]);
    assert_eq!(
        EffectiveSelector::from(generalised).evaluate(&another),
        matched(&["orders.de/1"])
    );
}

// Scope

#[test]
fn the_common_scopes_say_who_something_applies_to() {
    let ingest_7 = from_ingest(ApiKey::PRODUCE);
    let ingest_8_elsewhere = Fields::new()
        .with(ConnectionNumber::new(8))
        .with("10.0.4.99".parse::<ClientHost>().unwrap())
        .with(Endpoint::Broker(BrokerId::from(1)))
        .with(client("order-ingest"))
        .with(ApiKey::PRODUCE);
    let ui_9 = Fields::new()
        .with(ConnectionNumber::new(9))
        .with("10.0.4.27".parse::<ClientHost>().unwrap())
        .with(Endpoint::Broker(BrokerId::from(2)))
        .with(client("kafka-ui"))
        .with(ApiKey::METADATA);

    let host = || "10.0.4.27".parse::<ClientHost>().unwrap();
    let cases = [
        (Scope::all(), [true, true, true]),
        (Scope::client(client("order-ingest")), [true, true, false]),
        (
            Scope::client_instance(client("order-ingest"), host()),
            [true, false, false],
        ),
        (Scope::connection(ConnectionNumber::new(7)), [true, false, false]),
        (
            Scope::endpoint(Endpoint::Broker(BrokerId::from(2))),
            [true, false, true],
        ),
    ];

    for (scope, expected) in cases {
        let effective = EffectiveSelector::from(scope.clone());
        let applies = [&ingest_7, &ingest_8_elsewhere, &ui_9].map(|call| effective.evaluate(call).is_matched());
        assert_eq!(applies, expected, "{scope}");
        assert!(effective.is_scope(), "{scope}");
    }
}

#[test]
fn a_scope_is_a_selector_that_only_uses_connection_fields() {
    let who = Conditions::new()
        .with(on(Field::ClientId, "order-*"))
        .with(is_not(Field::Host, "10.0.4.99"));

    assert_eq!(Scope::new(who.clone()).map(Scope::into_conditions), Ok(who.clone()));
    assert_eq!(
        Scope::new(who.clone().with(on(Field::Topic, "orders.*"))),
        Err(ScopeError::NotAConnectionField { field: Field::Topic })
    );
    assert_eq!(
        Scope::new(who.with(on(Field::Group, "analytics-etl"))),
        Err(ScopeError::NotAConnectionField { field: Field::Group })
    );
}

#[test]
fn the_effective_selector_of_a_rule_is_its_selector_and_its_scope() {
    // selector: order-producers
    // scope: { client.id: order-ingest }
    let selectors = workspace();
    let selector = SelectorRef::Named(name("order-producers"));
    let scope = Scope::client(client("order-ingest"));

    let effective = selectors.resolve(&selector).unwrap().and(scope);
    let from_payments = Fields::new()
        .with(client("payments-svc"))
        .with(ApiKey::PRODUCE)
        .with_part(partition("orders.eu/3"));

    assert_eq!(
        effective.to_string(),
        "api is Produce and topic matches orders.* and client.id is order-ingest"
    );
    assert_eq!(
        effective.evaluate(&produce(&["orders.eu/3"])),
        matched(&["orders.eu/3"])
    );
    assert_eq!(effective.evaluate(&from_payments), Match::NotMatched);
    assert_eq!(
        effective,
        selectors.effective(&name("order-producers-from-ingest")).unwrap()
    );
}

#[test]
fn a_scope_on_one_connection_ends_with_that_connection() {
    let one_connection = EffectiveSelector::from(Scope::connection(ConnectionNumber::new(7)));
    let instance = EffectiveSelector::from(Scope::client_instance(
        client("order-ingest"),
        "10.0.4.27".parse().unwrap(),
    ));
    let reconnected = Fields::new()
        .with(ConnectionNumber::new(12))
        .with("10.0.4.27".parse::<ClientHost>().unwrap())
        .with(client("order-ingest"))
        .with(ApiKey::PRODUCE);

    assert!(!one_connection.evaluate(&reconnected).is_matched());
    assert!(instance.evaluate(&reconnected).is_matched());
}

// Response fields and the two passes

#[test]
fn a_selector_over_connection_request_and_learned_fields_is_for_the_request_pass() {
    let selector = all([
        on(Field::ClientId, "order-ingest"),
        on(Field::Topic, "orders.*"),
        on(Field::Group, "analytics-etl"),
    ]);

    assert_eq!(selector.pass(), Pass::Request);
}

#[test]
fn a_selector_that_tests_a_response_field_is_for_the_response_pass() {
    let selectors = SelectorSet::new([
        Selector::new(name("succeeded"), on(Field::Outcome, "ok").into()),
        Selector::new(name("orders-that-succeeded"), on(Field::Topic, "orders.*").into()).including(name("succeeded")),
    ])
    .unwrap();

    assert_eq!(selectors.effective(&name("succeeded")).unwrap().pass(), Pass::Response);
    // The selector's own conditions only test the request. The include makes it wait for the truth.
    assert_eq!(
        selectors.effective(&name("orders-that-succeeded")).unwrap().pass(),
        Pass::Response
    );
    assert_eq!(
        all([is_not(Field::ErrorCode, "NOT_LEADER_OR_FOLLOWER")]).pass(),
        Pass::Response
    );
}

#[test]
fn a_selector_that_tests_a_response_field_cannot_match_before_the_backend_answers() {
    let orders_that_succeeded = all([on(Field::Topic, "orders.eu"), on(Field::Outcome, "ok")]);
    let asked = produce(&["orders.eu/3", "payments.settled/0"]);

    assert_eq!(orders_that_succeeded.evaluate(&asked), Match::Pending);
    assert_eq!(
        all([is_not(Field::ErrorCode, "NOT_LEADER_OR_FOLLOWER")]).evaluate(&asked),
        Match::Pending
    );
    assert_eq!(all([on(Field::Latency, "14ms")]).evaluate(&asked), Match::Pending);

    let answered = asked.with(Outcome::Ok).with(Latency::from_millis(14));
    assert_eq!(orders_that_succeeded.evaluate(&answered), matched(&["orders.eu/3"]));
    assert_eq!(
        all([is_not(Field::ErrorCode, "NOT_LEADER_OR_FOLLOWER")]).evaluate(&answered),
        matched(&["orders.eu/3", "payments.settled/0"])
    );
}

#[test]
fn a_request_condition_that_fails_decides_before_the_backend_answers() {
    let asked = produce(&["payments.settled/0"]);

    assert_eq!(
        all([on(Field::Topic, "orders.eu"), on(Field::Outcome, "ok")]).evaluate(&asked),
        Match::NotMatched
    );
    assert_eq!(
        all([on(Field::Api, "Fetch"), on(Field::Outcome, "ok")]).evaluate(&asked),
        Match::NotMatched
    );
    assert_eq!(
        all([is_not(Field::ClientId, "order-ingest"), on(Field::Latency, "14ms")]).evaluate(&asked),
        Match::NotMatched
    );
}

#[test]
fn a_condition_on_the_truth_applies_to_the_same_partition() {
    let answered = from_ingest(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition("orders.eu/3")).succeeded())
        .with_part(PartFields::new(partition("orders.eu/4")).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .with_part(PartFields::new(partition("payments.settled/0")).succeeded())
        .with(Latency::from_millis(14));

    assert_eq!(
        all([on(Field::Topic, "orders.*"), on(Field::Outcome, "ok")]).evaluate(&answered),
        matched(&["orders.eu/3"])
    );
    assert_eq!(
        all([on(Field::ErrorCode, "NOT_*")]).evaluate(&answered),
        matched(&["orders.eu/4"])
    );
    assert_eq!(
        all([on(Field::Topic, "payments.settled"), on(Field::Outcome, "error")]).evaluate(&answered),
        Match::NotMatched
    );
    assert_eq!(
        all([on(Field::Latency, "14ms"), on(Field::Outcome, "ok")]).evaluate(&answered),
        matched(&["orders.eu/3", "payments.settled/0"])
    );
}

#[test]
fn a_part_that_waits_for_its_truth_keeps_the_match_pending() {
    let half_answered = from_ingest(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition("orders.eu/3")).succeeded())
        .with_part(PartFields::new(partition("orders.eu/4")));

    assert_eq!(all([on(Field::Outcome, "ok")]).evaluate(&half_answered), Match::Pending);
    assert_eq!(
        all([on(Field::Partition, "3"), on(Field::Outcome, "ok")]).evaluate(&half_answered),
        matched(&["orders.eu/3"])
    );
    assert_eq!(
        all([on(Field::Partition, "4"), on(Field::Outcome, "ok")]).evaluate(&half_answered),
        Match::Pending
    );
}

#[test]
fn the_response_pass_skips_a_partition_that_was_refused_before_forwarding() {
    let answered = from_ingest(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition("orders.eu/3")).without_truth())
        .with_part(PartFields::new(partition("orders.eu/4")).succeeded());

    assert_eq!(
        all([on(Field::Outcome, "ok")]).evaluate(&answered),
        matched(&["orders.eu/4"])
    );
    assert_eq!(
        all([on(Field::Outcome, "error")]).evaluate(&answered),
        Match::NotMatched
    );
}

#[test]
fn a_call_that_a_lie_ended_before_forwarding_never_has_a_truth() {
    let silenced = produce(&["orders.eu/3"]).without_truth();

    assert_eq!(all([on(Field::Outcome, "ok")]).evaluate(&silenced), Match::NotMatched);
    assert_eq!(
        all([is_not(Field::Outcome, "ok")]).evaluate(&silenced),
        matched(&["orders.eu/3"])
    );
}

// The index by API

#[test]
fn most_calls_test_only_a_few_selectors() {
    let selectors = workspace();
    let index = ApiIndex::new([
        (
            "order-producers",
            selectors.effective(&name("order-producers")).unwrap(),
        ),
        ("stale-metadata", all([on(Field::Api, "Metadata")])),
        ("slow-group", all([is_any_of(Field::Api, &["JoinGroup", "SyncGroup"])])),
        ("everyone", Scope::all().into()),
    ]);
    let tested = |api: ApiKey| index.candidates(api).map(|(rule, _)| *rule).collect::<Vec<_>>();

    assert_eq!(tested(ApiKey::PRODUCE), ["order-producers", "everyone"]);
    assert_eq!(tested(ApiKey::METADATA), ["stale-metadata", "everyone"]);
    assert_eq!(tested(ApiKey::HEARTBEAT), ["everyone"]);

    let targets: Vec<(&str, Target)> = index
        .matches(&produce(&["orders.eu/3", "payments.settled/0"]))
        .map(|(rule, target)| (*rule, target))
        .collect();
    assert_eq!(
        targets,
        [
            ("order-producers", matched(&["orders.eu/3"]).into_target().unwrap()),
            (
                "everyone",
                matched(&["orders.eu/3", "payments.settled/0"]).into_target().unwrap()
            ),
        ]
    );
}

/// A call as another crate holds it: typed members, and the trait on top.
struct RecordedHeartbeat {
    group: GroupId,
}

impl FieldSource for RecordedHeartbeat {
    fn whole_call(&self, field: Field) -> Reading<FieldValues> {
        match field {
            Field::Api => Reading::Present(FieldValues::one(ApiKey::HEARTBEAT)),
            Field::Group => Reading::Present(FieldValues::one(self.group.clone())),
            Field::Outcome => Reading::Present(FieldValues::one(Outcome::Ok)),
            _ => Reading::Absent,
        }
    }

    fn parts(&self) -> &[PartFields] {
        &[]
    }
}

#[test]
fn the_same_selector_filters_a_recorded_call_and_targets_a_live_one() {
    let selector = all([on(Field::Api, "Heartbeat"), on(Field::Group, "analytics-*")]);
    let recorded = RecordedHeartbeat {
        group: group("analytics-etl"),
    };
    let live: &dyn FieldSource = &from_ingest(ApiKey::HEARTBEAT).with(group("analytics-etl"));

    assert_eq!(selector.evaluate(&recorded), Match::Matched(Target::Call));
    assert_eq!(selector.evaluate(live), Match::Matched(Target::Call));
}
