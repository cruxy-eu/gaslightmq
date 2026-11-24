//! The index of `docs/specs/domains/observation.md`, example by example.

mod common;

use common::{Proxy, Request};
use gaslightmq_field::prelude::*;
use gaslightmq_index::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_selector::prelude::*;

#[test]
fn facet_counts_respect_the_other_active_filters() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let etl = proxy.accepts("analytics").in_group("analytics-etl");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.us/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("payments.settled/0")));
    index.add(&etl.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));
    index.add(&etl.sends(Request::of(ApiKey::HEARTBEAT)));

    // Someone clicked `Produce` in the APIs, and then `orders.eu` in the topics.
    let produced = Condition::is(ApiKey::PRODUCE);
    let to_eu = Condition::parse(Field::Topic, "orders.eu").unwrap();
    let facets = index.facets(&EffectiveSelector::from(produced).and(to_eu));

    assert_eq!(facets.calls(), 1);
    // The clients are what is left under both conditions.
    assert_eq!(facets.get(Field::ClientId).unwrap().to_string(), "order-ingest (1)");
    // The topics leave their own condition out: they are what is left of the produced calls.
    assert_eq!(
        facets.get(Field::Topic).unwrap().to_string(),
        "orders.eu (1), orders.us (1), payments.settled (1); orders.* (2)"
    );
    // And the APIs are what is left of the calls for `orders.eu`.
    assert_eq!(facets.get(Field::Api).unwrap().to_string(), "Produce (1), Fetch (1)");
}

#[test]
fn an_index_groups_calls_into_the_facets_of_the_spec() {
    let first_broker = Endpoint::Broker(BrokerId::from(1));
    let second_broker = Endpoint::Broker(BrokerId::from(2));
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest").connected_to(first_broker);
    let payments = proxy
        .accepts("payments-svc")
        .connected_to(second_broker)
        .in_group("payments")
        .transacting_as("payments-tx-1");
    let etl = proxy
        .accepts("analytics")
        .connected_to(second_broker)
        .in_group("analytics-etl");
    // A client that only asks the bootstrap endpoint about the cluster.
    let ui = proxy.accepts("kafka-ui");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ui.sends(Request::of(ApiKey::METADATA).ok("orders.eu")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0").ok("orders.us/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).failed("orders.eu/1", ErrorCode::NOT_LEADER_OR_FOLLOWER)));
    index.add(&payments.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));
    index.add(&payments.sends(Request::of(ApiKey::PRODUCE).ok("payments.settled/0")));
    index.add(&etl.sends(Request::of(ApiKey::FETCH).ok("orders.us/0")));
    index.add(&etl.sends(Request::of(ApiKey::HEARTBEAT).timed_out()));

    let facets = index.facets(&EffectiveSelector::new());
    let grouped: Vec<(Field, String)> = facets.iter().map(|facet| (facet.field(), facet.to_string())).collect();

    assert_eq!(facets.calls(), 7);
    assert_eq!(
        grouped,
        [
            (
                Field::Topic,
                "orders.eu (4), orders.us (2), payments.settled (1); orders.* (5)".to_owned()
            ),
            (
                Field::Api,
                "Produce (3), Fetch (2), Metadata (1), Heartbeat (1)".to_owned()
            ),
            (Field::Group, "analytics-etl (2), payments (2)".to_owned()),
            (Field::TransactionalId, "payments-tx-1 (2)".to_owned()),
            (
                Field::ClientId,
                "analytics (2), order-ingest (2), payments-svc (2), kafka-ui (1)".to_owned()
            ),
            (Field::Endpoint, "broker 2 (4), broker 1 (2), bootstrap (1)".to_owned()),
            (Field::Outcome, "ok (5), error (1), timeout (1)".to_owned()),
        ]
    );
}

#[test]
fn a_filter_without_a_condition_leaves_every_call() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");

    let mut index = CallIndex::new(IndexConfiguration::new());
    assert!(index.is_empty());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/1")));

    let everything = EffectiveSelector::new();
    assert!(!index.is_empty());
    assert_eq!(index.calls(), 2);
    assert_eq!(index.count(&everything), 2);
    assert_eq!(index.facets(&everything).calls(), 2);
}

#[test]
fn an_index_without_a_call_has_empty_facets() {
    let index = CallIndex::new(IndexConfiguration::new());

    let facets = index.facets(&Condition::is(ApiKey::PRODUCE).into());

    assert_eq!(facets.calls(), 0);
    assert_eq!(facets.len(), IndexConfiguration::FACETS.len());
    assert!(facets.iter().all(Facet::is_empty));
}

#[test]
fn a_click_on_a_value_leaves_as_many_calls_as_the_facet_said() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let payments = proxy.accepts("payments-svc").in_group("payments");
    let etl = proxy.accepts("analytics").in_group("analytics-etl");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0").ok("orders.us/0")));
    index.add(
        &ingest.sends(
            Request::of(ApiKey::PRODUCE)
                .ok("orders.eu/1")
                .failed("payments.settled/0", ErrorCode::NOT_LEADER_OR_FOLLOWER),
        ),
    );
    index.add(&payments.sends(Request::of(ApiKey::PRODUCE).ok("payments.settled/0")));
    index.add(&payments.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0").ok("orders.eu/1")));
    index.add(&etl.sends(Request::of(ApiKey::FETCH).failed("orders.us/0", ErrorCode::NOT_LEADER_OR_FOLLOWER)));
    index.add(&etl.sends(Request::of(ApiKey::HEARTBEAT).timed_out()));
    index.add(&etl.sends(Request::of(ApiKey::OFFSET_COMMIT).never_forwarded()));

    let filters = [
        EffectiveSelector::new(),
        Condition::is(ApiKey::PRODUCE).into(),
        Condition::parse(Field::Topic, "orders.*").unwrap().into(),
        Condition::is(Outcome::Error).into(),
        Condition::is_not(ClientId::new("order-ingest").unwrap()).into(),
        EffectiveSelector::from(Condition::parse(Field::Topic, "orders.eu").unwrap()).and(Condition::is(Outcome::Ok)),
    ];

    for filter in filters {
        for facet in &index.facets(&filter) {
            let others: EffectiveSelector = filter
                .conditions()
                .iter()
                .filter(|condition| condition.field() != facet.field())
                .cloned()
                .collect();
            for value in facet.values() {
                let narrowed = others.clone().and(value.condition());
                assert_eq!(index.count(&narrowed), value.calls(), "{narrowed}");
            }
            for pattern in facet.patterns() {
                let narrowed = others.clone().and(pattern.condition());
                assert_eq!(index.count(&narrowed), pattern.calls(), "{narrowed}");
            }
        }
    }
}

#[test]
fn conditions_on_a_part_apply_to_the_same_part() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(
        &ingest.sends(
            Request::of(ApiKey::PRODUCE)
                .ok("orders.eu/0")
                .failed("payments.settled/0", ErrorCode::NOT_LEADER_OR_FOLLOWER),
        ),
    );

    let everything = EffectiveSelector::new();
    let to_eu: EffectiveSelector = Condition::parse(Field::Topic, "orders.eu").unwrap().into();
    let failed: EffectiveSelector = Condition::is(Outcome::Error).into();

    // Across the call, one partition succeeded and one failed.
    assert_eq!(
        index.facet(Field::Outcome, &everything).to_string(),
        "ok (1), error (1)"
    );
    // The `orders.eu` partition did not fail, so `outcome is error` would leave nothing of this filter.
    assert_eq!(index.facet(Field::Outcome, &to_eu).to_string(), "ok (1)");
    // And only `payments.settled` is left of the calls with a partition that failed.
    assert_eq!(index.facet(Field::Topic, &failed).to_string(), "payments.settled (1)");
}

#[test]
fn a_value_counts_whether_the_request_carries_it_or_the_client_context_knows_it() {
    let proxy = Proxy::new();
    let etl = proxy.accepts("analytics").in_group("analytics-etl");
    let ingest = proxy.accepts("order-ingest");

    let mut index = CallIndex::new(IndexConfiguration::new());
    // A Fetch never names a group. The client context of `analytics` knows it.
    index.add(&etl.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));
    index.add(&etl.sends(Request::of(ApiKey::FETCH).ok("orders.eu/1")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));

    let groups = index.facet(Field::Group, &EffectiveSelector::new());

    assert_eq!(groups.to_string(), "analytics-etl (2)");
    assert_eq!(index.count(&groups.values()[0].condition().into()), 2);
}

#[test]
fn a_call_that_never_reached_the_backend_has_no_outcome() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0").never_forwarded()));

    let facets = index.facets(&EffectiveSelector::new());

    assert_eq!(facets.get(Field::Topic).unwrap().to_string(), "orders.eu (2)");
    assert_eq!(facets.get(Field::Outcome).unwrap().to_string(), "ok (1)");
}

#[test]
fn a_filter_that_excludes_a_value_narrows_the_other_facets() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let ui = proxy.accepts("kafka-ui");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ui.sends(Request::of(ApiKey::METADATA).ok("orders.eu")));
    index.add(&ui.sends(Request::of(ApiKey::METADATA).ok("orders.us")));

    let not_the_ui: EffectiveSelector = Condition::is_not(ClientId::new("kafka-ui").unwrap()).into();
    let facets = index.facets(&not_the_ui);

    assert_eq!(facets.calls(), 1);
    assert_eq!(facets.get(Field::Api).unwrap().to_string(), "Produce (1)");
    assert_eq!(facets.get(Field::Topic).unwrap().to_string(), "orders.eu (1)");
    // The clients leave their own condition out, so the excluded client is still there to click.
    assert_eq!(
        facets.get(Field::ClientId).unwrap().to_string(),
        "kafka-ui (2), order-ingest (1)"
    );
}

#[test]
fn the_current_filter_can_be_saved_as_a_selector() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let etl = proxy.accepts("analytics");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.us/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("payments.settled/0")));
    index.add(&etl.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));

    // Two clicks: `Produce` in the APIs, and the pattern that the topics suggest.
    let everything = index.facets(&EffectiveSelector::new());
    let produce = everything.get(Field::Api).unwrap().values()[0].condition();
    let orders = everything.get(Field::Topic).unwrap().patterns()[0].condition();
    let filter = Conditions::new().with(produce).with(orders);

    // The filter is the `match` of a selector, and the selector matches what the filter left.
    let saved = Selector::new("order-producers".parse().unwrap(), filter.clone());
    let selectors = SelectorSet::new([saved]).unwrap();
    let by_name = selectors
        .resolve(&SelectorRef::Named("order-producers".parse().unwrap()))
        .unwrap();

    assert_eq!(by_name.to_string(), "api is Produce and topic matches orders.*");
    assert_eq!(index.facets(&filter.into()).calls(), 2);
    assert_eq!(index.count(&by_name), 2);
}

#[test]
fn every_field_can_have_a_facet() {
    let proxy = Proxy::new();
    // The same client on two connections.
    let ingest = proxy.accepts("order-ingest");
    let retry = proxy.accepts("order-ingest");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/3")));
    index.add(&retry.sends(Request::of(ApiKey::PRODUCE).failed("orders.eu/3", ErrorCode::NOT_LEADER_OR_FOLLOWER)));

    let everything = EffectiveSelector::new();

    assert!(index.facets(&everything).get(Field::Connection).is_none());
    let connections = index.facet(Field::Connection, &everything);
    assert_eq!(connections.calls_with(&ingest.connection().into()), 2);
    assert_eq!(connections.calls_with(&retry.connection().into()), 1);
    assert_eq!(connections.values().len(), 2);
    assert_eq!(index.facet(Field::Partition, &everything).to_string(), "3 (2), 0 (1)");
    assert_eq!(
        index.facet(Field::ErrorCode, &everything).to_string(),
        "NOT_LEADER_OR_FOLLOWER (1)"
    );
}

#[test]
fn the_configuration_says_which_facets_an_index_has() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");

    let mut configuration = IndexConfiguration::new();
    configuration.facets = vec![Field::Connection, Field::Topic];
    let mut index = CallIndex::new(configuration);
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));

    let facets = index.facets(&EffectiveSelector::new());
    let fields: Vec<Field> = facets.iter().map(Facet::field).collect();

    assert_eq!(fields, [Field::Connection, Field::Topic]);
    assert_eq!(index.configuration().facets, fields);
}

#[test]
fn the_configuration_says_when_values_suggest_a_pattern() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let worker = proxy.accepts("order-worker");

    let mut configuration = IndexConfiguration::new();
    configuration.pattern_separators = vec!['_'];
    let mut index = CallIndex::new(configuration);
    index.extend(&[
        ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders_eu/0")),
        ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders_us/0")),
        worker.sends(Request::of(ApiKey::FETCH).ok("orders_eu/0")),
    ]);

    let everything = EffectiveSelector::new();

    assert_eq!(
        index.facet(Field::Topic, &everything).to_string(),
        "orders_eu (2), orders_us (1); orders_* (3)"
    );
    // The client IDs share `order-`, and a `-` cuts no value in this configuration.
    assert_eq!(
        index.facet(Field::ClientId, &everything).to_string(),
        "order-ingest (2), order-worker (1)"
    );
}

#[test]
fn every_facet_suggests_the_patterns_its_values_share() {
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest");
    let worker = proxy.accepts("order-worker");
    let payments = proxy.accepts("payments-svc");

    let mut index = CallIndex::new(IndexConfiguration::new());
    index.add(&ingest.sends(Request::of(ApiKey::PRODUCE).ok("orders.eu/0")));
    index.add(&worker.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));
    index.add(&worker.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));
    index.add(&payments.sends(Request::of(ApiKey::FETCH).ok("orders.eu/0")));

    let clients = index.facet(Field::ClientId, &EffectiveSelector::new());
    let [order] = clients.patterns() else {
        panic!("the two order clients share one pattern");
    };

    assert_eq!(order.to_string(), "order-* (3)");
    assert_eq!(order.values(), 2);
    assert_eq!(order.condition(), Condition::parse(Field::ClientId, "order-*").unwrap());
}
