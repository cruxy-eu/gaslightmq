//! A subscription receives the calls that its selector matches, and no others.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_traffic::prelude::*;

use crate::support::ClientConnection;

#[test]
fn a_subscription_receives_the_calls_that_its_selector_matches() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let payments_settled: TopicPartition = "payments.settled/0".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let orders = traffic.subscribe(Condition::parse(Field::Topic, "orders.*")?.into());
    let everything = traffic.subscribe(EffectiveSelector::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    let to_orders = traffic.record(ingest.produce(&orders_eu).ended());
    let to_payments = traffic.record(ingest.produce(&payments_settled).ended());
    let heartbeat = traffic.record(ingest.heartbeat().ended());

    assert_eq!(to_orders, [orders, everything]);
    assert_eq!(to_payments, [everything]);
    assert_eq!(heartbeat, [everything]);
    Ok(())
}

#[test]
fn a_call_that_nobody_subscribed_to_goes_to_nobody() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let fetches = traffic.subscribe(Condition::is(ApiKey::FETCH).into());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    assert_eq!(traffic.record(ingest.heartbeat().ended()), []);
    assert_eq!(traffic.record(ingest.fetch(&orders_eu).ended()), [fetches]);
    Ok(())
}

#[test]
fn a_subscription_to_one_client_leaves_the_other_clients_out() -> anyhow::Result<()> {
    let order_ingest: ClientId = "order-ingest".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let of_ingest = traffic.subscribe(Scope::client(order_ingest.clone()).into());
    let mut ingest = ClientConnection::first(order_ingest);
    let mut payments = ClientConnection::second("payments-svc".parse()?);

    assert_eq!(traffic.record(ingest.heartbeat().ended()), [of_ingest]);
    assert_eq!(traffic.record(payments.heartbeat().ended()), []);
    Ok(())
}

#[test]
fn a_selector_on_the_truth_decides_at_once_because_a_record_is_of_a_call_that_ended() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let leader_flap: RuleName = "Leader flap".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let answered = traffic.subscribe(Condition::is(Outcome::Ok).into());
    let timed_out = traffic.subscribe(Condition::is(Outcome::Timeout).into());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    // The backend answers every call of these tests, also the one that a rule refuses after the response.
    let refused = ingest.produce(&orders_eu).refused_by(&leader_flap).ended();

    assert_eq!(traffic.record(refused), [answered]);
    assert!(traffic.selector_of(timed_out).is_some());
    Ok(())
}

#[test]
fn a_subscription_that_ended_receives_nothing_and_its_id_is_never_used_again() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let produce_calls = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));
    let first = traffic.subscribe(produce_calls.clone());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    assert_eq!(traffic.unsubscribe(first), Some(produce_calls.clone()));
    let second = traffic.subscribe(produce_calls);

    assert_ne!(first, second);
    assert_eq!(traffic.record(ingest.produce(&orders_eu).ended()), [second]);
    assert_eq!(traffic.selector_of(first), None);
    Ok(())
}

#[test]
fn ending_a_subscription_that_nobody_knows_changes_nothing() -> anyhow::Result<()> {
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let everything = traffic.subscribe(EffectiveSelector::new());
    let gone = traffic.subscribe(EffectiveSelector::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    traffic.unsubscribe(gone);

    assert_eq!(traffic.unsubscribe(gone), None);

    assert_eq!(traffic.record(ingest.heartbeat().ended()), [everything]);
    Ok(())
}

#[test]
fn a_traffic_lists_its_subscriptions_in_the_order_in_which_they_were_made() {
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let produce_calls = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));
    let every_call = EffectiveSelector::new();
    let first = traffic.subscribe(produce_calls.clone());
    let second = traffic.subscribe(every_call.clone());

    let subscriptions: Vec<_> = traffic.subscriptions().collect();

    assert_eq!(subscriptions, [(first, &produce_calls), (second, &every_call)]);
    assert_eq!(traffic.selector_of(second), Some(&every_call));
    assert_eq!(first.to_string(), "subscription 1");
    assert_eq!(second.to_string(), "subscription 2");
}

#[test]
fn a_new_subscription_finds_what_it_missed_in_the_live_window() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let orders_us: TopicPartition = "orders.us/1".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let before = ingest.produce(&orders_eu).ended();
    traffic.record(before.clone());
    traffic.record(ingest.heartbeat().ended());

    // Someone starts to watch the orders: the calls so far come from the window, and the next ones from the traffic.
    let orders = traffic.subscribe(Condition::parse(Field::Topic, "orders.*")?.into());
    let selector = traffic.selector_of(orders).expect("the subscription is there");
    let missed: Vec<_> = traffic.live_window().matching(selector).collect();

    assert_eq!(missed, [&before]);
    assert_eq!(traffic.record(ingest.produce(&orders_us).ended()), [orders]);
    Ok(())
}
