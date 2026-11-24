//! The live window keeps the newest calls, and lets go of the oldest.

use std::sync::Arc;

use gaslightmq_kernel::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_traffic::prelude::*;

use crate::support::ClientConnection;

#[test]
fn the_live_window_keeps_the_calls_in_the_order_in_which_they_were_recorded() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let mut payments = ClientConnection::second("payments-svc".parse()?);
    let produce = ingest.produce(&orders_eu).ended();
    let heartbeat = payments.heartbeat().ended();
    let fetch = ingest.fetch(&orders_eu).ended();
    assert!(traffic.live_window().is_empty());

    traffic.record(produce.clone());
    traffic.record(heartbeat.clone());
    traffic.record(fetch.clone());

    let window = traffic.live_window();
    let oldest_first: Vec<_> = window.calls().collect();
    assert_eq!(oldest_first, [&produce, &heartbeat, &fetch]);
    assert_eq!(window.len(), oldest_first.len());
    // Read from the back, the newest call comes first, as on a screen.
    assert_eq!(window.calls().next_back(), Some(&fetch));
    Ok(())
}

#[test]
fn a_call_that_arrives_at_a_full_window_pushes_the_oldest_one_out() -> anyhow::Result<()> {
    let two_calls = LiveWindowSize::new(2)?;
    let mut configuration = TrafficConfiguration::new();
    configuration.live_window = two_calls;
    let mut traffic = Traffic::new(configuration);
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let pushed_out = ingest.heartbeat().ended();
    let older = ingest.heartbeat().ended();
    let newer = ingest.heartbeat().ended();

    for record in [&pushed_out, &older, &newer] {
        traffic.record(record.clone());
    }

    let kept: Vec<_> = traffic.live_window().calls().collect();
    assert_eq!(kept, [&older, &newer]);
    assert_eq!(traffic.live_window().size(), two_calls);
    assert_eq!(traffic.configuration().live_window, two_calls);
    Ok(())
}

#[test]
fn the_window_hands_out_the_record_that_the_traffic_was_given() -> anyhow::Result<()> {
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let record = ingest.heartbeat().ended();

    traffic.record(record.clone());

    let kept = traffic.live_window().calls().next().expect("the window keeps the call");
    assert!(Arc::ptr_eq(kept, &record));
    Ok(())
}

#[test]
fn the_window_finds_the_calls_that_a_selector_matches() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let first_produce = ingest.produce(&orders_eu).ended();
    let heartbeat = ingest.heartbeat().ended();
    let second_produce = ingest.produce(&orders_eu).ended();
    for record in [&first_produce, &heartbeat, &second_produce] {
        traffic.record(record.clone());
    }

    // A selector that nobody saved and nobody subscribed with.
    let produce_calls = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));
    let matching: Vec<_> = traffic.live_window().matching(&produce_calls).collect();

    assert_eq!(matching, [&first_produce, &second_produce]);
    Ok(())
}
