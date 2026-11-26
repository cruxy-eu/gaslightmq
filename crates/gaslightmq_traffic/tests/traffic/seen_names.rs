//! The seen names keep the topics, groups and client IDs that the calls named, as many of each kind as configured.

use gaslightmq_kernel::prelude::*;
use gaslightmq_traffic::prelude::*;

use crate::support::Proxy;

#[test]
fn the_names_of_every_call_are_seen_once() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let orders_us: TopicPartition = "orders.us/0".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest".parse()?);
    let analytics = proxy.accepts("analytics".parse()?);

    traffic.record(ingest.produce(&orders_eu).ended());
    traffic.record(ingest.produce(&orders_us).ended());
    traffic.record(ingest.produce(&orders_eu).ended());
    traffic.record(analytics.fetch(&orders_eu).ended());
    traffic.record(analytics.heartbeat_in("analytics-etl".parse()?).ended());

    let seen = traffic.seen_names();
    let topics: Vec<&str> = seen.topics().map(TopicName::as_str).collect();
    let groups: Vec<&str> = seen.groups().map(GroupId::as_str).collect();
    let clients: Vec<&str> = seen.clients().map(ClientId::as_str).collect();
    assert_eq!(topics, ["orders.eu", "orders.us"]);
    assert_eq!(groups, ["analytics-etl"]);
    assert_eq!(clients, ["analytics", "order-ingest"]);
    Ok(())
}

#[test]
fn a_new_name_that_finds_its_kind_full_pushes_out_the_name_seen_longest_ago() -> anyhow::Result<()> {
    let mut configuration = TrafficConfiguration::new();
    configuration.seen_names = 2;
    let mut traffic = Traffic::new(configuration);
    let proxy = Proxy::new();
    let ingest = proxy.accepts("order-ingest".parse()?);
    let payments = proxy.accepts("payments-svc".parse()?);
    let analytics = proxy.accepts("analytics".parse()?);

    // `order-ingest` calls again after `payments-svc`, so `payments-svc` was seen longest ago once `analytics` comes.
    traffic.record(ingest.heartbeat().ended());
    traffic.record(payments.heartbeat().ended());
    traffic.record(ingest.heartbeat().ended());
    traffic.record(analytics.heartbeat().ended());

    let clients: Vec<&str> = traffic.seen_names().clients().map(ClientId::as_str).collect();
    assert_eq!(clients, ["analytics", "order-ingest"]);
    Ok(())
}

#[test]
fn a_traffic_configured_to_keep_no_name_sees_none() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut configuration = TrafficConfiguration::new();
    configuration.seen_names = 0;
    let mut traffic = Traffic::new(configuration);
    let ingest = Proxy::new().accepts("order-ingest".parse()?);

    traffic.record(ingest.produce(&orders_eu).ended());

    let seen = traffic.seen_names();
    assert_eq!(seen.topics().count(), 0);
    assert_eq!(seen.clients().count(), 0);
    assert_eq!(traffic.statistics().calls(), 1);
    Ok(())
}
