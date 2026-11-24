//! The statistics count the calls of each API, and how far each rule got.

use gaslightmq_call::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_traffic::prelude::*;

use crate::support::ClientConnection;

#[test]
fn every_call_counts_for_its_api() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    let mut payments = ClientConnection::second("payments-svc".parse()?);

    traffic.record(ingest.produce(&orders_eu).ended());
    traffic.record(payments.heartbeat().ended());
    traffic.record(ingest.produce(&orders_eu).ended());

    let statistics = traffic.statistics();
    assert_eq!(statistics.calls(), 3);
    assert_eq!(statistics.calls_of(ApiKey::PRODUCE), 2);
    assert_eq!(statistics.calls_of(ApiKey::HEARTBEAT), 1);
    assert_eq!(statistics.calls_of(ApiKey::FETCH), 0);
    let apis: Vec<_> = statistics.apis().collect();
    assert_eq!(apis, [(ApiKey::PRODUCE, 2), (ApiKey::HEARTBEAT, 1)]);
    Ok(())
}

#[test]
fn a_rule_counts_the_calls_it_matched_and_the_calls_it_lied_to() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let refuse_every_2nd: RuleName = "Refuse every 2nd".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    // The rule matches every Produce, and refuses the second one.
    traffic.record(ingest.produce(&orders_eu).held_by(&refuse_every_2nd).ended());
    traffic.record(ingest.produce(&orders_eu).refused_by(&refuse_every_2nd).ended());
    traffic.record(ingest.produce(&orders_eu).held_by(&refuse_every_2nd).ended());
    traffic.record(ingest.heartbeat().ended());

    let rule = traffic.statistics().rule(&refuse_every_2nd);
    assert_eq!(rule.matches(), 3);
    assert_eq!(rule.firings(), 1);
    assert_eq!(rule.calls(ExplanationStatus::Rolled), 3);
    assert_eq!(rule.calls(ExplanationStatus::Fired), 1);
    Ok(())
}

#[test]
fn a_lie_that_lost_everything_to_another_rule_is_no_firing() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let leader_flap: RuleName = "Leader flap".parse()?;
    let refuse_orders: RuleName = "Refuse orders".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    let refused_twice = ingest
        .produce(&orders_eu)
        .refused_by(&leader_flap)
        .refused_in_vain_by(&refuse_orders, &leader_flap);
    traffic.record(refused_twice.ended());

    let winner = traffic.statistics().rule(&leader_flap);
    assert_eq!(winner.firings(), 1);
    assert_eq!(winner.calls(ExplanationStatus::Shadowed), 0);
    // The loser fired, and its lifetime of firings does not count the call.
    let loser = traffic.statistics().rule(&refuse_orders);
    assert_eq!(loser.matches(), 1);
    assert_eq!(loser.calls(ExplanationStatus::Fired), 1);
    assert_eq!(loser.calls(ExplanationStatus::Shadowed), 1);
    assert_eq!(loser.firings(), 0);
    Ok(())
}

#[test]
fn the_statistics_list_every_rule_that_a_call_touched() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let leader_flap: RuleName = "Leader flap".parse()?;
    let refuse_orders: RuleName = "Refuse orders".parse()?;
    let never_matched: RuleName = "Lose heartbeats".parse()?;
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    traffic.record(ingest.produce(&orders_eu).refused_by(&refuse_orders).ended());
    traffic.record(ingest.fetch(&orders_eu).refused_by(&leader_flap).ended());

    let statistics = traffic.statistics();
    let firings: Vec<_> = statistics
        .rules()
        .map(|(name, rule)| (name.as_str(), rule.firings()))
        .collect();

    // The rules come in the order of their names.
    assert_eq!(firings, [(leader_flap.as_str(), 1), (refuse_orders.as_str(), 1)]);
    // A rule that no call touched did nothing, whether it exists or not.
    assert_eq!(statistics.rule(&never_matched), RuleStatistics::default());
    Ok(())
}

#[test]
fn the_statistics_go_on_counting_the_calls_that_left_the_live_window() -> anyhow::Result<()> {
    let orders_eu: TopicPartition = "orders.eu/3".parse()?;
    let leader_flap: RuleName = "Leader flap".parse()?;
    let one_call = LiveWindowSize::new(1)?;
    let mut configuration = TrafficConfiguration::new();
    configuration.live_window = one_call;
    let mut traffic = Traffic::new(configuration);
    let mut ingest = ClientConnection::first("order-ingest".parse()?);

    let three_calls = 3;
    for _ in 0..three_calls {
        traffic.record(ingest.produce(&orders_eu).refused_by(&leader_flap).ended());
    }

    assert_eq!(traffic.live_window().len(), one_call.calls());
    assert_eq!(traffic.statistics().calls(), three_calls);
    assert_eq!(traffic.statistics().rule(&leader_flap).firings(), three_calls);
    Ok(())
}

#[test]
fn a_copy_of_the_statistics_stays_at_its_moment_so_two_copies_give_a_rate() -> anyhow::Result<()> {
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    traffic.record(ingest.heartbeat().ended());
    let a_moment_ago = traffic.statistics().clone();

    traffic.record(ingest.heartbeat().ended());
    traffic.record(ingest.heartbeat().ended());

    let now = traffic.statistics();
    assert_eq!(a_moment_ago.calls_of(ApiKey::HEARTBEAT), 1);
    assert_eq!(
        now.calls_of(ApiKey::HEARTBEAT) - a_moment_ago.calls_of(ApiKey::HEARTBEAT),
        2
    );
    Ok(())
}

#[test]
fn a_traffic_that_fell_behind_says_how_many_events_it_never_saw() -> anyhow::Result<()> {
    let mut traffic = Traffic::new(TrafficConfiguration::new());
    let mut ingest = ClientConnection::first("order-ingest".parse()?);
    traffic.record(ingest.heartbeat().ended());
    assert_eq!(traffic.statistics().missed_events(), 0);

    // The engine tells whoever feeds the traffic how many events it missed, each time it fell behind.
    traffic.fell_behind(3);
    traffic.fell_behind(2);

    assert_eq!(traffic.statistics().missed_events(), 3 + 2);
    assert_eq!(traffic.statistics().calls(), 1);
    Ok(())
}
