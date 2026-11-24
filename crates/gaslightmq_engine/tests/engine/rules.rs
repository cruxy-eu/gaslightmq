//! The rule store publishes a snapshot to every session, and disarms a rule whose lifetime ended.

use std::sync::Arc;

use gaslightmq_clock::TimeSpan;
use gaslightmq_engine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::{ClientError, Producer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::{Partition, Record, ResponseError};

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn a_snapshot_published_while_a_call_is_in_flight_only_affects_the_next_call() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let producer = Arc::new(RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?));
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);

    // The first call is at the backend when someone arms a refusal of every Produce answer.
    proxy.backend().hold_answers();
    let first = tokio::spawn({
        let (producer, orders) = (Arc::clone(&producer), orders.clone());
        async move {
            let placed = [Record::new("placed")];
            producer.send(orders.as_str(), partition, &placed).await
        }
    });
    proxy.backend().calls_taken(1).await?;
    proxy.arm(&Rule::new(
        "Refuse the answer".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER).at(Stage::AfterResponse),
    ))?;
    proxy.backend().release_answers();

    // The first call finishes on the snapshot it started with, and the second one is evaluated against the new one.
    first.await??;
    let second = producer.send(orders.as_str(), partition, &[Record::new("paid")]).await;
    assert_eq!(
        second,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_rule_that_is_armed_reaches_the_sessions_that_are_open() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);
    let placed = [Record::new("placed")];
    let early = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    early.send(orders.as_str(), partition, &placed).await?;

    let flap = proxy.arm(&Rule::new(
        "Leader flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    ))?;

    // Both the session that was open and one that opens now refuse.
    let late = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    for client in [&early, &late] {
        let written = client.send(orders.as_str(), partition, &placed).await;
        assert_eq!(
            written,
            Err(ClientError::Refused {
                error: ResponseError::NotLeaderOrFollower
            })
        );
    }

    // Disarming reaches them the same way.
    proxy.engine().rules().disarm(flap.name())?;
    for client in [&early, &late] {
        client.send(orders.as_str(), partition, &placed).await?;
    }
    Ok(())
}

#[tokio::test]
async fn the_rule_that_was_armed_last_wins_a_conflict() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    proxy.arm(&Rule::new(
        "Leader flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    ))?;
    proxy.arm(&Rule::new(
        "Time out".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT),
    ))?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);

    let written = producer
        .send(orders.as_str(), partition, &[Record::new("placed")])
        .await;

    let armed = proxy.armed_rules();
    assert_eq!(armed, ["Time out".parse::<RuleName>()?, "Leader flap".parse()?]);
    // The broker's `REQUEST_TIMED_OUT` is the lie of the rule, and no client that waited in vain.
    assert_eq!(
        written,
        Err(ClientError::Refused {
            error: ResponseError::RequestTimedOut
        })
    );
    Ok(())
}

#[tokio::test]
async fn a_rule_is_disarmed_after_the_call_that_spent_its_last_firing() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let once = Rule::new(
        "Leader flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_lifetime(Lifetime::Firings(Count::ONE));
    proxy.arm(&once)?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);
    let placed = [Record::new("placed")];

    // The first Produce is refused. A client that tries again gets through.
    let refused = producer.send(orders.as_str(), partition, &placed).await;
    let written = producer.send(orders.as_str(), partition, &placed).await;
    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );
    assert!(written.is_ok(), "{written:?}");

    // The engine disarmed the rule with the record of the call that spent the firing.
    proxy.next_record().await?;
    assert_eq!(proxy.armed_rules(), Vec::<RuleName>::new());
    let disarmed_twice = proxy.engine().rules().disarm(once.name());
    let not_armed = DisarmError::NotArmed {
        rule: once.name().clone(),
    };
    assert_eq!(disarmed_twice.map(|rule| rule.name().clone()), Err(not_armed));
    Ok(())
}

#[tokio::test]
async fn a_rule_is_disarmed_when_the_clock_reaches_the_end_of_its_lifetime() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let short = Rule::new(
        "Leader flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    )
    .with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
    proxy.arm(&short)?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);
    let placed = [Record::new("placed")];

    // One second before the end, the rule still fires.
    proxy.advance(TimeSpan::from_secs(29));
    let refused = producer.send(orders.as_str(), partition, &placed).await;
    assert_eq!(
        refused,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );

    // At the end it fires no more, and the engine disarms it: no call has to come by for that.
    proxy.advance(TimeSpan::from_secs(1));
    producer.send(orders.as_str(), partition, &placed).await?;
    assert_eq!(proxy.armed_rules(), Vec::<RuleName>::new());
    Ok(())
}
