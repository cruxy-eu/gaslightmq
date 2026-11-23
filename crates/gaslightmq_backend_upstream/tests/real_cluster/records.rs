//! Records go to the cluster and come back from it.

use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{FetchRequest, FetchResponse, ProduceRequest, ProduceResponse, TopicName};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Offset, Record, RecordBatch, Request};

#[tokio::test]
async fn a_record_produced_through_the_backend_is_in_the_log_of_its_partition() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    let placed = Record::new("placed").with_key("o-1").with_timestamp(1_700_000_000_000);
    let partition = PartitionProduceData::default()
        .with_index(0)
        .with_records(Some(RecordBatch::new([placed.clone()]).encode()?));
    let topic = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(orders.name().to_string())))
        .with_partition_data(vec![partition]);
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30_000)
        .with_topic_data(vec![topic]);
    let request = Request::new(9, 1, Some("order-ingest"), &produce)?;
    let leader = Endpoint::Broker(orders.partition(0).leader().await?);
    let answer = backend
        .call(BackendRequest::new("#1.1".parse()?, leader, request))
        .await;

    let BackendAnswer::Truth(truth) = answer else {
        anyhow::bail!("the cluster answers a Produce with acks, and the backend made of it: {answer:?}");
    };
    let produced: ProduceResponse = truth.response().body()?;
    let written = &produced.responses[0].partition_responses[0];
    assert_eq!((written.error_code, written.base_offset), (0, 0));
    assert_eq!(
        orders.partition(0).records().await?,
        [placed.with_offset(Offset::new(0))]
    );
    Ok(())
}

#[tokio::test]
async fn a_produce_without_acks_has_no_response_and_its_record_is_in_the_log() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    // One client writes two records on one connection: the first without acks, and the second with them.
    let leader = Endpoint::Broker(orders.partition(0).leader().await?);
    let mut answers = Vec::new();
    for (correlation_id, acks, value) in [(1, 0, "placed"), (2, -1, "paid")] {
        let record = Record::new(value).with_timestamp(1_700_000_000_000);
        let partition = PartitionProduceData::default()
            .with_index(0)
            .with_records(Some(RecordBatch::new([record]).encode()?));
        let topic = TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(orders.name().to_string())))
            .with_partition_data(vec![partition]);
        let produce = ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![topic]);
        let request = Request::new(9, correlation_id, Some("order-ingest"), &produce)?;
        let call = format!("#1.{correlation_id}").parse()?;
        answers.push(backend.call(BackendRequest::new(call, leader, request)).await);
    }

    assert!(matches!(answers[0], BackendAnswer::NoResponse), "{:?}", answers[0]);
    // The broker handles the requests of one connection in order, so the first record is written before the second.
    let BackendAnswer::Truth(truth) = &answers[1] else {
        anyhow::bail!(
            "the cluster answers a Produce with acks, and the backend made of it: {:?}",
            answers[1]
        );
    };
    let produced: ProduceResponse = truth.response().body()?;
    assert_eq!(produced.responses[0].partition_responses[0].base_offset, 1);
    let values: Vec<_> = orders
        .partition(0)
        .records()
        .await?
        .into_iter()
        .filter_map(|record| record.value)
        .collect();
    assert_eq!(values, ["placed", "paid"]);
    Ok(())
}

#[tokio::test]
async fn a_fetch_through_the_backend_reads_the_log_of_a_partition_and_gets_no_fetch_session() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let orders = cluster.topics().create("orders").await?;
    orders
        .partition(0)
        .produce(&[
            Record::new("placed").with_key("o-1"),
            Record::new("paid").with_key("o-1"),
        ])
        .await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    // A consumer asks for the records from offset 0 on, and for a fetch session.
    let partition = FetchPartition::default()
        .with_partition(0)
        .with_fetch_offset(0)
        .with_partition_max_bytes(1024 * 1024);
    let topic = FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(orders.name().to_string())))
        .with_partitions(vec![partition]);
    let fetch = FetchRequest::default()
        .with_replica_id((-1).into())
        .with_max_wait_ms(5_000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_session_id(0)
        .with_session_epoch(0)
        .with_topics(vec![topic]);
    let request = Request::new(12, 1, Some("order-billing"), &fetch)?;
    let leader = Endpoint::Broker(orders.partition(0).leader().await?);
    let answer = backend
        .call(BackendRequest::new("#1.1".parse()?, leader, request))
        .await;

    let BackendAnswer::Truth(truth) = answer else {
        anyhow::bail!("the cluster answers a Fetch, and the backend made of it: {answer:?}");
    };
    let fetched: FetchResponse = truth.response().body()?;
    assert_eq!((fetched.error_code, fetched.session_id), (0, 0));
    let Some(batches) = &fetched.responses[0].partitions[0].records else {
        anyhow::bail!("the partition holds two records, and the answer carries none");
    };
    let records: Vec<Record> = RecordBatch::decode_all(batches)?
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect();
    assert_eq!(records, orders.partition(0).records().await?);
    assert_eq!(records.len(), 2);
    Ok(())
}
