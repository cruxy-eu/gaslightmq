//! Clients are told the listeners of the proxy, and the listener of a broker stands for that broker alone.
//!
//! Each test opens the listeners of a proxy as an engine does: one for every endpoint, on a free port of this machine.

use std::collections::BTreeMap;

use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::BrokerId;
use gaslightmq_test_kafka::{KafkaCluster, NewTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, TopicName};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Record, RecordBatch, Request, ResponseError};
use tokio::net::TcpListener;

#[tokio::test]
async fn a_client_is_told_the_listener_of_every_broker_and_never_its_real_address() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    let endpoints = backend.endpoints(&Endpoints::none()).await;
    let mut listeners = BTreeMap::new();
    for endpoint in endpoints.iter() {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        backend.listens(endpoint, listener.local_addr()?).await?;
        listeners.insert(endpoint, listener);
    }
    let metadata = Request::new(12, 1, Some("order-ingest"), &MetadataRequest::default())?;
    let answer = backend
        .call(BackendRequest::new(
            CallId::FIRST,
            ConnectionNumber::new(1),
            Endpoint::Bootstrap,
            metadata,
        ))
        .await;

    let broker = |id: u16| Endpoint::Broker(BrokerId::from(id));
    assert_eq!(
        endpoints.iter().collect::<Vec<_>>(),
        [Endpoint::Bootstrap, broker(1), broker(2), broker(3)]
    );
    let BackendAnswer::Truth(truth) = answer else {
        anyhow::bail!("the cluster answers a Metadata request, and the backend made of it: {answer:?}");
    };
    let addresses = |response: MetadataResponse| {
        let mut brokers: Vec<(i32, String)> = response
            .brokers
            .iter()
            .map(|broker| (broker.node_id.0, format!("{}:{}", broker.host, broker.port)))
            .collect();
        brokers.sort();
        brokers
    };
    // A client is sent to the advertised host, and to the port that the listener of each broker got.
    let mut listening = Vec::new();
    let mut real = Vec::new();
    for (id, broker) in (1..).zip(cluster.brokers()) {
        let port = listeners[&Endpoint::Broker(broker.id())].local_addr()?.port();
        listening.push((id, format!("gaslight.test:{port}")));
        real.push((id, broker.address().to_string()));
    }
    assert_eq!(addresses(truth.response().body()?), listening);
    // The record of the call keeps what the cluster answered: where each broker really is.
    let Some(of_the_cluster) = truth.upstream() else {
        anyhow::bail!("the overlay changed the answer, and the truth keeps none of the cluster next to it");
    };
    assert_eq!(addresses(of_the_cluster.body()?), real);
    Ok(())
}

#[tokio::test]
async fn a_request_on_the_listener_of_a_broker_goes_to_that_broker() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
    // Broker 2 leads the one partition, and it is the only broker that takes records for it.
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_replica_assignment([[BrokerId::from(2)]]))
        .await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    let mut listeners = BTreeMap::new();
    for endpoint in backend.endpoints(&Endpoints::none()).await.iter() {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        backend.listens(endpoint, listener.local_addr()?).await?;
        listeners.insert(endpoint, listener);
    }

    // One client per broker writes a record to the partition, each on the listener of its broker.
    // The proxy counts their calls together, and each client has a connection of its own.
    let calls = std::iter::successors(Some(CallId::FIRST), |call| Some(call.next()));
    let mut answered = Vec::new();
    for (broker, call) in cluster.brokers().iter().zip(calls) {
        let placed = RecordBatch::new([Record::new("placed")]).encode()?;
        let partition = PartitionProduceData::default().with_index(0).with_records(Some(placed));
        let topic = TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(orders.name().to_string())))
            .with_partition_data(vec![partition]);
        let produce = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![topic]);
        let request = Request::new(10, 1, Some("order-ingest"), &produce)?;
        let connection = ConnectionNumber::new(call.value());
        let answer = backend
            .call(BackendRequest::new(
                call,
                connection,
                Endpoint::Broker(broker.id()),
                request,
            ))
            .await;
        let BackendAnswer::Truth(truth) = answer else {
            anyhow::bail!(
                "broker {} answers a Produce, and the backend made of it: {answer:?}",
                broker.id()
            );
        };
        answered.push(truth);
    }

    let mut refusals = Vec::new();
    for truth in &answered {
        let produced: ProduceResponse = truth.response().body()?;
        refusals.push(ResponseError::try_from_code(
            produced.responses[0].partition_responses[0].error_code,
        ));
    }
    assert_eq!(
        refusals,
        [
            Some(ResponseError::NotLeaderOrFollower),
            None,
            Some(ResponseError::NotLeaderOrFollower)
        ]
    );
    assert_eq!(orders.partition(0).records().await?.len(), 1);
    // A broker that does not lead the partition says who does, and the client is sent to the listener of that broker.
    let turned_away: ProduceResponse = answered[0].response().body()?;
    let leaders: Vec<(i32, String)> = turned_away
        .node_endpoints
        .iter()
        .map(|leader| (leader.node_id.0, format!("{}:{}", leader.host, leader.port)))
        .collect();
    let of_the_leader = listeners[&Endpoint::Broker(BrokerId::from(2))].local_addr()?.port();
    assert_eq!(leaders, [(2, format!("gaslight.test:{of_the_leader}"))]);
    Ok(())
}
