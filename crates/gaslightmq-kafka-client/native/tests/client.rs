//! The client against the in-process fake broker.

use std::time::Duration;

use gaslightmq::{Broker, BrokerBuilder, Buffer};
use gaslightmq_testing::proxy::FakeCluster;
use gaslightmq_kafka_client_native::{ClientConfig, ClientError, KafkaClient};
use gaslightmq_kernel::{Offset, Record};
use kafka_protocol::error::ResponseError;

async fn broker(topics: &[(&str, i32)]) -> Broker {
    let mut config = BrokerBuilder::new().bind("127.0.0.1:0").build_config();
    for (name, partitions) in topics {
        config.topics.insert(name.to_string(), Box::new(Buffer::new(*partitions)));
    }
    Broker::start(config).await.unwrap()
}

fn values(records: &[Record]) -> Vec<String> {
    records.iter().map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_fetch_and_watermarks() {
    let broker = broker(&[("orders", 2)]).await;
    let client = KafkaClient::from(broker.addr());

    let first = client.produce("orders", 1, &[Record::new("a"), Record::new("b")]).await.unwrap();
    client.produce("orders", 1, &[Record::new("c")]).await.unwrap();

    assert_eq!(first, Offset::from(0i64));
    assert_eq!(values(&client.fetch("orders", 1, 0).await.unwrap()), ["a", "b", "c"]);
    let marks = client.watermarks("orders", 1).await.unwrap();
    assert_eq!((i64::from(marks.low), i64::from(marks.high)), (0, 3));
    let topics = client.topic_metadata(Some("orders")).await.unwrap();
    assert_eq!(topics[0].partitions.len(), 2);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn routes_to_partition_leaders() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let client = KafkaClient::from(cluster.addr(0));

    for p in 0..3 {
        client.produce("orders", p, &[Record::new(format!("p{p}"))]).await.unwrap();
    }

    // Node n leads partition n; each record must be stored on its leader only.
    for p in 0..3 {
        for node in 0..3 {
            let expected: Vec<String> = if node == p as usize { vec![format!("p{p}")] } else { vec![] };
            assert_eq!(cluster.values(node, "orders", p).await, expected, "node {node}, partition {p}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_topic_times_out_with_last_error() {
    let broker = broker(&[]).await;
    let client = KafkaClient::new(ClientConfig::new(broker.addr().to_string()).delivery_timeout(Duration::from_millis(300)));

    let err = client.produce("nope", 0, &[Record::new("x")]).await.unwrap_err();

    assert_eq!(err, ClientError::DeliveryTimeout(Box::new(ClientError::Broker(ResponseError::UnknownTopicOrPartition))));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_bootstrap_is_a_delivery_timeout() {
    let client = KafkaClient::new(ClientConfig::new("127.0.0.1:1").delivery_timeout(Duration::from_millis(200)));

    let err = client.produce("orders", 0, &[Record::new("x")]).await.unwrap_err();

    assert!(matches!(err, ClientError::DeliveryTimeout(ref last) if matches!(**last, ClientError::Io(_))), "{err}");
}
