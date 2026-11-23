//! A real cluster, set up and looked into through its own handles.
//!
//! The tests need Docker. Most of them share one cluster of three brokers, and keep apart by the names of their topics.

#![expect(
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod records;
mod starting;
mod topics;
mod transactions;

use gaslightmq_test_kafka::KafkaCluster;
use tokio::sync::OnceCell;

/// The cluster of three brokers that the tests of this binary share.
/// The first test that asks for it starts it, and it ends with the process.
async fn kafka() -> &'static KafkaCluster {
    static CLUSTER: OnceCell<KafkaCluster> = OnceCell::const_new();
    CLUSTER
        .get_or_init(|| async {
            KafkaCluster::plaintext()
                .with_brokers(3)
                .start()
                .await
                .unwrap_or_else(|error| panic!("{error}"))
        })
        .await
}
