//! A real cluster, set up and looked into through its own handles.
//!
//! The tests need Docker. Most of them share one cluster of three brokers, and keep apart by the names of their topics.

mod records;
mod starting;
mod topics;
mod transactions;

use gaslightmq_test_kafka::{KafkaCluster, KafkaClusterError};
use tokio::sync::OnceCell;

/// The cluster of three brokers that the tests of this binary share.
/// The first test that asks for it starts it, and it ends with the process.
async fn kafka() -> Result<&'static KafkaCluster, KafkaClusterError> {
    static CLUSTER: OnceCell<KafkaCluster> = OnceCell::const_new();
    CLUSTER
        .get_or_try_init(|| KafkaCluster::plaintext().with_brokers(3).start())
        .await
}
