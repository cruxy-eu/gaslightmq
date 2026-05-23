use std::net::SocketAddr;

use gaslightmq::broker::Broker;
use gaslightmq::builder::BrokerBuilder;

pub struct Harness<C> {
    pub broker: Broker,
    pub client: C,
}

impl<C> Harness<C> {
    pub async fn start(
        builder: BrokerBuilder,
        make_client: impl FnOnce(SocketAddr) -> C,
    ) -> Self {
        let broker = builder.start().await.unwrap();
        let client = make_client(broker.addr());
        Self { broker, client }
    }

    pub async fn shutdown(self) {
        self.broker.shutdown().await;
    }
}
