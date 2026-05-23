use std::net::SocketAddr;

use gaslightmq::broker::Broker;
use gaslightmq::builder::BrokerConfig;

pub struct Harness<C> {
    pub broker: Broker,
    pub client: C,
}

impl<C> Harness<C> {
    pub async fn start(
        builder: impl Into<BrokerConfig>,
        make_client: impl FnOnce(SocketAddr) -> C,
    ) -> Self {
        let mut config = builder.into();
        config.bind_addr = "127.0.0.1:0".to_string();
        let broker = Broker::start(config).await.unwrap();
        let client = make_client(broker.addr());
        Self { broker, client }
    }

    pub async fn shutdown(self) {
        self.broker.shutdown().await;
    }
}
