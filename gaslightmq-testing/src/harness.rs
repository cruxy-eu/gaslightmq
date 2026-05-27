use std::net::SocketAddr;
use std::sync::Mutex;

use gaslightmq::broker::Broker;
use gaslightmq::builder::{BrokerBuilder, BrokerConfig};
use tokio::sync::OnceCell;

pub struct Harness {
    config: Mutex<Option<BrokerConfig>>,
    broker: OnceCell<Broker>,
}

impl Default for Harness {
    fn default() -> Self {
        Self::with(BrokerBuilder::new())
    }
}

impl Harness {
    pub fn with(builder: impl Into<BrokerConfig>) -> Self {
        let mut config = builder.into();
        config.bind_addr = "127.0.0.1:0".to_string();
        Self {
            config: Mutex::new(Some(config)),
            broker: OnceCell::new(),
        }
    }

    pub async fn client<C: From<SocketAddr>>(&self) -> C {
        C::from(self.addr().await)
    }

    pub async fn addr(&self) -> SocketAddr {
        let broker = self.broker.get_or_init(|| async {
            let config = self.config.lock().unwrap().take().unwrap();
            Broker::start(config).await.unwrap()
        }).await;
        broker.addr()
    }

    pub async fn shutdown(self) {
        if let Some(broker) = self.broker.into_inner() {
            broker.shutdown().await;
        }
    }
}
