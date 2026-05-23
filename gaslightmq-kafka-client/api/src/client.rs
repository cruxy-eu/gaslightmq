use std::net::SocketAddr;

pub struct RawKafkaClient {
    addr: SocketAddr,
}

impl RawKafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}
