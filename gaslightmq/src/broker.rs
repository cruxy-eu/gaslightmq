use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use kafka_protocol::messages::ApiKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::builder::BrokerConfig;
use crate::protocol;
use crate::registry::{TopicRegistry, TopicRegistryHandle};

/// A running Kafka-protocol broker that accepts TCP connections and dispatches
/// requests to the registered topic handlers.
pub struct Broker {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl Broker {
    /// Start a broker from the given configuration. Binds TCP, spawns the
    /// accept loop, and returns immediately.
    pub async fn start(config: BrokerConfig) -> std::io::Result<Broker> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let seed: HashMap<String, Arc<dyn crate::topic::Topic>> = config
            .topics
            .into_iter()
            .map(|(k, v)| (k, Arc::from(v) as Arc<dyn crate::topic::Topic>))
            .collect();
        let registry = TopicRegistry::spawn(seed, config.default_topic_mode);
        let host = addr.ip().to_string();
        let port = addr.port() as i32;

        let join_handle = tokio::spawn(accept_loop(listener, registry, host, port, shutdown_rx));

        Ok(Broker {
            addr,
            shutdown_tx,
            join_handle,
        })
    }

    /// Returns the actual bound address (useful when binding to port 0).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully shut down the broker. Signals all tasks to stop and waits
    /// for the accept loop to finish.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join_handle.await;
    }
}

async fn accept_loop(
    listener: TcpListener,
    registry: TopicRegistryHandle,
    host: String,
    port: i32,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _peer)) => {
                        let registry = registry.clone();
                        let host = host.clone();
                        tokio::spawn(handle_connection(
                            stream, registry, host, port, shutdown_rx.clone(),
                        ));
                    }
                    Err(_) => break,
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    registry: TopicRegistryHandle,
    host: String,
    port: i32,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (mut reader, mut writer) = stream.into_split();

    loop {
        tokio::select! {
            result = protocol::read_request(&mut reader) => {
                let parsed = match result {
                    Ok(Some(p)) => p,
                    Ok(None) => break, // clean disconnect
                    Err(_) => break,   // protocol error, close connection
                };

                let api_key = parsed.api_key;
                let api_version = parsed.api_version;
                let correlation_id = parsed.header.correlation_id;
                let mut body = parsed.body;

                let response_body = match ApiKey::try_from(api_key) {
                    Ok(ApiKey::ApiVersions) => {
                        let resp = protocol::handle_api_versions(api_version);
                        // ApiVersions response is always encoded at the requested version,
                        // but capped at what we support. Use version 0 for safety when
                        // client asks for version < 3 (pre-flexible).
                        let encode_version = api_version.min(3);
                        protocol::encode_response_body(&resp, encode_version)
                    }
                    Ok(ApiKey::Metadata) => {
                        match protocol::handle_metadata(
                            &mut body,
                            api_version,
                            &registry,
                            &host,
                            port,
                        ).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    Ok(ApiKey::Produce) => {
                        match protocol::handle_produce(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    Ok(ApiKey::Fetch) => {
                        match protocol::handle_fetch(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    Ok(ApiKey::ListOffsets) => {
                        match protocol::handle_list_offsets(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    Ok(ApiKey::FindCoordinator) => {
                        let resp = protocol::handle_find_coordinator(&host, port);
                        protocol::encode_response_body(&resp, api_version)
                    }
                    Ok(ApiKey::JoinGroup) => {
                        let resp = protocol::handle_join_group(api_version);
                        protocol::encode_response_body(&resp, api_version)
                    }
                    Ok(ApiKey::SyncGroup) => {
                        let resp = protocol::handle_sync_group(api_version);
                        protocol::encode_response_body(&resp, api_version)
                    }
                    Ok(ApiKey::Heartbeat) => {
                        let resp = protocol::handle_heartbeat(api_version);
                        protocol::encode_response_body(&resp, api_version)
                    }
                    Ok(ApiKey::CreateTopics) => {
                        match protocol::handle_create_topics(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    Ok(ApiKey::DeleteTopics) => {
                        match protocol::handle_delete_topics(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(_) => continue,
                        }
                    }
                    _ => {
                        // Unsupported API key, skip
                        continue;
                    }
                };

                let response_bytes = match response_body {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if protocol::write_response(
                    &mut writer,
                    api_key,
                    api_version,
                    correlation_id,
                    &response_bytes,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlackHole, BrokerBuilder};

    #[tokio::test]
    async fn broker_starts_and_shuts_down() {
        let broker = BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0")
            .start()
            .await
            .unwrap();

        let addr = broker.addr();
        assert_ne!(addr.port(), 0);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn broker_accepts_tcp_connection() {
        let broker = BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0")
            .start()
            .await
            .unwrap();

        let conn = tokio::net::TcpStream::connect(broker.addr()).await;
        assert!(conn.is_ok());
        broker.shutdown().await;
    }
}
