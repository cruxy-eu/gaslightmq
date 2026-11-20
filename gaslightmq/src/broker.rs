use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use kafka_protocol::messages::ApiKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, warn};

use crate::builder::BrokerConfig;
use crate::cluster::ClusterView;
use crate::protocol;
use crate::registry::{TopicRegistry, TopicRegistryHandle};

pub struct Broker {
    addr: SocketAddr,
    cluster: Arc<RwLock<ClusterView>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl Broker {
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
        let cluster = Arc::new(RwLock::new(ClusterView::single(addr)));

        eprintln!("[broker] listening on {}", addr);
        let join_handle =
            tokio::spawn(accept_loop(listener, registry, cluster.clone(), shutdown_rx));

        Ok(Broker {
            addr,
            cluster,
            shutdown_tx,
            join_handle,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Replace the cluster this broker advertises in Metadata and
    /// FindCoordinator responses. Lets several in-process brokers pose as
    /// one multi-node cluster.
    pub fn set_cluster(&self, view: ClusterView) {
        *self.cluster.write().unwrap() = view;
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join_handle.await;
    }
}

async fn accept_loop(
    listener: TcpListener,
    registry: TopicRegistryHandle,
    cluster: Arc<RwLock<ClusterView>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        eprintln!("[broker] connection from {}", peer);
                        let registry = registry.clone();
                        let cluster = cluster.clone();
                        tokio::spawn(handle_connection(
                            stream, registry, cluster, shutdown_rx.clone(),
                        ));
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }
}

fn api_key_name(api_key: i16) -> String {
    ApiKey::try_from(api_key)
        .map(|k| format!("{:?}", k))
        .unwrap_or_else(|_| format!("Unknown({})", api_key))
}

async fn handle_connection(
    stream: TcpStream,
    registry: TopicRegistryHandle,
    cluster: Arc<RwLock<ClusterView>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (mut reader, mut writer) = stream.into_split();

    loop {
        tokio::select! {
            result = protocol::read_request(&mut reader) => {
                let parsed = match result {
                    Ok(Some(p)) => p,
                    Ok(None) => break,
                    Err(e) => {
                        warn!(error = %e, "failed to read request frame");
                        continue;
                    }
                };

                let api_key = parsed.api_key;
                let api_version = parsed.api_version;
                let correlation_id = parsed.header.correlation_id;
                let mut body = parsed.body;
                let api_name = api_key_name(api_key);

                eprintln!("[broker] <-- {} v{} cid={}", api_name, api_version, correlation_id);
                debug!(api = %api_name, version = api_version, cid = correlation_id, "request");

                let response_body = match ApiKey::try_from(api_key) {
                    Ok(ApiKey::ApiVersions) => {
                        let resp = protocol::handle_api_versions(api_version);
                        let encode_version = api_version.min(3);
                        protocol::encode_response_body(&resp, encode_version)
                    }
                    Ok(ApiKey::Metadata) => {
                        let view = cluster.read().unwrap().clone();
                        match protocol::handle_metadata(&mut body, api_version, &registry, &view).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::Produce) => {
                        match protocol::handle_produce(&mut body, api_version, &registry).await {
                            Ok(Some(resp)) => protocol::encode_response_body(&resp, api_version),
                            Ok(None) => continue,
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::Fetch) => {
                        match protocol::handle_fetch(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::ListOffsets) => {
                        match protocol::handle_list_offsets(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::FindCoordinator) => {
                        let view = cluster.read().unwrap().clone();
                        let resp = protocol::handle_find_coordinator(&view);
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
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::DeleteTopics) => {
                        match protocol::handle_delete_topics(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    Ok(ApiKey::DescribeConfigs) => {
                        match protocol::handle_describe_configs(&mut body, api_version, &registry).await {
                            Ok(resp) => protocol::encode_response_body(&resp, api_version),
                            Err(e) => {
                                error!(api = %api_name, version = api_version, error = %e, "handler failed");
                                continue;
                            }
                        }
                    }
                    _ => {
                        warn!(api_key = api_key, "unsupported api key");
                        continue;
                    }
                };

                let response_bytes = match response_body {
                    Ok(b) => b,
                    Err(e) => {
                        error!(api = %api_name, version = api_version, error = %e, "response encode failed");
                        continue;
                    }
                };

                debug!(api = %api_name, version = api_version, cid = correlation_id, bytes = response_bytes.len(), "response");

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
