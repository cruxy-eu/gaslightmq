use std::collections::HashMap;
use std::sync::Arc;

use gaslightmq_scenario::TopicMode;
use tokio::sync::{mpsc, oneshot};

use crate::topic::{BlackHole, Buffer, Topic};

#[derive(Debug, PartialEq)]
pub enum CreateResult {
    Success,
    TopicAlreadyExists,
}

#[derive(Debug, PartialEq)]
pub enum DeleteResult {
    Success,
    UnknownTopicOrPartition,
}

enum RegistryRequest {
    Lookup {
        name: String,
        reply: oneshot::Sender<Option<Arc<dyn Topic>>>,
    },
    Create {
        name: String,
        partitions: i32,
        reply: oneshot::Sender<CreateResult>,
    },
    Delete {
        name: String,
        reply: oneshot::Sender<DeleteResult>,
    },
    ListAll {
        reply: oneshot::Sender<HashMap<String, Arc<dyn Topic>>>,
    },
}

pub struct TopicRegistry {
    topics: HashMap<String, Arc<dyn Topic>>,
    default_mode: TopicMode,
    rx: mpsc::Receiver<RegistryRequest>,
}

impl TopicRegistry {
    pub fn spawn(
        topics: HashMap<String, Arc<dyn Topic>>,
        default_mode: TopicMode,
    ) -> TopicRegistryHandle {
        let (tx, rx) = mpsc::channel(256);
        let registry = Self {
            topics,
            default_mode,
            rx,
        };
        tokio::spawn(registry.run());
        TopicRegistryHandle { sender: tx }
    }

    async fn run(mut self) {
        while let Some(req) = self.rx.recv().await {
            match req {
                RegistryRequest::Lookup { name, reply } => {
                    let _ = reply.send(self.topics.get(&name).cloned());
                }
                RegistryRequest::Create {
                    name,
                    partitions,
                    reply,
                } => {
                    if self.topics.contains_key(&name) {
                        let _ = reply.send(CreateResult::TopicAlreadyExists);
                    } else {
                        let topic: Arc<dyn Topic> = match self.default_mode {
                            TopicMode::Buffer => Arc::new(Buffer::new(partitions)),
                            TopicMode::BlackHole => Arc::new(BlackHole::new(partitions)),
                        };
                        self.topics.insert(name, topic);
                        let _ = reply.send(CreateResult::Success);
                    }
                }
                RegistryRequest::Delete { name, reply } => {
                    if self.topics.remove(&name).is_some() {
                        let _ = reply.send(DeleteResult::Success);
                    } else {
                        let _ = reply.send(DeleteResult::UnknownTopicOrPartition);
                    }
                }
                RegistryRequest::ListAll { reply } => {
                    let _ = reply.send(self.topics.clone());
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct TopicRegistryHandle {
    sender: mpsc::Sender<RegistryRequest>,
}

impl TopicRegistryHandle {
    pub async fn lookup(&self, name: &str) -> Option<Arc<dyn Topic>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(RegistryRequest::Lookup {
            name: name.to_string(),
            reply: tx,
        }).await;
        rx.await.unwrap_or(None)
    }

    pub async fn create(&self, name: &str, partitions: i32) -> CreateResult {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(RegistryRequest::Create {
            name: name.to_string(),
            partitions,
            reply: tx,
        }).await;
        rx.await.unwrap_or(CreateResult::TopicAlreadyExists)
    }

    pub async fn delete(&self, name: &str) -> DeleteResult {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(RegistryRequest::Delete {
            name: name.to_string(),
            reply: tx,
        }).await;
        rx.await.unwrap_or(DeleteResult::UnknownTopicOrPartition)
    }

    pub async fn list_all(&self) -> HashMap<String, Arc<dyn Topic>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(RegistryRequest::ListAll { reply: tx }).await;
        rx.await.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::{BlackHole, Buffer};

    #[tokio::test]
    async fn lookup_seeded_topic() {
        let mut seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        seed.insert("orders".to_string(), Arc::new(Buffer::new(3)));
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let topic = handle.lookup("orders").await;
        assert!(topic.is_some());
        assert_eq!(topic.unwrap().partitions(), 3);
    }

    #[tokio::test]
    async fn lookup_missing_topic_returns_none() {
        let seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let topic = handle.lookup("nope").await;
        assert!(topic.is_none());
    }

    #[tokio::test]
    async fn create_topic_then_lookup() {
        let seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let result = handle.create("orders", 3).await;
        assert_eq!(result, CreateResult::Success);
        let topic = handle.lookup("orders").await;
        assert!(topic.is_some());
        assert_eq!(topic.unwrap().partitions(), 3);
    }

    #[tokio::test]
    async fn create_duplicate_topic_returns_already_exists() {
        let mut seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        seed.insert("orders".to_string(), Arc::new(Buffer::new(1)));
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let result = handle.create("orders", 3).await;
        assert_eq!(result, CreateResult::TopicAlreadyExists);
    }

    #[tokio::test]
    async fn delete_existing_topic() {
        let mut seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        seed.insert("orders".to_string(), Arc::new(Buffer::new(1)));
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let result = handle.delete("orders").await;
        assert_eq!(result, DeleteResult::Success);
        let topic = handle.lookup("orders").await;
        assert!(topic.is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_topic_returns_unknown() {
        let seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let result = handle.delete("nope").await;
        assert_eq!(result, DeleteResult::UnknownTopicOrPartition);
    }

    #[tokio::test]
    async fn list_all_returns_all_topics() {
        let mut seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        seed.insert("orders".to_string(), Arc::new(Buffer::new(3)));
        seed.insert("events".to_string(), Arc::new(BlackHole::new(1)));
        let handle = TopicRegistry::spawn(seed, TopicMode::Buffer);
        let all = handle.list_all().await;
        assert_eq!(all.len(), 2);
        assert!(all.contains_key("orders"));
        assert!(all.contains_key("events"));
    }

    #[tokio::test]
    async fn create_uses_blackhole_when_configured() {
        let seed: HashMap<String, Arc<dyn Topic>> = HashMap::new();
        let handle = TopicRegistry::spawn(seed, TopicMode::BlackHole);
        handle.create("void", 2).await;
        let topic = handle.lookup("void").await.unwrap();
        assert_eq!(topic.partitions(), 2);
    }
}
