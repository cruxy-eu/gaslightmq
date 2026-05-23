use gaslightmq_kernel::{Partition, Offset};
use crate::chaos::{ChaosAction, ChaosBehavior};
use crate::generator::Generator;
use crate::record::{Api, FetchResult, ProduceResult, Record, TopicFuture};
use crate::topic::Topic;

pub struct ChaosLayer<T, C> {
    inner: T,
    api: Api,
    behavior: C,
}

impl<T, C> ChaosLayer<T, C> {
    pub fn new(inner: T, api: Api, behavior: C) -> Self {
        Self { inner, api, behavior }
    }
}

impl<T: Topic, C: ChaosBehavior> Topic for ChaosLayer<T, C> {
    fn partitions(&self) -> i32 {
        self.inner.partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async move {
            if self.api == Api::Produce {
                match self.behavior.next_action() {
                    ChaosAction::Proceed => self.inner.on_produce(partition, records).await,
                    ChaosAction::Delay(d) => {
                        tokio::time::sleep(d).await;
                        self.inner.on_produce(partition, records).await
                    }
                    ChaosAction::Error(code) => ProduceResult::Error(code),
                    ChaosAction::Timeout => std::future::pending().await,
                }
            } else {
                self.inner.on_produce(partition, records).await
            }
        })
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            if self.api == Api::Fetch {
                match self.behavior.next_action() {
                    ChaosAction::Proceed => self.inner.on_fetch(partition, offset).await,
                    ChaosAction::Delay(d) => {
                        tokio::time::sleep(d).await;
                        self.inner.on_fetch(partition, offset).await
                    }
                    ChaosAction::Error(code) => FetchResult::Error(code),
                    ChaosAction::Timeout => std::future::pending().await,
                }
            } else {
                self.inner.on_fetch(partition, offset).await
            }
        })
    }
}

pub struct GeneratorLayer<T, G> {
    inner: T,
    generator: G,
}

impl<T, G> GeneratorLayer<T, G> {
    pub fn new(inner: T, generator: G) -> Self {
        Self { inner, generator }
    }
}

impl<T: Topic, G: Generator> Topic for GeneratorLayer<T, G> {
    fn partitions(&self) -> i32 {
        self.inner.partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        self.inner.on_produce(partition, records)
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let result = self.inner.on_fetch(partition, offset).await;
            if result == FetchResult::Empty {
                use futures::StreamExt;
                let records: Vec<Record> = self.generator.stream().take(1).collect().await;
                if records.is_empty() {
                    FetchResult::Empty
                } else {
                    FetchResult::Records(records)
                }
            } else {
                result
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Api, BlackHole, Buffer, Delay, ErrorResponse, FetchResult, ProduceResult, Record};
    use crate::generator::RandomBytes;
    use gaslightmq_kernel::{Partition, Offset};
    use kafka_protocol::error::ResponseError;
    use std::time::Duration;

    #[tokio::test]
    async fn chaos_layer_applies_error_on_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            ErrorResponse::new(ResponseError::NotLeaderOrFollower),
        );

        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Error(ResponseError::NotLeaderOrFollower));
    }

    #[tokio::test]
    async fn chaos_layer_passes_through_on_non_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Produce,
            ErrorResponse::new(ResponseError::NotLeaderOrFollower),
        );

        // Chaos targets Produce, so Fetch should pass through
        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("hello")]));
    }

    #[tokio::test]
    async fn chaos_layer_applies_error_on_produce() {
        let chaos = ChaosLayer::new(
            BlackHole::new(1),
            Api::Produce,
            ErrorResponse::new(ResponseError::RequestTimedOut),
        );

        let result = chaos.on_produce(Partition::new(0), &[Record::new("hello")]).await;
        assert_eq!(result, ProduceResult::Error(ResponseError::RequestTimedOut));
    }

    #[tokio::test]
    async fn chaos_layer_delay_still_returns_result() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("data")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            Delay::new(Duration::from_millis(1)),
        );

        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("data")]));
    }

    #[tokio::test]
    async fn generator_layer_fills_empty_fetch() {
        let topic = BlackHole::new(1);
        let layer = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = layer.on_fetch(Partition::new(0), Offset::new(0)).await;
        match result {
            FetchResult::Records(records) => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].value.as_ref().unwrap().len(), 32);
            }
            other => panic!("expected Records, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generator_layer_does_not_override_real_data() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("real-data")]).await;

        let layer = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = layer.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("real-data")]));
    }
}
