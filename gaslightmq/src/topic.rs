use crate::record::{FetchResult, ProduceResult, Record, TopicFuture};

pub trait Topic: Send + Sync {
    fn partitions(&self) -> i32 {
        1
    }

    fn on_produce<'a>(
        &'a self,
        _partition: i32,
        _records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async { ProduceResult::Success })
    }

    fn on_fetch(&self, _partition: i32, _offset: i64) -> TopicFuture<'_, FetchResult> {
        Box::pin(async { FetchResult::Empty })
    }
}

pub struct BlackHole {
    num_partitions: i32,
}

impl BlackHole {
    pub fn new(partitions: i32) -> Self {
        Self {
            num_partitions: partitions,
        }
    }
}

impl Topic for BlackHole {
    fn partitions(&self) -> i32 {
        self.num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use bytes::Bytes;

    #[tokio::test]
    async fn blackhole_returns_success_on_produce() {
        let topic = BlackHole::new(3);
        let records = vec![Record::new("hello")];
        let result = topic.on_produce(0, &records).await;
        assert_eq!(result, ProduceResult::Success);
    }

    #[tokio::test]
    async fn blackhole_returns_empty_on_fetch() {
        let topic = BlackHole::new(3);
        let result = topic.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn blackhole_reports_partition_count() {
        let topic = BlackHole::new(5);
        assert_eq!(topic.partitions(), 5);
    }
}
