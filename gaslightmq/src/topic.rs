use gaslightmq_kernel::{Partition, Offset};
use crate::record::{FetchResult, ProduceResult, Record, TopicFuture};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait Topic: Send + Sync {
    fn partitions(&self) -> i32 {
        1
    }

    fn on_produce<'a>(
        &'a self,
        _partition: Partition,
        _records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async { ProduceResult::Success })
    }

    fn on_fetch(&self, _partition: Partition, _offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async { FetchResult::Empty })
    }
}

impl Topic for Box<dyn Topic> {
    fn partitions(&self) -> i32 {
        (**self).partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        (**self).on_produce(partition, records)
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        (**self).on_fetch(partition, offset)
    }
}

impl Topic for Arc<dyn Topic> {
    fn partitions(&self) -> i32 {
        (**self).partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        (**self).on_produce(partition, records)
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        (**self).on_fetch(partition, offset)
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

pub struct Buffer {
    num_partitions: i32,
    partitions: Mutex<HashMap<Partition, Vec<Record>>>,
}

impl Buffer {
    pub fn new(partitions: i32) -> Self {
        Self {
            num_partitions: partitions,
            partitions: Mutex::new(HashMap::new()),
        }
    }
}

impl Topic for Buffer {
    fn partitions(&self) -> i32 {
        self.num_partitions
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async move {
            let mut store = self.partitions.lock().unwrap();
            store.entry(partition).or_default().extend(records.iter().cloned());
            ProduceResult::Success
        })
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let store = self.partitions.lock().unwrap();
            match store.get(&partition) {
                Some(records) if (offset.value() as usize) < records.len() => {
                    FetchResult::Records(records[offset.value() as usize..].to_vec())
                }
                _ => FetchResult::Empty,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use gaslightmq_kernel::{Partition, Offset};

    #[tokio::test]
    async fn blackhole_returns_success_on_produce() {
        let topic = BlackHole::new(3);
        let records = vec![Record::new("hello")];
        let result = topic.on_produce(Partition::new(0), &records).await;
        assert_eq!(result, ProduceResult::Success);
    }

    #[tokio::test]
    async fn blackhole_returns_empty_on_fetch() {
        let topic = BlackHole::new(3);
        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn blackhole_reports_partition_count() {
        let topic = BlackHole::new(5);
        assert_eq!(topic.partitions(), 5);
    }

    #[tokio::test]
    async fn buffer_stores_and_retrieves_records() {
        let topic = Buffer::new(1);
        let records = vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ];
        let result = topic.on_produce(Partition::new(0), &records).await;
        assert_eq!(result, ProduceResult::Success);

        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ]));
    }

    #[tokio::test]
    async fn buffer_returns_empty_when_no_records() {
        let topic = Buffer::new(1);
        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn buffer_respects_offset() {
        let topic = Buffer::new(1);
        let records = vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
            Record::new("msg-2"),
        ];
        topic.on_produce(Partition::new(0), &records).await;

        let result = topic.on_fetch(Partition::new(0), Offset::new(2)).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-2"),
        ]));
    }

    #[tokio::test]
    async fn buffer_isolates_partitions() {
        let topic = Buffer::new(2);
        topic.on_produce(Partition::new(0), &[Record::new("p0")]).await;
        topic.on_produce(Partition::new(1), &[Record::new("p1")]).await;

        let r0 = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        let r1 = topic.on_fetch(Partition::new(1), Offset::new(0)).await;
        assert_eq!(r0, FetchResult::Records(vec![Record::new("p0")]));
        assert_eq!(r1, FetchResult::Records(vec![Record::new("p1")]));
    }
}
