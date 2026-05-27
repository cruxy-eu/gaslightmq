use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_kafka_client_api::{
    Consumer, HeartbeatResult, JoinGroupResult, Producer, ProduceResult, RecordStream,
    SyncGroupResult,
};
use gaslightmq_kernel::Record;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdConsumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as RdProducer};

pub struct RdkafkaClient {
    producer: BaseProducer,
    consumer: BaseConsumer,
    bootstrap: String,
}

impl From<SocketAddr> for RdkafkaClient {
    fn from(addr: SocketAddr) -> Self {
        Self::new(addr)
    }
}

impl RdkafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        let bootstrap = format!("{}", addr);

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("failed to create rdkafka producer");

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", "gaslightmq-test")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create rdkafka consumer");

        Self {
            producer,
            consumer,
            bootstrap,
        }
    }
}

#[async_trait]
impl Producer for RdkafkaClient {
    async fn send(
        &self,
        topic: &str,
        partition: Partition,
        records: &[Record],
    ) -> ProduceResult {
        for record in records {
            let mut base_record = BaseRecord::to(topic).partition(i32::from(partition));

            if let Some(ref key) = record.key {
                base_record = base_record.key(key.as_ref());
            }
            if let Some(ref value) = record.value {
                base_record = base_record.payload(value.as_ref());
            }

            self.producer.send(base_record).unwrap();
        }
        self.producer.flush(Duration::from_secs(5)).unwrap();

        ProduceResult {
            error: None,
            partition,
            offset: Offset::new(0),
        }
    }
}

#[async_trait]
impl Consumer for RdkafkaClient {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        use rdkafka::topic_partition_list::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            topic,
            i32::from(partition),
            rdkafka::Offset::Offset(i64::from(offset)),
        )
        .unwrap();
        self.consumer.assign(&tpl).unwrap();

        let mut records = Vec::new();
        while let Some(Ok(msg)) = self.consumer.poll(Duration::from_secs(1)) {
            let key = msg.key().map(Bytes::copy_from_slice);
            let value = msg.payload().map(Bytes::copy_from_slice);
            let mut record = Record::empty();
            record.key = key;
            record.value = value;
            records.push(record);
        }
        records
    }

    fn stream(&self, _topic: &str, _partition: Partition, _offset: Offset) -> RecordStream {
        Box::pin(futures::stream::empty())
    }

    // REVIEW: This doesn't make sense
    async fn join_group(&self, _group_id: &str) -> JoinGroupResult {
        JoinGroupResult {
            error: None,
            member_id: String::new(),
            leader: String::new(),
        }
    }

    // REVIEW: This doesn't make sense
    async fn sync_group(&self, _group_id: &str) -> SyncGroupResult {
        SyncGroupResult { error: None }
    }

    // REVIEW: This doesn't make sense
    async fn heartbeat(&self, _group_id: &str) -> HeartbeatResult {
        HeartbeatResult { error: None }
    }
}
