use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_kafka_client_api::Consumer;
use gaslightmq_kernel::Record;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdConsumer};
use rdkafka::topic_partition_list::TopicPartitionList;

pub struct RdkafkaConsumer {
    consumer: BaseConsumer,
}

impl From<SocketAddr> for RdkafkaConsumer {
    fn from(addr: SocketAddr) -> Self {
        Self::new(addr)
    }
}

impl RdkafkaConsumer {
    pub fn new(addr: SocketAddr) -> Self {
        let bootstrap = addr.to_string();

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", "gaslightmq-test")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create rdkafka consumer");

        Self { consumer }
    }
}

impl Consumer for RdkafkaConsumer {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
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
}
