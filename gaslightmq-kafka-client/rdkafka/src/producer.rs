use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_kafka_client_api::{Producer, ProduceResult};
use gaslightmq_kernel::Record;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as RdProducer};

use crate::context::TestContext;

pub struct RdkafkaProducer {
    producer: BaseProducer<TestContext>,
    context: TestContext,
}

impl From<SocketAddr> for RdkafkaProducer {
    fn from(addr: SocketAddr) -> Self {
        Self::new(addr)
    }
}

impl RdkafkaProducer {
    pub fn new(addr: SocketAddr) -> Self {
        let bootstrap = addr.to_string();
        let context = TestContext::new();

        let producer: BaseProducer<TestContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("message.timeout.ms", "5000")
            .set("log_level", "7")
            .create_with_context(context.clone())
            .expect("failed to create rdkafka producer");

        eprintln!("[rdkafka-producer] created for {}", bootstrap);
        Self { producer, context }
    }

    pub fn assert_no_errors(&self) {
        self.context.assert_no_errors();
    }
}

impl Producer for RdkafkaProducer {
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
