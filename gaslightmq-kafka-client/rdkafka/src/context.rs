use std::sync::{Arc, Mutex};

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::producer::ProducerContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::message::DeliveryResult;

#[derive(Clone)]
pub struct TestContext {
    errors: Arc<Mutex<Vec<String>>>,
}

impl TestContext {
    pub fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn assert_no_errors(&self) {
        let errors = self.errors.lock().unwrap();
        assert!(errors.is_empty(), "rdkafka protocol errors:\n{}", errors.join("\n"));
    }
}

impl ClientContext for TestContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                eprintln!("[rdkafka-error] {}: {}", fac, log_message);
                if !is_transport_error(log_message) {
                    self.errors.lock().unwrap().push(format!("{}: {}", fac, log_message));
                }
            }
            _ => {}
        }
    }

    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        eprintln!("[rdkafka-error] {}: {}", error, reason);
        if !is_transport_error(reason) {
            self.errors.lock().unwrap().push(format!("{}: {}", error, reason));
        }
    }
}

impl ConsumerContext for TestContext {}

impl ProducerContext for TestContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {}
}

fn is_transport_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("connection refused")
        || lower.contains("disconnected")
        || lower.contains("timed out")
        || lower.contains("resolve failed")
}
