use bytes::Bytes;
use crate::record::{Record, RecordStream};
use fake::Fake;
use fake::faker::name::en::Name;
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::{Word, Sentence};
use futures::stream;

pub trait Generator: Send + Sync {
    fn stream(&self) -> RecordStream;
}

pub struct RandomBytes {
    size: usize,
}

impl RandomBytes {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl Generator for RandomBytes {
    fn stream(&self) -> RecordStream {
        let size = self.size;
        Box::pin(stream::repeat_with(move || {
            use rand::Rng;
            let mut rng = rand::rng();
            Record {
                key: Some(Bytes::from(rng.random::<u64>().to_be_bytes().to_vec())),
                value: Some(Bytes::copy_from_slice(
                    &(0..size).map(|_| rng.random::<u8>()).collect::<Vec<_>>(),
                )),
                headers: Vec::new(),
            }
        }))
    }
}

pub struct JsonTemplate {
    template: String,
    key_template: Option<String>,
}

impl JsonTemplate {
    pub fn new(template: &str) -> Self {
        Self {
            template: template.to_string(),
            key_template: None,
        }
    }

    pub fn with_key(mut self, key_template: &str) -> Self {
        self.key_template = Some(key_template.to_string());
        self
    }

    fn render(template: &str) -> Bytes {
        let mut result = template.to_string();
        while let Some(start) = result.find("{{") {
            if let Some(end_rel) = result[start..].find("}}") {
                let end = start + end_rel + 2;
                let placeholder = result[start + 2..end - 2].to_string();
                let replacement = Self::generate_value(&placeholder);
                result.replace_range(start..end, &replacement);
            } else {
                break;
            }
        }
        Bytes::from(result)
    }

    fn generate_value(placeholder: &str) -> String {
        use rand::Rng;
        let mut rng = rand::rng();
        match placeholder {
            "uuid" => uuid::Uuid::new_v4().to_string(),
            "name" => Name().fake::<String>(),
            "email" => SafeEmail().fake::<String>(),
            "word" => Word().fake::<String>(),
            "sentence" => Sentence(3..8).fake::<String>(),
            "i32" => rng.random::<i32>().to_string(),
            "i64" => rng.random::<i64>().to_string(),
            "bool" => rng.random::<bool>().to_string(),
            _ => format!("{{{{{placeholder}}}}}"),
        }
    }
}

impl Generator for JsonTemplate {
    fn stream(&self) -> RecordStream {
        let template = self.template.clone();
        let key_template = self.key_template.clone();
        Box::pin(stream::repeat_with(move || Record {
            key: key_template.as_deref().map(Self::render),
            value: Some(Self::render(&template)),
            headers: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn random_bytes_stream_yields_correct_size() {
        let generator = RandomBytes::new(64);
        let record = generator.stream().next().await.unwrap();
        assert_eq!(record.value.as_ref().unwrap().len(), 64);
        assert!(record.key.is_some());
    }

    #[tokio::test]
    async fn random_bytes_stream_is_infinite() {
        let generator = RandomBytes::new(8);
        let records: Vec<_> = generator.stream().take(100).collect().await;
        assert_eq!(records.len(), 100);
    }

    #[tokio::test]
    async fn json_template_replaces_uuid_placeholder() {
        let generator = JsonTemplate::new(r#"{"id": "{{uuid}}"}"#).with_key("{{uuid}}");
        let record = generator.stream().next().await.unwrap();
        assert!(record.key.is_some());
        let value = std::str::from_utf8(record.value.as_ref().unwrap()).unwrap();
        assert!(value.starts_with(r#"{"id": ""#));
        assert!(value.ends_with(r#""}"#));
        assert_ne!(value, r#"{"id": "{{uuid}}"}"#);
    }

    #[tokio::test]
    async fn json_template_replaces_multiple_placeholders() {
        let generator = JsonTemplate::new(r#"{"a": "{{uuid}}", "b": "{{uuid}}"}"#);
        let record = generator.stream().next().await.unwrap();
        let value = std::str::from_utf8(record.value.as_ref().unwrap()).unwrap();
        assert!(!value.contains("{{uuid}}"));
    }
}
