use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

pub type Headers = Vec<Header>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Headers,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: Some(value.into()),
            headers: Vec::new(),
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: Some(value.into()),
        });
        self
    }
}
