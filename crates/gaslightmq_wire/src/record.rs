use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record as KafkaRecord,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use crate::{DecodeError, EncodeError};

/// One header of a [`Record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

/// One record of a partition: what a producer wrote and a consumer reads.
///
/// `offset` and `timestamp` are `None` on a record that was never in a log.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
    pub offset: Option<Offset>,
    pub timestamp: Option<i64>,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            value: Some(value.into()),
            ..Self::empty()
        }
    }

    /// A record without a value, which a compacted topic reads as a tombstone.
    pub fn empty() -> Self {
        Self {
            key: None,
            value: None,
            headers: Vec::new(),
            offset: None,
            timestamp: None,
        }
    }

    #[must_use]
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader {
            key: key.into(),
            value: Some(value.into()),
        });
        self
    }

    #[must_use]
    pub fn with_headers(mut self, headers: Vec<RecordHeader>) -> Self {
        self.headers = headers;
        self
    }

    #[must_use]
    pub fn with_offset(mut self, offset: impl Into<Offset>) -> Self {
        self.offset = Some(offset.into());
        self
    }

    #[must_use]
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Records as they travel inside a Produce request and a Fetch response.
///
/// The `records` field of those messages holds zero or more batches, one
/// after the other. [`encode`](Self::encode) writes one batch, and
/// [`decode_all`](Self::decode_all) reads every batch of such a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub records: Vec<Record>,
    pub compression: Compression,
}

impl RecordBatch {
    /// An uncompressed batch.
    pub fn new(records: impl Into<Vec<Record>>) -> Self {
        Self {
            records: records.into(),
            compression: Compression::None,
        }
    }

    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Encode as one batch of the current format, version 2.
    ///
    /// A record without an offset continues from the first record's offset,
    /// or from 0. A record without a timestamp gets 0. A batch without
    /// records encodes to no bytes, because the format has no empty batch.
    ///
    /// # Errors
    ///
    /// An [`EncodeError`] when a record is too large for the format, or
    /// compressing the batch fails.
    pub fn encode(&self) -> Result<Bytes, EncodeError> {
        if self.records.is_empty() {
            return Ok(Bytes::new());
        }
        let base = self.records[0].offset.map_or(0, i64::from);
        let records: Vec<KafkaRecord> = self
            .records
            .iter()
            .zip(0i32..)
            .map(|(record, sequence)| to_kafka(record, base + i64::from(sequence), sequence))
            .collect();
        let options = RecordEncodeOptions {
            version: 2,
            compression: self.compression,
        };
        let mut out = BytesMut::new();
        RecordBatchEncoder::encode(&mut out, &records, &options).map_err(|e| EncodeError::new("record batch", e))?;
        Ok(out.freeze())
    }

    /// Decode every batch in the `records` field of a message.
    ///
    /// A Fetch response may end in the middle of a batch, when the broker
    /// cut it off at the size the client asked for. That last piece is
    /// skipped, as a Kafka client skips it. A batch that is all there and
    /// does not decode is an error.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`] when a batch announces a negative length, or is
    /// all there and does not decode.
    pub fn decode_all(bytes: &Bytes) -> Result<Vec<Self>, DecodeError> {
        /// A batch starts with its base offset (8 bytes) and the length of the rest (4 bytes).
        const PREFIX: usize = 12;

        let mut rest = bytes.clone();
        let mut batches = Vec::new();
        while let Some(&[.., l0, l1, l2, l3]) = rest.first_chunk::<PREFIX>() {
            let Ok(len) = usize::try_from(i32::from_be_bytes([l0, l1, l2, l3])) else {
                return Err(DecodeError::new("record batch", "negative length"));
            };
            if rest.len() < PREFIX + len {
                break;
            }
            let mut batch = rest.split_to(PREFIX + len);
            let set = RecordBatchDecoder::decode(&mut batch).map_err(|e| DecodeError::new("record batch", e))?;
            batches.push(Self {
                records: set.records.iter().map(from_kafka).collect(),
                compression: set.compression,
            });
        }
        Ok(batches)
    }
}

impl FromIterator<Record> for RecordBatch {
    fn from_iter<I: IntoIterator<Item = Record>>(records: I) -> Self {
        Self::new(records.into_iter().collect::<Vec<_>>())
    }
}

fn to_kafka(record: &Record, next_offset: i64, sequence: i32) -> KafkaRecord {
    let mut headers = IndexMap::new();
    for header in &record.headers {
        headers.insert(StrBytes::from_string(header.key.clone()), header.value.clone());
    }
    KafkaRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: record.offset.map_or(next_offset, i64::from),
        sequence,
        timestamp: record.timestamp.unwrap_or(0),
        key: record.key.clone(),
        value: record.value.clone(),
        headers,
    }
}

fn from_kafka(record: &KafkaRecord) -> Record {
    Record {
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record
            .headers
            .iter()
            .map(|(key, value)| RecordHeader {
                key: key.to_string(),
                value: value.clone(),
            })
            .collect(),
        offset: Some(Offset::from(record.offset)),
        timestamp: Some(record.timestamp),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Partition(u32);

impl Partition {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for Partition {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<Partition> for u32 {
    fn from(p: Partition) -> Self {
        p.0
    }
}

/// From the number on the wire, which is signed. The bits are kept, so a
/// negative number converts back to itself.
impl From<i32> for Partition {
    fn from(v: i32) -> Self {
        Self(v.cast_unsigned())
    }
}

/// To the number on the wire, which is signed. The bits are kept.
impl From<Partition> for i32 {
    fn from(p: Partition) -> Self {
        p.0.cast_signed()
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> Self {
        o.0
    }
}

/// From the number on the wire, which is signed. The bits are kept, so a
/// negative number such as the `-1` of a failed Produce converts back to
/// itself.
impl From<i64> for Offset {
    fn from(v: i64) -> Self {
        Self(v.cast_unsigned())
    }
}

/// To the number on the wire, which is signed. The bits are kept.
impl From<Offset> for i64 {
    fn from(o: Offset) -> Self {
        o.0.cast_signed()
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders() -> Vec<Record> {
        vec![
            Record::new("a")
                .with_key("k1")
                .with_header("trace", "t-1")
                .with_timestamp(1_000),
            Record::new("b").with_timestamp(1_001),
            Record::empty().with_key("k1").with_timestamp(1_002),
        ]
    }

    #[test]
    fn batch_decodes_what_it_encoded() {
        let bytes = RecordBatch::new(orders()).encode().unwrap();

        let batches = RecordBatch::decode_all(&bytes).unwrap();

        assert_eq!(batches.len(), 1);
        let expected: Vec<Record> = orders()
            .into_iter()
            .enumerate()
            .map(|(i, r)| r.with_offset(i as u64))
            .collect();
        assert_eq!(batches[0].records, expected);
        assert_eq!(batches[0].compression, Compression::None);
    }

    #[test]
    fn compressed_batch_decodes_too() {
        let bytes = RecordBatch::new(orders())
            .with_compression(Compression::Gzip)
            .encode()
            .unwrap();

        let batches = RecordBatch::decode_all(&bytes).unwrap();

        assert_eq!(batches[0].compression, Compression::Gzip);
        assert_eq!(batches[0].records.len(), 3);
    }

    #[test]
    fn offsets_continue_from_the_first_record() {
        let batch = RecordBatch::new(vec![
            Record::new("a").with_offset(40u64),
            Record::new("b"),
            Record::new("c"),
        ]);

        let decoded = RecordBatch::decode_all(&batch.encode().unwrap()).unwrap();

        let offsets: Vec<u64> = decoded[0].records.iter().map(|r| r.offset.unwrap().value()).collect();
        assert_eq!(offsets, [40, 41, 42]);
    }

    #[test]
    fn decodes_every_batch_of_a_field() {
        let mut field = BytesMut::new();
        field.extend_from_slice(&RecordBatch::new(vec![Record::new("a")]).encode().unwrap());
        field.extend_from_slice(
            &RecordBatch::new(vec![Record::new("b").with_offset(1u64)])
                .encode()
                .unwrap(),
        );

        let batches = RecordBatch::decode_all(&field.freeze()).unwrap();

        let values: Vec<_> = batches
            .iter()
            .flat_map(|b| &b.records)
            .map(|r| r.value.clone().unwrap())
            .collect();
        assert_eq!(values, ["a", "b"]);
    }

    #[test]
    fn skips_a_last_batch_that_is_cut_off() {
        let whole = RecordBatch::new(vec![Record::new("a")]).encode().unwrap();
        let mut field = BytesMut::from(&whole[..]);
        field.extend_from_slice(&whole[..whole.len() - 3]);

        let batches = RecordBatch::decode_all(&field.freeze()).unwrap();

        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn a_whole_batch_that_is_corrupt_is_an_error() {
        let mut bytes = BytesMut::from(&RecordBatch::new(vec![Record::new("a")]).encode().unwrap()[..]);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;

        let error = RecordBatch::decode_all(&bytes.freeze()).unwrap_err();
        assert!(error.to_string().starts_with("cannot decode record batch: "), "{error}");
    }

    #[test]
    fn no_records_are_no_bytes() {
        assert!(RecordBatch::new(vec![]).encode().unwrap().is_empty());
        assert_eq!(RecordBatch::decode_all(&Bytes::new()).unwrap(), vec![]);
    }

    #[test]
    fn partition_new_and_value() {
        let p = Partition::new(42);
        assert_eq!(p.value(), 42);
    }

    #[test]
    fn partition_from_u32() {
        let p = Partition::from(5u32);
        assert_eq!(p.value(), 5);
    }

    #[test]
    fn partition_from_i32() {
        let p = Partition::from(7i32);
        assert_eq!(p.value(), 7);
    }

    #[test]
    fn partition_to_i32() {
        let p = Partition::new(7);
        let val: i32 = p.into();
        assert_eq!(val, 7);
    }

    #[test]
    fn partition_equality() {
        assert_eq!(Partition::new(1), Partition::new(1));
        assert_ne!(Partition::new(1), Partition::new(2));
    }

    #[test]
    fn offset_new_and_value() {
        let o = Offset::new(100);
        assert_eq!(o.value(), 100);
    }

    #[test]
    fn offset_from_i64() {
        let o = Offset::from(42i64);
        assert_eq!(o.value(), 42);
    }

    #[test]
    fn offset_to_i64() {
        let o = Offset::new(99);
        let val: i64 = o.into();
        assert_eq!(val, 99);
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::new(5) > Offset::new(3));
    }
}
