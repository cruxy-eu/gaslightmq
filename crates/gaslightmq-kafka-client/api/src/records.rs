//! Conversion between facade [`Record`]s and Kafka record batches.

use bytes::{Bytes, BytesMut};
use gaslightmq_kernel::{Header, Offset, Record};
use indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record as KafkaRecord,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet, TimestampType,
};

/// Convert facade records to wire records, numbering offsets from 0.
pub fn to_kafka(records: &[Record]) -> Vec<KafkaRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers = IndexMap::new();
            for h in &r.headers {
                headers.insert(StrBytes::from_string(h.key.clone()), h.value.clone());
            }
            KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: i as i32,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

/// Convert a decoded record set to facade records.
pub fn from_kafka(batch: &RecordSet) -> Vec<Record> {
    batch
        .records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| Header {
                    key: k.as_str().to_string(),
                    value: v.clone(),
                })
                .collect();
            let mut record = Record::empty()
                .with_headers(headers)
                .with_offset(Offset::from(r.offset))
                .with_timestamp(r.timestamp);
            record.key = r.key.clone();
            record.value = r.value.clone();
            record
        })
        .collect()
}

/// Encode records as one uncompressed v2 record batch.
pub fn encode_batch(records: &[Record]) -> Bytes {
    let kafka_records = to_kafka(records);
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &kafka_records, &options).unwrap();
    Bytes::from(buf)
}

/// Decode every record batch in a fetched `records` blob.
pub fn decode_batches(raw: &Bytes) -> Vec<Record> {
    let mut buf = raw.clone();
    let mut records = Vec::new();
    while !buf.is_empty() {
        match RecordBatchDecoder::decode(&mut buf) {
            Ok(set) => records.extend(from_kafka(&set)),
            Err(_) => break,
        }
    }
    records
}
