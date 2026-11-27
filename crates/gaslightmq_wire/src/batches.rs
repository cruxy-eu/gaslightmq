use bytes::{Bytes, BytesMut};
use kafka_protocol::records::{
    Compression, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet, TimestampType,
};

use crate::{DecodeError, EncodeError};

/// The batches of a `records` field with everything that each batch and each record holds,
/// so that they encode again as they arrived.
///
/// A [`RecordBatch`](crate::RecordBatch) is what someone writes records with, and it keeps their content only.
/// `RecordBatches` keeps the rest too: the producer ID and epoch, the sequence numbers, the leader epoch,
/// whether a batch is transactional or a control batch, the type of its timestamps, and its compression.
/// It is what a rewrite of a `records` field reads, changes and writes again.
///
/// ```
/// use gaslightmq_wire::{DecodeError, Record, RecordBatch, RecordBatches};
///
/// let produced = RecordBatch::new(vec![Record::new("4111 1111 1111 1111").with_key("card").with_header("trace", "t-1")]);
/// let bytes = produced.encode().expect("a batch");
///
/// let kept = RecordBatches::decode(&bytes)?.without_payloads().encode().expect("the same batches");
///
/// let record = &RecordBatch::decode_all(&kept)?[0].records[0];
/// assert_eq!((record.key.as_ref(), record.value.as_ref()), (None, None));
/// assert!(record.headers.is_empty());
/// assert_eq!(record.offset, Some(0u64.into()));
/// # Ok::<(), DecodeError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatches(Vec<RecordSet>);

impl RecordBatches {
    /// Decode every batch in the `records` field of a message.
    ///
    /// A Fetch response may end in the middle of a batch, when the broker cut it off at the size the client asked for.
    /// That last piece is left out, as a Kafka client skips it.
    ///
    /// # Errors
    ///
    /// A [`DecodeError`] when a batch announces a negative length, is all there and does not decode,
    /// or is of a message format older than version 2.
    pub fn decode(bytes: &Bytes) -> Result<Self, DecodeError> {
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
            batches.push(RecordBatchDecoder::decode(&mut batch).map_err(|e| DecodeError::new("record batch", e))?);
        }
        Ok(Self(batches))
    }

    /// The same batches, with the key, the value and the headers of every record taken out.
    ///
    /// Everything else stays: the offsets, the timestamps and their type, the sequence numbers, the producer,
    /// and whether a batch is transactional. A control batch, which marks the end of a transaction, keeps its
    /// records as they are: their key and value are the marker, and no data of a producer.
    ///
    /// A batch that lost its payloads encodes without compression: what is left of it is a few numbers,
    /// and whoever keeps it compresses what it keeps.
    #[must_use]
    pub fn without_payloads(mut self) -> Self {
        for set in &mut self.0 {
            let mut stripped = false;
            for record in set.records.iter_mut().filter(|record| !record.control) {
                record.key = None;
                record.value = None;
                record.headers.clear();
                stripped = true;
            }
            if stripped {
                set.compression = Compression::None;
            }
        }
        self
    }

    /// How many records the batches hold together.
    pub fn records(&self) -> usize {
        self.0.iter().map(|set| set.records.len()).sum()
    }

    /// Encode the batches one after the other, each with its own version and compression.
    ///
    /// A batch without records encodes to no bytes, because the format has no empty batch.
    ///
    /// # Errors
    ///
    /// An [`EncodeError`] when a batch cannot be written, for example because compressing it fails.
    pub fn encode(&self) -> Result<Bytes, EncodeError> {
        let mut out = BytesMut::new();
        for set in &self.0 {
            let options = RecordEncodeOptions {
                version: set.version,
                compression: set.compression,
            };
            let start = out.len();
            RecordBatchEncoder::encode(&mut out, &set.records, &options)
                .map_err(|e| EncodeError::new("record batch", e))?;
            let appended_by_the_log = set
                .records
                .first()
                .is_some_and(|record| record.timestamp_type == TimestampType::LogAppend);
            if appended_by_the_log {
                mark_log_append_time(&mut out[start..]);
            }
        }
        Ok(out.freeze())
    }
}

/// Says in every batch of `batches` that its timestamps are the time the log appended it.
///
/// The encoder writes the attributes of a batch without the type of its timestamps, which is one bit of them.
/// This sets the bit, and writes the checksum of the batch again, because the checksum covers the attributes.
/// `batches` holds whole batches of the message format version 2, one after the other, as the encoder wrote them.
fn mark_log_append_time(batches: &mut [u8]) {
    /// The base offset (8 bytes) and the length of the rest (4 bytes).
    const PREFIX: usize = 12;
    /// The checksum stands behind the leader epoch (4 bytes) and the magic byte, and covers everything behind itself.
    const CHECKSUM: std::ops::Range<usize> = 17..21;
    /// The attributes are the two bytes behind the checksum, and the type of the timestamps is bit 3 of them.
    const ATTRIBUTES_LOW_BYTE: usize = 22;
    const LOG_APPEND_TIME: u8 = 1 << 3;

    let mut rest = batches;
    while let Some(&[.., l0, l1, l2, l3]) = rest.first_chunk::<PREFIX>() {
        let Ok(len) = usize::try_from(i32::from_be_bytes([l0, l1, l2, l3])) else {
            return;
        };
        if rest.len() < PREFIX + len || PREFIX + len <= ATTRIBUTES_LOW_BYTE {
            return;
        }
        let (batch, following) = rest.split_at_mut(PREFIX + len);
        batch[ATTRIBUTES_LOW_BYTE] |= LOG_APPEND_TIME;
        let checksum = crc32c::crc32c(&batch[CHECKSUM.end..]);
        batch[CHECKSUM].copy_from_slice(&checksum.to_be_bytes());
        rest = following;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Compression, Record, RecordBatch};

    fn field(batches: &[RecordBatch]) -> Bytes {
        let mut field = BytesMut::new();
        for batch in batches {
            field.extend_from_slice(&batch.encode().unwrap());
        }
        field.freeze()
    }

    #[test]
    fn batches_encode_as_they_arrived() {
        let bytes = field(&[
            RecordBatch::new(vec![Record::new("a").with_key("k").with_timestamp(1_000)]),
            RecordBatch::new(vec![Record::new("b").with_offset(1u64).with_header("trace", "t-1")])
                .with_compression(Compression::Gzip),
        ]);

        let batches = RecordBatches::decode(&bytes).unwrap();

        assert_eq!(batches.records(), 2);
        assert_eq!(batches.encode().unwrap(), bytes);
    }

    #[test]
    fn without_payloads_keeps_every_record_and_where_it_is() {
        let bytes = field(&[RecordBatch::new(vec![
            Record::new("a")
                .with_key("k1")
                .with_header("trace", "t-1")
                .with_offset(40u64)
                .with_timestamp(1_000),
            Record::empty().with_key("k2").with_timestamp(1_001),
        ])
        .with_compression(Compression::Gzip)]);

        let kept = RecordBatches::decode(&bytes)
            .unwrap()
            .without_payloads()
            .encode()
            .unwrap();

        let decoded = RecordBatch::decode_all(&kept).unwrap();
        assert_eq!(decoded[0].compression, Compression::None);
        let records = &decoded[0].records;
        assert_eq!(
            records,
            &[
                Record::empty().with_offset(40u64).with_timestamp(1_000),
                Record::empty().with_offset(41u64).with_timestamp(1_001)
            ]
        );
    }

    #[test]
    fn a_control_batch_keeps_its_records_and_its_compression() {
        let mut batches = RecordBatches::decode(&field(&[RecordBatch::new(vec![
            Record::new("commit").with_key("marker"),
        ])
        .with_compression(Compression::Gzip)]))
        .unwrap();
        for record in batches.0.iter_mut().flat_map(|set| set.records.iter_mut()) {
            record.control = true;
        }

        let kept = batches.clone().without_payloads();

        assert_eq!(kept, batches);
    }

    #[test]
    fn a_batch_keeps_the_type_of_its_timestamps() {
        let mut batches = RecordBatches::decode(&field(&[
            RecordBatch::new(vec![
                Record::new("a").with_timestamp(1_000),
                Record::new("b").with_timestamp(1_001),
            ]),
            RecordBatch::new(vec![Record::new("c").with_offset(2u64).with_timestamp(1_002)]),
        ]))
        .unwrap();
        // The broker appended the first batch with its own time, and left the second with the time of the producer.
        for record in &mut batches.0[0].records {
            record.timestamp_type = TimestampType::LogAppend;
        }

        let read_again = RecordBatches::decode(&batches.encode().unwrap()).unwrap();

        assert_eq!(read_again, batches);
        assert_eq!(read_again.0[0].records[0].timestamp_type, TimestampType::LogAppend);
        assert_eq!(read_again.0[1].records[0].timestamp_type, TimestampType::Creation);
        // The decoder checks the checksum of every batch, so the one that was written again is right.
        assert_eq!(read_again.without_payloads().records(), 3);
    }

    #[test]
    fn a_last_batch_that_is_cut_off_is_left_out() {
        let whole = field(&[RecordBatch::new(vec![Record::new("a")])]);
        let mut cut = BytesMut::from(&whole[..]);
        cut.extend_from_slice(&whole[..whole.len() - 3]);

        let batches = RecordBatches::decode(&cut.freeze()).unwrap();

        assert_eq!(batches.records(), 1);
        assert_eq!(batches.encode().unwrap(), whole);
    }

    #[test]
    fn a_whole_batch_that_is_corrupt_is_an_error() {
        let mut bytes = BytesMut::from(&field(&[RecordBatch::new(vec![Record::new("a")])])[..]);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;

        let error = RecordBatches::decode(&bytes.freeze()).unwrap_err();
        assert!(error.to_string().starts_with("cannot decode record batch: "), "{error}");
    }

    #[test]
    fn no_bytes_are_no_batches() {
        let batches = RecordBatches::decode(&Bytes::new()).unwrap();

        assert_eq!(batches.records(), 0);
        assert!(batches.encode().unwrap().is_empty());
    }
}
