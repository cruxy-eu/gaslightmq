use crate::number::{number_sentinel, number_value};
use crate::text::{self, text_value};

number_value! {
    /// The ID a cluster gives an idempotent or a transactional producer.
    ///
    /// A producer ID is between zero and [`i64::MAX`].
    ///
    /// Kafka writes "no producer ID" as `-1`, as in a batch from a producer that is not idempotent.
    /// That number is not a `ProducerId`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let producer = ProducerId::from_wire(4000)?;
    /// assert_eq!(producer, Some(ProducerId::from(4000)));
    /// assert_eq!(ProducerId::from_wire(-1)?, None);
    /// # Ok::<(), KernelError>(())
    /// ```
    ProducerId, "producer ID", value: u64, wire: i64, small: u32
}

number_sentinel!(ProducerId, "producer ID", wire: i64, none: "\"no producer ID\"");

number_value! {
    /// The epoch of a producer ID.
    ///
    /// A cluster fences a producer by giving its producer ID the next epoch.
    /// A producer epoch is between zero and [`i16::MAX`].
    ///
    /// Kafka writes "no producer epoch" as `-1`.
    /// That number is not a `ProducerEpoch`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let epoch = ProducerEpoch::from(0);
    /// assert_eq!(epoch.checked_next(), Some(ProducerEpoch::from(1)));
    /// assert_eq!(ProducerEpoch::from_wire(-1)?, None);
    /// # Ok::<(), KernelError>(())
    /// ```
    ProducerEpoch, "producer epoch", value: u16, wire: i16, small: u8
}

number_sentinel!(ProducerEpoch, "producer epoch", wire: i16, none: "\"no producer epoch\"");

impl ProducerEpoch {
    /// The epoch after this one, or `None` when this is the highest epoch.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).and_then(|value| Self::new(value).ok())
    }
}

text_value! {
    /// The `transactional.id` of a transactional producer.
    ///
    /// Kafka does not restrict the characters of a transactional ID.
    /// A transactional ID has at least one byte and at most 32,767, the longest string a Kafka message carries.
    ///
    /// A producer that is not transactional has no transactional ID.
    /// That is `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let transactional = TransactionalId::new("payments-tx-1")?;
    /// assert_eq!(transactional, "payments-tx-1");
    /// assert!(TransactionalId::new("").is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    TransactionalId, "transactional ID", max: text::MAX_STRING_LEN, rule: text::non_empty
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KernelError;

    const MAX_ID: u64 = i64::MAX as u64;
    const MAX_EPOCH: u16 = i16::MAX as u16;

    #[test]
    fn a_producer_id_is_between_zero_and_the_highest_i64() {
        assert_eq!(ProducerId::new(0).map(ProducerId::value), Ok(0));
        assert_eq!(ProducerId::new(MAX_ID).map(ProducerId::value), Ok(MAX_ID));
        assert_eq!(
            ProducerId::new(MAX_ID + 1),
            Err(KernelError::TooLarge { what: "producer ID", value: MAX_ID + 1, max: MAX_ID })
        );
    }

    #[test]
    fn a_producer_id_converts_from_and_to_every_raw_type() {
        let producer = ProducerId::new(4000).unwrap();

        assert_eq!(ProducerId::from(4000_u32), producer);
        assert_eq!(ProducerId::try_from(4000_u64), Ok(producer));
        assert_eq!(ProducerId::try_from(4000_i64), Ok(producer));
        assert_eq!(ProducerId::try_from(-1_i64), Err(KernelError::Negative { what: "producer ID", value: -1 }));
        assert!(ProducerId::try_from(u64::MAX).is_err());
        assert_eq!(producer.value(), 4000);
        assert_eq!(u64::from(producer), 4000);
        assert_eq!(i64::from(producer), 4000);
    }

    #[test]
    fn the_wire_writes_no_producer_id_as_minus_one() {
        assert_eq!(ProducerId::from_wire(-1), Ok(None));
        assert_eq!(ProducerId::from_wire(4000), Ok(Some(ProducerId::from(4000))));
        assert_eq!(ProducerId::from_wire(-2), Err(KernelError::Negative { what: "producer ID", value: -2 }));
        assert_eq!(ProducerId::to_wire(None), -1);
        assert_eq!(ProducerId::to_wire(Some(ProducerId::from(4000))), 4000);
    }

    #[test]
    fn a_producer_id_reads_and_prints_decimal_digits() {
        assert_eq!("4000".parse::<ProducerId>(), Ok(ProducerId::from(4000)));
        assert_eq!(ProducerId::from(4000).to_string(), "4000");
        assert!(ProducerId::from(9) < ProducerId::from(10));
    }

    #[test]
    fn a_producer_epoch_is_between_zero_and_the_highest_i16() {
        assert_eq!(ProducerEpoch::new(0).map(ProducerEpoch::value), Ok(0));
        assert_eq!(ProducerEpoch::new(MAX_EPOCH).map(ProducerEpoch::value), Ok(MAX_EPOCH));
        assert_eq!(
            ProducerEpoch::new(MAX_EPOCH + 1),
            Err(KernelError::TooLarge { what: "producer epoch", value: 32_768, max: 32_767 })
        );
    }

    #[test]
    fn a_producer_epoch_converts_from_and_to_every_raw_type() {
        let epoch = ProducerEpoch::new(5).unwrap();

        assert_eq!(ProducerEpoch::from(5_u8), epoch);
        assert_eq!(ProducerEpoch::try_from(5_u16), Ok(epoch));
        assert_eq!(ProducerEpoch::try_from(5_i16), Ok(epoch));
        assert_eq!(ProducerEpoch::try_from(-1_i16), Err(KernelError::Negative { what: "producer epoch", value: -1 }));
        assert!(ProducerEpoch::try_from(u16::MAX).is_err());
        assert_eq!(epoch.value(), 5);
        assert_eq!(u16::from(epoch), 5);
        assert_eq!(i16::from(epoch), 5);
        assert_eq!(i16::from(ProducerEpoch::new(MAX_EPOCH).unwrap()), i16::MAX);
    }

    #[test]
    fn the_wire_writes_no_producer_epoch_as_minus_one() {
        assert_eq!(ProducerEpoch::from_wire(-1), Ok(None));
        assert_eq!(ProducerEpoch::from_wire(5), Ok(Some(ProducerEpoch::from(5))));
        assert_eq!(ProducerEpoch::from_wire(-2), Err(KernelError::Negative { what: "producer epoch", value: -2 }));
        assert_eq!(ProducerEpoch::to_wire(None), -1);
        assert_eq!(ProducerEpoch::to_wire(Some(ProducerEpoch::from(5))), 5);
    }

    #[test]
    fn a_producer_epoch_reads_and_prints_decimal_digits() {
        assert_eq!("5".parse::<ProducerEpoch>(), Ok(ProducerEpoch::from(5)));
        assert_eq!(ProducerEpoch::from(5).to_string(), "5");
        assert_eq!(
            "32768".parse::<ProducerEpoch>(),
            Err(KernelError::TooLarge { what: "producer epoch", value: 32_768, max: 32_767 })
        );
    }

    #[test]
    fn a_producer_epoch_has_a_next_one_until_the_highest() {
        assert_eq!(ProducerEpoch::from(5).checked_next(), Some(ProducerEpoch::from(6)));
        assert_eq!(ProducerEpoch::new(MAX_EPOCH).unwrap().checked_next(), None);
        assert!(ProducerEpoch::from(5) < ProducerEpoch::from(6));
    }

    #[test]
    fn a_transactional_id_takes_any_text_but_none() {
        assert_eq!(TransactionalId::new("payments tx/1").unwrap().as_str(), "payments tx/1");
        assert_eq!(TransactionalId::new(""), Err(KernelError::Empty { what: "transactional ID" }));
        assert_eq!(TransactionalId::MAX_LEN, 32_767);
        assert_eq!(
            TransactionalId::new("a".repeat(32_768)),
            Err(KernelError::TooLong { what: "transactional ID", len: 32_768, max: 32_767 })
        );
    }

    #[test]
    fn a_transactional_id_converts_from_and_to_text() {
        let transactional = TransactionalId::new("payments-tx-1").unwrap();

        assert_eq!("payments-tx-1".parse::<TransactionalId>(), Ok(transactional.clone()));
        assert_eq!(TransactionalId::try_from("payments-tx-1"), Ok(transactional.clone()));
        assert_eq!(TransactionalId::try_from(String::from("payments-tx-1")), Ok(transactional.clone()));
        assert_eq!(String::from(transactional.clone()), "payments-tx-1");
        assert_eq!(transactional.to_string(), "payments-tx-1");
        assert_eq!(transactional, "payments-tx-1");
        assert!(TransactionalId::try_from("").is_err());
    }
}
