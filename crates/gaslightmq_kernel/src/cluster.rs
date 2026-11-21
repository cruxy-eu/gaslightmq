use crate::number::{number_sentinel, number_value};

number_value! {
    /// The ID of a broker: the node ID that the cluster knows it by.
    ///
    /// A broker ID is between zero and [`i32::MAX`].
    ///
    /// Kafka writes "no broker" as `-1`: a partition without a leader, or a cluster without a controller.
    /// That number is not a `BrokerId`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let leader = BrokerId::from_wire(2)?;
    /// assert_eq!(leader, Some(BrokerId::from(2)));
    /// assert_eq!(BrokerId::from_wire(-1)?, None);
    /// # Ok::<(), KernelError>(())
    /// ```
    BrokerId, "broker ID", value: u32, wire: i32, small: u16
}

number_sentinel!(BrokerId, "broker ID", wire: i32, none: "\"no broker\"");

number_value! {
    /// The epoch of a partition's leader.
    ///
    /// Every election of a leader gives the partition the next epoch.
    /// A leader epoch is between zero and [`i32::MAX`].
    ///
    /// Kafka writes "unknown epoch" as `-1`.
    /// That number is not a `LeaderEpoch`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let epoch = LeaderEpoch::from(7);
    /// assert_eq!(epoch.checked_next(), Some(LeaderEpoch::from(8)));
    /// assert_eq!(LeaderEpoch::from_wire(-1)?, None);
    /// # Ok::<(), KernelError>(())
    /// ```
    LeaderEpoch, "leader epoch", value: u32, wire: i32, small: u16
}

number_sentinel!(LeaderEpoch, "leader epoch", wire: i32, none: "\"unknown epoch\"");

impl LeaderEpoch {
    /// The epoch after this one, or `None` when this is the highest epoch.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).and_then(|value| Self::new(value).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KernelError;

    const MAX: u32 = i32::MAX as u32;

    #[test]
    fn a_broker_id_is_between_zero_and_the_highest_i32() {
        assert_eq!(BrokerId::new(0).map(BrokerId::value), Ok(0));
        assert_eq!(BrokerId::new(MAX).map(BrokerId::value), Ok(MAX));
        assert_eq!(
            BrokerId::new(MAX + 1),
            Err(KernelError::TooLarge { what: "broker ID", value: u64::from(MAX) + 1, max: u64::from(MAX) })
        );
    }

    #[test]
    fn a_broker_id_converts_from_and_to_every_raw_type() {
        let broker = BrokerId::new(2).unwrap();

        assert_eq!(BrokerId::from(2_u16), broker);
        assert_eq!(BrokerId::try_from(2_u32), Ok(broker));
        assert_eq!(BrokerId::try_from(2_i32), Ok(broker));
        assert_eq!(BrokerId::try_from(-1_i32), Err(KernelError::Negative { what: "broker ID", value: -1 }));
        assert!(BrokerId::try_from(u32::MAX).is_err());
        assert_eq!(broker.value(), 2);
        assert_eq!(u32::from(broker), 2);
        assert_eq!(i32::from(broker), 2);
        assert_eq!(i32::from(BrokerId::new(MAX).unwrap()), i32::MAX);
    }

    #[test]
    fn the_wire_writes_no_broker_as_minus_one() {
        assert_eq!(BrokerId::from_wire(-1), Ok(None));
        assert_eq!(BrokerId::from_wire(0), Ok(Some(BrokerId::from(0))));
        assert_eq!(BrokerId::from_wire(-2), Err(KernelError::Negative { what: "broker ID", value: -2 }));
        assert_eq!(BrokerId::to_wire(None), -1);
        assert_eq!(BrokerId::to_wire(Some(BrokerId::from(2))), 2);
    }

    #[test]
    fn a_broker_id_reads_and_prints_decimal_digits() {
        assert_eq!("2".parse::<BrokerId>(), Ok(BrokerId::from(2)));
        assert_eq!(BrokerId::from(2).to_string(), "2");
        assert_eq!(
            "broker 2".parse::<BrokerId>(),
            Err(KernelError::NotANumber { what: "broker ID", input: "broker 2".into() })
        );
        assert_eq!(
            "2147483648".parse::<BrokerId>(),
            Err(KernelError::TooLarge { what: "broker ID", value: 2_147_483_648, max: 2_147_483_647 })
        );
    }

    #[test]
    fn a_leader_epoch_is_between_zero_and_the_highest_i32() {
        assert_eq!(LeaderEpoch::new(MAX).map(LeaderEpoch::value), Ok(MAX));
        assert_eq!(
            LeaderEpoch::new(MAX + 1),
            Err(KernelError::TooLarge { what: "leader epoch", value: u64::from(MAX) + 1, max: u64::from(MAX) })
        );
    }

    #[test]
    fn a_leader_epoch_converts_from_and_to_every_raw_type() {
        let epoch = LeaderEpoch::new(7).unwrap();

        assert_eq!(LeaderEpoch::from(7_u16), epoch);
        assert_eq!(LeaderEpoch::try_from(7_u32), Ok(epoch));
        assert_eq!(LeaderEpoch::try_from(7_i32), Ok(epoch));
        assert_eq!(LeaderEpoch::try_from(-1_i32), Err(KernelError::Negative { what: "leader epoch", value: -1 }));
        assert_eq!(u32::from(epoch), 7);
        assert_eq!(i32::from(epoch), 7);
    }

    #[test]
    fn the_wire_writes_an_unknown_epoch_as_minus_one() {
        assert_eq!(LeaderEpoch::from_wire(-1), Ok(None));
        assert_eq!(LeaderEpoch::from_wire(7), Ok(Some(LeaderEpoch::from(7))));
        assert_eq!(LeaderEpoch::from_wire(-7), Err(KernelError::Negative { what: "leader epoch", value: -7 }));
        assert_eq!(LeaderEpoch::to_wire(None), -1);
        assert_eq!(LeaderEpoch::to_wire(Some(LeaderEpoch::from(7))), 7);
    }

    #[test]
    fn a_leader_epoch_reads_and_prints_decimal_digits() {
        assert_eq!("7".parse::<LeaderEpoch>(), Ok(LeaderEpoch::from(7)));
        assert_eq!(LeaderEpoch::from(7).to_string(), "7");
    }

    #[test]
    fn a_leader_epoch_has_a_next_one_until_the_highest() {
        assert_eq!(LeaderEpoch::from(0).checked_next(), Some(LeaderEpoch::from(1)));
        assert_eq!(LeaderEpoch::new(MAX).unwrap().checked_next(), None);
        assert!(LeaderEpoch::from(7) < LeaderEpoch::from(8));
    }
}
