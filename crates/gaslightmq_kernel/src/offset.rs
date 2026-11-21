use crate::number::{number_sentinel, number_value};

number_value! {
    /// A position in the log of a partition.
    ///
    /// The offset of a record, a log start offset, a high watermark and a committed offset are all offsets.
    /// An offset is between zero and [`i64::MAX`].
    ///
    /// Kafka writes "no offset" as `-1`.
    /// That number is not an `Offset`: [`from_wire`](Self::from_wire) reads it as `None`.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let committed = Offset::from_wire(42)?;
    /// assert_eq!(committed, Some(Offset::from(42)));
    /// assert_eq!(Offset::from_wire(-1)?, None);
    /// assert_eq!(Offset::to_wire(None), -1);
    /// assert!(Offset::try_from(-1_i64).is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    Offset, "offset", value: u64, wire: i64, small: u32
}

number_sentinel!(Offset, "offset", wire: i64, none: "\"no offset\"");

impl Offset {
    /// The offset of the first record a log ever holds.
    pub const ZERO: Self = Self(0);

    /// The offset that lies `records` further, or `None` when no offset is that high.
    pub fn checked_add(self, records: u64) -> Option<Self> {
        self.0.checked_add(records).and_then(|value| Self::new(value).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KernelError;

    const MAX: u64 = i64::MAX as u64;

    #[test]
    fn an_offset_is_between_zero_and_the_highest_i64() {
        assert_eq!(Offset::new(0).map(Offset::value), Ok(0));
        assert_eq!(Offset::new(MAX).map(Offset::value), Ok(MAX));
        assert_eq!(Offset::new(MAX + 1), Err(KernelError::TooLarge { what: "offset", value: MAX + 1, max: MAX }));
        assert_eq!(Offset::ZERO, Offset::from(0));
    }

    #[test]
    fn an_offset_converts_from_every_raw_type() {
        let offset = Offset::new(42).unwrap();

        assert_eq!(Offset::from(42_u32), offset);
        assert_eq!(Offset::try_from(42_u64), Ok(offset));
        assert_eq!(Offset::try_from(42_i64), Ok(offset));
        assert_eq!(Offset::try_from(i64::MAX).map(Offset::value), Ok(MAX));
        assert_eq!(
            Offset::try_from(u64::MAX),
            Err(KernelError::TooLarge { what: "offset", value: u64::MAX, max: MAX })
        );
        assert_eq!(Offset::try_from(-1_i64), Err(KernelError::Negative { what: "offset", value: -1 }));
        assert_eq!(Offset::try_from(i64::MIN), Err(KernelError::Negative { what: "offset", value: i64::MIN }));
    }

    #[test]
    fn an_offset_converts_to_every_raw_type() {
        let offset = Offset::new(42).unwrap();

        assert_eq!(offset.value(), 42);
        assert_eq!(u64::from(offset), 42);
        assert_eq!(i64::from(offset), 42);
        assert_eq!(i64::from(Offset::new(MAX).unwrap()), i64::MAX);
    }

    #[test]
    fn the_wire_writes_no_offset_as_minus_one() {
        assert_eq!(Offset::from_wire(-1), Ok(None));
        assert_eq!(Offset::from_wire(0), Ok(Some(Offset::ZERO)));
        assert_eq!(Offset::from_wire(42), Ok(Some(Offset::from(42))));
        assert_eq!(Offset::from_wire(-2), Err(KernelError::Negative { what: "offset", value: -2 }));
        assert_eq!(Offset::to_wire(None), -1);
        assert_eq!(Offset::to_wire(Some(Offset::from(42))), 42);
    }

    #[test]
    fn an_offset_reads_and_prints_decimal_digits() {
        assert_eq!("42".parse::<Offset>(), Ok(Offset::from(42)));
        assert_eq!(Offset::from(42).to_string(), "42");
        assert_eq!(format!("{:>4}|{:?}", Offset::from(42), Offset::from(42)), "  42|Offset(42)");
        assert_eq!(MAX.to_string().parse::<Offset>().map(Offset::value), Ok(MAX));
        assert_eq!(
            "9223372036854775808".parse::<Offset>(),
            Err(KernelError::TooLarge { what: "offset", value: MAX + 1, max: MAX })
        );
        assert_eq!("-1".parse::<Offset>(), Err(KernelError::Negative { what: "offset", value: -1 }));
        assert_eq!("latest".parse::<Offset>(), Err(KernelError::NotANumber { what: "offset", input: "latest".into() }));
        assert_eq!("".parse::<Offset>(), Err(KernelError::Empty { what: "offset" }));
    }

    #[test]
    fn offsets_order_as_numbers() {
        assert!(Offset::from(9) < Offset::from(10));
        assert_eq!(Offset::from(3).max(Offset::from(7)), Offset::from(7));
    }

    #[test]
    fn an_offset_adds_records_up_to_the_highest_offset() {
        assert_eq!(Offset::from(40).checked_add(2), Some(Offset::from(42)));
        assert_eq!(Offset::ZERO.checked_add(MAX).map(Offset::value), Some(MAX));
        assert_eq!(Offset::from(1).checked_add(MAX), None);
        assert_eq!(Offset::from(1).checked_add(u64::MAX), None);
    }
}
