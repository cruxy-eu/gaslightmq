use std::fmt;
use std::str::FromStr;

use crate::number::number_value;
use crate::text::text_form;
use crate::{KernelError, Offset, TopicName};

number_value! {
    /// The number of a partition within its topic.
    ///
    /// A partition number is between zero and [`i32::MAX`].
    /// Kafka has no number that means "no partition", so every negative number is an error.
    ///
    /// ```
    /// use gaslightmq_kernel::prelude::*;
    ///
    /// let partition = PartitionNumber::try_from(3_i32)?;
    /// assert_eq!(partition, PartitionNumber::from(3));
    /// assert_eq!(i32::from(partition), 3);
    /// assert!(PartitionNumber::try_from(-1_i32).is_err());
    /// # Ok::<(), KernelError>(())
    /// ```
    PartitionNumber, "partition number", value: u32, wire: i32, small: u16
}

impl PartitionNumber {
    /// That offset in the partition with this number.
    pub fn at(self, offset: Offset) -> PartitionOffset {
        PartitionOffset::new(self, offset)
    }
}

/// One partition of one topic.
///
/// The text form is `topic/partition`, such as `orders.eu/3`.
/// A topic name has no `/`, so the form has one reading.
///
/// Topic partitions order by topic name, and then by partition number as a number.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let partition: TopicPartition = "orders.eu/3".parse()?;
/// assert_eq!(partition.topic(), "orders.eu");
/// assert_eq!(partition.partition(), PartitionNumber::from(3));
/// assert_eq!(partition.to_string(), "orders.eu/3");
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    topic: TopicName,
    partition: PartitionNumber,
}

impl TopicPartition {
    const WHAT: &'static str = "topic partition";
    const FORM: &'static str = "topic/partition";

    pub fn new(topic: TopicName, partition: PartitionNumber) -> Self {
        Self { topic, partition }
    }

    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn partition(&self) -> PartitionNumber {
        self.partition
    }

    /// That offset in this partition.
    pub fn at(&self, offset: Offset) -> TopicPartitionOffset {
        TopicPartitionOffset::new(self.clone(), offset)
    }

    pub fn into_parts(self) -> (TopicName, PartitionNumber) {
        (self.topic, self.partition)
    }
}

impl From<(TopicName, PartitionNumber)> for TopicPartition {
    fn from((topic, partition): (TopicName, PartitionNumber)) -> Self {
        Self::new(topic, partition)
    }
}

impl From<TopicPartition> for (TopicName, PartitionNumber) {
    fn from(topic_partition: TopicPartition) -> Self {
        topic_partition.into_parts()
    }
}

/// Reads the text form `topic/partition`.
impl FromStr for TopicPartition {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (topic, partition) = split(text, '/', Self::WHAT, Self::FORM)?;
        Ok(Self::new(topic.parse()?, partition.parse()?))
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

impl fmt::Debug for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TopicPartition({self})")
    }
}

text_form!(TopicPartition);

/// An offset in one partition of a topic that the context names.
///
/// The text form is `partition@offset`, such as `3@42`.
///
/// Partition offsets order by partition number, and then by offset.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let position: PartitionOffset = "3@42".parse()?;
/// assert_eq!(position.offset(), Offset::from(42));
/// assert_eq!(position.in_topic(&TopicName::new("orders.eu")?).to_string(), "orders.eu/3@42");
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionOffset {
    partition: PartitionNumber,
    offset: Offset,
}

impl PartitionOffset {
    const WHAT: &'static str = "partition offset";
    const FORM: &'static str = "partition@offset";

    pub fn new(partition: PartitionNumber, offset: Offset) -> Self {
        Self { partition, offset }
    }

    pub fn partition(&self) -> PartitionNumber {
        self.partition
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// This offset and partition in that topic.
    pub fn in_topic(&self, topic: &TopicName) -> TopicPartitionOffset {
        topic.partition(self.partition).at(self.offset)
    }

    pub fn into_parts(self) -> (PartitionNumber, Offset) {
        (self.partition, self.offset)
    }
}

impl From<(PartitionNumber, Offset)> for PartitionOffset {
    fn from((partition, offset): (PartitionNumber, Offset)) -> Self {
        Self::new(partition, offset)
    }
}

impl From<PartitionOffset> for (PartitionNumber, Offset) {
    fn from(partition_offset: PartitionOffset) -> Self {
        partition_offset.into_parts()
    }
}

/// Reads the text form `partition@offset`.
impl FromStr for PartitionOffset {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (partition, offset) = split(text, '@', Self::WHAT, Self::FORM)?;
        Ok(Self::new(partition.parse()?, offset.parse()?))
    }
}

impl fmt::Display for PartitionOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.partition, self.offset)
    }
}

impl fmt::Debug for PartitionOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PartitionOffset({self})")
    }
}

text_form!(PartitionOffset);

/// An offset in one partition of one topic: the place of one record in a cluster.
///
/// The text form is `topic/partition@offset`, such as `orders.eu/3@42`.
/// A topic name has no `/` and no `@`, so the form has one reading.
///
/// Topic partition offsets order by topic name, then by partition number, and then by offset.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let orders = TopicName::new("orders.eu")?;
/// let position = orders.partition(PartitionNumber::from(3)).at(Offset::from(42));
/// assert_eq!(position.to_string(), "orders.eu/3@42");
/// assert_eq!("orders.eu/3@42".parse::<TopicPartitionOffset>()?, position);
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartitionOffset {
    topic_partition: TopicPartition,
    offset: Offset,
}

impl TopicPartitionOffset {
    const WHAT: &'static str = "topic partition offset";
    const FORM: &'static str = "topic/partition@offset";

    pub fn new(topic_partition: TopicPartition, offset: Offset) -> Self {
        Self { topic_partition, offset }
    }

    pub fn topic(&self) -> &TopicName {
        self.topic_partition.topic()
    }

    pub fn partition(&self) -> PartitionNumber {
        self.topic_partition.partition()
    }

    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }

    /// The partition number and the offset, without the topic.
    pub fn partition_offset(&self) -> PartitionOffset {
        PartitionOffset::new(self.partition(), self.offset)
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    pub fn into_parts(self) -> (TopicPartition, Offset) {
        (self.topic_partition, self.offset)
    }
}

impl From<(TopicPartition, Offset)> for TopicPartitionOffset {
    fn from((topic_partition, offset): (TopicPartition, Offset)) -> Self {
        Self::new(topic_partition, offset)
    }
}

impl From<(TopicName, PartitionNumber, Offset)> for TopicPartitionOffset {
    fn from((topic, partition, offset): (TopicName, PartitionNumber, Offset)) -> Self {
        Self::new(TopicPartition::new(topic, partition), offset)
    }
}

impl From<TopicPartitionOffset> for (TopicPartition, Offset) {
    fn from(position: TopicPartitionOffset) -> Self {
        position.into_parts()
    }
}

impl From<TopicPartitionOffset> for (TopicName, PartitionNumber, Offset) {
    fn from(position: TopicPartitionOffset) -> Self {
        let (topic, partition) = position.topic_partition.into_parts();
        (topic, partition, position.offset)
    }
}

/// Reads the text form `topic/partition@offset`.
impl FromStr for TopicPartitionOffset {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (topic_partition, offset) = split(text, '@', Self::WHAT, Self::FORM)?;
        let (topic, partition) = split(topic_partition, '/', Self::WHAT, Self::FORM)
            .map_err(|_| KernelError::Malformed { what: Self::WHAT, input: text.to_owned(), expected: Self::FORM })?;
        Ok(Self::new(TopicPartition::new(topic.parse()?, partition.parse()?), offset.parse()?))
    }
}

impl fmt::Display for TopicPartitionOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.topic_partition, self.offset)
    }
}

impl fmt::Debug for TopicPartitionOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TopicPartitionOffset({self})")
    }
}

text_form!(TopicPartitionOffset);

/// Cut a text form in two at its last separator.
///
/// Only the last part of a form is always a number, so the last separator is the right one
/// even when the first part is not valid.
fn split<'a>(
    text: &'a str,
    separator: char,
    what: &'static str,
    expected: &'static str,
) -> Result<(&'a str, &'a str), KernelError> {
    if text.is_empty() {
        return Err(KernelError::Empty { what });
    }
    text.rsplit_once(separator).ok_or_else(|| KernelError::Malformed { what, input: text.to_owned(), expected })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const MAX: u32 = i32::MAX as u32;

    fn orders() -> TopicName {
        TopicName::new("orders.eu").unwrap()
    }

    fn partition(number: u16) -> PartitionNumber {
        PartitionNumber::from(number)
    }

    #[test]
    fn a_partition_number_is_between_zero_and_the_highest_i32() {
        assert_eq!(PartitionNumber::new(0).map(PartitionNumber::value), Ok(0));
        assert_eq!(PartitionNumber::new(MAX).map(PartitionNumber::value), Ok(MAX));
        assert_eq!(
            PartitionNumber::new(MAX + 1),
            Err(KernelError::TooLarge { what: "partition number", value: u64::from(MAX) + 1, max: u64::from(MAX) })
        );
    }

    #[test]
    fn a_partition_number_converts_from_every_raw_type() {
        let three = PartitionNumber::new(3).unwrap();

        assert_eq!(PartitionNumber::from(3_u16), three);
        assert_eq!(PartitionNumber::try_from(3_u32), Ok(three));
        assert_eq!(PartitionNumber::try_from(3_i32), Ok(three));
        assert_eq!(PartitionNumber::try_from(i32::MAX).map(PartitionNumber::value), Ok(MAX));
        assert!(PartitionNumber::try_from(u32::MAX).is_err());
        assert_eq!(
            PartitionNumber::try_from(-1_i32),
            Err(KernelError::Negative { what: "partition number", value: -1 })
        );
    }

    #[test]
    fn a_partition_number_converts_to_every_raw_type() {
        let three = PartitionNumber::new(3).unwrap();

        assert_eq!(three.value(), 3);
        assert_eq!(u32::from(three), 3);
        assert_eq!(i32::from(three), 3);
        assert_eq!(i32::from(PartitionNumber::new(MAX).unwrap()), i32::MAX);
    }

    #[test]
    fn a_partition_number_reads_and_prints_decimal_digits() {
        assert_eq!("3".parse::<PartitionNumber>(), Ok(partition(3)));
        assert_eq!(partition(3).to_string(), "3");
        assert_eq!(format!("{:?}", partition(3)), "PartitionNumber(3)");
        assert_eq!(
            "2147483648".parse::<PartitionNumber>(),
            Err(KernelError::TooLarge { what: "partition number", value: 2_147_483_648, max: 2_147_483_647 })
        );
        assert_eq!("-1".parse::<PartitionNumber>(), Err(KernelError::Negative { what: "partition number", value: -1 }));
        assert_eq!(
            "three".parse::<PartitionNumber>(),
            Err(KernelError::NotANumber { what: "partition number", input: "three".into() })
        );
    }

    #[test]
    fn partition_numbers_order_as_numbers() {
        assert!(partition(9) < partition(10));
    }

    #[test]
    fn a_topic_partition_reads_and_prints_topic_slash_partition() {
        let topic_partition = TopicPartition::new(orders(), partition(3));

        assert_eq!(topic_partition.to_string(), "orders.eu/3");
        assert_eq!("orders.eu/3".parse::<TopicPartition>(), Ok(topic_partition.clone()));
        assert_eq!(TopicPartition::try_from("orders.eu/3"), Ok(topic_partition.clone()));
        assert_eq!(TopicPartition::try_from(String::from("orders.eu/3")), Ok(topic_partition.clone()));
        assert_eq!(String::from(topic_partition.clone()), "orders.eu/3");
        assert_eq!(format!("{topic_partition:?}"), "TopicPartition(orders.eu/3)");
    }

    #[test]
    fn a_topic_partition_gives_its_parts() {
        let topic_partition = TopicPartition::from((orders(), partition(3)));

        assert_eq!(topic_partition.topic(), &orders());
        assert_eq!(topic_partition.partition(), partition(3));
        assert_eq!(topic_partition.clone().into_parts(), (orders(), partition(3)));
        assert_eq!(<(TopicName, PartitionNumber)>::from(topic_partition), (orders(), partition(3)));
    }

    #[test]
    fn a_topic_partition_needs_both_parts() {
        let malformed = |input: &str| {
            Err(KernelError::Malformed {
                what: "topic partition",
                input: input.to_owned(),
                expected: "topic/partition",
            })
        };

        assert_eq!("orders.eu".parse::<TopicPartition>(), malformed("orders.eu"));
        assert_eq!("3".parse::<TopicPartition>(), malformed("3"));
        assert_eq!("".parse::<TopicPartition>(), Err(KernelError::Empty { what: "topic partition" }));
        assert_eq!("/3".parse::<TopicPartition>(), Err(KernelError::Empty { what: "topic name" }));
        assert_eq!("orders.eu/".parse::<TopicPartition>(), Err(KernelError::Empty { what: "partition number" }));
    }

    #[test]
    fn a_topic_partition_checks_both_parts() {
        assert_eq!(
            "orders.*/3".parse::<TopicPartition>(),
            Err(KernelError::InvalidCharacter { what: "topic name", character: '*', position: 7 })
        );
        assert_eq!(
            "orders/eu/3".parse::<TopicPartition>(),
            Err(KernelError::InvalidCharacter { what: "topic name", character: '/', position: 6 })
        );
        assert_eq!(
            "orders.eu/x".parse::<TopicPartition>(),
            Err(KernelError::NotANumber { what: "partition number", input: "x".into() })
        );
        assert_eq!(
            "orders.eu/-1".parse::<TopicPartition>(),
            Err(KernelError::Negative { what: "partition number", value: -1 })
        );
        assert_eq!(
            "orders.eu/3@42".parse::<TopicPartition>(),
            Err(KernelError::NotANumber { what: "partition number", input: "3@42".into() })
        );
    }

    #[test]
    fn topic_partitions_order_by_topic_and_then_by_number() {
        let sorted: BTreeSet<TopicPartition> = ["payments/0", "orders.eu/10", "orders.eu/9", "orders.eu/1"]
            .into_iter()
            .map(|p| p.parse().unwrap())
            .collect();

        let sorted: Vec<String> = sorted.iter().map(ToString::to_string).collect();
        assert_eq!(sorted, ["orders.eu/1", "orders.eu/9", "orders.eu/10", "payments/0"]);
    }

    #[test]
    fn a_partition_offset_reads_and_prints_partition_at_offset() {
        let position = PartitionOffset::new(partition(3), Offset::from(42));

        assert_eq!(position.to_string(), "3@42");
        assert_eq!("3@42".parse::<PartitionOffset>(), Ok(position));
        assert_eq!(PartitionOffset::try_from("3@42"), Ok(position));
        assert_eq!(PartitionOffset::try_from(String::from("3@42")), Ok(position));
        assert_eq!(String::from(position), "3@42");
        assert_eq!(format!("{position:?}"), "PartitionOffset(3@42)");
        assert_eq!(partition(3).at(Offset::from(42)), position);
    }

    #[test]
    fn a_partition_offset_gives_its_parts() {
        let position = PartitionOffset::from((partition(3), Offset::from(42)));

        assert_eq!(position.partition(), partition(3));
        assert_eq!(position.offset(), Offset::from(42));
        assert_eq!(position.into_parts(), (partition(3), Offset::from(42)));
        assert_eq!(<(PartitionNumber, Offset)>::from(position), (partition(3), Offset::from(42)));
        assert_eq!(position.in_topic(&orders()).to_string(), "orders.eu/3@42");
    }

    #[test]
    fn a_partition_offset_needs_and_checks_both_parts() {
        assert_eq!(
            "3".parse::<PartitionOffset>(),
            Err(KernelError::Malformed { what: "partition offset", input: "3".into(), expected: "partition@offset" })
        );
        assert_eq!("".parse::<PartitionOffset>(), Err(KernelError::Empty { what: "partition offset" }));
        assert_eq!("@42".parse::<PartitionOffset>(), Err(KernelError::Empty { what: "partition number" }));
        assert_eq!("3@".parse::<PartitionOffset>(), Err(KernelError::Empty { what: "offset" }));
        assert_eq!("3@-1".parse::<PartitionOffset>(), Err(KernelError::Negative { what: "offset", value: -1 }));
        assert_eq!(
            "x@42".parse::<PartitionOffset>(),
            Err(KernelError::NotANumber { what: "partition number", input: "x".into() })
        );
        assert_eq!(
            "orders.eu/3@42".parse::<PartitionOffset>(),
            Err(KernelError::NotANumber { what: "partition number", input: "orders.eu/3".into() })
        );
    }

    #[test]
    fn partition_offsets_order_by_partition_and_then_by_offset() {
        let position = |text: &str| text.parse::<PartitionOffset>().unwrap();

        assert!(position("2@100") < position("10@0"));
        assert!(position("3@9") < position("3@10"));
    }

    #[test]
    fn a_topic_partition_offset_reads_and_prints_topic_slash_partition_at_offset() {
        let position = TopicPartitionOffset::new(TopicPartition::new(orders(), partition(3)), Offset::from(42));

        assert_eq!(position.to_string(), "orders.eu/3@42");
        assert_eq!("orders.eu/3@42".parse::<TopicPartitionOffset>(), Ok(position.clone()));
        assert_eq!(TopicPartitionOffset::try_from("orders.eu/3@42"), Ok(position.clone()));
        assert_eq!(TopicPartitionOffset::try_from(String::from("orders.eu/3@42")), Ok(position.clone()));
        assert_eq!(String::from(position.clone()), "orders.eu/3@42");
        assert_eq!(format!("{position:?}"), "TopicPartitionOffset(orders.eu/3@42)");
        assert_eq!(orders().partition(partition(3)).at(Offset::from(42)), position);
    }

    #[test]
    fn a_topic_partition_offset_gives_its_parts() {
        let topic_partition = TopicPartition::new(orders(), partition(3));
        let position = TopicPartitionOffset::from((topic_partition.clone(), Offset::from(42)));

        assert_eq!(position.topic(), &orders());
        assert_eq!(position.partition(), partition(3));
        assert_eq!(position.offset(), Offset::from(42));
        assert_eq!(position.topic_partition(), &topic_partition);
        assert_eq!(position.partition_offset(), PartitionOffset::new(partition(3), Offset::from(42)));
        assert_eq!(position.clone().into_parts(), (topic_partition.clone(), Offset::from(42)));
        assert_eq!(<(TopicPartition, Offset)>::from(position.clone()), (topic_partition, Offset::from(42)));
        assert_eq!(
            <(TopicName, PartitionNumber, Offset)>::from(position.clone()),
            (orders(), partition(3), Offset::from(42))
        );
        assert_eq!(TopicPartitionOffset::from((orders(), partition(3), Offset::from(42))), position);
    }

    #[test]
    fn a_topic_partition_offset_needs_all_three_parts() {
        let malformed = |input: &str| {
            Err(KernelError::Malformed {
                what: "topic partition offset",
                input: input.to_owned(),
                expected: "topic/partition@offset",
            })
        };

        assert_eq!("orders.eu/3".parse::<TopicPartitionOffset>(), malformed("orders.eu/3"));
        assert_eq!("orders.eu@42".parse::<TopicPartitionOffset>(), malformed("orders.eu@42"));
        assert_eq!("3@42".parse::<TopicPartitionOffset>(), malformed("3@42"));
        assert_eq!("orders.eu".parse::<TopicPartitionOffset>(), malformed("orders.eu"));
        assert_eq!("".parse::<TopicPartitionOffset>(), Err(KernelError::Empty { what: "topic partition offset" }));
    }

    #[test]
    fn a_topic_partition_offset_checks_all_three_parts() {
        assert_eq!(
            "orders eu/3@42".parse::<TopicPartitionOffset>(),
            Err(KernelError::InvalidCharacter { what: "topic name", character: ' ', position: 6 })
        );
        assert_eq!(
            "orders.eu/x@42".parse::<TopicPartitionOffset>(),
            Err(KernelError::NotANumber { what: "partition number", input: "x".into() })
        );
        assert_eq!(
            "orders.eu/3@latest".parse::<TopicPartitionOffset>(),
            Err(KernelError::NotANumber { what: "offset", input: "latest".into() })
        );
        assert_eq!(
            "orders.eu/3@-1".parse::<TopicPartitionOffset>(),
            Err(KernelError::Negative { what: "offset", value: -1 })
        );
        assert_eq!(
            "orders@eu/3@42".parse::<TopicPartitionOffset>(),
            Err(KernelError::InvalidCharacter { what: "topic name", character: '@', position: 6 })
        );
    }

    #[test]
    fn topic_partition_offsets_order_by_topic_then_partition_then_offset() {
        let position = |text: &str| text.parse::<TopicPartitionOffset>().unwrap();

        assert!(position("orders.eu/9@100") < position("orders.eu/10@0"));
        assert!(position("orders.eu/3@9") < position("orders.eu/3@10"));
        assert!(position("orders.eu/3@9") < position("payments/0@0"));
    }
}
