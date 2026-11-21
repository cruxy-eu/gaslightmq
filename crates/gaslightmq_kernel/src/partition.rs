use std::fmt;
use std::str::FromStr;

use crate::number::number_value;
use crate::text::text_form;
use crate::{Offset, OffsetError, TopicName, TopicNameError};

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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    PartitionNumber, PartitionNumberError, "partition number", value: u32, wire: i32, small: u16
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
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    topic: TopicName,
    partition: PartitionNumber,
}

impl TopicPartition {
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
    type Err = TopicPartitionError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() {
            return Err(TopicPartitionError::Empty);
        }
        // Only the last part of a form is always a number,
        // so the last separator is the right one even when the first part is not valid.
        let (topic, partition) = text
            .rsplit_once('/')
            .ok_or_else(|| TopicPartitionError::Malformed { input: text.to_owned() })?;
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

text_form!(TopicPartition, TopicPartitionError);

/// Why a text is not a [`TopicPartition`].
///
/// The text has two parts, and the error says which one is wrong.
/// Its message is the message of that part's error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicPartitionError {
    /// The text has no characters.
    Empty,
    /// The text has no `/` between a topic and a partition.
    Malformed { input: String },
    /// The text before the last `/` is not a topic name.
    InvalidTopic { error: TopicNameError },
    /// The text after the last `/` is not a partition number.
    InvalidPartition { error: PartitionNumberError },
}

impl fmt::Display for TopicPartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("topic partition is empty"),
            Self::Malformed { input } => {
                write!(f, "topic partition {input:?} does not have the form topic/partition")
            }
            Self::InvalidTopic { error } => fmt::Display::fmt(error, f),
            Self::InvalidPartition { error } => fmt::Display::fmt(error, f),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for TopicPartitionError {}

impl From<TopicNameError> for TopicPartitionError {
    fn from(error: TopicNameError) -> Self {
        Self::InvalidTopic { error }
    }
}

impl From<PartitionNumberError> for TopicPartitionError {
    fn from(error: PartitionNumberError) -> Self {
        Self::InvalidPartition { error }
    }
}

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
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionOffset {
    partition: PartitionNumber,
    offset: Offset,
}

impl PartitionOffset {
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
    type Err = PartitionOffsetError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() {
            return Err(PartitionOffsetError::Empty);
        }
        let (partition, offset) = text
            .rsplit_once('@')
            .ok_or_else(|| PartitionOffsetError::Malformed { input: text.to_owned() })?;
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

text_form!(PartitionOffset, PartitionOffsetError);

/// Why a text is not a [`PartitionOffset`].
///
/// The text has two parts, and the error says which one is wrong.
/// Its message is the message of that part's error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionOffsetError {
    /// The text has no characters.
    Empty,
    /// The text has no `@` between a partition and an offset.
    Malformed { input: String },
    /// The text before the last `@` is not a partition number.
    InvalidPartition { error: PartitionNumberError },
    /// The text after the last `@` is not an offset.
    InvalidOffset { error: OffsetError },
}

impl fmt::Display for PartitionOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("partition offset is empty"),
            Self::Malformed { input } => {
                write!(f, "partition offset {input:?} does not have the form partition@offset")
            }
            Self::InvalidPartition { error } => fmt::Display::fmt(error, f),
            Self::InvalidOffset { error } => fmt::Display::fmt(error, f),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for PartitionOffsetError {}

impl From<PartitionNumberError> for PartitionOffsetError {
    fn from(error: PartitionNumberError) -> Self {
        Self::InvalidPartition { error }
    }
}

impl From<OffsetError> for PartitionOffsetError {
    fn from(error: OffsetError) -> Self {
        Self::InvalidOffset { error }
    }
}

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
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartitionOffset {
    topic_partition: TopicPartition,
    offset: Offset,
}

impl TopicPartitionOffset {
    pub fn new(topic_partition: TopicPartition, offset: Offset) -> Self {
        Self {
            topic_partition,
            offset,
        }
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
    type Err = TopicPartitionOffsetError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() {
            return Err(TopicPartitionOffsetError::Empty);
        }
        let malformed = || TopicPartitionOffsetError::Malformed { input: text.to_owned() };
        let (topic_partition, offset) = text.rsplit_once('@').ok_or_else(malformed)?;
        let (topic, partition) = topic_partition.rsplit_once('/').ok_or_else(malformed)?;
        Ok(Self::new(
            TopicPartition::new(topic.parse()?, partition.parse()?),
            offset.parse()?,
        ))
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

text_form!(TopicPartitionOffset, TopicPartitionOffsetError);

/// Why a text is not a [`TopicPartitionOffset`].
///
/// The text has three parts, and the error says which one is wrong.
/// Its message is the message of that part's error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicPartitionOffsetError {
    /// The text has no characters.
    Empty,
    /// The text has no `@` before an offset, or no `/` between a topic and a partition.
    Malformed { input: String },
    /// The text before the last `/` is not a topic name.
    InvalidTopic { error: TopicNameError },
    /// The text between the last `/` and the last `@` is not a partition number.
    InvalidPartition { error: PartitionNumberError },
    /// The text after the last `@` is not an offset.
    InvalidOffset { error: OffsetError },
}

impl fmt::Display for TopicPartitionOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("topic partition offset is empty"),
            Self::Malformed { input } => write!(
                f,
                "topic partition offset {input:?} does not have the form topic/partition@offset"
            ),
            Self::InvalidTopic { error } => fmt::Display::fmt(error, f),
            Self::InvalidPartition { error } => fmt::Display::fmt(error, f),
            Self::InvalidOffset { error } => fmt::Display::fmt(error, f),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for TopicPartitionOffsetError {}

impl From<TopicNameError> for TopicPartitionOffsetError {
    fn from(error: TopicNameError) -> Self {
        Self::InvalidTopic { error }
    }
}

impl From<PartitionNumberError> for TopicPartitionOffsetError {
    fn from(error: PartitionNumberError) -> Self {
        Self::InvalidPartition { error }
    }
}

impl From<OffsetError> for TopicPartitionOffsetError {
    fn from(error: OffsetError) -> Self {
        Self::InvalidOffset { error }
    }
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
            Err(PartitionNumberError::TooLarge {
                value: u64::from(MAX) + 1,
                max: u64::from(MAX)
            })
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
            Err(PartitionNumberError::Negative { value: -1 })
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
            Err(PartitionNumberError::TooLarge {
                value: 2_147_483_648,
                max: 2_147_483_647
            })
        );
        assert_eq!(
            "-1".parse::<PartitionNumber>(),
            Err(PartitionNumberError::Negative { value: -1 })
        );
        assert_eq!(
            "three".parse::<PartitionNumber>(),
            Err(PartitionNumberError::NotANumber { input: "three".into() })
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
        assert_eq!(
            TopicPartition::try_from(String::from("orders.eu/3")),
            Ok(topic_partition.clone())
        );
        assert_eq!(String::from(topic_partition.clone()), "orders.eu/3");
        assert_eq!(format!("{topic_partition:?}"), "TopicPartition(orders.eu/3)");
    }

    #[test]
    fn a_topic_partition_gives_its_parts() {
        let topic_partition = TopicPartition::from((orders(), partition(3)));

        assert_eq!(topic_partition.topic(), &orders());
        assert_eq!(topic_partition.partition(), partition(3));
        assert_eq!(topic_partition.clone().into_parts(), (orders(), partition(3)));
        assert_eq!(
            <(TopicName, PartitionNumber)>::from(topic_partition),
            (orders(), partition(3))
        );
    }

    #[test]
    fn a_topic_partition_needs_both_parts() {
        let malformed = |input: &str| {
            Err(TopicPartitionError::Malformed {
                input: input.to_owned(),
            })
        };

        assert_eq!("orders.eu".parse::<TopicPartition>(), malformed("orders.eu"));
        assert_eq!("3".parse::<TopicPartition>(), malformed("3"));
        assert_eq!("".parse::<TopicPartition>(), Err(TopicPartitionError::Empty));
        assert_eq!(
            "/3".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidTopic {
                error: TopicNameError::Empty
            })
        );
        assert_eq!(
            "orders.eu/".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidPartition {
                error: PartitionNumberError::Empty
            })
        );
    }

    #[test]
    fn a_topic_partition_checks_both_parts() {
        assert_eq!(
            "orders.*/3".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidTopic {
                error: TopicNameError::InvalidCharacter {
                    character: '*',
                    position: 7
                }
            })
        );
        assert_eq!(
            "orders/eu/3".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidTopic {
                error: TopicNameError::InvalidCharacter {
                    character: '/',
                    position: 6
                }
            })
        );
        assert_eq!(
            "orders.eu/x".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidPartition {
                error: PartitionNumberError::NotANumber { input: "x".into() }
            })
        );
        assert_eq!(
            "orders.eu/-1".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidPartition {
                error: PartitionNumberError::Negative { value: -1 }
            })
        );
        assert_eq!(
            "orders.eu/3@42".parse::<TopicPartition>(),
            Err(TopicPartitionError::InvalidPartition {
                error: PartitionNumberError::NotANumber { input: "3@42".into() }
            })
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
        assert_eq!(
            <(PartitionNumber, Offset)>::from(position),
            (partition(3), Offset::from(42))
        );
        assert_eq!(position.in_topic(&orders()).to_string(), "orders.eu/3@42");
    }

    #[test]
    fn a_partition_offset_needs_and_checks_both_parts() {
        assert_eq!(
            "3".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::Malformed { input: "3".into() })
        );
        assert_eq!("".parse::<PartitionOffset>(), Err(PartitionOffsetError::Empty));
        assert_eq!(
            "@42".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::InvalidPartition {
                error: PartitionNumberError::Empty
            })
        );
        assert_eq!(
            "3@".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::InvalidOffset {
                error: OffsetError::Empty
            })
        );
        assert_eq!(
            "3@-1".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::InvalidOffset {
                error: OffsetError::Negative { value: -1 }
            })
        );
        assert_eq!(
            "x@42".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::InvalidPartition {
                error: PartitionNumberError::NotANumber { input: "x".into() }
            })
        );
        assert_eq!(
            "orders.eu/3@42".parse::<PartitionOffset>(),
            Err(PartitionOffsetError::InvalidPartition {
                error: PartitionNumberError::NotANumber {
                    input: "orders.eu/3".into()
                }
            })
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
        assert_eq!(
            TopicPartitionOffset::try_from(String::from("orders.eu/3@42")),
            Ok(position.clone())
        );
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
        assert_eq!(
            position.partition_offset(),
            PartitionOffset::new(partition(3), Offset::from(42))
        );
        assert_eq!(
            position.clone().into_parts(),
            (topic_partition.clone(), Offset::from(42))
        );
        assert_eq!(
            <(TopicPartition, Offset)>::from(position.clone()),
            (topic_partition, Offset::from(42))
        );
        assert_eq!(
            <(TopicName, PartitionNumber, Offset)>::from(position.clone()),
            (orders(), partition(3), Offset::from(42))
        );
        assert_eq!(
            TopicPartitionOffset::from((orders(), partition(3), Offset::from(42))),
            position
        );
    }

    #[test]
    fn a_topic_partition_offset_needs_all_three_parts() {
        let malformed = |input: &str| {
            Err(TopicPartitionOffsetError::Malformed {
                input: input.to_owned(),
            })
        };

        assert_eq!("orders.eu/3".parse::<TopicPartitionOffset>(), malformed("orders.eu/3"));
        assert_eq!(
            "orders.eu@42".parse::<TopicPartitionOffset>(),
            malformed("orders.eu@42")
        );
        assert_eq!("3@42".parse::<TopicPartitionOffset>(), malformed("3@42"));
        assert_eq!("orders.eu".parse::<TopicPartitionOffset>(), malformed("orders.eu"));
        assert_eq!(
            "".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::Empty)
        );
    }

    #[test]
    fn a_topic_partition_offset_checks_all_three_parts() {
        assert_eq!(
            "orders eu/3@42".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::InvalidTopic {
                error: TopicNameError::InvalidCharacter {
                    character: ' ',
                    position: 6
                }
            })
        );
        assert_eq!(
            "orders.eu/x@42".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::InvalidPartition {
                error: PartitionNumberError::NotANumber { input: "x".into() }
            })
        );
        assert_eq!(
            "orders.eu/3@latest".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::InvalidOffset {
                error: OffsetError::NotANumber { input: "latest".into() }
            })
        );
        assert_eq!(
            "orders.eu/3@-1".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::InvalidOffset {
                error: OffsetError::Negative { value: -1 }
            })
        );
        assert_eq!(
            "orders@eu/3@42".parse::<TopicPartitionOffset>(),
            Err(TopicPartitionOffsetError::InvalidTopic {
                error: TopicNameError::InvalidCharacter {
                    character: '@',
                    position: 6
                }
            })
        );
    }

    #[test]
    fn topic_partition_offsets_order_by_topic_then_partition_then_offset() {
        let position = |text: &str| text.parse::<TopicPartitionOffset>().unwrap();

        assert!(position("orders.eu/9@100") < position("orders.eu/10@0"));
        assert!(position("orders.eu/3@9") < position("orders.eu/3@10"));
        assert!(position("orders.eu/3@9") < position("payments/0@0"));
    }

    #[test]
    fn the_error_of_a_position_says_which_part_is_wrong_once() {
        use std::error::Error;

        let cases: [(Box<dyn Error>, &str); 6] = [
            (
                Box::new("orders".parse::<TopicPartition>().unwrap_err()),
                "topic partition \"orders\" does not have the form topic/partition",
            ),
            (
                Box::new("orders eu/3".parse::<TopicPartition>().unwrap_err()),
                "topic name has the invalid character ' ' at position 6",
            ),
            (
                Box::new("3".parse::<PartitionOffset>().unwrap_err()),
                "partition offset \"3\" does not have the form partition@offset",
            ),
            (
                Box::new("3@-1".parse::<PartitionOffset>().unwrap_err()),
                "offset cannot be negative: -1",
            ),
            (
                Box::new("orders.eu@42".parse::<TopicPartitionOffset>().unwrap_err()),
                "topic partition offset \"orders.eu@42\" does not have the form topic/partition@offset",
            ),
            (
                Box::new("orders.eu/x@42".parse::<TopicPartitionOffset>().unwrap_err()),
                "partition number is not a number: \"x\"",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "the message is complete: {error}");
        }
    }
}
