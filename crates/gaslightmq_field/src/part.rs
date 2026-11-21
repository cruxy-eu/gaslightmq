use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use gaslightmq_kernel::prelude::*;

use crate::text::text_form;
use crate::{Field, FieldValue, Outcome, Reading};

/// One piece of a call that a lie can change on its own: a topic, or one partition of a topic.
///
/// Most Kafka requests carry several topics and partitions.
/// A Produce names partitions, so each of its parts is a partition.
/// A Metadata request names topics only, so each of its parts is a topic.
/// A `Heartbeat` names neither, and it has no parts.
///
/// The text form is `orders.eu` for a topic and `orders.eu/3` for a partition.
/// A topic name has no `/`, so the two never read as each other.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let part: Part = "orders.eu/3".parse()?;
/// assert_eq!(part.topic(), "orders.eu");
/// assert_eq!(part.partition(), Some(PartitionNumber::from(3)));
///
/// let topic = Part::from(TopicName::new("orders.eu")?);
/// assert_eq!(topic.partition(), None);
/// assert_eq!(topic.to_string(), "orders.eu");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Part {
    /// A topic that a request names without partitions.
    Topic(TopicName),
    /// One partition of a topic.
    Partition(TopicPartition),
}

impl Part {
    /// The topic of the part.
    pub fn topic(&self) -> &TopicName {
        match self {
            Part::Topic(topic) => topic,
            Part::Partition(partition) => partition.topic(),
        }
    }

    /// The partition number of the part, if the part is a partition.
    pub fn partition(&self) -> Option<PartitionNumber> {
        self.topic_partition().map(TopicPartition::partition)
    }

    /// The part as a topic partition, if it is one.
    pub fn topic_partition(&self) -> Option<&TopicPartition> {
        match self {
            Part::Topic(_) => None,
            Part::Partition(partition) => Some(partition),
        }
    }
}

impl From<TopicName> for Part {
    fn from(topic: TopicName) -> Self {
        Part::Topic(topic)
    }
}

impl From<TopicPartition> for Part {
    fn from(partition: TopicPartition) -> Self {
        Part::Partition(partition)
    }
}

/// Reads `orders.eu/3` as a partition, and text without a `/` as a topic.
impl FromStr for Part {
    type Err = PartError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.contains('/') {
            Ok(Part::Partition(text.parse()?))
        } else {
            Ok(Part::Topic(text.parse()?))
        }
    }
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Part::Topic(topic) => fmt::Display::fmt(topic, f),
            Part::Partition(partition) => fmt::Display::fmt(partition, f),
        }
    }
}

text_form!(Part, PartError);

/// Why a text is not a [`Part`].
///
/// A part is a topic or a partition of a topic, and the error says which of the two the text failed to be.
/// Its message is the message of that error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartError {
    /// The text has no `/`, and it is not a topic name.
    InvalidTopic { error: TopicNameError },
    /// The text has a `/`, and it is not a topic partition.
    InvalidPartition { error: TopicPartitionError },
}

impl fmt::Display for PartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopic { error } => fmt::Display::fmt(error, f),
            Self::InvalidPartition { error } => fmt::Display::fmt(error, f),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for PartError {}

impl From<TopicNameError> for PartError {
    fn from(error: TopicNameError) -> Self {
        Self::InvalidTopic { error }
    }
}

impl From<TopicPartitionError> for PartError {
    fn from(error: TopicPartitionError) -> Self {
        Self::InvalidPartition { error }
    }
}

/// A part of a call as a [`FieldSource`](crate::FieldSource) shows it:
/// which part it is, and the truth of that part once there is one.
///
/// A part has its own `topic` and `partition`.
/// It can also have its own `outcome` and `error.code`,
/// because a backend answers a Produce or a Fetch partition by partition.
///
/// | Built with | `outcome` of the part | `error.code` of the part |
/// |---|---|---|
/// | [`new`](Self::new) alone | That of the call. | That of the call. |
/// | [`succeeded`](Self::succeeded) | `ok` | Absent. |
/// | [`failed`](Self::failed) | `error` | The code. |
/// | [`without_truth`](Self::without_truth) | Absent. | Absent. |
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let partition: TopicPartition = "orders.eu/3".parse()?;
///
/// // Before the backend answers, the part only says which part it is.
/// let asked = PartFields::new(partition.clone());
/// assert_eq!(asked.own(Field::Partition), Some(Reading::Present(PartitionNumber::from(3).into())));
/// assert_eq!(asked.own(Field::Outcome), None);
///
/// // After the answer, it has a truth of its own.
/// let answered = asked.failed(ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// assert_eq!(answered.own(Field::Outcome), Some(Reading::Present(Outcome::Error.into())));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartFields {
    part: Part,
    truth: Truth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Truth {
    /// The part has no truth of its own, so the response fields of the call apply.
    OfCall,
    Succeeded,
    Failed(ErrorCode),
    /// The part never reached the backend.
    None,
}

impl PartFields {
    /// A part without a truth of its own: the response fields of the call apply to it.
    pub fn new(part: impl Into<Part>) -> Self {
        Self {
            part: part.into(),
            truth: Truth::OfCall,
        }
    }

    /// The backend answered this part without an error.
    #[must_use]
    pub fn succeeded(self) -> Self {
        Self {
            truth: Truth::Succeeded,
            ..self
        }
    }

    /// The backend answered this part with an error.
    #[must_use]
    pub fn failed(self, code: ErrorCode) -> Self {
        Self {
            truth: Truth::Failed(code),
            ..self
        }
    }

    /// This part never reached the backend, as after a refusal before forwarding.
    /// It has no truth, whatever the backend answered for the rest of the call.
    #[must_use]
    pub fn without_truth(self) -> Self {
        Self {
            truth: Truth::None,
            ..self
        }
    }

    /// Which part of the call this is.
    pub fn part(&self) -> &Part {
        &self.part
    }

    /// What the part itself has for `field`.
    ///
    /// `None` means that the field is not the part's own, so the reading of the whole call applies.
    /// [`FieldSource::read_in`](crate::FieldSource::read_in) combines the two.
    pub fn own(&self, field: Field) -> Option<Reading<FieldValue>> {
        match field {
            Field::Topic => Some(Reading::Present(self.part.topic().clone().into())),
            Field::Partition => Some(self.part.partition().map(FieldValue::from).into()),
            Field::Outcome => match self.truth {
                Truth::OfCall => None,
                Truth::Succeeded => Some(Reading::Present(Outcome::Ok.into())),
                Truth::Failed(_) => Some(Reading::Present(Outcome::Error.into())),
                Truth::None => Some(Reading::Absent),
            },
            Field::ErrorCode => match self.truth {
                Truth::OfCall => None,
                Truth::Failed(code) => Some(Reading::Present(code.into())),
                Truth::Succeeded | Truth::None => Some(Reading::Absent),
            },
            _ => None,
        }
    }
}

impl From<Part> for PartFields {
    fn from(part: Part) -> Self {
        Self::new(part)
    }
}

impl From<TopicName> for PartFields {
    fn from(topic: TopicName) -> Self {
        Self::new(topic)
    }
}

impl From<TopicPartition> for PartFields {
    fn from(partition: TopicPartition) -> Self {
        Self::new(partition)
    }
}

/// The part of a call that a lie changes.
///
/// A selector yields the target when it matches, and a lie applies to it.
///
/// - On a request with [parts](Part), the target is the set of parts that the selector matched.
///   A selector on `orders.*` matches a Produce for `orders.eu` and `payments.settled`,
///   and its target is the `orders.eu` partitions only.
/// - On a request without parts, such as `FindCoordinator` or `Heartbeat`, the target is the whole call.
///
/// A match always has a target with at least one part.
/// A selector that matches no part does not match the call, which is what [`of_parts`](Self::of_parts) says.
/// A [`PartSet`] always has a part, so no target is a target of nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let eu: Part = "orders.eu/3".parse()?;
/// let settled: Part = "payments.settled/3".parse()?;
///
/// let target = Target::of_parts([eu.clone()]).expect("one part");
/// assert!(target.covers(&eu));
/// assert!(!target.covers(&settled));
/// assert_eq!(target.to_string(), "orders.eu/3");
///
/// assert!(Target::Call.covers(&settled));
/// assert_eq!(Target::of_parts([]), None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(rename_all = "lowercase"))]
pub enum Target {
    /// The whole call.
    Call,
    /// Some parts of the call, and at least one.
    Parts(PartSet),
}

/// Reads `call`, or a map with the one entry `parts` and the list of parts.
/// It rejects a list without a part, as [`Target::of_parts`] does.
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Target {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// A target as its form has it, before the check that it has a part.
        #[derive(serde::Deserialize)]
        #[serde(rename = "Target", rename_all = "lowercase")]
        enum Form {
            Call,
            Parts(BTreeSet<Part>),
        }

        match Form::deserialize(deserializer)? {
            Form::Call => Ok(Target::Call),
            Form::Parts(parts) => {
                Target::of_parts(parts).ok_or_else(|| serde::de::Error::invalid_length(0, &"at least one part"))
            }
        }
    }
}

impl Target {
    /// The target of those parts, or `None` when there is no part.
    pub fn of_parts(parts: impl IntoIterator<Item = Part>) -> Option<Self> {
        PartSet::new(parts).ok().map(Target::Parts)
    }

    /// Whether the target is the whole call.
    pub const fn is_call(&self) -> bool {
        matches!(self, Target::Call)
    }

    /// The parts of the target in order. The whole call names no parts.
    pub fn parts(&self) -> impl Iterator<Item = &Part> {
        match self {
            Target::Call => None,
            Target::Parts(parts) => Some(parts.iter()),
        }
        .into_iter()
        .flatten()
    }

    /// Whether a lie on this target changes `part`.
    /// The whole call covers every part.
    pub fn covers(&self, part: &Part) -> bool {
        match self {
            Target::Call => true,
            Target::Parts(parts) => parts.contains(part),
        }
    }
}

/// Writes `call`, or the parts with a comma and a space between them.
impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_call() {
            return f.write_str("call");
        }
        for (index, part) in self.parts().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            fmt::Display::fmt(part, f)?;
        }
        Ok(())
    }
}

/// The target of those parts.
impl From<PartSet> for Target {
    fn from(parts: PartSet) -> Self {
        Target::Parts(parts)
    }
}

/// One or more parts of a call: what a [`Target`] of parts holds.
///
/// The set always has a part.
/// It takes more parts, and it never gives one up, so a target of parts is never a target of nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let eu: Part = "orders.eu/3".parse()?;
/// let settled: Part = "payments.settled/3".parse()?;
///
/// let mut parts = PartSet::one(eu.clone());
/// parts.insert(settled.clone());
/// assert_eq!(parts.len(), 2);
/// assert!(parts.contains(&settled));
///
/// assert_eq!(PartSet::new([]), Err(PartSetError::Empty));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(transparent))]
pub struct PartSet(BTreeSet<Part>);

impl PartSet {
    /// The set of those parts.
    ///
    /// # Errors
    ///
    /// [`PartSetError::Empty`] when there is no part.
    pub fn new(parts: impl IntoIterator<Item = Part>) -> Result<Self, PartSetError> {
        let parts: BTreeSet<Part> = parts.into_iter().collect();
        if parts.is_empty() {
            Err(PartSetError::Empty)
        } else {
            Ok(Self(parts))
        }
    }

    /// The set of one part.
    pub fn one(part: impl Into<Part>) -> Self {
        Self(BTreeSet::from([part.into()]))
    }

    /// Adds a part, and says whether the set did not have it yet.
    pub fn insert(&mut self, part: Part) -> bool {
        self.0.insert(part)
    }

    /// The parts as a plain set, to read them.
    pub const fn as_set(&self) -> &BTreeSet<Part> {
        &self.0
    }
}

/// Reads the parts as a plain set: `iter`, `contains`, `len` and the comparisons of sets.
impl std::ops::Deref for PartSet {
    type Target = BTreeSet<Part>;

    fn deref(&self) -> &BTreeSet<Part> {
        &self.0
    }
}

impl<'a> IntoIterator for &'a PartSet {
    type Item = &'a Part;
    type IntoIter = std::collections::btree_set::Iter<'a, Part>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// The set of that one part.
impl From<Part> for PartSet {
    fn from(part: Part) -> Self {
        Self::one(part)
    }
}

/// Why parts are not a [`PartSet`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartSetError {
    /// There is no part, and a target of no part is no target.
    Empty,
}

impl fmt::Display for PartSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("a target needs at least one part"),
        }
    }
}

impl std::error::Error for PartSetError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn a_part_is_a_topic_or_a_partition() {
        let topic = part("orders.eu");
        let partition = part("orders.eu/3");

        assert_eq!(topic, Part::Topic(TopicName::new("orders.eu").unwrap()));
        assert_eq!(partition, Part::Partition("orders.eu/3".parse().unwrap()));
        assert_eq!(topic.topic(), partition.topic());
        assert_eq!(topic.partition(), None);
        assert_eq!(topic.topic_partition(), None);
        assert_eq!(partition.partition(), Some(PartitionNumber::from(3)));
        assert_eq!(
            partition.topic_partition().map(ToString::to_string).as_deref(),
            Some("orders.eu/3")
        );
    }

    #[test]
    fn a_part_reads_and_writes_the_text_form_of_what_it_holds() {
        for text in ["orders.eu", "orders.eu/3", "__consumer_offsets/49"] {
            assert_eq!(part(text).to_string(), text);
        }
        assert_eq!(
            "".parse::<Part>(),
            Err(PartError::InvalidTopic {
                error: TopicNameError::Empty
            })
        );
        assert_eq!(
            "orders.eu/".parse::<Part>(),
            Err(PartError::InvalidPartition {
                error: TopicPartitionError::InvalidPartition {
                    error: PartitionNumberError::Empty
                }
            })
        );
        assert!(matches!(
            "orders.eu/x".parse::<Part>(),
            Err(PartError::InvalidPartition { .. })
        ));
        assert!(matches!(
            "orders eu".parse::<Part>(),
            Err(PartError::InvalidTopic { .. })
        ));
    }

    #[test]
    fn a_part_converts_from_what_it_holds() {
        let topic = TopicName::new("orders.eu").unwrap();

        assert_eq!(Part::from(topic.clone()), part("orders.eu"));
        assert_eq!(
            Part::from(topic.partition(PartitionNumber::from(3))),
            part("orders.eu/3")
        );
    }

    #[test]
    fn a_part_has_its_own_topic_and_partition() {
        let partition = PartFields::new(part("orders.eu/3"));
        let topic = PartFields::new(part("orders.eu"));

        assert_eq!(partition.part(), &part("orders.eu/3"));
        assert_eq!(
            partition.own(Field::Topic),
            Some(Reading::Present(TopicName::new("orders.eu").unwrap().into()))
        );
        assert_eq!(
            partition.own(Field::Partition),
            Some(Reading::Present(PartitionNumber::from(3).into()))
        );
        assert_eq!(topic.own(Field::Topic), partition.own(Field::Topic));
        assert_eq!(topic.own(Field::Partition), Some(Reading::Absent));
    }

    #[test]
    fn a_part_leaves_every_other_field_to_the_call() {
        let answered = PartFields::new(part("orders.eu/3")).succeeded();

        for field in Field::ALL.into_iter().filter(|field| !field.is_per_part()) {
            assert_eq!(answered.own(field), None, "{field}");
        }
    }

    #[test]
    fn a_part_without_a_truth_of_its_own_leaves_the_truth_to_the_call() {
        let asked = PartFields::new(part("orders.eu/3"));

        assert_eq!(asked.own(Field::Outcome), None);
        assert_eq!(asked.own(Field::ErrorCode), None);
    }

    #[test]
    fn a_part_that_succeeded_is_ok_and_has_no_error_code() {
        let answered = PartFields::new(part("orders.eu/3")).succeeded();

        assert_eq!(answered.own(Field::Outcome), Some(Reading::Present(Outcome::Ok.into())));
        assert_eq!(answered.own(Field::ErrorCode), Some(Reading::Absent));
    }

    #[test]
    fn a_part_that_failed_is_an_error_with_its_code() {
        let answered = PartFields::new(part("orders.eu/3")).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER);

        assert_eq!(
            answered.own(Field::Outcome),
            Some(Reading::Present(Outcome::Error.into()))
        );
        assert_eq!(
            answered.own(Field::ErrorCode),
            Some(Reading::Present(ErrorCode::NOT_LEADER_OR_FOLLOWER.into()))
        );
    }

    #[test]
    fn a_part_that_never_reached_the_backend_has_no_truth() {
        let refused = PartFields::new(part("orders.eu/3")).without_truth();

        assert_eq!(refused.own(Field::Outcome), Some(Reading::Absent));
        assert_eq!(refused.own(Field::ErrorCode), Some(Reading::Absent));
    }

    #[test]
    fn part_fields_convert_from_a_part_and_from_what_a_part_holds() {
        let topic = TopicName::new("orders.eu").unwrap();

        assert_eq!(PartFields::from(topic.clone()), PartFields::new(part("orders.eu")));
        assert_eq!(
            PartFields::from(topic.partition(PartitionNumber::from(3))),
            PartFields::new(part("orders.eu/3"))
        );
        assert_eq!(
            PartFields::from(part("orders.eu/3")),
            PartFields::new(part("orders.eu/3"))
        );
    }

    #[test]
    fn a_target_of_parts_holds_each_part_once_and_in_order() {
        let target = Target::of_parts([
            part("payments.settled/3"),
            part("orders.eu/4"),
            part("orders.eu/3"),
            part("orders.eu/4"),
        ])
        .unwrap();

        assert!(!target.is_call());
        assert_eq!(target.to_string(), "orders.eu/3, orders.eu/4, payments.settled/3");
        assert_eq!(target.parts().count(), 3);
    }

    #[test]
    fn a_target_without_parts_is_no_target() {
        assert_eq!(Target::of_parts([]), None);
    }

    #[test]
    fn a_target_covers_its_own_parts_only() {
        let target = Target::of_parts([part("orders.eu/3"), part("orders.eu/4")]).unwrap();

        assert!(target.covers(&part("orders.eu/3")));
        assert!(!target.covers(&part("orders.eu/1")));
        assert!(!target.covers(&part("payments.settled/3")));
        assert!(!target.covers(&part("orders.eu")));
    }

    #[test]
    fn the_whole_call_covers_every_part_and_names_none() {
        assert!(Target::Call.is_call());
        assert!(Target::Call.covers(&part("orders.eu/3")));
        assert!(Target::Call.covers(&part("orders.eu")));
        assert_eq!(Target::Call.parts().count(), 0);
        assert_eq!(Target::Call.to_string(), "call");
    }

    #[test]
    fn a_target_of_topics_names_topics() {
        let target = Target::of_parts([part("orders.eu"), part("orders.us")]).unwrap();

        assert_eq!(target.to_string(), "orders.eu, orders.us");
        assert!(target.covers(&part("orders.eu")));
        assert!(!target.covers(&part("orders.eu/3")));
    }
}
