use crate::{Field, FieldValue, FieldValues, Origin, PartFields, Target};

/// What a [`FieldSource`] answers when it is asked for a field.
///
/// A source can miss a field for two reasons, and a selector treats them differently.
///
/// | Reading | Means | Example |
/// |---|---|---|
/// | `Present` | The call has the field. | `api` on every request. |
/// | `Absent` | The call does not have the field, and waiting does not change that. | `acks` on a Fetch, `client.id` before the session is identified, `group` before the client named one. |
/// | `Pending` | The call will have the field, and does not have it yet. | `outcome` before the backend answers. |
///
/// Only response fields are ever pending, and only until the backend answers.
/// A condition on an absent field can be decided, and a condition on a pending field cannot.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let acks: Reading<Acks> = Some(Acks::ALL).into();
/// assert_eq!(acks, Reading::Present(Acks::ALL));
/// assert_eq!(acks.map(|acks| acks.value()).present(), Some(-1));
///
/// let none: Reading<Acks> = None.into();
/// assert!(none.is_absent());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reading<T> {
    /// The call has the field.
    Present(T),
    /// The call does not have the field.
    Absent,
    /// The call does not have the field yet.
    Pending,
}

impl<T> Reading<T> {
    /// What is present, or `None` for a reading that is absent or pending.
    pub fn present(self) -> Option<T> {
        match self {
            Reading::Present(value) => Some(value),
            Reading::Absent | Reading::Pending => None,
        }
    }

    /// The same reading of a reference.
    pub const fn as_ref(&self) -> Reading<&T> {
        match self {
            Reading::Present(value) => Reading::Present(value),
            Reading::Absent => Reading::Absent,
            Reading::Pending => Reading::Pending,
        }
    }

    /// The same reading of what `f` makes of the present value.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Reading<U> {
        match self {
            Reading::Present(value) => Reading::Present(f(value)),
            Reading::Absent => Reading::Absent,
            Reading::Pending => Reading::Pending,
        }
    }

    /// Whether the call has the field.
    pub const fn is_present(&self) -> bool {
        matches!(self, Reading::Present(_))
    }

    /// Whether the call does not have the field.
    pub const fn is_absent(&self) -> bool {
        matches!(self, Reading::Absent)
    }

    /// Whether the call does not have the field yet.
    pub const fn is_pending(&self) -> bool {
        matches!(self, Reading::Pending)
    }
}

/// `None` is a field that the call does not have.
impl<T> From<Option<T>> for Reading<T> {
    fn from(value: Option<T>) -> Self {
        value.map_or(Reading::Absent, Reading::Present)
    }
}

/// Anything that has fields: a call in the pipeline, a recorded call, or a call that a test wrote down.
///
/// A selector matches against a source and knows nothing else about the call.
/// The same selector therefore filters recorded traffic and targets live calls.
///
/// # What a source provides
///
/// A source provides two things.
///
/// - [`whole_call`](Self::whole_call) is what the call has once: who is calling, the API, the group.
/// - [`parts`](Self::parts) are the topics or partitions that the request names.
///   Each [part](PartFields) has its own `topic` and `partition`, and its own truth once the backend answered.
///
/// A source resolves a learned field itself.
/// When the request does not carry the group, the source answers with the groups of the client context,
/// and says so in the [`Origin`] of the values.
///
/// # What a selector asks
///
/// The provided methods hold the rules of multi-valued fields once, so every source follows them.
///
/// - [`read`](Self::read) is every value of a field across the call.
///   `is not` tests it: `topic is not orders.eu` does not match a Produce that carries `orders.eu` among others.
/// - [`read_in`](Self::read_in) is a field as one part sees it.
///   Conditions that test one part apply to the same partition:
///   `topic is orders.eu` with `partition is 3` matches `orders.eu/3`,
///   and not a request that carries `orders.eu/1` and `payments.settled/3`.
/// - [`whole_target`](Self::whole_target) is the target of a selector that tests no part.
///
/// The parts that pass every condition are the [`Target`] of the match.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let eu = TopicName::new("orders.eu")?;
/// let settled = TopicName::new("payments.settled")?;
/// let produce = Fields::new()
///     .with(ApiKey::PRODUCE)
///     .with_part(eu.partition(PartitionNumber::from(1)))
///     .with_part(settled.partition(PartitionNumber::from(3)));
///
/// // Across the call, the request names two topics and two partitions.
/// let topics = produce.read(Field::Topic).present().expect("the request names topics");
/// assert_eq!(topics.to_string(), "orders.eu, payments.settled");
///
/// // No single part is `orders.eu/3`.
/// let is = |part: &PartFields, value: FieldValue| {
///     produce.read_in(part, value.field()).present().is_some_and(|values| values.contains(&value))
/// };
/// let matched = produce
///     .parts()
///     .iter()
///     .filter(|part| is(part, eu.clone().into()) && is(part, PartitionNumber::from(3).into()))
///     .map(|part| part.part().clone());
/// assert_eq!(Target::of_parts(matched), None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait FieldSource {
    /// What the call as a whole has for `field`.
    ///
    /// For `outcome` and `error.code`, this is what applies to a part without a truth of its own,
    /// and to a call without parts.
    /// Nobody asks it for `topic` or `partition`, which only parts have.
    fn whole_call(&self, field: Field) -> Reading<FieldValues>;

    /// The parts of the call, in the order of the request: one for each topic or partition that the request names.
    ///
    /// A request that names no topic has no parts.
    fn parts(&self) -> &[PartFields];

    /// Every value of `field` across the call.
    ///
    /// For a field that [each part has its own value of](Field::is_per_part),
    /// these are the values of all parts together.
    /// They are pending when no part has a value and at least one part waits for its truth.
    fn read(&self, field: Field) -> Reading<FieldValues> {
        let parts = self.parts();
        if !field.is_per_part() {
            return self.whole_call(field);
        }
        if parts.is_empty() {
            return if matches!(field, Field::Topic | Field::Partition) {
                Reading::Absent
            } else {
                self.whole_call(field)
            };
        }

        let mut values: Vec<FieldValue> = Vec::new();
        let mut pending = false;
        let mut of_call = false;
        for part in parts {
            match part.own(field) {
                Some(Reading::Present(value)) => values.push(value),
                Some(Reading::Pending) => pending = true,
                Some(Reading::Absent) => {}
                None => of_call = true,
            }
        }
        if of_call {
            match self.whole_call(field) {
                Reading::Present(of_call) => values.extend(of_call.iter().cloned()),
                Reading::Pending => pending = true,
                Reading::Absent => {}
            }
        }

        match FieldValues::collect(values.into_iter(), Origin::Call) {
            Some(values) => Reading::Present(values),
            None if pending => Reading::Pending,
            None => Reading::Absent,
        }
    }

    /// The values of `field` as `part` sees them: the part's own value, or else what the whole call has.
    fn read_in(&self, part: &PartFields, field: Field) -> Reading<FieldValues> {
        match part.own(field) {
            Some(own) => own.map(FieldValues::one),
            None => self.whole_call(field),
        }
    }

    /// The target of a match that tests no part: every part of the call, or the whole call when it has none.
    fn whole_target(&self) -> Target {
        Target::of_parts(self.parts().iter().map(|part| part.part().clone())).unwrap_or(Target::Call)
    }
}

impl<S: FieldSource + ?Sized> FieldSource for &S {
    fn whole_call(&self, field: Field) -> Reading<FieldValues> {
        (**self).whole_call(field)
    }

    fn parts(&self) -> &[PartFields] {
        (**self).parts()
    }

    fn read(&self, field: Field) -> Reading<FieldValues> {
        (**self).read(field)
    }

    fn read_in(&self, part: &PartFields, field: Field) -> Reading<FieldValues> {
        (**self).read_in(part, field)
    }

    fn whole_target(&self) -> Target {
        (**self).whole_target()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reading_is_present_absent_or_pending() {
        let present = Reading::Present(3);
        let absent: Reading<i32> = Reading::Absent;
        let pending: Reading<i32> = Reading::Pending;

        assert!(present.is_present() && !present.is_absent() && !present.is_pending());
        assert!(absent.is_absent() && !absent.is_present() && !absent.is_pending());
        assert!(pending.is_pending() && !pending.is_present() && !pending.is_absent());
        assert_eq!(
            [present.present(), absent.present(), pending.present()],
            [Some(3), None, None]
        );
    }

    #[test]
    fn mapping_a_reading_keeps_why_it_is_missing() {
        assert_eq!(Reading::Present(3).map(|value| value + 1), Reading::Present(4));
        assert_eq!(Reading::<i32>::Absent.map(|value| value + 1), Reading::Absent);
        assert_eq!(Reading::<i32>::Pending.map(|value| value + 1), Reading::Pending);
        assert_eq!(
            Reading::Present(String::from("x")).as_ref(),
            Reading::Present(&String::from("x"))
        );
        assert_eq!(Reading::<String>::Pending.as_ref(), Reading::Pending);
        assert_eq!(Reading::<String>::Absent.as_ref(), Reading::Absent);
    }

    #[test]
    fn no_value_is_an_absent_reading() {
        assert_eq!(Reading::from(Some(3)), Reading::Present(3));
        assert_eq!(Reading::<i32>::from(None), Reading::Absent);
    }
}
