use std::collections::BTreeMap;

use crate::{
    CallValue, Field, FieldGroup, FieldSource, FieldValue, FieldValues, LearnedValue, Origin, PartFields, Reading,
};

/// The fields of one call as plain data, and the simplest [`FieldSource`].
///
/// A test writes down the call it needs, value by value.
/// Whoever already holds the fields of a call as data can keep them in a `Fields` too.
/// Code that matches a call takes any [`FieldSource`], and never names this type.
///
/// A new `Fields` has no value and no part, and its response fields are pending:
/// it is a call that the backend did not answer yet.
///
/// | To say that | Call |
/// |---|---|
/// | The call or its session has a value. | [`with`](Self::with) |
/// | The client context provides a value. | [`with_learned`](Self::with_learned) |
/// | The request names a topic or a partition. | [`with_part`](Self::with_part) |
/// | The backend answered. | [`with`](Self::with) and a value of a response field. |
/// | The call never reached the backend. | [`without_truth`](Self::without_truth) |
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let orders = TopicName::new("orders.eu")?;
/// let fetch = Fields::new()
///     .with(ConnectionNumber::new(7))
///     .with(ClientId::new("analytics")?)
///     .with(ApiKey::FETCH)
///     .with(ApiVersion::from(17))
///     .with_learned(GroupId::new("analytics-etl")?)
///     .with_part(orders.partition(PartitionNumber::from(3)));
///
/// // A Fetch never names a group. The client context knows it.
/// let group = fetch.read(Field::Group).present().expect("the client named a group before");
/// assert_eq!(group.to_string(), "analytics-etl");
/// assert_eq!(group.origin(), Origin::ClientContext);
///
/// // A Fetch has no acks, and the backend did not answer yet.
/// assert_eq!(fetch.read(Field::Acks), Reading::Absent);
/// assert_eq!(fetch.read(Field::Outcome), Reading::Pending);
///
/// // Then the backend answers.
/// let answered = fetch.with(Outcome::Ok).with(Latency::from_millis(14));
/// assert_eq!(answered.read(Field::Outcome), Reading::Present(FieldValues::one(Outcome::Ok)));
/// assert_eq!(answered.read(Field::ErrorCode), Reading::Absent);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fields {
    values: BTreeMap<Field, FieldValues>,
    parts: Vec<PartFields>,
    truth: Truth,
}

/// What the call knows about the answer of the backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Truth {
    /// The backend did not answer yet.
    #[default]
    Pending,
    /// The backend answered, and the response fields that have no value are absent.
    Known,
    /// The call never reached the backend.
    None,
}

impl Fields {
    /// A call without a value and without a part, which waits for the backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// The call or its session has `value`.
    ///
    /// A second value of the same field makes the field multi-valued.
    /// A value replaces what the client context provides for its field,
    /// because a request that carries a learned field provides it alone.
    ///
    /// A value of a response field also says that the backend answered:
    /// from then on, every response field without a value is absent instead of pending.
    #[must_use]
    pub fn with(mut self, value: impl CallValue) -> Self {
        let value: FieldValue = value.into();
        let field = value.field();
        if field.group() == FieldGroup::Response {
            self.truth = Truth::Known;
        }
        match self.values.get_mut(&field) {
            Some(values) if values.origin() == Origin::Call => values.insert(value),
            _ => {
                self.values.insert(field, FieldValues::one(value));
            }
        }
        self
    }

    /// The client context provides `value`, because another call of the same client carried it.
    ///
    /// A second value of the same field makes the field multi-valued:
    /// a client that runs several consumers under one `client.id` has several groups.
    /// A field that the call itself has a value of keeps that value alone.
    #[must_use]
    pub fn with_learned(mut self, value: impl LearnedValue) -> Self {
        let value: FieldValue = value.into();
        match self.values.get_mut(&value.field()) {
            Some(values) if values.origin() == Origin::ClientContext => values.insert(value),
            Some(_) => {}
            None => {
                let learned = FieldValues::collect([value].into_iter(), Origin::ClientContext);
                self.values.extend(learned.map(|values| (values.field(), values)));
            }
        }
        self
    }

    /// The request names `part`: a topic, a partition, or either with its own truth.
    #[must_use]
    pub fn with_part(mut self, part: impl Into<PartFields>) -> Self {
        self.parts.push(part.into());
        self
    }

    /// The call never reached the backend, as after a silence or a refusal before forwarding.
    /// Its response fields are absent, and they stay so.
    #[must_use]
    pub fn without_truth(mut self) -> Self {
        self.values.retain(|field, _| field.group() != FieldGroup::Response);
        self.truth = Truth::None;
        self
    }
}

impl FieldSource for Fields {
    fn whole_call(&self, field: Field) -> Reading<FieldValues> {
        match self.values.get(&field) {
            Some(values) => Reading::Present(values.clone()),
            None if field.group() == FieldGroup::Response && self.truth == Truth::Pending => Reading::Pending,
            None => Reading::Absent,
        }
    }

    fn parts(&self) -> &[PartFields] {
        &self.parts
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::{Acks, ConnectionNumber, Latency, Outcome, Part, Target};

    fn topic(name: &str) -> TopicName {
        TopicName::new(name).unwrap()
    }

    fn partition(text: &str) -> TopicPartition {
        text.parse().unwrap()
    }

    fn group(id: &str) -> GroupId {
        GroupId::new(id).unwrap()
    }

    fn text(reading: Reading<FieldValues>) -> String {
        match reading {
            Reading::Present(values) => values.to_string(),
            Reading::Absent => "(absent)".to_owned(),
            Reading::Pending => "(pending)".to_owned(),
        }
    }

    #[test]
    fn a_new_call_has_no_value_and_waits_for_the_backend() {
        let call = Fields::new();

        for field in Field::ALL {
            let expected = if field.group() == FieldGroup::Response {
                Reading::Pending
            } else {
                Reading::Absent
            };
            assert_eq!(call.read(field), expected, "{field}");
            assert_eq!(call.whole_call(field), expected, "{field}");
        }
        assert_eq!(call.parts(), []);
        assert_eq!(call, Fields::default());
    }

    #[test]
    fn a_value_is_a_value_of_its_own_field_only() {
        let call = Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::PRODUCE)
            .with(Acks::ALL);

        assert_eq!(text(call.read(Field::Connection)), "#7");
        assert_eq!(text(call.read(Field::Api)), "Produce");
        assert_eq!(text(call.read(Field::Acks)), "-1");
        assert_eq!(call.read(Field::ClientId), Reading::Absent);
        assert_eq!(
            call.read(Field::Api).present().map(|values| values.origin()),
            Some(Origin::Call)
        );
    }

    #[test]
    fn a_second_value_makes_a_field_multi_valued() {
        let call = Fields::new()
            .with(group("payments"))
            .with(group("analytics-etl"))
            .with(group("payments"));

        assert_eq!(text(call.read(Field::Group)), "analytics-etl, payments");
    }

    #[test]
    fn the_client_context_provides_a_learned_field_the_request_does_not_carry() {
        let call = Fields::new()
            .with(ApiKey::FETCH)
            .with_learned(group("payments"))
            .with_learned(group("analytics-etl"));
        let groups = call.read(Field::Group).present().unwrap();

        assert_eq!(groups.to_string(), "analytics-etl, payments");
        assert_eq!(groups.origin(), Origin::ClientContext);
    }

    #[test]
    fn a_request_that_carries_a_learned_field_provides_it_alone() {
        let carried_last = Fields::new()
            .with_learned(group("payments"))
            .with(group("analytics-etl"));
        let carried_first = Fields::new()
            .with(group("analytics-etl"))
            .with_learned(group("payments"));

        for call in [carried_last, carried_first] {
            let groups = call.read(Field::Group).present().unwrap();
            assert_eq!(groups.to_string(), "analytics-etl");
            assert_eq!(groups.origin(), Origin::Call);
        }
    }

    #[test]
    fn a_response_field_is_pending_until_the_backend_answers() {
        let asked = Fields::new().with(ApiKey::FIND_COORDINATOR);
        assert_eq!(asked.read(Field::Outcome), Reading::Pending);
        assert_eq!(asked.read(Field::ErrorCode), Reading::Pending);
        assert_eq!(asked.read(Field::Latency), Reading::Pending);

        let answered = asked.with(Outcome::Ok).with(Latency::from_millis(14));
        assert_eq!(text(answered.read(Field::Outcome)), "ok");
        assert_eq!(answered.read(Field::ErrorCode), Reading::Absent);
        assert_eq!(text(answered.read(Field::Latency)), "14ms");
    }

    #[test]
    fn a_call_that_never_reached_the_backend_has_no_response_fields() {
        let silenced = Fields::new().with(ApiKey::FETCH).with(Outcome::Ok).without_truth();

        for field in FieldGroup::Response.fields() {
            assert_eq!(silenced.read(*field), Reading::Absent, "{field}");
        }
        assert_eq!(text(silenced.read(Field::Api)), "Fetch");
    }

    #[test]
    fn a_field_that_a_request_cannot_have_is_absent_and_not_pending() {
        let fetch = Fields::new().with(ApiKey::FETCH).with_part(partition("orders.eu/3"));

        assert_eq!(fetch.read(Field::Acks), Reading::Absent);
        assert_eq!(fetch.read(Field::Group), Reading::Absent);
        assert_eq!(fetch.read(Field::Outcome), Reading::Pending);
    }

    #[test]
    fn the_topics_and_partitions_of_a_call_are_those_of_its_parts() {
        let produce = Fields::new()
            .with(ApiKey::PRODUCE)
            .with_part(partition("orders.eu/1"))
            .with_part(partition("orders.eu/3"))
            .with_part(partition("payments.settled/3"));

        assert_eq!(text(produce.read(Field::Topic)), "orders.eu, payments.settled");
        assert_eq!(text(produce.read(Field::Partition)), "1, 3");
        assert_eq!(produce.parts().len(), 3);
    }

    #[test]
    fn a_request_without_parts_has_no_topic_and_no_partition() {
        let heartbeat = Fields::new().with(ApiKey::HEARTBEAT).with(group("analytics-etl"));

        assert_eq!(heartbeat.read(Field::Topic), Reading::Absent);
        assert_eq!(heartbeat.read(Field::Partition), Reading::Absent);
        assert_eq!(heartbeat.whole_target(), Target::Call);
    }

    #[test]
    fn a_request_that_names_topics_only_has_no_partition() {
        let metadata = Fields::new()
            .with(ApiKey::METADATA)
            .with_part(topic("orders.eu"))
            .with_part(topic("orders.us"));

        assert_eq!(text(metadata.read(Field::Topic)), "orders.eu, orders.us");
        assert_eq!(metadata.read(Field::Partition), Reading::Absent);
        assert_eq!(metadata.whole_target().to_string(), "orders.eu, orders.us");
    }

    #[test]
    fn a_part_sees_its_own_topic_and_partition_and_the_rest_of_the_call() {
        let produce = Fields::new()
            .with(ApiKey::PRODUCE)
            .with(Acks::ALL)
            .with_part(partition("orders.eu/1"))
            .with_part(partition("payments.settled/3"));
        let [eu, settled] = produce.parts() else {
            panic!("two parts")
        };

        assert_eq!(text(produce.read_in(eu, Field::Topic)), "orders.eu");
        assert_eq!(text(produce.read_in(eu, Field::Partition)), "1");
        assert_eq!(text(produce.read_in(settled, Field::Topic)), "payments.settled");
        assert_eq!(text(produce.read_in(settled, Field::Partition)), "3");
        for part in [eu, settled] {
            assert_eq!(text(produce.read_in(part, Field::Api)), "Produce");
            assert_eq!(text(produce.read_in(part, Field::Acks)), "-1");
            assert_eq!(produce.read_in(part, Field::Group), Reading::Absent);
            assert_eq!(produce.read_in(part, Field::Outcome), Reading::Pending);
        }
    }

    #[test]
    fn the_whole_target_of_a_call_with_parts_is_every_part() {
        let produce = Fields::new()
            .with_part(partition("payments.settled/3"))
            .with_part(partition("orders.eu/1"));

        assert_eq!(
            produce.whole_target(),
            Target::of_parts([
                Part::from(partition("orders.eu/1")),
                Part::from(partition("payments.settled/3"))
            ])
            .unwrap()
        );
    }

    #[test]
    fn a_part_without_its_own_truth_has_the_truth_of_the_call() {
        let answered = Fields::new()
            .with(ApiKey::FETCH)
            .with_part(partition("orders.eu/3"))
            .with(Outcome::Error)
            .with(ErrorCode::REQUEST_TIMED_OUT);
        let [part] = answered.parts() else { panic!("one part") };

        assert_eq!(text(answered.read_in(part, Field::Outcome)), "error");
        assert_eq!(text(answered.read_in(part, Field::ErrorCode)), "REQUEST_TIMED_OUT");
        assert_eq!(text(answered.read(Field::Outcome)), "error");
        assert_eq!(text(answered.read(Field::ErrorCode)), "REQUEST_TIMED_OUT");
    }

    #[test]
    fn the_truth_of_a_call_is_the_truth_of_its_parts_together() {
        let answered = Fields::new()
            .with(ApiKey::PRODUCE)
            .with_part(PartFields::new(partition("orders.eu/3")).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
            .with_part(PartFields::new(partition("orders.eu/4")).succeeded())
            .with_part(PartFields::new(partition("payments.settled/0")).without_truth())
            .with(Latency::from_millis(14));
        let [failed, succeeded, refused] = answered.parts() else {
            panic!("three parts")
        };

        assert_eq!(text(answered.read(Field::Outcome)), "ok, error");
        assert_eq!(text(answered.read(Field::ErrorCode)), "NOT_LEADER_OR_FOLLOWER");

        assert_eq!(text(answered.read_in(failed, Field::Outcome)), "error");
        assert_eq!(
            text(answered.read_in(failed, Field::ErrorCode)),
            "NOT_LEADER_OR_FOLLOWER"
        );
        assert_eq!(text(answered.read_in(succeeded, Field::Outcome)), "ok");
        assert_eq!(answered.read_in(succeeded, Field::ErrorCode), Reading::Absent);
        assert_eq!(answered.read_in(refused, Field::Outcome), Reading::Absent);
        assert_eq!(answered.read_in(refused, Field::ErrorCode), Reading::Absent);
        for part in answered.parts() {
            assert_eq!(text(answered.read_in(part, Field::Latency)), "14ms");
        }
    }

    #[test]
    fn the_truth_of_a_call_is_pending_while_its_parts_wait_for_it() {
        let asked = Fields::new()
            .with_part(PartFields::new(partition("orders.eu/3")))
            .with_part(PartFields::new(partition("orders.eu/4")).without_truth());

        assert_eq!(asked.read(Field::Outcome), Reading::Pending);
        assert_eq!(asked.read(Field::ErrorCode), Reading::Pending);
    }

    #[test]
    fn a_call_whose_parts_all_miss_the_truth_has_none() {
        let refused = Fields::new()
            .with_part(PartFields::new(partition("orders.eu/3")).without_truth())
            .with_part(PartFields::new(partition("orders.eu/4")).without_truth());

        assert_eq!(refused.read(Field::Outcome), Reading::Absent);
        assert_eq!(refused.read(Field::ErrorCode), Reading::Absent);
        // The latency is not a part's own, so the call still waits for it.
        assert_eq!(refused.read(Field::Latency), Reading::Pending);
    }

    #[test]
    fn a_reference_to_a_source_is_a_source() {
        fn topics(source: impl FieldSource) -> String {
            let whole = source
                .whole_call(Field::Api)
                .present()
                .map_or_default(|values| values.to_string());
            let parts = source.parts().len();
            let first = source
                .parts()
                .first()
                .map_or_default(|part| text(source.read_in(part, Field::Topic)));
            format!(
                "{whole} {parts} {first} {} {}",
                text(source.read(Field::Topic)),
                source.whole_target()
            )
        }

        let produce = Fields::new().with(ApiKey::PRODUCE).with_part(partition("orders.eu/1"));
        let dynamic: &dyn FieldSource = &produce;

        assert_eq!(topics(&produce), "Produce 1 orders.eu orders.eu orders.eu/1");
        assert_eq!(topics(dynamic), topics(&produce));
    }
}
