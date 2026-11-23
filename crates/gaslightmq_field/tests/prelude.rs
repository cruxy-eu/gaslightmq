//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// the kernel's prelude and the clock.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_clock::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;

    /// A source that a caller writes by hand, with the names of the prelude alone.
    struct Heartbeat;

    impl FieldSource for Heartbeat {
        fn whole_call(&self, field: Field) -> Reading<FieldValues> {
            match field {
                Field::Api => Reading::Present(FieldValues::one(ApiKey::HEARTBEAT)),
                Field::Group => FieldValues::learned([GroupId::new("analytics-etl").unwrap()]).into(),
                field if field.group() == FieldGroup::Response => Reading::Pending,
                _ => Reading::Absent,
            }
        }

        fn parts(&self) -> &[PartFields] {
            &[]
        }
    }

    fn text(value: impl CallValue) -> String {
        let value: FieldValue = value.into();
        value.to_string()
    }

    fn learned(value: impl LearnedValue) -> Origin {
        FieldValues::learned([value]).map_or(Origin::Call, |values| values.origin())
    }

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let part: Part = "orders.eu/3".parse().unwrap();
        let call = Fields::new().with(Acks::ALL).with_part(PartFields::new(part.clone()));

        vec![
            Field::ClientId.to_string(),
            FieldGroup::Learned.to_string(),
            ConnectionNumber::new(7).to_string(),
            "10.0.4.27:51459".parse::<ClientAddress>().unwrap().to_string(),
            "10.0.4.27".parse::<ClientHost>().unwrap().to_string(),
            Endpoint::Broker(BrokerId::from(2)).to_string(),
            ClientSoftware::new("librdkafka", "2.6.1").unwrap().to_string(),
            text(ApiVersion::from(11)),
            text(Outcome::Timeout),
            text(Latency::new(TimeSpan::from_millis(14))),
            call.read(Field::Acks).present().unwrap().to_string(),
            call.whole_target().to_string(),
            Target::of_parts([part]).unwrap().to_string(),
            format!("{:?}", learned(GroupId::new("analytics-etl").unwrap())),
            Heartbeat.read(Field::Api).present().unwrap().to_string(),
            Heartbeat.read(Field::Group).present().unwrap().to_string(),
            format!("{:?}", Heartbeat.read(Field::Outcome)),
            Heartbeat.whole_target().to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let input = || "x".to_owned();
        vec![
            message_of(FieldError::Unknown { name: input() }),
            message_of(FieldGroupError::Unknown { name: input() }),
            message_of(ConnectionNumberError::Malformed { input: input() }),
            message_of(ClientAddressError::Malformed { input: input() }),
            message_of(ClientHostError::Malformed { input: input() }),
            message_of(EndpointError::Malformed { input: input() }),
            message_of(EndpointError::InvalidBroker {
                input: "broker -1".to_owned(),
                error: BrokerIdError::Negative { value: -1 },
            }),
            message_of(ClientSoftwareError::Malformed { input: input() }),
            message_of(ApiVersionError::Malformed { input: input() }),
            message_of(ApiVersionError::OutOfRange {
                input: "40000".to_owned(),
            }),
            message_of(AcksError::Malformed { input: input() }),
            message_of(AcksError::OutOfRange {
                input: "40000".to_owned(),
            }),
            message_of(OutcomeError::Unknown { name: input() }),
            message_of(LatencyError::Malformed { input: input() }),
            message_of(PartError::InvalidTopic {
                error: TopicNameError::Empty,
            }),
            message_of(PartError::InvalidPartition {
                error: TopicPartitionError::Malformed { input: input() },
            }),
            message_of(FieldValueError::Topic {
                error: TopicNameError::Empty,
            }),
            message_of(PartSet::new([]).unwrap_err()),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        [
            "client.id",
            "learned",
            "#7",
            "10.0.4.27:51459",
            "10.0.4.27",
            "broker 2",
            "librdkafka 2.6.1",
            "11",
            "timeout",
            "14ms",
            "-1",
            "orders.eu/3",
            "orders.eu/3",
            "ClientContext",
            "Heartbeat",
            "analytics-etl",
            "Pending",
            "call",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "no field is named \"x\"",
            "unknown field group: \"x\"",
            "connection number \"x\" does not have the form #N",
            "client address \"x\" does not have the form an IP address and a port, such as 10.0.4.27:51459",
            "client host \"x\" does not have the form an IP address, such as 10.0.4.27",
            "endpoint \"x\" does not have the form bootstrap or broker N",
            "endpoint \"broker -1\": broker ID cannot be negative: -1",
            "client software \"x\" does not have the form a name and a version, such as librdkafka 2.6.1",
            "API version \"x\" does not have the form a whole number",
            "API version 40000 is not between 0 and 32767",
            "acks \"x\" does not have the form a whole number",
            "acks 40000 is not between -32768 and 32767",
            "unknown outcome: \"x\"",
            "latency \"x\" does not have the form a whole number and a unit, such as 14ms",
            "part: topic name is empty",
            "part: topic partition \"x\" does not have the form topic/partition",
            "topic: topic name is empty",
            "a target needs at least one part",
        ]
    );
}

#[test]
fn the_crate_root_exports_what_the_prelude_holds() {
    use gaslightmq_field::{Field, FieldValue, prelude};

    let from_root: FieldValue = Field::Api.parse_value("Fetch").unwrap();
    let from_prelude: prelude::FieldValue = prelude::Field::Api.parse_value("Fetch").unwrap();

    assert_eq!(from_root, from_prelude);
}
