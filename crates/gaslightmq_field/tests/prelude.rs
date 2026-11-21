//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// the kernel's prelude and the clock.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![allow(unused_imports)]

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
        let input = || "x".to_owned();
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(FieldError::Unknown { name: input() }),
            Box::new(FieldGroupError::Unknown { name: input() }),
            Box::new(ConnectionNumberError::Malformed { input: input() }),
            Box::new(ClientAddressError::Malformed { input: input() }),
            Box::new(ClientHostError::Malformed { input: input() }),
            Box::new(EndpointError::Malformed { input: input() }),
            Box::new(EndpointError::InvalidBroker {
                error: BrokerIdError::Negative { value: -1 },
            }),
            Box::new(ClientSoftwareError::Malformed { input: input() }),
            Box::new(ApiVersionError::Malformed { input: input() }),
            Box::new(ApiVersionError::OutOfRange {
                input: "40000".to_owned(),
            }),
            Box::new(AcksError::Malformed { input: input() }),
            Box::new(AcksError::OutOfRange {
                input: "40000".to_owned(),
            }),
            Box::new(OutcomeError::Unknown { name: input() }),
            Box::new(LatencyError::Malformed { input: input() }),
            Box::new(PartError::InvalidTopic {
                error: TopicNameError::Empty,
            }),
            Box::new(PartError::InvalidPartition {
                error: TopicPartitionError::Malformed { input: input() },
            }),
            Box::new(FieldValueError::Topic {
                error: TopicNameError::Empty,
            }),
            Box::new(PartSet::new([]).unwrap_err()),
        ];

        errors
            .iter()
            .map(|error| {
                assert!(error.source().is_none(), "{error}");
                error.to_string()
            })
            .collect()
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
            "broker ID cannot be negative: -1",
            "client software \"x\" does not have the form a name and a version, such as librdkafka 2.6.1",
            "API version \"x\" does not have the form a whole number",
            "API version 40000 is not between 0 and 32767",
            "acks \"x\" does not have the form a whole number",
            "acks 40000 is not between -32768 and 32767",
            "unknown outcome: \"x\"",
            "latency \"x\" does not have the form a whole number and a unit, such as 14ms",
            "topic name is empty",
            "topic partition \"x\" does not have the form topic/partition",
            "topic name is empty",
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
