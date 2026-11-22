//! The prelude holds every value object and its error, and its names clash with none of `std`.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std {
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

    use gaslightmq_kernel::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let topic = TopicName::new("orders.eu").unwrap();
        let partition = PartitionNumber::from(3);
        let offset = Offset::from(42);
        let topic_partition = TopicPartition::new(topic.clone(), partition);
        let partition_offset = PartitionOffset::new(partition, offset);
        let position = TopicPartitionOffset::new(topic_partition.clone(), offset);

        vec![
            topic.to_string(),
            TopicId::new([1; 16]).unwrap().to_string(),
            partition.to_string(),
            offset.to_string(),
            topic_partition.to_string(),
            partition_offset.to_string(),
            position.to_string(),
            BrokerId::from(2).to_string(),
            LeaderEpoch::from(7).to_string(),
            ClientId::new("order-ingest").unwrap().to_string(),
            GroupId::new("analytics-etl").unwrap().to_string(),
            MemberId::new("consumer-1-4f2a").unwrap().to_string(),
            GroupInstanceId::new("etl-0").unwrap().to_string(),
            TransactionalId::new("payments-tx-1").unwrap().to_string(),
            ProducerId::from(4000).to_string(),
            ProducerEpoch::from(5).to_string(),
            ApiKey::PRODUCE.to_string(),
            ErrorCode::NOT_LEADER_OR_FOLLOWER.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        vec![
            TopicNameError::Empty.to_string(),
            TopicIdError::Empty.to_string(),
            PartitionNumberError::Empty.to_string(),
            OffsetError::Empty.to_string(),
            TopicPartitionError::Empty.to_string(),
            PartitionOffsetError::Empty.to_string(),
            TopicPartitionOffsetError::Empty.to_string(),
            BrokerIdError::Empty.to_string(),
            LeaderEpochError::Empty.to_string(),
            ClientIdError::TooLong { len: 2, max: 1 }.to_string(),
            GroupIdError::Empty.to_string(),
            MemberIdError::Empty.to_string(),
            GroupInstanceIdError::Empty.to_string(),
            TransactionalIdError::Empty.to_string(),
            ProducerIdError::Empty.to_string(),
            ProducerEpochError::Empty.to_string(),
            ApiKeyError::Empty.to_string(),
            ErrorCodeError::Empty.to_string(),
        ]
    }
}

/// Imports the prelude alone, as a consumer does, and uses the standard prelude as usual.
mod alone {
    use gaslightmq_kernel::prelude::*;

    /// The names of the standard prelude still mean what they mean everywhere.
    pub fn std_names_keep_their_meaning() -> Result<Option<String>, TopicNameError> {
        let topic: TopicName = TryFrom::try_from("orders.eu")?;
        let position = topic.partition(From::from(3_u16)).at(Offset::ZERO);
        let boxed: Box<TopicPartitionOffset> = Box::new(position.clone());
        let text: Vec<String> = vec![ToString::to_string(&*boxed), String::from(position)];
        Ok(text.into_iter().next())
    }
}

#[test]
fn the_prelude_holds_every_value_object() {
    assert_eq!(
        next_to_std::every_item(),
        [
            "orders.eu",
            "AQEBAQEBAQEBAQEBAQEBAQ",
            "3",
            "42",
            "orders.eu/3",
            "3@42",
            "orders.eu/3@42",
            "2",
            "7",
            "order-ingest",
            "analytics-etl",
            "consumer-1-4f2a",
            "etl-0",
            "payments-tx-1",
            "4000",
            "5",
            "Produce",
            "NOT_LEADER_OR_FOLLOWER",
        ]
    );
}

#[test]
fn the_prelude_holds_the_error_of_every_value_object() {
    assert_eq!(
        next_to_std::every_error(),
        [
            "topic name is empty",
            "topic ID is empty",
            "partition number is empty",
            "offset is empty",
            "topic partition is empty",
            "partition offset is empty",
            "topic partition offset is empty",
            "broker ID is empty",
            "leader epoch is empty",
            "client ID of 2 bytes exceeds the limit of 1",
            "group ID is empty",
            "member ID is empty",
            "group instance ID is empty",
            "transactional ID is empty",
            "producer ID is empty",
            "producer epoch is empty",
            "API key is empty",
            "error code is empty",
        ]
    );
}

#[test]
fn a_glob_import_of_the_prelude_leaves_the_names_of_std_alone() {
    assert_eq!(
        alone::std_names_keep_their_meaning(),
        Ok(Some("orders.eu/3@0".to_owned()))
    );
}

#[test]
fn the_prelude_and_the_crate_root_export_the_same_types() {
    let from_root: gaslightmq_kernel::TopicPartition = "orders.eu/3".parse().unwrap();
    let from_prelude: gaslightmq_kernel::prelude::TopicPartition = from_root.clone();

    assert_eq!(from_root, from_prelude);
}
