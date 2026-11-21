//! The `serde` form of every type, and what reading rejects.

#![cfg(feature = "serde")]

use std::fmt::Debug;

use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// `json` reads as the value, though the value writes a shorter form.
fn reads<T: DeserializeOwned + PartialEq + Debug>(json: &str, value: &T) {
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

fn partition(text: &str) -> TopicPartition {
    text.parse().unwrap()
}

fn topic(text: &str) -> TopicName {
    TopicName::new(text).unwrap()
}

fn group(text: &str) -> GroupId {
    GroupId::new(text).unwrap()
}

fn transactional_id(text: &str) -> TransactionalId {
    TransactionalId::new(text).unwrap()
}

#[test]
fn a_kind_writes_its_name() {
    writes(&IncidentKind::LeaderMoves, r#""leader-moves""#);
    writes(&IncidentKind::CommittedOffsetReset, r#""committed-offset-reset""#);
    for kind in IncidentKind::ALL {
        writes(&kind, &format!("{:?}", kind.name()));
    }
    rejects::<IncidentKind>(r#""leader-move""#, "no incident is called \"leader-move\"");
    rejects::<IncidentKind>("3", "expected a string");
}

#[test]
fn the_action_of_the_leader_election_scenario_reads_as_a_definition() {
    // `action: { incident: leader-moves, partition: orders.eu/3 }`
    writes(
        &IncidentDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: None,
        },
        r#"{"incident":"leader-moves","partition":"orders.eu/3"}"#,
    );
}

#[test]
fn the_leader_election_preset_names_no_partition() {
    // `incident: leader-moves`
    writes(
        &IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        },
        r#"{"incident":"leader-moves"}"#,
    );
}

#[test]
fn every_definition_writes_a_map_with_its_parameters() {
    writes(
        &IncidentDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: Some(BrokerId::from(2)),
        },
        r#"{"incident":"leader-moves","partition":"orders.eu/3","to":2}"#,
    );
    writes(
        &IncidentDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: true,
        },
        r#"{"incident":"broker-goes-down","broker":2,"failover":true}"#,
    );
    writes(
        &IncidentDefinition::BrokerGoesUnresponsive {
            broker: Some(BrokerId::from(2)),
        },
        r#"{"incident":"broker-goes-unresponsive","broker":2}"#,
    );
    writes(
        &IncidentDefinition::BrokerComesBack {
            broker: Some(BrokerId::from(2)),
        },
        r#"{"incident":"broker-comes-back","broker":2}"#,
    );
    writes(
        &IncidentDefinition::Rebalance {
            group: Some(group("payments")),
        },
        r#"{"incident":"rebalance","group":"payments"}"#,
    );
    writes(
        &IncidentDefinition::CoordinatorMoves {
            coordinator: Some(group("payments").into()),
            to: Some(BrokerId::from(3)),
        },
        r#"{"incident":"coordinator-moves","group":"payments","to":3}"#,
    );
    writes(
        &IncidentDefinition::CoordinatorMoves {
            coordinator: Some(transactional_id("payments-tx-1").into()),
            to: None,
        },
        r#"{"incident":"coordinator-moves","transactional.id":"payments-tx-1"}"#,
    );
    writes(
        &IncidentDefinition::PartitionAdded {
            topic: Some(topic("orders.eu")),
            count: PartitionCount::new(2).unwrap(),
        },
        r#"{"incident":"partition-added","topic":"orders.eu","count":2}"#,
    );
    writes(
        &IncidentDefinition::TopicDeleted {
            topic: Some(topic("orders.eu")),
        },
        r#"{"incident":"topic-deleted","topic":"orders.eu"}"#,
    );
    writes(
        &IncidentDefinition::RecordsAppended {
            partition: Some(partition("orders.eu/3")),
            count: RecordCount::new(500).unwrap(),
        },
        r#"{"incident":"records-appended","partition":"orders.eu/3","count":500}"#,
    );
    writes(
        &IncidentDefinition::RecordsExpired {
            partition: Some(partition("orders.eu/3")),
            to: Offset::from(5000),
        },
        r#"{"incident":"records-expired","partition":"orders.eu/3","to":5000}"#,
    );
    writes(
        &IncidentDefinition::LogTruncated {
            partition: Some(partition("orders.eu/3")),
            to: Offset::from(40),
        },
        r#"{"incident":"log-truncated","partition":"orders.eu/3","to":40}"#,
    );
    writes(
        &IncidentDefinition::CommittedOffsetReset {
            group: Some(group("payments")),
            partition: Some(partition("orders.eu/3")),
            to: Offset::from(0),
        },
        r#"{"incident":"committed-offset-reset","partition":"orders.eu/3","group":"payments","to":0}"#,
    );
    writes(
        &IncidentDefinition::ProducerFenced {
            transactional_id: Some(transactional_id("payments-tx-1")),
        },
        r#"{"incident":"producer-fenced","transactional.id":"payments-tx-1"}"#,
    );
}

#[test]
fn every_definition_may_leave_its_subject_out() {
    writes(
        &IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: false,
        },
        r#"{"incident":"broker-goes-down"}"#,
    );
    writes(
        &IncidentDefinition::BrokerGoesUnresponsive { broker: None },
        r#"{"incident":"broker-goes-unresponsive"}"#,
    );
    writes(
        &IncidentDefinition::BrokerComesBack { broker: None },
        r#"{"incident":"broker-comes-back"}"#,
    );
    writes(
        &IncidentDefinition::Rebalance { group: None },
        r#"{"incident":"rebalance"}"#,
    );
    writes(
        &IncidentDefinition::CoordinatorMoves {
            coordinator: None,
            to: None,
        },
        r#"{"incident":"coordinator-moves"}"#,
    );
    writes(
        &IncidentDefinition::PartitionAdded {
            topic: None,
            count: PartitionCount::ONE,
        },
        r#"{"incident":"partition-added"}"#,
    );
    writes(
        &IncidentDefinition::TopicDeleted { topic: None },
        r#"{"incident":"topic-deleted"}"#,
    );
    writes(
        &IncidentDefinition::RecordsAppended {
            partition: None,
            count: RecordCount::ONE,
        },
        r#"{"incident":"records-appended"}"#,
    );
    writes(
        &IncidentDefinition::RecordsExpired {
            partition: None,
            to: Offset::from(5000),
        },
        r#"{"incident":"records-expired","to":5000}"#,
    );
    writes(
        &IncidentDefinition::LogTruncated {
            partition: None,
            to: Offset::from(40),
        },
        r#"{"incident":"log-truncated","to":40}"#,
    );
    writes(
        &IncidentDefinition::CommittedOffsetReset {
            group: None,
            partition: None,
            to: Offset::from(0),
        },
        r#"{"incident":"committed-offset-reset","to":0}"#,
    );
    writes(
        &IncidentDefinition::ProducerFenced { transactional_id: None },
        r#"{"incident":"producer-fenced"}"#,
    );
}

#[test]
fn a_definition_reads_the_defaults_it_does_not_write() {
    reads(
        r#"{"incident":"broker-goes-down","broker":2,"failover":false}"#,
        &IncidentDefinition::BrokerGoesDown {
            broker: Some(BrokerId::from(2)),
            failover: false,
        },
    );
    reads(
        r#"{"incident":"partition-added","count":1}"#,
        &IncidentDefinition::PartitionAdded {
            topic: None,
            count: PartitionCount::ONE,
        },
    );
    reads(
        r#"{"incident":"records-appended","count":1}"#,
        &IncidentDefinition::RecordsAppended {
            partition: None,
            count: RecordCount::ONE,
        },
    );
}

#[test]
fn the_keys_of_a_definition_come_in_any_order() {
    reads(
        r#"{"to":2,"partition":"orders.eu/3","incident":"leader-moves"}"#,
        &IncidentDefinition::LeaderMoves {
            partition: Some(partition("orders.eu/3")),
            to: Some(BrokerId::from(2)),
        },
    );
    reads(
        r#"{"to":5000,"incident":"records-expired"}"#,
        &IncidentDefinition::RecordsExpired {
            partition: None,
            to: Offset::from(5000),
        },
    );
}

#[test]
fn a_definition_needs_an_incident_key_with_a_known_name() {
    rejects::<IncidentDefinition>(r#"{"partition":"orders.eu/3"}"#, "missing field `incident`");
    rejects::<IncidentDefinition>(r#"{"incident":"leader-move"}"#, "no incident is called \"leader-move\"");
    rejects::<IncidentDefinition>(r#""leader-moves""#, "expected a map with an `incident` key");
}

#[test]
fn a_definition_rejects_a_key_that_no_incident_has() {
    // Without this, the typo would mean "the partition of the call".
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","partiton":"orders.eu/3"}"#,
        "unknown field `partiton`",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","name":"Leader election"}"#,
        "unknown field `name`",
    );
}

#[test]
fn a_definition_rejects_a_key_of_another_incident() {
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","partition":"orders.eu/3","failover":true}"#,
        "`failover` does not belong to leader-moves",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"broker-goes-down","partition":"orders.eu/3"}"#,
        "`partition` does not belong to broker-goes-down",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"rebalance","to":2}"#,
        "`to` does not belong to rebalance",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"topic-deleted","topic":"orders.eu","count":2}"#,
        "`count` does not belong to topic-deleted",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"rebalance","consequences":[]}"#,
        "`consequences` does not belong to rebalance",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"rebalance","consequence":"leaderless"}"#,
        "`consequence` does not belong to rebalance",
    );
}

#[test]
fn a_definition_rejects_a_key_that_comes_twice() {
    rejects::<IncidentDefinition>(
        r#"{"incident":"rebalance","group":"a","group":"b"}"#,
        "duplicate field `group`",
    );
}

#[test]
fn a_definition_applies_the_rule_of_every_value() {
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","partition":"orders.eu"}"#,
        "topic/partition",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"topic-deleted","topic":"orders/eu"}"#,
        "invalid character",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","to":2147483648}"#,
        "broker ID 2147483648 exceeds the maximum of 2147483647",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"leader-moves","to":99999999999}"#,
        "broker ID 99999999999 exceeds the maximum of 2147483647",
    );
    rejects::<IncidentDefinition>(r#"{"incident":"leader-moves","to":-1}"#, "invalid value");
    rejects::<IncidentDefinition>(
        r#"{"incident":"records-expired","to":9223372036854775808}"#,
        "offset 9223372036854775808 exceeds the maximum of 9223372036854775807",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"partition-added","count":0}"#,
        "partition count 0 is not between 1 and 2147483647",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"partition-added","count":99999999999}"#,
        "partition count 99999999999 is not between 1 and 2147483647",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"records-appended","count":0}"#,
        "record count 0 is not between 1 and 9223372036854775807",
    );
    rejects::<IncidentDefinition>(
        r#"{"incident":"broker-goes-down","failover":"yes"}"#,
        "expected a boolean",
    );
}

#[test]
fn an_offset_incident_needs_its_offset() {
    for kind in ["records-expired", "log-truncated", "committed-offset-reset"] {
        rejects::<IncidentDefinition>(&format!(r#"{{"incident":"{kind}"}}"#), "missing field `to`");
    }
}

#[test]
fn a_coordinator_is_the_one_of_a_group_or_of_a_transactional_id() {
    rejects::<IncidentDefinition>(
        r#"{"incident":"coordinator-moves","group":"payments","transactional.id":"payments-tx-1"}"#,
        "a coordinator is the one of a `group` or of a `transactional.id`, not of both",
    );
}

/// A preset as the `rule` crate may write it: a name next to the keys of the action.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Preset {
    name: String,
    #[serde(flatten)]
    action: Action,
}

/// An action that is a lie or an incident, told apart by its key.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Action {
    Incident(IncidentDefinition),
    Lie { lie: String },
}

#[test]
fn a_definition_sits_in_a_preset_next_to_its_name() {
    // name: Leader election
    // incident: leader-moves
    writes(
        &Preset {
            name: "Leader election".into(),
            action: Action::Incident(IncidentDefinition::LeaderMoves {
                partition: None,
                to: None,
            }),
        },
        r#"{"name":"Leader election","incident":"leader-moves"}"#,
    );
    writes(
        &Preset {
            name: "Fail over broker 2".into(),
            action: Action::Incident(IncidentDefinition::BrokerGoesDown {
                broker: Some(BrokerId::from(2)),
                failover: true,
            }),
        },
        r#"{"name":"Fail over broker 2","incident":"broker-goes-down","broker":2,"failover":true}"#,
    );
    writes(
        &Preset {
            name: "Leader flap".into(),
            action: Action::Lie { lie: "refusal".into() },
        },
        r#"{"name":"Leader flap","lie":"refusal"}"#,
    );
}

#[test]
fn a_definition_in_a_preset_still_rejects_a_misspelt_key() {
    assert!(
        serde_json::from_str::<Preset>(
            r#"{"name":"Leader election","incident":"leader-moves","partiton":"orders.eu/3"}"#
        )
        .is_err()
    );
}

#[test]
fn an_incident_writes_the_map_of_a_definition_that_names_every_subject() {
    writes(
        &Incident::LeaderMoves {
            partition: partition("orders.eu/3"),
            to: None,
        },
        r#"{"incident":"leader-moves","partition":"orders.eu/3"}"#,
    );
    writes(
        &Incident::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: false,
        },
        r#"{"incident":"broker-goes-down","broker":2}"#,
    );
    writes(
        &Incident::CoordinatorMoves {
            coordinator: transactional_id("payments-tx-1").into(),
            to: Some(BrokerId::from(1)),
        },
        r#"{"incident":"coordinator-moves","transactional.id":"payments-tx-1","to":1}"#,
    );
    writes(
        &Incident::PartitionAdded {
            topic: topic("orders.eu"),
            count: PartitionCount::ONE,
        },
        r#"{"incident":"partition-added","topic":"orders.eu"}"#,
    );
    writes(
        &Incident::CommittedOffsetReset {
            group: group("payments"),
            partition: partition("orders.eu/3"),
            to: Offset::from(0),
        },
        r#"{"incident":"committed-offset-reset","partition":"orders.eu/3","group":"payments","to":0}"#,
    );
}

#[test]
fn an_incident_names_its_subject() {
    rejects::<Incident>(
        r#"{"incident":"leader-moves"}"#,
        "leader-moves needs a partition, and none is named",
    );
    rejects::<Incident>(
        r#"{"incident":"committed-offset-reset","group":"payments","to":0}"#,
        "committed-offset-reset needs a partition, and none is named",
    );
    rejects::<Incident>(
        r#"{"incident":"coordinator-moves","to":1}"#,
        "coordinator-moves needs a group or transactional ID, and none is named",
    );
    rejects::<Incident>(
        r#"{"incident":"rebalance","group":"payments","broker":2}"#,
        "`broker` does not belong to rebalance",
    );
}

#[test]
fn a_consequence_writes_its_name_or_the_incident_that_follows() {
    writes(
        &Consequence::Leaderless {
            partition: partition("orders.eu/3"),
        },
        r#"{"consequence":"leaderless","partition":"orders.eu/3"}"#,
    );
    writes(
        &Consequence::CoordinatorUnavailable {
            coordinator: group("payments").into(),
        },
        r#"{"consequence":"coordinator-unavailable","group":"payments"}"#,
    );
    writes(
        &Consequence::CoordinatorUnavailable {
            coordinator: transactional_id("payments-tx-1").into(),
        },
        r#"{"consequence":"coordinator-unavailable","transactional.id":"payments-tx-1"}"#,
    );
    writes(
        &Consequence::Incident(Incident::LeaderMoves {
            partition: partition("orders.eu/3"),
            to: None,
        }),
        r#"{"incident":"leader-moves","partition":"orders.eu/3"}"#,
    );
}

#[test]
fn a_consequence_rejects_what_does_not_belong_to_it() {
    rejects::<Consequence>(r#"{"consequence":"leaderless"}"#, "missing field `partition`");
    rejects::<Consequence>(
        r#"{"consequence":"leaderless","partition":"orders.eu/3","group":"payments"}"#,
        "`group` does not belong to leaderless",
    );
    rejects::<Consequence>(
        r#"{"consequence":"leaderless","partition":"orders.eu/3","incident":"leader-moves"}"#,
        "`incident` does not belong to leaderless",
    );
    rejects::<Consequence>(
        r#"{"consequence":"coordinator-unavailable"}"#,
        "coordinator-unavailable needs a `group` or a `transactional.id`",
    );
    rejects::<Consequence>(
        r#"{"consequence":"gone","partition":"orders.eu/3"}"#,
        "unknown variant `gone`",
    );
    rejects::<Consequence>(r#"{"partition":"orders.eu/3"}"#, "missing field `incident`");
    rejects::<Consequence>(r#"{"incident":"leader-moves"}"#, "leader-moves needs a partition");
}

/// Broker 2 leads `orders.eu/3` and hosts the coordinator of `payments`.
struct Cluster;

impl Topology for Cluster {
    fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
        vec![partition("orders.eu/3")]
    }

    fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
        vec![group("payments").into()]
    }
}

#[test]
fn an_expanded_incident_writes_the_incident_with_its_consequences() {
    writes(
        &Incident::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: false,
        }
        .expand(&Cluster),
        concat!(
            r#"{"incident":"broker-goes-down","broker":2,"consequences":["#,
            r#"{"consequence":"leaderless","partition":"orders.eu/3"},"#,
            r#"{"consequence":"coordinator-unavailable","group":"payments"}]}"#,
        ),
    );
    writes(
        &Incident::BrokerGoesDown {
            broker: BrokerId::from(2),
            failover: true,
        }
        .expand(&Cluster),
        concat!(
            r#"{"incident":"broker-goes-down","broker":2,"failover":true,"consequences":["#,
            r#"{"incident":"leader-moves","partition":"orders.eu/3"},"#,
            r#"{"incident":"coordinator-moves","group":"payments"}]}"#,
        ),
    );
}

#[test]
fn an_expanded_incident_without_consequences_writes_the_incident_alone() {
    let expanded = Incident::BrokerComesBack {
        broker: BrokerId::from(2),
    }
    .expand(&Cluster);
    writes(&expanded.clone(), r#"{"incident":"broker-comes-back","broker":2}"#);
    reads(
        r#"{"incident":"broker-comes-back","broker":2,"consequences":[]}"#,
        &expanded,
    );
    rejects::<ExpandedIncident>(
        r#"{"incident":"broker-comes-back"}"#,
        "broker-comes-back needs a broker",
    );
}

#[test]
fn support_writes_its_name() {
    writes(&Support::Applied, r#""applied""#);
    writes(&Support::Emulated, r#""emulated""#);
    writes(&Support::Unsupported, r#""unsupported""#);
    rejects::<Support>(r#""partly""#, "unknown variant `partly`");
}

#[test]
fn incident_support_writes_every_kind_with_its_support() {
    let backend = IncidentSupport::none()
        .applies([IncidentKind::RecordsAppended])
        .emulates([IncidentKind::BrokerGoesDown, IncidentKind::Rebalance]);
    writes(
        &backend,
        concat!(
            r#"{"leader-moves":"unsupported","broker-goes-down":"emulated","#,
            r#""broker-goes-unresponsive":"unsupported","broker-comes-back":"unsupported","#,
            r#""rebalance":"emulated","coordinator-moves":"unsupported","partition-added":"unsupported","#,
            r#""topic-deleted":"unsupported","records-appended":"applied","records-expired":"unsupported","#,
            r#""log-truncated":"unsupported","committed-offset-reset":"unsupported","#,
            r#""producer-fenced":"unsupported"}"#,
        ),
    );
}

#[test]
fn incident_support_reads_a_kind_that_is_left_out_as_unsupported() {
    reads(
        r#"{"rebalance":"emulated","leader-moves":"applied","log-truncated":"unsupported"}"#,
        &IncidentSupport::none()
            .applies([IncidentKind::LeaderMoves])
            .emulates([IncidentKind::Rebalance]),
    );
    reads("{}", &IncidentSupport::none());
    rejects::<IncidentSupport>(r#"{"leader-move":"applied"}"#, "no incident is called \"leader-move\"");
    rejects::<IncidentSupport>(
        r#"{"rebalance":"emulated","rebalance":"applied"}"#,
        "duplicate kind `rebalance`",
    );
}

#[test]
fn a_count_writes_a_number() {
    writes(&PartitionCount::new(3).unwrap(), "3");
    writes(&RecordCount::new(500).unwrap(), "500");
    rejects::<PartitionCount>("0", "partition count 0 is not between 1 and 2147483647");
    rejects::<RecordCount>("0", "record count 0 is not between 1 and 9223372036854775807");
    rejects::<RecordCount>("-1", "invalid value");
}
