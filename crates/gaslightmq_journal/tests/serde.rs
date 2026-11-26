//! The `serde` form of every type, and what reading rejects.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::fmt::Debug;

use gaslightmq_clock::{Duration, Instant};
use gaslightmq_incident::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

fn name(text: &str) -> RecordedName {
    text.parse().unwrap()
}

/// A cluster in which broker 2 leads one partition, and hosts no coordinator.
struct Cluster;

impl Topology for Cluster {
    fn partitions_led_by(&self, _broker: BrokerId) -> Vec<TopicPartition> {
        vec!["orders.eu/3".parse().unwrap()]
    }

    fn coordinators_hosted_by(&self, _broker: BrokerId) -> Vec<Coordinator> {
        vec![]
    }
}

/// Broker 2 goes down without a failover, which leaves its partition without a leader.
fn broker_2_went_down() -> ExpandedIncident {
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::from(2),
        failover: false,
    };
    down.expand(&Cluster)
}

const BROKER_2_WENT_DOWN: &str = r#"{"incident":"broker-goes-down","broker":2,"consequences":[{"consequence":"leaderless","partition":"orders.eu/3"}]}"#;

#[test]
fn an_entry_id_and_a_revision_are_numbers() {
    writes(&JournalEntryId::FIRST.next().next(), "3");
    rejects::<JournalEntryId>("0", "a journal entry ID is at least one");
    rejects::<JournalEntryId>(r#""3""#, "invalid type");

    writes(&Revision::new(7), "7");
    rejects::<Revision>(r#""7""#, "invalid type");
    rejects::<Revision>("-1", "invalid value");
}

#[test]
fn a_recorded_name_is_its_text_and_a_definition_is_its_kind_and_its_name() {
    writes(&name("slow-order-producers"), r#""slow-order-producers""#);
    rejects::<RecordedName>(r#""""#, "recorded name \"\" is empty");

    writes(
        &RecordedDefinition::Selector(name("order-producers")),
        r#"{"selector":"order-producers"}"#,
    );
    writes(
        &RecordedDefinition::Preset(name("delay-1s")),
        r#"{"preset":"delay-1s"}"#,
    );
    writes(
        &RecordedDefinition::Rule(name("slow-order-producers")),
        r#"{"rule":"slow-order-producers"}"#,
    );
    writes(
        &RecordedDefinition::Layout(name("order-desk")),
        r#"{"layout":"order-desk"}"#,
    );
    writes(
        &RecordedDefinition::Environment(name("orders-sim")),
        r#"{"environment":"orders-sim"}"#,
    );
    rejects::<RecordedDefinition>(r#"{"scenario":"black-friday"}"#, "unknown variant");
}

#[test]
fn a_surface_a_cause_and_a_parameter_are_their_names() {
    let surfaces = [
        (ChangeSurface::Pad, r#""pad""#),
        (ChangeSurface::QuickInject, r#""quick-inject""#),
        (ChangeSurface::Ui, r#""ui""#),
        (ChangeSurface::Cli, r#""cli""#),
        (ChangeSurface::Test, r#""test""#),
        (ChangeSurface::Api, r#""api""#),
        (ChangeSurface::Step, r#""step""#),
        (ChangeSurface::Lifetime, r#""lifetime""#),
        (ChangeSurface::File, r#""file""#),
    ];
    for (surface, json) in surfaces {
        writes(&surface, json);
        assert_eq!(json, format!("{:?}", surface.name()));
    }
    rejects::<ChangeSurface>(r#""operator""#, "unknown variant");

    writes(&ChangeCause::Undo, r#""undo""#);
    writes(&ChangeCause::Redo, r#""redo""#);

    for parameter in [
        Parameter::Chance,
        Parameter::Every,
        Parameter::Delay,
        Parameter::Jitter,
        Parameter::Throttle,
        Parameter::Error,
        Parameter::Lifetime,
    ] {
        writes(&parameter, &format!("{:?}", parameter.name()));
    }
}

#[test]
fn a_source_leaves_out_the_cause_it_does_not_have() {
    writes(&ChangeSource::new(ChangeSurface::Pad), r#"{"surface":"pad"}"#);
    writes(
        &ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo),
        r#"{"surface":"ui","cause":"undo"}"#,
    );
    rejects::<ChangeSource>(r#"{"cause":"undo"}"#, "missing field `surface`");
    rejects::<ChangeSource>(r#"{"surface":"ui","operator":"ana"}"#, "unknown field `operator`");
}

#[test]
fn a_change_names_itself_in_its_change_key() {
    let rule = || name("slow-order-producers");
    let definition = || RecordedDefinition::Preset(name("delay-1s"));

    writes(
        &Change::RuleArmed { rule: rule() },
        r#"{"change":"rule-armed","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::RuleDisarmed { rule: rule() },
        r#"{"change":"rule-disarmed","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::ParameterChanged {
            rule: rule(),
            parameter: Parameter::Delay,
            old: "1s".to_owned(),
            new: "1500ms".to_owned(),
        },
        r#"{"change":"parameter-changed","rule":"slow-order-producers","parameter":"delay","old":"1s","new":"1500ms"}"#,
    );
    writes(
        &Change::RuleFiredByHand { rule: rule() },
        r#"{"change":"rule-fired-by-hand","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::LiveRuleMade { rule: rule() },
        r#"{"change":"live-rule-made","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::LiveRuleChanged { rule: rule() },
        r#"{"change":"live-rule-changed","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::LiveRuleKept { rule: rule() },
        r#"{"change":"live-rule-kept","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::LiveRuleDeleted { rule: rule() },
        r#"{"change":"live-rule-deleted","rule":"slow-order-producers"}"#,
    );
    writes(
        &Change::DefinitionCreated {
            definition: definition(),
        },
        r#"{"change":"definition-created","definition":{"preset":"delay-1s"}}"#,
    );
    writes(
        &Change::DefinitionChanged {
            definition: definition(),
        },
        r#"{"change":"definition-changed","definition":{"preset":"delay-1s"}}"#,
    );
    writes(
        &Change::DefinitionDeleted {
            definition: definition(),
        },
        r#"{"change":"definition-deleted","definition":{"preset":"delay-1s"}}"#,
    );
    writes(
        &Change::IncidentApplied {
            incident: broker_2_went_down(),
        },
        &format!(r#"{{"change":"incident-applied","incident":{BROKER_2_WENT_DOWN}}}"#),
    );
    writes(
        &Change::IncidentEmulated {
            incident: broker_2_went_down(),
        },
        &format!(r#"{{"change":"incident-emulated","incident":{BROKER_2_WENT_DOWN}}}"#),
    );
}

#[test]
fn reading_a_change_rejects_what_it_does_not_know() {
    rejects::<Change>(
        r#"{"change":"step-advanced","scenario":"black-friday"}"#,
        "unknown variant",
    );
    rejects::<Change>(
        r#"{"change":"rule-armed","rule":"slow-order-producers","by":"ana"}"#,
        "unknown field `by`",
    );
    rejects::<Change>(r#"{"change":"rule-armed","rule":"Slow producers"}"#, "recorded name");
    rejects::<Change>(r#"{"rule":"slow-order-producers"}"#, "missing field `change`");
}

#[test]
fn an_entry_is_its_change_with_its_other_parts_next_to_it() {
    let armed = JournalEntry {
        id: JournalEntryId::new(7).unwrap(),
        at: Instant::ZERO + Duration::from_millis(4200),
        revision: Some(Revision::new(12)),
        from: ChangeSource::new(ChangeSurface::Pad).with_cause(ChangeCause::Redo),
        change: Change::RuleArmed {
            rule: name("slow-order-producers"),
        },
    };

    writes(
        &armed,
        r#"{"id":7,"at":4200000000,"revision":12,"from":{"surface":"pad","cause":"redo"},"change":"rule-armed","rule":"slow-order-producers"}"#,
    );
}

#[test]
fn an_entry_of_what_left_no_revision_leaves_the_key_out() {
    let fired = JournalEntry {
        id: JournalEntryId::FIRST,
        at: Instant::ZERO,
        revision: None,
        from: ChangeSource::new(ChangeSurface::Cli),
        change: Change::RuleFiredByHand {
            rule: name("slow-order-producers"),
        },
    };

    writes(
        &fired,
        r#"{"id":1,"at":0,"from":{"surface":"cli"},"change":"rule-fired-by-hand","rule":"slow-order-producers"}"#,
    );
    rejects::<JournalEntry>(
        r#"{"id":0,"at":0,"from":{"surface":"cli"},"change":"rule-fired-by-hand","rule":"slow-order-producers"}"#,
        "a journal entry ID is at least one",
    );
    rejects::<JournalEntry>(
        r#"{"id":1,"at":0,"change":"rule-fired-by-hand","rule":"slow-order-producers"}"#,
        "missing field `from`",
    );
}
