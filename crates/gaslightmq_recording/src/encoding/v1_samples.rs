//! The samples of version 1 of the encoding, and the bytes that they archive to.
//!
//! A type cannot change without a version, and these tests are what holds version 1 to that:
//!
//! - [`samples`] builds a value of every stored type of version 1, with every case of every choice at least once,
//!   a text that is empty and one that is not, a list that is empty and one that is not,
//!   and a value that is missing and one that is there.
//!   It names every field and matches every choice in full, so a new field or a new case does not compile until it
//!   has a sample.
//! - `v1.samples`, next to this file, holds the bytes of those samples, and it is committed.
//!
//! A stored type that changes gives other bytes, and so does a release of the library that archives which lays a
//! type out in another way. Either is a new version of the encoding.
//!
//! # The file of a released version is never written again
//!
//! Version 1 is open until a release writes it, so its stored types still change in place.
//! While it is open, this command writes its file anew:
//!
//! ```text
//! cargo test -p gaslightmq_recording --lib -- --ignored --exact encoding::v1_samples::write_the_file_of_the_samples
//! ```
//!
//! With the release that first writes version 1, that test goes away, and nobody writes `v1.samples` again.
//! The module of the next version brings its own samples, its own file and its own command.

use rkyv::rancor;

use super::v1::*;
use super::{ALIGNMENT, Aligned, Framed, Head, Kind, check};

/// The bytes of the samples of version 1, as a build wrote them.
const FILE: &[u8] = include_bytes!("v1.samples");

/// The samples of a choice: every case with a sample of it.
/// A case without an arm does not compile, and an arm does not compile without its sample.
macro_rules! every_case {
    ($type:ty { $($case:pat => $sample:expr),+ $(,)? }) => {{
        let samples: Vec<$type> = vec![$($sample),+];
        for sample in &samples {
            match sample {
                $($case => {}),+
            }
        }
        samples
    }};
}

fn text(text: &str) -> String {
    text.to_owned()
}

fn hosts() -> Vec<Host> {
    every_case!(Host {
        Host::V4(_) => Host::V4([10, 0, 4, 27]),
        Host::V6(_) => Host::V6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x27]),
    })
}

fn topic_partition() -> TopicPartition {
    TopicPartition {
        topic: text("orders.eu"),
        partition: 3,
    }
}

fn parts() -> Vec<Part> {
    every_case!(Part {
        Part::Topic(_) => Part::Topic(text("payments.settled")),
        Part::Partition(_) => Part::Partition(topic_partition()),
    })
}

fn targets() -> Vec<Target> {
    every_case!(Target {
        Target::Call => Target::Call,
        Target::Parts(_) => Target::Parts(parts()),
    })
}

fn outcomes() -> Vec<Outcome> {
    every_case!(Outcome {
        Outcome::Ok => Outcome::Ok,
        Outcome::Error => Outcome::Error,
        Outcome::Timeout => Outcome::Timeout,
    })
}

fn field_values() -> Vec<FieldValue> {
    let [v4, v6] = <[Host; 2]>::try_from(hosts()).unwrap();
    let mut values = every_case!(FieldValue {
        FieldValue::Connection(_) => FieldValue::Connection(7),
        FieldValue::Address { .. } => FieldValue::Address { host: v4.clone(), port: 51_234 },
        FieldValue::Host(_) => FieldValue::Host(v6.clone()),
        FieldValue::Endpoint(_) => FieldValue::Endpoint(Some(2)),
        FieldValue::ClientId(_) => FieldValue::ClientId(text("order-ingest")),
        FieldValue::ClientSoftware { .. } => FieldValue::ClientSoftware {
            name: text("apache-kafka-java"),
            version: text("3.9.0"),
        },
        FieldValue::Api(_) => FieldValue::Api(1),
        FieldValue::ApiVersion(_) => FieldValue::ApiVersion(17),
        FieldValue::Topic(_) => FieldValue::Topic(text("orders.eu")),
        FieldValue::Partition(_) => FieldValue::Partition(3),
        FieldValue::Acks(_) => FieldValue::Acks(-1),
        FieldValue::Group(_) => FieldValue::Group(text("order-processors")),
        FieldValue::MemberId(_) => FieldValue::MemberId(text("consumer-1-4f1c")),
        FieldValue::GroupInstanceId(_) => FieldValue::GroupInstanceId(text("processor-a")),
        FieldValue::TransactionalId(_) => FieldValue::TransactionalId(text("order-writer-1")),
        FieldValue::Outcome(_) => FieldValue::Outcome(Outcome::Ok),
        FieldValue::ErrorCode(_) => FieldValue::ErrorCode(6),
        FieldValue::Latency(_) => FieldValue::Latency(750),
    });
    // The endpoint of the bootstrap listener, a text that is empty, and every outcome.
    values.push(FieldValue::Endpoint(None));
    values.push(FieldValue::ClientId(String::new()));
    values.extend(outcomes().into_iter().map(FieldValue::Outcome));
    values
}

fn part_fields() -> Vec<PartFields> {
    let truths = every_case!(PartTruth {
        PartTruth::OfCall => PartTruth::OfCall,
        PartTruth::Succeeded => PartTruth::Succeeded,
        PartTruth::Failed(_) => PartTruth::Failed(6),
        PartTruth::None => PartTruth::None,
    });
    let parts = parts().into_iter().cycle();
    truths
        .into_iter()
        .zip(parts)
        .map(|(truth, part)| PartFields { part, truth })
        .collect()
}

fn fields() -> Vec<Fields> {
    let learned = vec![FieldValue::Group(text("order-processors"))];
    vec![
        Fields {
            values: field_values(),
            learned,
            parts: part_fields(),
        },
        Fields {
            values: Vec::new(),
            learned: Vec::new(),
            parts: Vec::new(),
        },
    ]
}

fn answers() -> Vec<Answer> {
    every_case!(Answer {
        Answer::Ok => Answer::Ok,
        Answer::Error(_) => Answer::Error(6),
    })
}

fn responses() -> Vec<ResponseSummary> {
    let parts = parts().into_iter().zip(answers());
    vec![
        ResponseSummary {
            answer: Answer::Ok,
            parts: parts.map(|(part, answer)| PartAnswer { part, answer }).collect(),
        },
        ResponseSummary {
            answer: Answer::Error(41),
            parts: Vec::new(),
        },
    ]
}

fn delivered() -> Vec<Delivered> {
    let [answered, refused] = <[ResponseSummary; 2]>::try_from(responses()).unwrap();
    let mut delivered = every_case!(Delivered {
        Delivered::Response(_) => Delivered::Response(answered.clone()),
        Delivered::UnreadResponse => Delivered::UnreadResponse,
        Delivered::Nothing => Delivered::Nothing,
        Delivered::ConnectionClosed => Delivered::ConnectionClosed,
    });
    delivered.push(Delivered::Response(refused));
    delivered
}

fn timings() -> Vec<Timing> {
    vec![
        Timing {
            arrived: 250_000_000,
            forwarded: Some(250_400_000),
            answered: Some(262_000_000),
            delivered: Some(1_012_000_000),
        },
        Timing {
            arrived: 0,
            forwarded: None,
            answered: None,
            delivered: None,
        },
    ]
}

fn stages() -> Vec<Stage> {
    every_case!(Stage {
        Stage::BeforeForwarding => Stage::BeforeForwarding,
        Stage::AfterResponse => Stage::AfterResponse,
    })
}

fn lies() -> Vec<Lie> {
    let [before, after] = <[Stage; 2]>::try_from(stages()).unwrap();
    every_case!(Lie {
        Lie::Refusal { .. } => Lie::Refusal { error: 6, stage: before },
        Lie::Timing { .. } => Lie::Timing {
            delay: 750,
            jitter: 50,
            throttle: 1000,
            stage: after,
        },
        Lie::Silence => Lie::Silence,
        Lie::HangUp { .. } => Lie::HangUp { stage: after },
    })
}

fn targeted_lies() -> Vec<TargetedLie> {
    let targets = targets().into_iter().rev().cycle();
    lies()
        .into_iter()
        .zip(targets)
        .map(|(lie, target)| TargetedLie { lie, target })
        .collect()
}

fn lost_targets() -> Vec<LostTarget> {
    let lost = |(on, to): (Target, &str)| LostTarget { on, to: text(to) };
    targets()
        .into_iter()
        .zip(["leader-flap", "slow-fetch"])
        .map(lost)
        .collect()
}

fn lie_fates() -> Vec<LieFate> {
    let [whole_call, some_parts] = <[Target; 2]>::try_from(targets()).unwrap();
    let mut fates: Vec<LieFate> = targeted_lies()
        .into_iter()
        .map(|lie| LieFate {
            applied: Some(lie.target.clone()),
            lie,
            shadowed: Vec::new(),
            pre_empted: Vec::new(),
            untold: None,
        })
        .collect();
    fates.push(LieFate {
        lie: TargetedLie {
            lie: Lie::Refusal {
                error: 6,
                stage: Stage::BeforeForwarding,
            },
            target: some_parts,
        },
        applied: None,
        shadowed: lost_targets(),
        pre_empted: lost_targets(),
        untold: Some(text("the response of the backend could not be rewritten")),
    });
    fates.push(LieFate {
        lie: TargetedLie {
            lie: Lie::Silence,
            target: whole_call,
        },
        applied: None,
        shadowed: Vec::new(),
        pre_empted: Vec::new(),
        untold: Some(String::new()),
    });
    fates
}

fn triggers() -> Vec<TriggerExplanation> {
    let decisions = every_case!(TriggerDecision {
        TriggerDecision::Fired => TriggerDecision::Fired,
        TriggerDecision::FiredByHand => TriggerDecision::FiredByHand,
        TriggerDecision::Held => TriggerDecision::Held,
        TriggerDecision::BudgetEmpty => TriggerDecision::BudgetEmpty,
        TriggerDecision::NothingToActOn => TriggerDecision::NothingToActOn,
        TriggerDecision::LifetimeOver => TriggerDecision::LifetimeOver,
    });
    let everything = |decision| TriggerExplanation {
        decision,
        match_number: Some(7),
        count_agreed: Some(true),
        die: Some(DieCheck {
            roll: 374_213,
            chance: 400_000,
        }),
        window: Some(WindowCheck {
            opens_after: 10_000,
            closes_after: Some(40_000),
            since_armed: 12_000,
        }),
        budget: Some(BudgetCheck { left: 2, limit: 3 }),
        reason: Some(text("the lifetime of the rule ended")),
    };
    let nothing = |decision| TriggerExplanation {
        decision,
        match_number: None,
        count_agreed: None,
        die: None,
        window: None,
        budget: None,
        reason: None,
    };
    let mut triggers: Vec<TriggerExplanation> = decisions.iter().copied().map(everything).collect();
    triggers.extend(decisions.into_iter().map(nothing));
    triggers.push(TriggerExplanation {
        decision: TriggerDecision::Held,
        match_number: Some(1),
        count_agreed: Some(false),
        die: None,
        window: Some(WindowCheck {
            opens_after: 0,
            closes_after: None,
            since_armed: 0,
        }),
        budget: None,
        reason: Some(String::new()),
    });
    triggers
}

fn coordinators() -> Vec<Coordinator> {
    every_case!(Coordinator {
        Coordinator::Group(_) => Coordinator::Group(text("order-processors")),
        Coordinator::Transaction(_) => Coordinator::Transaction(text("order-writer-1")),
    })
}

fn incidents() -> Vec<Incident> {
    let [group, transaction] = <[Coordinator; 2]>::try_from(coordinators()).unwrap();
    let mut incidents = every_case!(Incident {
        Incident::LeaderMoves { .. } => Incident::LeaderMoves {
            partition: topic_partition(),
            to: Some(2),
        },
        Incident::BrokerGoesDown { .. } => Incident::BrokerGoesDown {
            broker: 1,
            failover: true,
        },
        Incident::BrokerGoesUnresponsive { .. } => Incident::BrokerGoesUnresponsive { broker: 1 },
        Incident::BrokerComesBack { .. } => Incident::BrokerComesBack { broker: 1 },
        Incident::Rebalance { .. } => Incident::Rebalance {
            group: text("order-processors"),
        },
        Incident::CoordinatorMoves { .. } => Incident::CoordinatorMoves {
            coordinator: group.clone(),
            to: Some(3),
        },
        Incident::PartitionAdded { .. } => Incident::PartitionAdded {
            topic: text("orders.eu"),
            count: 2,
        },
        Incident::TopicDeleted { .. } => Incident::TopicDeleted {
            topic: text("orders.eu"),
        },
        Incident::RecordsAppended { .. } => Incident::RecordsAppended {
            partition: topic_partition(),
            count: 500,
        },
        Incident::RecordsExpired { .. } => Incident::RecordsExpired {
            partition: topic_partition(),
            to: 1200,
        },
        Incident::LogTruncated { .. } => Incident::LogTruncated {
            partition: topic_partition(),
            to: 800,
        },
        Incident::CommittedOffsetReset { .. } => Incident::CommittedOffsetReset {
            group: text("order-processors"),
            partition: topic_partition(),
            to: 0,
        },
        Incident::ProducerFenced { .. } => Incident::ProducerFenced {
            transactional_id: text("order-writer-1"),
        },
    });
    // A move to a broker that the cluster picks, and a broker that goes down without a failover.
    incidents.push(Incident::LeaderMoves {
        partition: topic_partition(),
        to: None,
    });
    incidents.push(Incident::CoordinatorMoves {
        coordinator: transaction,
        to: None,
    });
    incidents.push(Incident::BrokerGoesDown {
        broker: 2,
        failover: false,
    });
    incidents
}

fn rule_explanations() -> Vec<RuleExplanation> {
    let [whole_call, some_parts] = <[Target; 2]>::try_from(targets()).unwrap();
    let triggers = triggers();
    let trigger = |index: usize| triggers[index % triggers.len()].clone();
    let mut courses = every_case!(Course {
        Course::EndedBeforeTruth { .. } => Course::EndedBeforeTruth {
            ended_by: text("refuse-produce"),
        },
        Course::Held { .. } => Course::Held {
            matched: whole_call.clone(),
            trigger: trigger(2),
        },
        Course::Lied { .. } => Course::Lied {
            matched: some_parts.clone(),
            trigger: trigger(0),
            lie: lie_fates().remove(0),
        },
        Course::FiredIncidents { .. } => Course::FiredIncidents {
            matched: whole_call.clone(),
            trigger: trigger(1),
            incidents: incidents(),
        },
    });
    // Every fate of a lie, and every trigger.
    let fates = lie_fates().into_iter().enumerate().map(|(index, lie)| Course::Lied {
        matched: some_parts.clone(),
        trigger: trigger(index),
        lie,
    });
    courses.extend(fates);
    let held = triggers.iter().cloned().map(|trigger| Course::Held {
        matched: whole_call.clone(),
        trigger,
    });
    courses.extend(held);
    let line = |(priority, course): (usize, Course)| RuleExplanation {
        rule: format!("rule-{priority}"),
        priority: priority as u64,
        course,
    };
    courses.into_iter().enumerate().map(line).collect()
}

fn explanations() -> Vec<Explanation> {
    vec![
        Explanation {
            rules: rule_explanations(),
            failure: Some(text("the truth of the backend did not decode")),
        },
        Explanation {
            rules: Vec::new(),
            failure: None,
        },
        Explanation {
            rules: Vec::new(),
            failure: Some(String::new()),
        },
    ]
}

fn frame_slots() -> [FrameSlot; 4] {
    let states = every_case!(FrameState {
        FrameState::None => FrameState::None,
        FrameState::Whole => FrameState::Whole,
        FrameState::Redacted => FrameState::Redacted,
        FrameState::Withheld => FrameState::Withheld,
    });
    let [none, whole, redacted, withheld] = <[FrameState; 4]>::try_from(states).unwrap();
    [
        FrameSlot {
            state: whole,
            on_the_wire: 61,
            kept: 61,
        },
        FrameSlot {
            state: none,
            on_the_wire: 0,
            kept: 0,
        },
        FrameSlot {
            state: redacted,
            on_the_wire: 4_194_304,
            kept: 512,
        },
        FrameSlot {
            state: withheld,
            on_the_wire: 73,
            kept: 0,
        },
    ]
}

fn calls() -> Vec<Call> {
    let (fields, explanations, timings, responses) = (fields(), explanations(), timings(), responses());
    let call = |(index, delivered): (usize, Delivered)| Call {
        id: 42 + index as u64,
        connection: 7,
        fields: fields[index % fields.len()].clone(),
        upstream: (index % 2 == 0).then(|| responses[index % responses.len()].clone()),
        delivered,
        explanation: explanations[index % explanations.len()].clone(),
        timing: timings[index % timings.len()],
        frames: frame_slots(),
    };
    delivered().into_iter().enumerate().map(call).collect()
}

fn software() -> ClientSoftware {
    ClientSoftware {
        name: text("apache-kafka-java"),
        version: text("3.9.0"),
    }
}

fn session_events() -> Vec<SessionEvent> {
    let closers = every_case!(Closer {
        Closer::Client => Closer::Client,
        Closer::Backend => Closer::Backend,
        Closer::Lie => Closer::Lie,
        Closer::Proxy => Closer::Proxy,
    });
    let [v4, v6] = <[Host; 2]>::try_from(hosts()).unwrap();
    let mut changes = every_case!(SessionChange {
        SessionChange::Opened { .. } => SessionChange::Opened {
            host: v4.clone(),
            port: 51_234,
            endpoint: Some(2),
            client_id: Some(text("order-ingest")),
            software: Some(software()),
        },
        SessionChange::Identified { .. } => SessionChange::Identified {
            client_id: Some(text("order-ingest")),
        },
        SessionChange::SoftwareNamed { .. } => SessionChange::SoftwareNamed { software: software() },
        SessionChange::Closed { .. } => SessionChange::Closed { by: Closer::Client },
    });
    // A session that opened on the bootstrap endpoint and has learned nothing yet, and a client without an ID.
    changes.push(SessionChange::Opened {
        host: v6,
        port: 9092,
        endpoint: None,
        client_id: None,
        software: None,
    });
    changes.push(SessionChange::Identified { client_id: None });
    changes.extend(closers.into_iter().map(|by| SessionChange::Closed { by }));
    let event = |(index, change): (usize, SessionChange)| SessionEvent {
        connection: 7 + index as u64,
        at: 1_000_000 * index as u64,
        change,
    };
    changes.into_iter().enumerate().map(event).collect()
}

fn definitions() -> Vec<Definition> {
    let kinds = every_case!(DefinitionKind {
        DefinitionKind::Selector => DefinitionKind::Selector,
        DefinitionKind::Preset => DefinitionKind::Preset,
        DefinitionKind::Rule => DefinitionKind::Rule,
        DefinitionKind::Scenario => DefinitionKind::Scenario,
        DefinitionKind::Layout => DefinitionKind::Layout,
        DefinitionKind::Environment => DefinitionKind::Environment,
    });
    let definition = |kind| Definition {
        kind,
        name: text("order-producers"),
    };
    kinds.into_iter().map(definition).collect()
}

fn expanded_incidents() -> Vec<ExpandedIncident> {
    let [group, _] = <[Coordinator; 2]>::try_from(coordinators()).unwrap();
    let consequences = every_case!(Consequence {
        Consequence::Leaderless { .. } => Consequence::Leaderless {
            partition: topic_partition(),
        },
        Consequence::CoordinatorUnavailable { .. } => Consequence::CoordinatorUnavailable {
            coordinator: group.clone(),
        },
        Consequence::Incident(_) => Consequence::Incident(Incident::LeaderMoves {
            partition: topic_partition(),
            to: None,
        }),
    });
    let mut expanded = vec![ExpandedIncident {
        incident: Incident::BrokerGoesDown {
            broker: 1,
            failover: false,
        },
        consequences,
    }];
    let alone = |incident| ExpandedIncident {
        incident,
        consequences: Vec::new(),
    };
    expanded.extend(incidents().into_iter().map(alone));
    expanded
}

fn changes() -> Vec<Change> {
    let parameters = every_case!(Parameter {
        Parameter::Chance => Parameter::Chance,
        Parameter::Every => Parameter::Every,
        Parameter::Delay => Parameter::Delay,
        Parameter::Jitter => Parameter::Jitter,
        Parameter::Throttle => Parameter::Throttle,
        Parameter::Error => Parameter::Error,
        Parameter::Lifetime => Parameter::Lifetime,
    });
    let stop_causes = every_case!(RecordingStopCause {
        RecordingStopCause::FellBehind => RecordingStopCause::FellBehind,
        RecordingStopCause::CouldNotWrite => RecordingStopCause::CouldNotWrite,
    });
    let (definitions, expanded) = (definitions(), expanded_incidents());
    let rule = || text("leader-flap");
    let mut changes = every_case!(Change {
        Change::RuleArmed { .. } => Change::RuleArmed { rule: rule() },
        Change::RuleDisarmed { .. } => Change::RuleDisarmed { rule: rule() },
        Change::ParameterChanged { .. } => Change::ParameterChanged {
            rule: rule(),
            parameter: Parameter::Chance,
            old: text("40%"),
            new: String::new(),
        },
        Change::RuleFiredByHand { .. } => Change::RuleFiredByHand { rule: rule() },
        Change::LiveRuleMade { .. } => Change::LiveRuleMade { rule: rule() },
        Change::LiveRuleChanged { .. } => Change::LiveRuleChanged { rule: rule() },
        Change::LiveRuleKept { .. } => Change::LiveRuleKept { rule: rule() },
        Change::LiveRuleDeleted { .. } => Change::LiveRuleDeleted { rule: rule() },
        Change::DefinitionCreated { .. } => Change::DefinitionCreated {
            definition: definitions[0].clone(),
        },
        Change::DefinitionChanged { .. } => Change::DefinitionChanged {
            definition: definitions[1].clone(),
        },
        Change::DefinitionDeleted { .. } => Change::DefinitionDeleted {
            definition: definitions[2].clone(),
        },
        Change::ScenarioActivated { .. } => Change::ScenarioActivated {
            scenario: text("leader-election"),
            step: Some(text("warm-up")),
        },
        Change::ScenarioDeactivated { .. } => Change::ScenarioDeactivated {
            scenario: text("leader-election"),
            step: None,
        },
        Change::StepAdvanced { .. } => Change::StepAdvanced {
            scenario: String::new(),
            step: Some(String::new()),
        },
        Change::IncidentApplied { .. } => Change::IncidentApplied {
            incident: expanded[0].clone(),
        },
        Change::IncidentEmulated { .. } => Change::IncidentEmulated {
            incident: expanded[1].clone(),
        },
        Change::RecordingStopped { .. } => Change::RecordingStopped {
            why: RecordingStopCause::FellBehind,
        },
        Change::CaptureStarted => Change::CaptureStarted,
        Change::CaptureStopped => Change::CaptureStopped,
    });
    // Every parameter, every kind of definition, every incident and every reason for a recording to stop.
    changes.extend(parameters.into_iter().map(|parameter| Change::ParameterChanged {
        rule: rule(),
        parameter,
        old: text("750ms"),
        new: text("1s"),
    }));
    changes.extend(
        definitions
            .into_iter()
            .map(|definition| Change::DefinitionCreated { definition }),
    );
    changes.extend(
        expanded
            .into_iter()
            .map(|incident| Change::IncidentApplied { incident }),
    );
    changes.extend(stop_causes.into_iter().map(|why| Change::RecordingStopped { why }));
    changes
}

fn journal_entries() -> Vec<JournalEntry> {
    let surfaces = every_case!(ChangeSurface {
        ChangeSurface::Pad => ChangeSurface::Pad,
        ChangeSurface::QuickInject => ChangeSurface::QuickInject,
        ChangeSurface::Ui => ChangeSurface::Ui,
        ChangeSurface::Cli => ChangeSurface::Cli,
        ChangeSurface::Test => ChangeSurface::Test,
        ChangeSurface::Api => ChangeSurface::Api,
        ChangeSurface::Step => ChangeSurface::Step,
        ChangeSurface::Lifetime => ChangeSurface::Lifetime,
        ChangeSurface::File => ChangeSurface::File,
    });
    let causes = every_case!(ChangeCause {
        ChangeCause::Undo => ChangeCause::Undo,
        ChangeCause::Redo => ChangeCause::Redo,
    });
    let causes = [None, Some(causes[0]), Some(causes[1])];
    let entry = |(index, change): (usize, Change)| JournalEntry {
        id: 1 + index as u64,
        at: 30_000_000_000 + index as u64,
        revision: (index % 2 == 0).then_some(7 + index as u64),
        surface: surfaces[index % surfaces.len()],
        cause: causes[index % causes.len()],
        change,
    };
    changes().into_iter().enumerate().map(entry).collect()
}

fn descriptions() -> Vec<Description> {
    let modes = every_case!(RecordingMode {
        RecordingMode::Full => RecordingMode::Full,
        RecordingMode::MetadataOnly => RecordingMode::MetadataOnly,
    });
    let backends = every_case!(Backend {
        Backend::Upstream => Backend::Upstream,
        Backend::Simulated => Backend::Simulated,
    });
    vec![
        Description {
            environment: text("orders-sim"),
            started: 1_795_269_731_527,
            clock: 42_000_000_000,
            mode: modes[0],
            backend: backends[1],
            written_by: text("0.1.0"),
            armed: vec![text("leader-flap"), text("slow-fetch")],
            scenario: Some(ActiveScenario {
                scenario: text("black-friday"),
                step: Some(text("warm-up")),
            }),
            journal_after: Some(118),
        },
        Description {
            environment: text("staging-proxy"),
            started: 0,
            clock: 0,
            mode: modes[1],
            backend: backends[0],
            written_by: String::new(),
            armed: Vec::new(),
            scenario: Some(ActiveScenario {
                scenario: text("chaos"),
                step: None,
            }),
            journal_after: None,
        },
        Description {
            environment: text("staging-proxy"),
            started: 1,
            clock: 1,
            mode: modes[1],
            backend: backends[0],
            written_by: text("0.1.0"),
            armed: Vec::new(),
            scenario: None,
            journal_after: None,
        },
    ]
}

fn summaries() -> Vec<Summary> {
    let fields = every_case!(Field {
        Field::Connection => Field::Connection,
        Field::Address => Field::Address,
        Field::Host => Field::Host,
        Field::Endpoint => Field::Endpoint,
        Field::ClientId => Field::ClientId,
        Field::ClientSoftware => Field::ClientSoftware,
        Field::Api => Field::Api,
        Field::ApiVersion => Field::ApiVersion,
        Field::Topic => Field::Topic,
        Field::Partition => Field::Partition,
        Field::Acks => Field::Acks,
        Field::Group => Field::Group,
        Field::MemberId => Field::MemberId,
        Field::GroupInstanceId => Field::GroupInstanceId,
        Field::TransactionalId => Field::TransactionalId,
        Field::Outcome => Field::Outcome,
        Field::ErrorCode => Field::ErrorCode,
        Field::Latency => Field::Latency,
    });
    let values = every_case!(SummaryValues {
        SummaryValues::Listed(_) => SummaryValues::Listed(field_values()),
        SummaryValues::Any => SummaryValues::Any,
    });
    let mut values = values.into_iter().chain([SummaryValues::Listed(Vec::new())]).cycle();
    let mut of_field = |field| FieldSummary {
        field,
        values: values.next().unwrap(),
    };
    vec![
        Summary {
            instants: Some(Instants {
                from: 250_000_000,
                to: 90_000_000_000,
            }),
            calls: 1180,
            call_ids: Some(Ids {
                lowest: 42,
                highest: 1221,
            }),
            journal: 12,
            journal_ids: Some(Ids {
                lowest: 119,
                highest: 130,
            }),
            sessions: 12,
            fields: fields.into_iter().map(&mut of_field).collect(),
        },
        Summary {
            instants: None,
            calls: 0,
            call_ids: None,
            journal: 0,
            journal_ids: None,
            sessions: 0,
            fields: Vec::new(),
        },
    ]
}

fn ends() -> Vec<End> {
    let endings = every_case!(Ending {
        Ending::Stopped => Ending::Stopped,
        Ending::EnvironmentStopped => Ending::EnvironmentStopped,
        Ending::FellBehind => Ending::FellBehind,
    });
    let end = |ending| End {
        ending,
        at: 90_000_000_000,
        entries: 1204,
        calls: 1180,
    };
    endings.into_iter().map(end).collect()
}

/// A value of every stored type of version 1, in the six kinds of message that a recording holds.
#[derive(Debug, PartialEq)]
struct Samples {
    session_events: Vec<SessionEvent>,
    calls: Vec<Call>,
    journal_entries: Vec<JournalEntry>,
    descriptions: Vec<Description>,
    summaries: Vec<Summary>,
    ends: Vec<End>,
}

fn samples() -> Samples {
    Samples {
        session_events: session_events(),
        calls: calls(),
        journal_entries: journal_entries(),
        descriptions: descriptions(),
        summaries: summaries(),
        ends: ends(),
    }
}

impl Samples {
    /// The samples as their bytes: every sample as a message behind its head, one after the other.
    fn archive(&self) -> Vec<u8> {
        let Samples {
            session_events,
            calls,
            journal_entries,
            descriptions,
            summaries,
            ends,
        } = self;
        let mut messages = Vec::new();
        messages.extend(session_events.iter().map(|event| Framed::archive(Kind::Session, event)));
        messages.extend(calls.iter().map(|call| Framed::archive(Kind::Call, call)));
        messages.extend(
            journal_entries
                .iter()
                .map(|entry| Framed::archive(Kind::Journal, entry)),
        );
        messages.extend(
            descriptions
                .iter()
                .map(|description| Framed::archive(Kind::Description, description)),
        );
        messages.extend(summaries.iter().map(|summary| Framed::archive(Kind::Summary, summary)));
        messages.extend(ends.iter().map(|end| Framed::archive(Kind::End, end)));
        messages.into_iter().flat_map(Framed::into_bytes).collect()
    }

    /// Reads the samples back from their bytes: checks every message, and builds its stored type.
    fn read(bytes: &[u8]) -> Samples {
        // A file lies anywhere in memory, and a message starts at a multiple of 16.
        let mut aligned = Aligned::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let mut samples = Samples {
            session_events: Vec::new(),
            calls: Vec::new(),
            journal_entries: Vec::new(),
            descriptions: Vec::new(),
            summaries: Vec::new(),
            ends: Vec::new(),
        };
        let mut at = 0;
        while at < aligned.len() {
            let head = Head::read(&aligned[at..]).unwrap();
            assert_eq!(at % ALIGNMENT, 0, "a head stands at a multiple of 16");
            let message = &aligned[at + Head::LEN..at + Head::LEN + head.len];
            match head.kind {
                Kind::Session => samples.session_events.push(stored::<ArchivedSessionEvent, _>(message)),
                Kind::Call => samples.calls.push(stored::<ArchivedCall, _>(message)),
                Kind::Journal => samples.journal_entries.push(stored::<ArchivedJournalEntry, _>(message)),
                Kind::Description => samples.descriptions.push(stored::<ArchivedDescription, _>(message)),
                Kind::Summary => samples.summaries.push(stored::<ArchivedSummary, _>(message)),
                Kind::End => samples.ends.push(stored::<ArchivedEnd, _>(message)),
            }
            at += head.stride();
        }
        samples
    }
}

/// The stored type that `message` is the archive of, after the check that every reader runs.
fn stored<A, T>(message: &[u8]) -> T
where
    A: rkyv::Portable
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rancor::Error>>,
{
    check::<A>(message).unwrap();
    let archived = rkyv::access::<A, rancor::Error>(message).unwrap();
    rkyv::deserialize::<T, rancor::Error>(archived).unwrap()
}

#[test]
fn writing_the_samples_of_version_1_gives_the_bytes_of_their_file() {
    let written = samples().archive();

    assert!(
        written == FILE,
        "a stored type of version 1 changed, or the library that archives lays one out in another way: \
         either is a new version of the encoding"
    );
}

#[test]
fn the_file_of_version_1_passes_the_check_and_reads_as_its_samples() {
    assert_eq!(Samples::read(FILE), samples());
}

#[test]
fn the_same_samples_give_the_same_bytes() {
    assert!(samples().archive() == samples().archive());
}

#[test]
#[ignore = "it writes the file of the samples of version 1, which is open until a release writes it"]
fn write_the_file_of_the_samples() {
    let written = samples().archive();
    assert_eq!(Samples::read(&written), samples());

    std::fs::write(concat!(env!("CARGO_MANIFEST_DIR"), "/src/encoding/v1.samples"), written).unwrap();
}
