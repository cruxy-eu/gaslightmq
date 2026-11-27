//! The way from the types of a call, a journal and a recording to the stored types of the current version.
//!
//! Every function takes a type apart field by field and case by case, so a new field or a new case of a type
//! does not compile here until the stored type has a place for it.
//! The one exception is a lie: other crates bring new lies, so a lie without a stored type is an
//! [error](CallEntryError), and no silent loss.

use std::net::IpAddr;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::{Consequence, Coordinator, ExpandedIncident, Incident};
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::{LieDefinition, Stage, TargetedLie};

use super::v1 as stored;
use crate::{
    ActiveScenario, CallEntryError, Closer, Description, End, Ending, FrameState, RecordedBackend, SessionChange,
    SessionEvent, Summary, SummaryValues,
};

fn instant(instant: Instant) -> u64 {
    instant.as_nanos()
}

fn millis(span: TimeSpan) -> u64 {
    span.as_millis()
}

fn host(ip: IpAddr) -> stored::Host {
    match ip {
        IpAddr::V4(ip) => stored::Host::V4(ip.octets()),
        IpAddr::V6(ip) => stored::Host::V6(ip.octets()),
    }
}

fn topic_partition(partition: &TopicPartition) -> stored::TopicPartition {
    stored::TopicPartition {
        topic: partition.topic().as_str().to_owned(),
        partition: partition.partition().value(),
    }
}

fn part(part: &Part) -> stored::Part {
    match part {
        Part::Topic(topic) => stored::Part::Topic(topic.as_str().to_owned()),
        Part::Partition(partition) => stored::Part::Partition(topic_partition(partition)),
    }
}

fn target(target: &Target) -> stored::Target {
    match target {
        Target::Call => stored::Target::Call,
        Target::Parts(parts) => stored::Target::Parts(parts.iter().map(part).collect()),
    }
}

fn outcome(outcome: Outcome) -> stored::Outcome {
    match outcome {
        Outcome::Ok => stored::Outcome::Ok,
        Outcome::Error => stored::Outcome::Error,
        Outcome::Timeout => stored::Outcome::Timeout,
    }
}

fn software(software: &ClientSoftware) -> stored::ClientSoftware {
    stored::ClientSoftware {
        name: software.name().to_owned(),
        version: software.version().to_owned(),
    }
}

pub(crate) fn field_value(value: &FieldValue) -> stored::FieldValue {
    match value {
        FieldValue::Connection(connection) => stored::FieldValue::Connection(connection.value()),
        FieldValue::Address(address) => stored::FieldValue::Address {
            host: host(address.host().ip()),
            port: address.port(),
        },
        FieldValue::Host(client) => stored::FieldValue::Host(host(client.ip())),
        FieldValue::Endpoint(endpoint) => stored::FieldValue::Endpoint(endpoint.broker().map(BrokerId::value)),
        FieldValue::ClientId(id) => stored::FieldValue::ClientId(id.as_str().to_owned()),
        FieldValue::ClientSoftware(software) => stored::FieldValue::ClientSoftware {
            name: software.name().to_owned(),
            version: software.version().to_owned(),
        },
        FieldValue::Api(api) => stored::FieldValue::Api(api.value()),
        FieldValue::ApiVersion(version) => stored::FieldValue::ApiVersion(version.value()),
        FieldValue::Topic(topic) => stored::FieldValue::Topic(topic.as_str().to_owned()),
        FieldValue::Partition(partition) => stored::FieldValue::Partition(partition.value()),
        FieldValue::Acks(acks) => stored::FieldValue::Acks(acks.value()),
        FieldValue::Group(group) => stored::FieldValue::Group(group.as_str().to_owned()),
        FieldValue::MemberId(member) => stored::FieldValue::MemberId(member.as_str().to_owned()),
        FieldValue::GroupInstanceId(instance) => stored::FieldValue::GroupInstanceId(instance.as_str().to_owned()),
        FieldValue::TransactionalId(id) => stored::FieldValue::TransactionalId(id.as_str().to_owned()),
        FieldValue::Outcome(value) => stored::FieldValue::Outcome(outcome(*value)),
        FieldValue::ErrorCode(code) => stored::FieldValue::ErrorCode(code.value()),
        FieldValue::Latency(latency) => stored::FieldValue::Latency(millis(latency.span())),
    }
}

pub(crate) fn field(field: Field) -> stored::Field {
    match field {
        Field::Connection => stored::Field::Connection,
        Field::Address => stored::Field::Address,
        Field::Host => stored::Field::Host,
        Field::Endpoint => stored::Field::Endpoint,
        Field::ClientId => stored::Field::ClientId,
        Field::ClientSoftware => stored::Field::ClientSoftware,
        Field::Api => stored::Field::Api,
        Field::ApiVersion => stored::Field::ApiVersion,
        Field::Topic => stored::Field::Topic,
        Field::Partition => stored::Field::Partition,
        Field::Acks => stored::Field::Acks,
        Field::Group => stored::Field::Group,
        Field::MemberId => stored::Field::MemberId,
        Field::GroupInstanceId => stored::Field::GroupInstanceId,
        Field::TransactionalId => stored::Field::TransactionalId,
        Field::Outcome => stored::Field::Outcome,
        Field::ErrorCode => stored::Field::ErrorCode,
        Field::Latency => stored::Field::Latency,
    }
}

/// The truth of a part, as its own readings say it.
fn part_truth(part: &PartFields) -> stored::PartTruth {
    match (part.own(Field::Outcome), part.own(Field::ErrorCode)) {
        (_, Some(Reading::Present(FieldValue::ErrorCode(code)))) => stored::PartTruth::Failed(code.value()),
        (Some(Reading::Present(_)), _) => stored::PartTruth::Succeeded,
        (Some(Reading::Absent), _) => stored::PartTruth::None,
        (Some(Reading::Pending) | None, _) => stored::PartTruth::OfCall,
    }
}

fn fields(fields: &Fields) -> stored::Fields {
    let mut stored = stored::Fields {
        values: Vec::new(),
        learned: Vec::new(),
        parts: fields
            .parts()
            .iter()
            .map(|own| stored::PartFields {
                part: part(own.part()),
                truth: part_truth(own),
            })
            .collect(),
    };
    for field in Field::ALL {
        if let Some(values) = fields.whole_call(field).present() {
            let list = match values.origin() {
                Origin::Call => &mut stored.values,
                Origin::ClientContext => &mut stored.learned,
            };
            list.extend(values.iter().map(field_value));
        }
    }
    stored
}

fn answer(answer: Answer) -> stored::Answer {
    match answer {
        Answer::Ok => stored::Answer::Ok,
        Answer::Error(code) => stored::Answer::Error(code.value()),
    }
}

fn response(response: &ResponseSummary) -> stored::ResponseSummary {
    stored::ResponseSummary {
        answer: answer(response.answer()),
        parts: response
            .parts()
            .map(|(of, its)| stored::PartAnswer {
                part: part(of),
                answer: answer(its),
            })
            .collect(),
    }
}

fn delivered(delivered: &Delivered) -> stored::Delivered {
    match delivered {
        Delivered::Response(summary) => stored::Delivered::Response(response(summary)),
        Delivered::UnreadResponse => stored::Delivered::UnreadResponse,
        Delivered::Nothing => stored::Delivered::Nothing,
        Delivered::ConnectionClosed => stored::Delivered::ConnectionClosed,
    }
}

fn timing(timing: CallTiming) -> stored::Timing {
    stored::Timing {
        arrived: instant(timing.arrived_at()),
        forwarded: timing.forwarded_at().map(instant),
        answered: timing.answered_at().map(instant),
        delivered: timing.delivered_at().map(instant),
    }
}

fn stage(stage: Stage) -> stored::Stage {
    match stage {
        Stage::BeforeForwarding => stored::Stage::BeforeForwarding,
        Stage::AfterResponse => stored::Stage::AfterResponse,
    }
}

fn lie(definition: &LieDefinition) -> Result<stored::Lie, CallEntryError> {
    Ok(match definition {
        LieDefinition::Refusal(refusal) => stored::Lie::Refusal {
            error: refusal.error.value(),
            stage: stage(refusal.stage),
        },
        LieDefinition::Timing(timing) => stored::Lie::Timing {
            delay: millis(timing.delay()),
            jitter: millis(timing.jitter()),
            throttle: millis(timing.throttle()),
            stage: stage(timing.stage()),
        },
        LieDefinition::Silence => stored::Lie::Silence,
        LieDefinition::HangUp(hang_up) => stored::Lie::HangUp {
            stage: stage(hang_up.stage),
        },
        // A lie that another crate brought, and that this version of the encoding has no stored type for.
        other => {
            return Err(CallEntryError::LieNotStored {
                lie: other.kind().name().to_owned(),
            });
        }
    })
}

fn targeted_lie(targeted: &TargetedLie) -> Result<stored::TargetedLie, CallEntryError> {
    Ok(stored::TargetedLie {
        lie: lie(targeted.lie())?,
        target: target(targeted.target()),
    })
}

fn lost(loss: &LostTarget) -> stored::LostTarget {
    stored::LostTarget {
        on: target(loss.on()),
        to: loss.to().as_str().to_owned(),
    }
}

fn lie_fate(fate: &LieFate) -> Result<stored::LieFate, CallEntryError> {
    Ok(stored::LieFate {
        lie: targeted_lie(fate.lie())?,
        applied: fate.applied().map(target),
        shadowed: fate.shadowed().iter().map(lost).collect(),
        pre_empted: fate.pre_empted().iter().map(lost).collect(),
        untold: fate.failure().map(|failure| failure.message().to_owned()),
    })
}

fn decision(decision: TriggerDecision) -> stored::TriggerDecision {
    match decision {
        TriggerDecision::Fired => stored::TriggerDecision::Fired,
        TriggerDecision::FiredByHand => stored::TriggerDecision::FiredByHand,
        TriggerDecision::Held => stored::TriggerDecision::Held,
        TriggerDecision::BudgetEmpty => stored::TriggerDecision::BudgetEmpty,
        TriggerDecision::NothingToActOn => stored::TriggerDecision::NothingToActOn,
        TriggerDecision::LifetimeOver => stored::TriggerDecision::LifetimeOver,
    }
}

fn trigger(trigger: &TriggerExplanation) -> stored::TriggerExplanation {
    stored::TriggerExplanation {
        decision: decision(trigger.decision()),
        match_number: trigger.match_number().map(std::num::NonZeroU64::get),
        count_agreed: trigger.checks().count_agreed(),
        die: trigger.checks().die().map(|die| stored::DieCheck {
            roll: die.roll_ppm(),
            chance: die.chance_ppm(),
        }),
        window: trigger.checks().window().map(|window| stored::WindowCheck {
            opens_after: millis(window.opens_after()),
            closes_after: window.closes_after().map(millis),
            since_armed: millis(window.since_armed()),
        }),
        budget: trigger.budget().map(|budget| stored::BudgetCheck {
            left: budget.left(),
            limit: budget.limit().get(),
        }),
        reason: trigger.reason().map(str::to_owned),
    }
}

fn coordinator(coordinator: &Coordinator) -> stored::Coordinator {
    match coordinator {
        Coordinator::Group(group) => stored::Coordinator::Group(group.as_str().to_owned()),
        Coordinator::Transaction(id) => stored::Coordinator::Transaction(id.as_str().to_owned()),
    }
}

fn incident(incident: &Incident) -> stored::Incident {
    match incident {
        Incident::LeaderMoves { partition, to } => stored::Incident::LeaderMoves {
            partition: topic_partition(partition),
            to: to.map(BrokerId::value),
        },
        Incident::BrokerGoesDown { broker, failover } => stored::Incident::BrokerGoesDown {
            broker: broker.value(),
            failover: *failover,
        },
        Incident::BrokerGoesUnresponsive { broker } => {
            stored::Incident::BrokerGoesUnresponsive { broker: broker.value() }
        }
        Incident::BrokerComesBack { broker } => stored::Incident::BrokerComesBack { broker: broker.value() },
        Incident::Rebalance { group } => stored::Incident::Rebalance {
            group: group.as_str().to_owned(),
        },
        Incident::CoordinatorMoves { coordinator: moved, to } => stored::Incident::CoordinatorMoves {
            coordinator: coordinator(moved),
            to: to.map(BrokerId::value),
        },
        Incident::PartitionAdded { topic, count } => stored::Incident::PartitionAdded {
            topic: topic.as_str().to_owned(),
            count: count.value(),
        },
        Incident::TopicDeleted { topic } => stored::Incident::TopicDeleted {
            topic: topic.as_str().to_owned(),
        },
        Incident::RecordsAppended { partition, count } => stored::Incident::RecordsAppended {
            partition: topic_partition(partition),
            count: count.value(),
        },
        Incident::RecordsExpired { partition, to } => stored::Incident::RecordsExpired {
            partition: topic_partition(partition),
            to: to.value(),
        },
        Incident::LogTruncated { partition, to } => stored::Incident::LogTruncated {
            partition: topic_partition(partition),
            to: to.value(),
        },
        Incident::CommittedOffsetReset { group, partition, to } => stored::Incident::CommittedOffsetReset {
            group: group.as_str().to_owned(),
            partition: topic_partition(partition),
            to: to.value(),
        },
        Incident::ProducerFenced { transactional_id } => stored::Incident::ProducerFenced {
            transactional_id: transactional_id.as_str().to_owned(),
        },
    }
}

/// How far a rule got, from what its line says: a line names the rule that ended the call, or it holds what
/// its selector matched and what its trigger decided, with the lie or the incidents that it fired.
fn course(line: &RuleExplanation) -> Result<stored::Course, CallEntryError> {
    if let Some(ended_by) = line.ended_by() {
        return Ok(stored::Course::EndedBeforeTruth {
            ended_by: ended_by.as_str().to_owned(),
        });
    }
    let matched = line
        .matched()
        .expect("a line of a rule that the call reached holds what its selector matched");
    let decided = line
        .trigger()
        .expect("a line of a rule that the call reached holds what its trigger decided");
    let (matched, trigger) = (target(matched), trigger(decided));
    Ok(match (line.lie(), line.incidents()) {
        (Some(fate), _) => stored::Course::Lied {
            matched,
            trigger,
            lie: lie_fate(fate)?,
        },
        (None, []) => stored::Course::Held { matched, trigger },
        (None, incidents) => stored::Course::FiredIncidents {
            matched,
            trigger,
            incidents: incidents.iter().map(incident).collect(),
        },
    })
}

fn explanation(explanation: &Explanation) -> Result<stored::Explanation, CallEntryError> {
    let rules = explanation.rules().iter().map(|line| {
        Ok(stored::RuleExplanation {
            rule: line.rule().as_str().to_owned(),
            priority: line.priority().position() as u64,
            course: course(line)?,
        })
    });
    Ok(stored::Explanation {
        rules: rules.collect::<Result<_, CallEntryError>>()?,
        failure: explanation.failure().map(|failure| failure.message().to_owned()),
    })
}

pub(crate) fn call(call: &Call, frames: [stored::FrameSlot; 4]) -> Result<stored::Call, CallEntryError> {
    Ok(stored::Call {
        id: call.id().value(),
        connection: call.connection().value(),
        fields: fields(call.fields()),
        upstream: call.upstream().map(response),
        delivered: delivered(call.delivered()),
        explanation: explanation(call.explanation())?,
        timing: timing(call.timing()),
        frames,
    })
}

pub(crate) fn frame_state(state: FrameState) -> stored::FrameState {
    match state {
        FrameState::None => stored::FrameState::None,
        FrameState::Whole => stored::FrameState::Whole,
        FrameState::Redacted => stored::FrameState::Redacted,
        FrameState::Withheld => stored::FrameState::Withheld,
    }
}

fn closer(by: Closer) -> stored::Closer {
    match by {
        Closer::Client => stored::Closer::Client,
        Closer::Backend => stored::Closer::Backend,
        Closer::Lie => stored::Closer::Lie,
        Closer::Proxy => stored::Closer::Proxy,
    }
}

pub(crate) fn session_event(event: &SessionEvent) -> stored::SessionEvent {
    let SessionEvent { connection, at, change } = event;
    let change = match change {
        SessionChange::Opened {
            address,
            endpoint,
            client_id,
            software: runs,
        } => stored::SessionChange::Opened {
            host: host(address.host().ip()),
            port: address.port(),
            endpoint: endpoint.broker().map(BrokerId::value),
            client_id: client_id.as_ref().map(|id| id.as_str().to_owned()),
            software: runs.as_ref().map(software),
        },
        SessionChange::Identified { client_id } => stored::SessionChange::Identified {
            client_id: client_id.as_ref().map(|id| id.as_str().to_owned()),
        },
        SessionChange::SoftwareNamed { software: runs } => stored::SessionChange::SoftwareNamed {
            software: software(runs),
        },
        SessionChange::Closed { by } => stored::SessionChange::Closed { by: closer(*by) },
    };
    stored::SessionEvent {
        connection: connection.value(),
        at: instant(*at),
        change,
    }
}

fn surface(surface: ChangeSurface) -> stored::ChangeSurface {
    match surface {
        ChangeSurface::Pad => stored::ChangeSurface::Pad,
        ChangeSurface::QuickInject => stored::ChangeSurface::QuickInject,
        ChangeSurface::Ui => stored::ChangeSurface::Ui,
        ChangeSurface::Cli => stored::ChangeSurface::Cli,
        ChangeSurface::Test => stored::ChangeSurface::Test,
        ChangeSurface::Api => stored::ChangeSurface::Api,
        ChangeSurface::Step => stored::ChangeSurface::Step,
        ChangeSurface::Lifetime => stored::ChangeSurface::Lifetime,
        ChangeSurface::File => stored::ChangeSurface::File,
    }
}

fn cause(cause: ChangeCause) -> stored::ChangeCause {
    match cause {
        ChangeCause::Undo => stored::ChangeCause::Undo,
        ChangeCause::Redo => stored::ChangeCause::Redo,
    }
}

fn parameter(parameter: Parameter) -> stored::Parameter {
    match parameter {
        Parameter::Chance => stored::Parameter::Chance,
        Parameter::Every => stored::Parameter::Every,
        Parameter::Delay => stored::Parameter::Delay,
        Parameter::Jitter => stored::Parameter::Jitter,
        Parameter::Throttle => stored::Parameter::Throttle,
        Parameter::Error => stored::Parameter::Error,
        Parameter::Lifetime => stored::Parameter::Lifetime,
    }
}

fn definition(definition: &RecordedDefinition) -> stored::Definition {
    let (kind, name) = match definition {
        RecordedDefinition::Selector(name) => (stored::DefinitionKind::Selector, name),
        RecordedDefinition::Preset(name) => (stored::DefinitionKind::Preset, name),
        RecordedDefinition::Rule(name) => (stored::DefinitionKind::Rule, name),
        RecordedDefinition::Scenario(name) => (stored::DefinitionKind::Scenario, name),
        RecordedDefinition::Layout(name) => (stored::DefinitionKind::Layout, name),
        RecordedDefinition::Environment(name) => (stored::DefinitionKind::Environment, name),
    };
    stored::Definition {
        kind,
        name: name.as_str().to_owned(),
    }
}

fn expanded(expanded: &ExpandedIncident) -> stored::ExpandedIncident {
    let consequences = expanded.consequences().iter().map(|consequence| match consequence {
        Consequence::Leaderless { partition } => stored::Consequence::Leaderless {
            partition: topic_partition(partition),
        },
        Consequence::CoordinatorUnavailable { coordinator: lost } => stored::Consequence::CoordinatorUnavailable {
            coordinator: coordinator(lost),
        },
        Consequence::Incident(follows) => stored::Consequence::Incident(incident(follows)),
    });
    stored::ExpandedIncident {
        incident: incident(expanded.incident()),
        consequences: consequences.collect(),
    }
}

fn change(change: &Change) -> stored::Change {
    let name = |rule: &RecordedName| rule.as_str().to_owned();
    match change {
        Change::RuleArmed { rule } => stored::Change::RuleArmed { rule: name(rule) },
        Change::RuleDisarmed { rule } => stored::Change::RuleDisarmed { rule: name(rule) },
        Change::ParameterChanged {
            rule,
            parameter: changed,
            old,
            new,
        } => stored::Change::ParameterChanged {
            rule: name(rule),
            parameter: parameter(*changed),
            old: old.clone(),
            new: new.clone(),
        },
        Change::RuleFiredByHand { rule } => stored::Change::RuleFiredByHand { rule: name(rule) },
        Change::LiveRuleMade { rule } => stored::Change::LiveRuleMade { rule: name(rule) },
        Change::LiveRuleChanged { rule } => stored::Change::LiveRuleChanged { rule: name(rule) },
        Change::LiveRuleKept { rule } => stored::Change::LiveRuleKept { rule: name(rule) },
        Change::LiveRuleDeleted { rule } => stored::Change::LiveRuleDeleted { rule: name(rule) },
        Change::DefinitionCreated { definition: created } => stored::Change::DefinitionCreated {
            definition: definition(created),
        },
        Change::DefinitionChanged { definition: changed } => stored::Change::DefinitionChanged {
            definition: definition(changed),
        },
        Change::DefinitionDeleted { definition: deleted } => stored::Change::DefinitionDeleted {
            definition: definition(deleted),
        },
        Change::IncidentApplied { incident } => stored::Change::IncidentApplied {
            incident: expanded(incident),
        },
        Change::IncidentEmulated { incident } => stored::Change::IncidentEmulated {
            incident: expanded(incident),
        },
        Change::RecordingStopped { why } => stored::Change::RecordingStopped {
            why: match why {
                RecordingStopCause::FellBehind => stored::RecordingStopCause::FellBehind,
                RecordingStopCause::CouldNotWrite => stored::RecordingStopCause::CouldNotWrite,
            },
        },
    }
}

pub(crate) fn journal_entry(entry: &JournalEntry) -> stored::JournalEntry {
    let JournalEntry {
        id,
        at,
        revision,
        from,
        change: changed,
    } = entry;
    let ChangeSource {
        surface: came_from,
        cause: because,
    } = from;
    stored::JournalEntry {
        id: id.value(),
        at: instant(*at),
        revision: revision.map(Revision::number),
        surface: surface(*came_from),
        cause: because.map(cause),
        change: change(changed),
    }
}

pub(crate) fn description(description: &Description) -> stored::Description {
    let Description {
        environment,
        started,
        clock,
        mode,
        backend,
        written_by,
        armed,
        scenario,
        journal_after,
    } = description;
    stored::Description {
        environment: environment.as_str().to_owned(),
        started: started.unix_millis(),
        clock: instant(*clock),
        mode: match mode {
            RecordingMode::Full => stored::RecordingMode::Full,
            RecordingMode::MetadataOnly => stored::RecordingMode::MetadataOnly,
        },
        backend: match backend {
            RecordedBackend::Upstream => stored::Backend::Upstream,
            RecordedBackend::Simulated => stored::Backend::Simulated,
        },
        written_by: written_by.clone(),
        armed: armed.iter().map(|rule| rule.as_str().to_owned()).collect(),
        scenario: scenario.as_ref().map(|active| {
            let ActiveScenario { scenario, step } = active;
            stored::ActiveScenario {
                scenario: scenario.as_str().to_owned(),
                step: step.as_ref().map(|step| step.as_str().to_owned()),
            }
        }),
        journal_after: journal_after.map(JournalEntryId::value),
    }
}

pub(crate) fn summary(summary: &Summary) -> stored::Summary {
    stored::Summary {
        instants: summary.instants().map(|(from, to)| stored::Instants {
            from: instant(from),
            to: instant(to),
        }),
        calls: summary.calls(),
        call_ids: summary.call_ids().map(|(lowest, highest)| stored::Ids {
            lowest: lowest.value(),
            highest: highest.value(),
        }),
        journal: summary.journal_entries(),
        journal_ids: summary.journal_ids().map(|(first, last)| stored::Ids {
            lowest: first.value(),
            highest: last.value(),
        }),
        sessions: summary.session_events(),
        fields: summary
            .fields()
            .map(|(of, values)| stored::FieldSummary {
                field: field(of),
                values: match values {
                    SummaryValues::Listed(values) => {
                        stored::SummaryValues::Listed(values.iter().map(field_value).collect())
                    }
                    SummaryValues::Any => stored::SummaryValues::Any,
                },
            })
            .collect(),
    }
}

pub(crate) fn end(end: &End) -> stored::End {
    let End {
        ending,
        at,
        entries,
        calls,
    } = end;
    stored::End {
        ending: match ending {
            Ending::Stopped => stored::Ending::Stopped,
            Ending::EnvironmentStopped => stored::Ending::EnvironmentStopped,
            Ending::FellBehind => stored::Ending::FellBehind,
        },
        at: instant(*at),
        entries: *entries,
        calls: *calls,
    }
}
