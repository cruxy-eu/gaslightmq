//! The way from the stored types of the current version, where they lie, to the types of a call, a journal and a
//! recording.
//!
//! Every function reads an archived value and builds the type through its own checks.
//! The check of a batch says that a text is text, and not that it is the name of a topic,
//! so this is the one place where a value that its type does not take is found.
//! What is wrong is text: the caller says which entry or which message it was.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU64;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::{Consequence, Coordinator, ExpandedIncident, Incident, PartitionCount, RecordCount};
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::{HangUpLie, LieDefinition, LieKind, Priority, RefusalLie, Stage, TargetedLie, TimingLie};
use rkyv::option::ArchivedOption;
use rkyv::string::ArchivedString;

use super::v1 as stored;
use crate::{
    ActiveScenario, Closer, Description, End, Ending, FrameState, RecordedBackend, RecordedSessionEvent, SessionChange,
    Summary, SummaryValues, UtcInstant,
};

/// What is wrong with a stored value, in words.
pub(crate) type Flaw = String;

fn flaw(error: impl std::fmt::Display) -> Flaw {
    error.to_string()
}

fn text(text: &ArchivedOption<ArchivedString>) -> Option<&str> {
    text.as_ref().map(ArchivedString::as_str)
}

pub(crate) fn instant(nanos: rkyv::rend::u64_le) -> Instant {
    Instant::from_nanos(nanos.to_native())
}

fn span(millis: rkyv::rend::u64_le) -> TimeSpan {
    TimeSpan::from_millis(millis.to_native())
}

fn ip(host: &stored::ArchivedHost) -> IpAddr {
    match host {
        stored::ArchivedHost::V4(octets) => IpAddr::V4(Ipv4Addr::from(*octets)),
        stored::ArchivedHost::V6(octets) => IpAddr::V6(Ipv6Addr::from(*octets)),
    }
}

fn error_code(code: rkyv::rend::i16_le) -> Result<ErrorCode, Flaw> {
    ErrorCode::new(code.to_native()).map_err(flaw)
}

fn broker(id: rkyv::rend::u32_le) -> Result<BrokerId, Flaw> {
    BrokerId::new(id.to_native()).map_err(flaw)
}

fn broker_or_none(id: ArchivedOption<rkyv::rend::u32_le>) -> Result<Option<BrokerId>, Flaw> {
    id.as_ref().map(|id| broker(*id)).transpose()
}

fn endpoint(of: ArchivedOption<rkyv::rend::u32_le>) -> Result<Endpoint, Flaw> {
    Ok(broker_or_none(of)?.map_or(Endpoint::Bootstrap, Endpoint::Broker))
}

fn topic(name: &ArchivedString) -> Result<TopicName, Flaw> {
    TopicName::new(name.as_str()).map_err(flaw)
}

fn topic_partition(partition: &stored::ArchivedTopicPartition) -> Result<TopicPartition, Flaw> {
    let number = PartitionNumber::new(partition.partition.to_native()).map_err(flaw)?;
    Ok(TopicPartition::new(topic(&partition.topic)?, number))
}

fn part(part: &stored::ArchivedPart) -> Result<Part, Flaw> {
    Ok(match part {
        stored::ArchivedPart::Topic(name) => Part::Topic(topic(name)?),
        stored::ArchivedPart::Partition(partition) => Part::Partition(topic_partition(partition)?),
    })
}

fn target(target: &stored::ArchivedTarget) -> Result<Target, Flaw> {
    match target {
        stored::ArchivedTarget::Call => Ok(Target::Call),
        stored::ArchivedTarget::Parts(parts) => {
            let parts = parts.iter().map(part).collect::<Result<Vec<_>, Flaw>>()?;
            Target::of_parts(parts).ok_or_else(|| "a target of parts names no part".to_owned())
        }
    }
}

fn software(name: &ArchivedString, version: &ArchivedString) -> Result<ClientSoftware, Flaw> {
    ClientSoftware::new(name.as_str(), version.as_str()).map_err(flaw)
}

pub(crate) fn field_value(value: &stored::ArchivedFieldValue) -> Result<FieldValue, Flaw> {
    use stored::ArchivedFieldValue as Stored;
    Ok(match value {
        Stored::Connection(number) => ConnectionNumber::new(number.to_native()).into(),
        Stored::Address { host, port } => ClientAddress::new(SocketAddr::new(ip(host), port.to_native())).into(),
        Stored::Host(host) => ClientHost::new(ip(host)).into(),
        Stored::Endpoint(of) => endpoint(*of)?.into(),
        Stored::ClientId(id) => ClientId::new(id.as_str()).map_err(flaw)?.into(),
        Stored::ClientSoftware { name, version } => software(name, version)?.into(),
        Stored::Api(key) => ApiKey::new(key.to_native()).into(),
        Stored::ApiVersion(version) => ApiVersion::new(version.to_native()).map_err(flaw)?.into(),
        Stored::Topic(name) => topic(name)?.into(),
        Stored::Partition(number) => PartitionNumber::new(number.to_native()).map_err(flaw)?.into(),
        Stored::Acks(acks) => Acks::new(acks.to_native()).into(),
        Stored::Group(group) => GroupId::new(group.as_str()).map_err(flaw)?.into(),
        Stored::MemberId(member) => MemberId::new(member.as_str()).map_err(flaw)?.into(),
        Stored::GroupInstanceId(instance) => GroupInstanceId::new(instance.as_str()).map_err(flaw)?.into(),
        Stored::TransactionalId(id) => TransactionalId::new(id.as_str()).map_err(flaw)?.into(),
        Stored::Outcome(outcome) => match outcome {
            stored::ArchivedOutcome::Ok => Outcome::Ok,
            stored::ArchivedOutcome::Error => Outcome::Error,
            stored::ArchivedOutcome::Timeout => Outcome::Timeout,
        }
        .into(),
        Stored::ErrorCode(code) => error_code(*code)?.into(),
        Stored::Latency(millis) => Latency::new(span(*millis)).into(),
    })
}

pub(crate) fn field(field: &stored::ArchivedField) -> Field {
    use stored::ArchivedField as Stored;
    match field {
        Stored::Connection => Field::Connection,
        Stored::Address => Field::Address,
        Stored::Host => Field::Host,
        Stored::Endpoint => Field::Endpoint,
        Stored::ClientId => Field::ClientId,
        Stored::ClientSoftware => Field::ClientSoftware,
        Stored::Api => Field::Api,
        Stored::ApiVersion => Field::ApiVersion,
        Stored::Topic => Field::Topic,
        Stored::Partition => Field::Partition,
        Stored::Acks => Field::Acks,
        Stored::Group => Field::Group,
        Stored::MemberId => Field::MemberId,
        Stored::GroupInstanceId => Field::GroupInstanceId,
        Stored::TransactionalId => Field::TransactionalId,
        Stored::Outcome => Field::Outcome,
        Stored::ErrorCode => Field::ErrorCode,
        Stored::Latency => Field::Latency,
    }
}

/// The call or its session has `value`.
fn with_value(fields: Fields, value: FieldValue) -> Result<Fields, Flaw> {
    Ok(match value {
        FieldValue::Connection(value) => fields.with(value),
        FieldValue::Address(value) => fields.with(value),
        FieldValue::Host(value) => fields.with(value),
        FieldValue::Endpoint(value) => fields.with(value),
        FieldValue::ClientId(value) => fields.with(value),
        FieldValue::ClientSoftware(value) => fields.with(value),
        FieldValue::Api(value) => fields.with(value),
        FieldValue::ApiVersion(value) => fields.with(value),
        FieldValue::Acks(value) => fields.with(value),
        FieldValue::Group(value) => fields.with(value),
        FieldValue::MemberId(value) => fields.with(value),
        FieldValue::GroupInstanceId(value) => fields.with(value),
        FieldValue::TransactionalId(value) => fields.with(value),
        FieldValue::Outcome(value) => fields.with(value),
        FieldValue::ErrorCode(value) => fields.with(value),
        FieldValue::Latency(value) => fields.with(value),
        FieldValue::Topic(_) | FieldValue::Partition(_) => {
            return Err(format!(
                "`{}` belongs to a part of the call, and not to its values",
                value.field()
            ));
        }
    })
}

/// The client context provides `value`.
fn with_learned(fields: Fields, value: FieldValue) -> Result<Fields, Flaw> {
    Ok(match value {
        FieldValue::Group(value) => fields.with_learned(value),
        FieldValue::MemberId(value) => fields.with_learned(value),
        FieldValue::GroupInstanceId(value) => fields.with_learned(value),
        FieldValue::TransactionalId(value) => fields.with_learned(value),
        other => return Err(format!("`{}` is not a learned field", other.field())),
    })
}

pub(crate) fn part_fields(own: &stored::ArchivedPartFields) -> Result<PartFields, Flaw> {
    let fields = PartFields::new(part(&own.part)?);
    Ok(match &own.truth {
        stored::ArchivedPartTruth::OfCall => fields,
        stored::ArchivedPartTruth::Succeeded => fields.succeeded(),
        stored::ArchivedPartTruth::Failed(code) => fields.failed(error_code(*code)?),
        stored::ArchivedPartTruth::None => fields.without_truth(),
    })
}

pub(crate) fn fields(stored: &stored::ArchivedFields) -> Result<Fields, Flaw> {
    let mut fields = Fields::new();
    let mut answered = false;
    for value in stored.values.iter() {
        let value = field_value(value)?;
        answered |= value.field().group() == FieldGroup::Response;
        fields = with_value(fields, value)?;
    }
    for value in stored.learned.iter() {
        fields = with_learned(fields, field_value(value)?)?;
    }
    for own in stored.parts.iter() {
        fields = fields.with_part(part_fields(own)?);
    }
    // A record is of a call that ended: without a response field, the call never reached the backend.
    Ok(if answered { fields } else { fields.without_truth() })
}

fn answer(answer: &stored::ArchivedAnswer) -> Result<Answer, Flaw> {
    Ok(match answer {
        stored::ArchivedAnswer::Ok => Answer::Ok,
        stored::ArchivedAnswer::Error(code) => Answer::Error(error_code(*code)?),
    })
}

fn response(stored: &stored::ArchivedResponseSummary) -> Result<ResponseSummary, Flaw> {
    let mut response = ResponseSummary::new(answer(&stored.answer)?);
    for own in stored.parts.iter() {
        response = response.with_part(part(&own.part)?, answer(&own.answer)?);
    }
    Ok(response)
}

fn delivered(delivered: &stored::ArchivedDelivered) -> Result<Delivered, Flaw> {
    Ok(match delivered {
        stored::ArchivedDelivered::Response(summary) => Delivered::Response(response(summary)?),
        stored::ArchivedDelivered::UnreadResponse => Delivered::UnreadResponse,
        stored::ArchivedDelivered::Nothing => Delivered::Nothing,
        stored::ArchivedDelivered::ConnectionClosed => Delivered::ConnectionClosed,
    })
}

fn timing(stored: &stored::ArchivedTiming) -> Result<CallTiming, Flaw> {
    let mut timing = CallTiming::arrived(instant(stored.arrived));
    if let Some(at) = stored.forwarded.as_ref() {
        timing = timing.forwarded(instant(*at)).map_err(flaw)?;
    }
    if let Some(at) = stored.answered.as_ref() {
        timing = timing.answered(instant(*at)).map_err(flaw)?;
    }
    if let Some(at) = stored.delivered.as_ref() {
        timing = timing.delivered(instant(*at)).map_err(flaw)?;
    }
    Ok(timing)
}

fn stage(kind: LieKind, stage: &stored::ArchivedStage) -> Result<Stage, Flaw> {
    let stage = match stage {
        stored::ArchivedStage::BeforeForwarding => Stage::BeforeForwarding,
        stored::ArchivedStage::AfterResponse => Stage::AfterResponse,
    };
    kind.stage_for(Some(stage)).map_err(flaw)
}

fn lie(lie: &stored::ArchivedLie) -> Result<LieDefinition, Flaw> {
    Ok(match lie {
        stored::ArchivedLie::Refusal { error, stage: at } => RefusalLie::new(error_code(*error)?)
            .at(stage(LieKind::Refusal, at)?)
            .into(),
        stored::ArchivedLie::Timing {
            delay,
            jitter,
            throttle,
            stage: at,
        } => {
            let (delay, jitter, throttle) = (span(*delay), span(*jitter), span(*throttle));
            let timing = TimingLie::new()
                .with_delay(delay)
                .with_jitter(jitter)
                .with_throttle(throttle)
                .at(stage(LieKind::Timing, at)?);
            // A timing lie shortens a jitter and a throttle that it does not take, and a recording holds what it took.
            if (timing.jitter(), timing.throttle()) != (jitter, throttle) {
                return Err(format!(
                    "a timing lie takes no jitter of {jitter} with a delay of {delay}, or no throttle of {throttle}"
                ));
            }
            timing.into()
        }
        stored::ArchivedLie::Silence => LieDefinition::Silence,
        stored::ArchivedLie::HangUp { stage: at } => HangUpLie::new().at(stage(LieKind::HangUp, at)?).into(),
    })
}

fn targeted_lie(targeted: &stored::ArchivedTargetedLie) -> Result<TargetedLie, Flaw> {
    Ok(TargetedLie::new(lie(&targeted.lie)?, target(&targeted.target)?))
}

fn rule(name: &ArchivedString) -> Result<RecordedRuleName, Flaw> {
    RecordedRuleName::new(name.as_str()).map_err(flaw)
}

fn lost(loss: &stored::ArchivedLostTarget) -> Result<LostTarget, Flaw> {
    Ok(LostTarget::new(target(&loss.on)?, rule(&loss.to)?))
}

fn lie_fate(fate: &stored::ArchivedLieFate) -> Result<LieFate, Flaw> {
    let fired = targeted_lie(&fate.lie)?;
    let applied = fate.applied.as_ref().map(target).transpose()?;
    let shadowed = fate.shadowed.iter().map(lost).collect::<Result<Vec<_>, Flaw>>()?;
    let pre_empted = fate.pre_empted.iter().map(lost).collect::<Result<Vec<_>, Flaw>>()?;
    match (text(&fate.untold), applied) {
        (None, applied) => LieFate::new(fired, applied, shadowed, pre_empted).map_err(flaw),
        (Some(_), Some(applied)) => Err(format!("a lie that could not be told applied on {applied}")),
        (Some(failure), None) => {
            // A lie that could not be told applied nowhere, whatever it claimed.
            let claimed = Some(fired.target().clone());
            let fate = LieFate::new(fired, claimed, shadowed, pre_empted).map_err(flaw)?;
            Ok(fate.untold(Failure::new(failure)))
        }
    }
}

fn decision(decision: &stored::ArchivedTriggerDecision) -> TriggerDecision {
    match decision {
        stored::ArchivedTriggerDecision::Fired => TriggerDecision::Fired,
        stored::ArchivedTriggerDecision::FiredByHand => TriggerDecision::FiredByHand,
        stored::ArchivedTriggerDecision::Held => TriggerDecision::Held,
        stored::ArchivedTriggerDecision::BudgetEmpty => TriggerDecision::BudgetEmpty,
        stored::ArchivedTriggerDecision::NothingToActOn => TriggerDecision::NothingToActOn,
        stored::ArchivedTriggerDecision::LifetimeOver => TriggerDecision::LifetimeOver,
    }
}

fn at_least_one(number: rkyv::rend::u64_le, what: &str) -> Result<NonZeroU64, Flaw> {
    NonZeroU64::new(number.to_native()).ok_or_else(|| format!("{what} is at least one"))
}

fn trigger(stored: &stored::ArchivedTriggerExplanation) -> Result<TriggerExplanation, Flaw> {
    let mut checks = TriggerChecks::new();
    if let Some(agreed) = stored.count_agreed.as_ref() {
        checks = checks.with_count(*agreed);
    }
    if let Some(die) = stored.die.as_ref() {
        checks = checks.with_die(DieCheck::new(die.roll.to_native(), die.chance.to_native()).map_err(flaw)?);
    }
    if let Some(window) = stored.window.as_ref() {
        let closes_after = window.closes_after.as_ref().map(|after| span(*after));
        let window = WindowCheck::new(span(window.opens_after), closes_after, span(window.since_armed));
        checks = checks.with_window(window.map_err(flaw)?);
    }
    let match_number = stored.match_number.as_ref();
    let match_number = match_number.map(|number| at_least_one(*number, "a match number"));
    let mut trigger =
        TriggerExplanation::new(decision(&stored.decision), match_number.transpose()?, checks).map_err(flaw)?;
    if let Some(budget) = stored.budget.as_ref() {
        let limit = at_least_one(budget.limit, "the limit of a budget")?;
        trigger = trigger.with_budget(BudgetCheck::new(budget.left.to_native(), limit).map_err(flaw)?);
    }
    if let Some(reason) = text(&stored.reason) {
        trigger = trigger.with_reason(reason);
    }
    Ok(trigger)
}

fn coordinator(coordinator: &stored::ArchivedCoordinator) -> Result<Coordinator, Flaw> {
    Ok(match coordinator {
        stored::ArchivedCoordinator::Group(group) => Coordinator::Group(GroupId::new(group.as_str()).map_err(flaw)?),
        stored::ArchivedCoordinator::Transaction(id) => {
            Coordinator::Transaction(TransactionalId::new(id.as_str()).map_err(flaw)?)
        }
    })
}

fn offset(offset: rkyv::rend::u64_le) -> Result<Offset, Flaw> {
    Offset::new(offset.to_native()).map_err(flaw)
}

fn incident(incident: &stored::ArchivedIncident) -> Result<Incident, Flaw> {
    use stored::ArchivedIncident as Stored;
    Ok(match incident {
        Stored::LeaderMoves { partition, to } => Incident::LeaderMoves {
            partition: topic_partition(partition)?,
            to: broker_or_none(*to)?,
        },
        Stored::BrokerGoesDown { broker: down, failover } => Incident::BrokerGoesDown {
            broker: broker(*down)?,
            failover: *failover,
        },
        Stored::BrokerGoesUnresponsive { broker: silent } => Incident::BrokerGoesUnresponsive {
            broker: broker(*silent)?,
        },
        Stored::BrokerComesBack { broker: back } => Incident::BrokerComesBack { broker: broker(*back)? },
        Stored::Rebalance { group } => Incident::Rebalance {
            group: GroupId::new(group.as_str()).map_err(flaw)?,
        },
        Stored::CoordinatorMoves { coordinator: moved, to } => Incident::CoordinatorMoves {
            coordinator: coordinator(moved)?,
            to: broker_or_none(*to)?,
        },
        Stored::PartitionAdded { topic: grown, count } => Incident::PartitionAdded {
            topic: topic(grown)?,
            count: PartitionCount::new(count.to_native()).map_err(flaw)?,
        },
        Stored::TopicDeleted { topic: deleted } => Incident::TopicDeleted { topic: topic(deleted)? },
        Stored::RecordsAppended { partition, count } => Incident::RecordsAppended {
            partition: topic_partition(partition)?,
            count: RecordCount::new(count.to_native()).map_err(flaw)?,
        },
        Stored::RecordsExpired { partition, to } => Incident::RecordsExpired {
            partition: topic_partition(partition)?,
            to: offset(*to)?,
        },
        Stored::LogTruncated { partition, to } => Incident::LogTruncated {
            partition: topic_partition(partition)?,
            to: offset(*to)?,
        },
        Stored::CommittedOffsetReset { group, partition, to } => Incident::CommittedOffsetReset {
            group: GroupId::new(group.as_str()).map_err(flaw)?,
            partition: topic_partition(partition)?,
            to: offset(*to)?,
        },
        Stored::ProducerFenced { transactional_id } => Incident::ProducerFenced {
            transactional_id: TransactionalId::new(transactional_id.as_str()).map_err(flaw)?,
        },
    })
}

pub(crate) fn priority(position: rkyv::rend::u64_le) -> Result<Priority, Flaw> {
    let position = usize::try_from(position.to_native()).map_err(flaw)?;
    Ok(Priority::new(position))
}

pub(crate) fn rule_explanation(line: &stored::ArchivedRuleExplanation) -> Result<RuleExplanation, Flaw> {
    let (name, priority) = (rule(&line.rule)?, priority(line.priority)?);
    match &line.course {
        stored::ArchivedCourse::EndedBeforeTruth { ended_by } => {
            Ok(RuleExplanation::ended_before_truth(name, priority, rule(ended_by)?))
        }
        stored::ArchivedCourse::Held {
            matched,
            trigger: decided,
        } => RuleExplanation::held(name, priority, target(matched)?, trigger(decided)?).map_err(flaw),
        stored::ArchivedCourse::Lied {
            matched,
            trigger: decided,
            lie,
        } => RuleExplanation::lied(name, priority, target(matched)?, trigger(decided)?, lie_fate(lie)?).map_err(flaw),
        stored::ArchivedCourse::FiredIncidents {
            matched,
            trigger: decided,
            incidents,
        } => {
            let incidents = incidents.iter().map(incident).collect::<Result<Vec<_>, Flaw>>()?;
            RuleExplanation::fired_incidents(name, priority, target(matched)?, trigger(decided)?, incidents)
                .map_err(flaw)
        }
    }
}

pub(crate) fn explanation(stored: &stored::ArchivedExplanation) -> Result<Explanation, Flaw> {
    let rules = stored.rules.iter().map(rule_explanation);
    let explanation = Explanation::new(rules.collect::<Result<Vec<_>, Flaw>>()?).map_err(flaw)?;
    Ok(match text(&stored.failure) {
        Some(failure) => explanation.with_failure(Failure::new(failure)),
        None => explanation,
    })
}

pub(crate) fn call_id(id: rkyv::rend::u64_le) -> Result<CallId, Flaw> {
    CallId::new(id.to_native()).map_err(flaw)
}

pub(crate) fn call(stored: &stored::ArchivedCall) -> Result<Call, Flaw> {
    let call = Call::new(
        call_id(stored.id)?,
        fields(&stored.fields)?,
        delivered(&stored.delivered)?,
        explanation(&stored.explanation)?,
        timing(&stored.timing)?,
    )
    .map_err(flaw)?;
    if call.connection().value() != stored.connection.to_native() {
        return Err(format!(
            "the call says connection {}, and its fields say connection {}",
            stored.connection,
            call.connection()
        ));
    }
    Ok(match stored.upstream.as_ref() {
        Some(upstream) => call.with_upstream(response(upstream)?),
        None => call,
    })
}

pub(crate) fn frame_state(state: &stored::ArchivedFrameState) -> FrameState {
    match state {
        stored::ArchivedFrameState::None => FrameState::None,
        stored::ArchivedFrameState::Whole => FrameState::Whole,
        stored::ArchivedFrameState::Redacted => FrameState::Redacted,
        stored::ArchivedFrameState::Withheld => FrameState::Withheld,
    }
}

fn client_id(id: &ArchivedOption<ArchivedString>) -> Result<Option<ClientId>, Flaw> {
    text(id).map(|id| ClientId::new(id).map_err(flaw)).transpose()
}

pub(crate) fn session_event(stored: &stored::ArchivedSessionEvent) -> Result<RecordedSessionEvent, Flaw> {
    use stored::ArchivedSessionChange as Stored;
    let change = match &stored.change {
        Stored::Opened {
            host,
            port,
            endpoint: of,
            client_id: id,
            software: runs,
        } => SessionChange::Opened {
            address: ClientAddress::new(SocketAddr::new(ip(host), port.to_native())),
            endpoint: endpoint(*of)?,
            client_id: client_id(id)?,
            software: runs
                .as_ref()
                .map(|runs| software(&runs.name, &runs.version))
                .transpose()?,
        },
        Stored::Identified { client_id: id } => SessionChange::Identified {
            client_id: client_id(id)?,
        },
        Stored::SoftwareNamed { software: runs } => SessionChange::SoftwareNamed {
            software: software(&runs.name, &runs.version)?,
        },
        Stored::Closed { by } => SessionChange::Closed {
            by: match by {
                stored::ArchivedCloser::Client => Closer::Client,
                stored::ArchivedCloser::Backend => Closer::Backend,
                stored::ArchivedCloser::Lie => Closer::Lie,
                stored::ArchivedCloser::Proxy => Closer::Proxy,
            },
        },
    };
    Ok(RecordedSessionEvent {
        connection: ConnectionNumber::new(stored.connection.to_native()),
        at: instant(stored.at),
        change,
    })
}

fn name(name: &ArchivedString) -> Result<RecordedName, Flaw> {
    RecordedName::new(name.as_str()).map_err(flaw)
}

fn definition(definition: &stored::ArchivedDefinition) -> Result<RecordedDefinition, Flaw> {
    let name = name(&definition.name)?;
    Ok(match definition.kind {
        stored::ArchivedDefinitionKind::Selector => RecordedDefinition::Selector(name),
        stored::ArchivedDefinitionKind::Preset => RecordedDefinition::Preset(name),
        stored::ArchivedDefinitionKind::Rule => RecordedDefinition::Rule(name),
        stored::ArchivedDefinitionKind::Scenario => RecordedDefinition::Scenario(name),
        stored::ArchivedDefinitionKind::Layout => RecordedDefinition::Layout(name),
        stored::ArchivedDefinitionKind::Environment => RecordedDefinition::Environment(name),
    })
}

fn expanded(expanded: &stored::ArchivedExpandedIncident) -> Result<ExpandedIncident, Flaw> {
    let consequences = expanded.consequences.iter().map(|consequence| {
        Ok(match consequence {
            stored::ArchivedConsequence::Leaderless { partition } => Consequence::Leaderless {
                partition: topic_partition(partition)?,
            },
            stored::ArchivedConsequence::CoordinatorUnavailable { coordinator: lost } => {
                Consequence::CoordinatorUnavailable {
                    coordinator: coordinator(lost)?,
                }
            }
            stored::ArchivedConsequence::Incident(follows) => Consequence::Incident(incident(follows)?),
        })
    });
    let consequences = consequences.collect::<Result<Vec<_>, Flaw>>()?;
    Ok(ExpandedIncident::from_parts(
        incident(&expanded.incident)?,
        consequences,
    ))
}

fn change(change: &stored::ArchivedChange) -> Result<Change, Flaw> {
    use stored::ArchivedChange as Stored;
    Ok(match change {
        Stored::RuleArmed { rule } => Change::RuleArmed { rule: name(rule)? },
        Stored::RuleDisarmed { rule } => Change::RuleDisarmed { rule: name(rule)? },
        Stored::ParameterChanged {
            rule,
            parameter,
            old,
            new,
        } => Change::ParameterChanged {
            rule: name(rule)?,
            parameter: match parameter {
                stored::ArchivedParameter::Chance => Parameter::Chance,
                stored::ArchivedParameter::Every => Parameter::Every,
                stored::ArchivedParameter::Delay => Parameter::Delay,
                stored::ArchivedParameter::Jitter => Parameter::Jitter,
                stored::ArchivedParameter::Throttle => Parameter::Throttle,
                stored::ArchivedParameter::Error => Parameter::Error,
                stored::ArchivedParameter::Lifetime => Parameter::Lifetime,
            },
            old: old.as_str().to_owned(),
            new: new.as_str().to_owned(),
        },
        Stored::RuleFiredByHand { rule } => Change::RuleFiredByHand { rule: name(rule)? },
        Stored::LiveRuleMade { rule } => Change::LiveRuleMade { rule: name(rule)? },
        Stored::LiveRuleChanged { rule } => Change::LiveRuleChanged { rule: name(rule)? },
        Stored::LiveRuleKept { rule } => Change::LiveRuleKept { rule: name(rule)? },
        Stored::LiveRuleDeleted { rule } => Change::LiveRuleDeleted { rule: name(rule)? },
        Stored::DefinitionCreated { definition: created } => Change::DefinitionCreated {
            definition: definition(created)?,
        },
        Stored::DefinitionChanged { definition: changed } => Change::DefinitionChanged {
            definition: definition(changed)?,
        },
        Stored::DefinitionDeleted { definition: deleted } => Change::DefinitionDeleted {
            definition: definition(deleted)?,
        },
        Stored::IncidentApplied { incident } => Change::IncidentApplied {
            incident: expanded(incident)?,
        },
        Stored::IncidentEmulated { incident } => Change::IncidentEmulated {
            incident: expanded(incident)?,
        },
        Stored::RecordingStopped { why } => Change::RecordingStopped {
            why: match why {
                stored::ArchivedRecordingStopCause::FellBehind => RecordingStopCause::FellBehind,
                stored::ArchivedRecordingStopCause::CouldNotWrite => RecordingStopCause::CouldNotWrite,
            },
        },
    })
}

pub(crate) fn journal_entry_id(id: rkyv::rend::u64_le) -> Result<JournalEntryId, Flaw> {
    JournalEntryId::new(id.to_native()).map_err(flaw)
}

pub(crate) fn journal_entry(stored: &stored::ArchivedJournalEntry) -> Result<JournalEntry, Flaw> {
    let surface = match stored.surface {
        stored::ArchivedChangeSurface::Pad => ChangeSurface::Pad,
        stored::ArchivedChangeSurface::QuickInject => ChangeSurface::QuickInject,
        stored::ArchivedChangeSurface::Ui => ChangeSurface::Ui,
        stored::ArchivedChangeSurface::Cli => ChangeSurface::Cli,
        stored::ArchivedChangeSurface::Test => ChangeSurface::Test,
        stored::ArchivedChangeSurface::Api => ChangeSurface::Api,
        stored::ArchivedChangeSurface::Step => ChangeSurface::Step,
        stored::ArchivedChangeSurface::Lifetime => ChangeSurface::Lifetime,
        stored::ArchivedChangeSurface::File => ChangeSurface::File,
    };
    let cause = stored.cause.as_ref().map(|cause| match cause {
        stored::ArchivedChangeCause::Undo => ChangeCause::Undo,
        stored::ArchivedChangeCause::Redo => ChangeCause::Redo,
    });
    Ok(JournalEntry {
        id: journal_entry_id(stored.id)?,
        at: instant(stored.at),
        revision: stored
            .revision
            .as_ref()
            .map(|revision| Revision::new(revision.to_native())),
        from: ChangeSource { surface, cause },
        change: change(&stored.change)?,
    })
}

pub(crate) fn description(stored: &stored::ArchivedDescription) -> Result<Description, Flaw> {
    let scenario = stored.scenario.as_ref().map(|active| {
        Ok::<_, Flaw>(ActiveScenario {
            scenario: name(&active.scenario)?,
            step: text(&active.step)
                .map(|step| RecordedName::new(step).map_err(flaw))
                .transpose()?,
        })
    });
    Ok(Description {
        environment: name(&stored.environment)?,
        started: UtcInstant::new(stored.started.to_native()).map_err(flaw)?,
        clock: instant(stored.clock),
        mode: match stored.mode {
            stored::ArchivedRecordingMode::Full => RecordingMode::Full,
            stored::ArchivedRecordingMode::MetadataOnly => RecordingMode::MetadataOnly,
        },
        backend: match stored.backend {
            stored::ArchivedBackend::Upstream => RecordedBackend::Upstream,
            stored::ArchivedBackend::Simulated => RecordedBackend::Simulated,
        },
        written_by: stored.written_by.as_str().to_owned(),
        armed: stored.armed.iter().map(name).collect::<Result<_, Flaw>>()?,
        scenario: scenario.transpose()?,
        journal_after: stored
            .journal_after
            .as_ref()
            .map(|id| journal_entry_id(*id))
            .transpose()?,
    })
}

pub(crate) fn summary(stored: &stored::ArchivedSummary) -> Result<Summary, Flaw> {
    let instants = stored
        .instants
        .as_ref()
        .map(|instants| (instant(instants.from), instant(instants.to)));
    let call_ids = stored
        .call_ids
        .as_ref()
        .map(|ids| Ok::<_, Flaw>((call_id(ids.lowest)?, call_id(ids.highest)?)));
    let journal_ids = stored
        .journal_ids
        .as_ref()
        .map(|ids| Ok::<_, Flaw>((journal_entry_id(ids.lowest)?, journal_entry_id(ids.highest)?)));
    let fields = stored.fields.iter().map(|of| {
        let values = match &of.values {
            stored::ArchivedSummaryValues::Listed(values) => {
                SummaryValues::Listed(values.iter().map(field_value).collect::<Result<_, Flaw>>()?)
            }
            stored::ArchivedSummaryValues::Any => SummaryValues::Any,
        };
        Ok::<_, Flaw>((field(&of.field), values))
    });
    Summary::from_parts(
        instants,
        stored.calls.to_native(),
        call_ids.transpose()?,
        stored.journal.to_native(),
        journal_ids.transpose()?,
        stored.sessions.to_native(),
        fields.collect::<Result<_, Flaw>>()?,
    )
}

pub(crate) fn end(stored: &stored::ArchivedEnd) -> End {
    End {
        ending: match stored.ending {
            stored::ArchivedEnding::Stopped => Ending::Stopped,
            stored::ArchivedEnding::EnvironmentStopped => Ending::EnvironmentStopped,
            stored::ArchivedEnding::FellBehind => Ending::FellBehind,
        },
        at: instant(stored.at),
        entries: stored.entries.to_native(),
        calls: stored.calls.to_native(),
    }
}
