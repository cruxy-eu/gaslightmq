//! The `serde` forms: each type writes a plain map or a text, and reading builds the type through its own checks.
//!
//! A form is a private mirror of a type with the keys a file has.
//! Writing copies a value into its form.
//! Reading takes the form and then calls the constructor of the type,
//! so a record that contradicts itself fails to read with the message of that type's error.

use std::num::NonZeroU64;

use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_incident::Incident;
use gaslightmq_kernel::ErrorCode;
use gaslightmq_lie::{Priority, TargetedLie};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::explanation::Course;
use crate::{
    BudgetCheck, BudgetCheckError, Call, CallId, CallNumber, CallTiming, CallTimingError, Delivered, DieCheck,
    DieCheckError, Explanation, Failure, LieFate, LieFateError, LostTarget, RecordedRuleName, ResponseSummary,
    RuleExplanation, TriggerChecks, TriggerDecision, TriggerExplanation, Verdict, WindowCheck, WindowCheckError,
};

/// Write a type as its form, and read it back through the checks of `TryFrom`.
macro_rules! through_form {
    ($($type:ty => $form:ty,)*) => {
        $(
            impl Serialize for $type {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    <$form>::from(self).serialize(serializer)
                }
            }

            impl<'de> Deserialize<'de> for $type {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    <$type>::try_from(<$form>::deserialize(deserializer)?).map_err(D::Error::custom)
                }
            }
        )*
    };
}

/// Write a type as its text, and read it back with `FromStr`.
macro_rules! as_text {
    ($($type:ty,)*) => {
        $(
            impl Serialize for $type {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serializer.collect_str(self)
                }
            }

            impl<'de> Deserialize<'de> for $type {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    String::deserialize(deserializer)?.parse().map_err(D::Error::custom)
                }
            }
        )*
    };
}

as_text!(CallId, RecordedRuleName, TriggerDecision,);

through_form!(
    DieCheck => DieForm,
    WindowCheck => WindowForm,
    BudgetCheck => BudgetForm,
    TriggerExplanation => TriggerForm,
    LostTarget => LostTargetForm,
    LieFate => LieFateForm,
    RuleExplanation => RuleForm,
    Explanation => ExplanationForm,
    CallTiming => CallTimingForm,
    Call => CallForm,
);

impl Serialize for CallNumber {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CallNumber {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        CallNumber::new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl Serialize for Failure {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.message())
    }
}

impl<'de> Deserialize<'de> for Failure {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Failure::new)
    }
}

/// A verdict is the list of its lies, and a pass is the empty list.
impl Serialize for Verdict {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.lies().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Verdict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<TargetedLie>::deserialize(deserializer).map(Verdict::lied)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "DieCheck", deny_unknown_fields)]
struct DieForm {
    roll: u32,
    chance: u32,
}

impl From<&DieCheck> for DieForm {
    fn from(die: &DieCheck) -> Self {
        DieForm {
            roll: die.roll_ppm(),
            chance: die.chance_ppm(),
        }
    }
}

impl TryFrom<DieForm> for DieCheck {
    type Error = DieCheckError;

    fn try_from(form: DieForm) -> Result<Self, Self::Error> {
        DieCheck::new(form.roll, form.chance)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "WindowCheck", rename_all = "kebab-case", deny_unknown_fields)]
struct WindowForm {
    opens_after: TimeSpan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    closes_after: Option<TimeSpan>,
    since_armed: TimeSpan,
}

impl From<&WindowCheck> for WindowForm {
    fn from(window: &WindowCheck) -> Self {
        WindowForm {
            opens_after: window.opens_after(),
            closes_after: window.closes_after(),
            since_armed: window.since_armed(),
        }
    }
}

impl TryFrom<WindowForm> for WindowCheck {
    type Error = WindowCheckError;

    fn try_from(form: WindowForm) -> Result<Self, Self::Error> {
        WindowCheck::new(form.opens_after, form.closes_after, form.since_armed)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "BudgetCheck", deny_unknown_fields)]
struct BudgetForm {
    left: u64,
    limit: NonZeroU64,
}

impl From<&BudgetCheck> for BudgetForm {
    fn from(budget: &BudgetCheck) -> Self {
        BudgetForm {
            left: budget.left(),
            limit: budget.limit(),
        }
    }
}

impl TryFrom<BudgetForm> for BudgetCheck {
    type Error = BudgetCheckError;

    fn try_from(form: BudgetForm) -> Result<Self, Self::Error> {
        BudgetCheck::new(form.left, form.limit)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "TriggerExplanation", rename_all = "kebab-case", deny_unknown_fields)]
struct TriggerForm {
    decision: TriggerDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    match_number: Option<NonZeroU64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    count_agreed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    die: Option<DieCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window: Option<WindowCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    budget: Option<BudgetCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl From<&TriggerExplanation> for TriggerForm {
    fn from(trigger: &TriggerExplanation) -> Self {
        TriggerForm {
            decision: trigger.decision(),
            match_number: trigger.match_number(),
            count_agreed: trigger.checks().count_agreed(),
            die: trigger.checks().die(),
            window: trigger.checks().window(),
            budget: trigger.budget(),
            reason: trigger.reason().map(str::to_owned),
        }
    }
}

impl TryFrom<TriggerForm> for TriggerExplanation {
    type Error = crate::TriggerExplanationError;

    fn try_from(form: TriggerForm) -> Result<Self, Self::Error> {
        let mut checks = TriggerChecks::new();
        if let Some(agreed) = form.count_agreed {
            checks = checks.with_count(agreed);
        }
        if let Some(die) = form.die {
            checks = checks.with_die(die);
        }
        if let Some(window) = form.window {
            checks = checks.with_window(window);
        }
        let mut trigger = TriggerExplanation::new(form.decision, form.match_number, checks)?;
        if let Some(budget) = form.budget {
            trigger = trigger.with_budget(budget);
        }
        if let Some(reason) = form.reason {
            trigger = trigger.with_reason(reason);
        }
        Ok(trigger)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "LostTarget", deny_unknown_fields)]
struct LostTargetForm {
    on: Target,
    to: RecordedRuleName,
}

impl From<&LostTarget> for LostTargetForm {
    fn from(loss: &LostTarget) -> Self {
        LostTargetForm {
            on: loss.on().clone(),
            to: loss.to().clone(),
        }
    }
}

impl From<LostTargetForm> for LostTarget {
    fn from(form: LostTargetForm) -> Self {
        LostTarget::new(form.on, form.to)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "LieFate", rename_all = "kebab-case", deny_unknown_fields)]
struct LieFateForm {
    fired: TargetedLie,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied: Option<Target>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    shadowed: Vec<LostTarget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pre_empted: Vec<LostTarget>,
    /// Why GaslightMQ could not tell the lie.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    untold: Option<Failure>,
}

impl From<&LieFate> for LieFateForm {
    fn from(fate: &LieFate) -> Self {
        LieFateForm {
            fired: fate.lie().clone(),
            applied: fate.applied().cloned(),
            shadowed: fate.shadowed().to_vec(),
            pre_empted: fate.pre_empted().to_vec(),
            untold: fate.failure().cloned(),
        }
    }
}

impl TryFrom<LieFateForm> for LieFate {
    type Error = LieFateError;

    fn try_from(form: LieFateForm) -> Result<Self, Self::Error> {
        match (form.untold, form.applied) {
            (None, applied) => LieFate::new(form.fired, applied, form.shadowed, form.pre_empted),
            (Some(_), Some(applied)) => Err(LieFateError::AppliedUntold { applied }),
            (Some(failure), None) => Ok(LieFate::untold_with_losses(
                form.fired,
                form.shadowed,
                form.pre_empted,
                failure,
            )),
        }
    }
}

/// One line of an explanation. The keys it has say how far the rule got.
#[derive(Serialize, Deserialize)]
#[serde(rename = "RuleExplanation", rename_all = "kebab-case", deny_unknown_fields)]
struct RuleForm {
    rule: RecordedRuleName,
    priority: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    matched: Option<Target>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trigger: Option<TriggerExplanation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lie: Option<LieFate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    incidents: Vec<Incident>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ended_by: Option<RecordedRuleName>,
}

impl From<&RuleExplanation> for RuleForm {
    fn from(line: &RuleExplanation) -> Self {
        let mut form = RuleForm {
            rule: line.rule().clone(),
            priority: line.priority().position(),
            matched: line.matched().cloned(),
            trigger: line.trigger().cloned(),
            lie: None,
            incidents: Vec::new(),
            ended_by: None,
        };
        match line.course() {
            Course::EndedBeforeTruth { ended_by } => form.ended_by = Some(ended_by.clone()),
            Course::Held { .. } => {}
            Course::Lied { lie, .. } => form.lie = Some(lie.clone()),
            Course::FiredIncidents { incidents, .. } => form.incidents.clone_from(incidents),
        }
        form
    }
}

impl TryFrom<RuleForm> for RuleExplanation {
    type Error = String;

    fn try_from(form: RuleForm) -> Result<Self, Self::Error> {
        let (rule, priority) = (form.rule, Priority::new(form.priority));
        let line = match (form.ended_by, form.matched, form.trigger, form.lie, form.incidents) {
            (Some(ended_by), None, None, None, incidents) if incidents.is_empty() => {
                return Ok(RuleExplanation::ended_before_truth(rule, priority, ended_by));
            }
            (None, Some(matched), Some(trigger), None, incidents) if incidents.is_empty() => {
                RuleExplanation::held(rule, priority, matched, trigger)
            }
            (None, Some(matched), Some(trigger), Some(lie), incidents) if incidents.is_empty() => {
                RuleExplanation::lied(rule, priority, matched, trigger, lie)
            }
            (None, Some(matched), Some(trigger), None, incidents) => {
                RuleExplanation::fired_incidents(rule, priority, matched, trigger, incidents)
            }
            _ => {
                return Err(format!(
                    "the line of rule {:?} has `ended-by` alone, \
                     or `matched` and `trigger` with at most one of `lie` and `incidents`",
                    rule.as_str()
                ));
            }
        };
        line.map_err(|error| error.to_string())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "Explanation", deny_unknown_fields)]
struct ExplanationForm {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rules: Vec<RuleExplanation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<Failure>,
}

impl From<&Explanation> for ExplanationForm {
    fn from(explanation: &Explanation) -> Self {
        ExplanationForm {
            rules: explanation.rules().to_vec(),
            failure: explanation.failure().cloned(),
        }
    }
}

impl TryFrom<ExplanationForm> for Explanation {
    type Error = crate::ExplanationError;

    fn try_from(form: ExplanationForm) -> Result<Self, Self::Error> {
        let explanation = Explanation::new(form.rules)?;
        Ok(match form.failure {
            Some(failure) => explanation.with_failure(failure),
            None => explanation,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "CallTiming", deny_unknown_fields)]
struct CallTimingForm {
    arrived: Instant,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    forwarded: Option<Instant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    answered: Option<Instant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivered: Option<Instant>,
}

impl From<&CallTiming> for CallTimingForm {
    fn from(timing: &CallTiming) -> Self {
        CallTimingForm {
            arrived: timing.arrived_at(),
            forwarded: timing.forwarded_at(),
            answered: timing.answered_at(),
            delivered: timing.delivered_at(),
        }
    }
}

impl TryFrom<CallTimingForm> for CallTiming {
    type Error = CallTimingError;

    fn try_from(form: CallTimingForm) -> Result<Self, Self::Error> {
        let mut timing = CallTiming::arrived(form.arrived);
        if let Some(at) = form.forwarded {
            timing = timing.forwarded(at)?;
        }
        if let Some(at) = form.answered {
            timing = timing.answered(at)?;
        }
        if let Some(at) = form.delivered {
            timing = timing.delivered(at)?;
        }
        Ok(timing)
    }
}

/// The truth of one part, next to the part.
#[derive(Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum PartTruth {
    /// The part has no truth of its own, and shares the truth of the call.
    #[default]
    OfCall,
    /// The backend answered this part without an error.
    Ok,
    /// The backend answered this part with an error.
    Error(ErrorCode),
    /// This part never reached the backend.
    None,
}

impl PartTruth {
    fn is_of_call(&self) -> bool {
        *self == PartTruth::OfCall
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "PartFields", deny_unknown_fields)]
struct PartForm {
    part: Part,
    #[serde(default, skip_serializing_if = "PartTruth::is_of_call")]
    truth: PartTruth,
}

impl From<&PartFields> for PartForm {
    fn from(part: &PartFields) -> Self {
        let truth = match (part.own(Field::Outcome), part.own(Field::ErrorCode)) {
            (_, Some(Reading::Present(FieldValue::ErrorCode(code)))) => PartTruth::Error(code),
            (Some(Reading::Present(_)), _) => PartTruth::Ok,
            (Some(Reading::Absent), _) => PartTruth::None,
            (Some(Reading::Pending) | None, _) => PartTruth::OfCall,
        };
        PartForm {
            part: part.part().clone(),
            truth,
        }
    }
}

impl From<PartForm> for PartFields {
    fn from(form: PartForm) -> Self {
        let part = PartFields::new(form.part);
        match form.truth {
            PartTruth::OfCall => part,
            PartTruth::Ok => part.succeeded(),
            PartTruth::Error(code) => part.failed(code),
            PartTruth::None => part.without_truth(),
        }
    }
}

/// The fields of a call: the values the call has itself, those the client context provides, and the parts.
#[derive(Serialize, Deserialize)]
#[serde(rename = "Fields", deny_unknown_fields)]
struct FieldsForm {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<FieldValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    learned: Vec<FieldValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parts: Vec<PartForm>,
}

impl From<&Fields> for FieldsForm {
    fn from(fields: &Fields) -> Self {
        let mut form = FieldsForm {
            values: Vec::new(),
            learned: Vec::new(),
            parts: fields.parts().iter().map(PartForm::from).collect(),
        };
        for field in Field::ALL {
            if let Some(values) = fields.whole_call(field).present() {
                let list = match values.origin() {
                    Origin::Call => &mut form.values,
                    Origin::ClientContext => &mut form.learned,
                };
                list.extend(values.iter().cloned());
            }
        }
        form
    }
}

impl TryFrom<FieldsForm> for Fields {
    type Error = String;

    fn try_from(form: FieldsForm) -> Result<Self, Self::Error> {
        let answered = form
            .values
            .iter()
            .any(|value| value.field().group() == FieldGroup::Response);
        let mut fields = Fields::new();
        for value in form.values {
            fields = with_value(fields, value)?;
        }
        for value in form.learned {
            fields = with_learned(fields, value)?;
        }
        for part in form.parts {
            fields = fields.with_part(part);
        }
        // A record is of a call that ended: without a response field, the call never reached the backend.
        Ok(if answered { fields } else { fields.without_truth() })
    }
}

/// The call or its session has `value`.
fn with_value(fields: Fields, value: FieldValue) -> Result<Fields, String> {
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
fn with_learned(fields: Fields, value: FieldValue) -> Result<Fields, String> {
    Ok(match value {
        FieldValue::Group(value) => fields.with_learned(value),
        FieldValue::MemberId(value) => fields.with_learned(value),
        FieldValue::GroupInstanceId(value) => fields.with_learned(value),
        FieldValue::TransactionalId(value) => fields.with_learned(value),
        other => return Err(format!("`{}` is not a learned field", other.field())),
    })
}

/// A call record. It writes its verdict for whoever reads the file, and reading takes the verdict from the explanation.
#[derive(Serialize, Deserialize)]
#[serde(rename = "Call")]
struct CallForm {
    number: CallNumber,
    fields: FieldsForm,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream: Option<ResponseSummary>,
    delivered: Delivered,
    #[serde(default, skip_deserializing)]
    verdict: Verdict,
    explanation: Explanation,
    timing: CallTiming,
}

impl From<&Call> for CallForm {
    fn from(call: &Call) -> Self {
        CallForm {
            number: call.number(),
            fields: FieldsForm::from(call.fields()),
            upstream: call.upstream().cloned(),
            delivered: call.delivered().clone(),
            verdict: call.verdict(),
            explanation: call.explanation().clone(),
            timing: call.timing(),
        }
    }
}

impl TryFrom<CallForm> for Call {
    type Error = String;

    fn try_from(form: CallForm) -> Result<Self, Self::Error> {
        let fields = Fields::try_from(form.fields)?;
        let call = Call::new(form.number, fields, form.delivered, form.explanation, form.timing)
            .map_err(|error| error.to_string())?;
        Ok(match form.upstream {
            Some(upstream) => call.with_upstream(upstream),
            None => call,
        })
    }
}
