//! The record of one call.

use std::fmt;

use gaslightmq_field::prelude::*;

use crate::{CallId, CallNumber, CallTiming, Delivered, Explanation, ResponseSummary, Verdict};

/// The record of one call: one request and its response on one session, and what GaslightMQ did to it.
///
/// | Part | Here |
/// |---|---|
/// | Session | The connection fields of [`fields`](Self::fields), and the [`connection`](Self::connection) itself. |
/// | Request | The request fields and the learned fields of [`fields`](Self::fields), and its parts. |
/// | Truth | The response fields of [`fields`](Self::fields), for the call and for each part: `outcome`, `error.code` and `latency`. |
/// | Upstream | [`upstream`](Self::upstream): what the real cluster answered, when the overlay changed it. |
/// | Delivered | [`delivered`](Self::delivered): what the client actually received. |
/// | Verdict | [`verdict`](Self::verdict): pass, or the lies that applied. |
/// | Explanation | [`explanation`](Self::explanation): why the verdict came out this way. |
/// | Timing | [`timing`](Self::timing): when the call arrived, how long the backend took, how long the client waited. |
///
/// A record holds no frame and no payload.
/// It is what travels to the UI, what a scenario counts, and what a recording keeps next to the frames.
///
/// # A record is a field source
///
/// A call record is a [`FieldSource`], so the selector that targeted the live call also filters the recorded one.
/// The fields are those the rules saw when the call ended.
/// The truth of a record is settled: a response field is present or absent, and never pending.
///
/// # The verdict follows from the explanation
///
/// A record keeps the explanation, and [`verdict`](Self::verdict) reads the verdict from it.
/// The two can therefore never disagree.
///
/// ```
/// use std::num::NonZeroU64;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // A Produce for one partition, which the backend never saw: a rule refused it before forwarding.
/// let partition: TopicPartition = "orders.eu/3".parse()?;
/// let fields = Fields::new()
///     .with(ConnectionNumber::new(7))
///     .with(ClientId::new("order-ingest")?)
///     .with(ApiKey::PRODUCE)
///     .with_part(PartFields::new(partition.clone()).without_truth())
///     .without_truth();
///
/// let target = Target::of_parts([partition.clone().into()]).expect("one part");
/// let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(target.clone());
/// let fired = TriggerExplanation::new(TriggerDecision::Fired, Some(NonZeroU64::MIN), TriggerChecks::new())?;
/// let explanation = Explanation::new([RuleExplanation::lied(
///     "Leader flap".parse()?,
///     Priority::HIGHEST,
///     target,
///     fired,
///     LieFate::all_applied(refusal),
/// )?])?;
///
/// let arrived = Instant::ZERO + Duration::from_millis(250);
/// let call = Call::new(
///     CallNumber::FIRST,
///     fields,
///     ResponseSummary::new(Answer::Ok).with_part(partition, ErrorCode::NOT_LEADER_OR_FOLLOWER).into(),
///     explanation,
///     CallTiming::arrived(arrived).delivered(arrived)?,
/// )?;
///
/// assert_eq!(call.id().to_string(), "#7.1");
/// assert_eq!(call.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
/// assert!(call.explanation().applied("Leader flap"));
///
/// // The record answers like the live call did.
/// assert_eq!(call.read(Field::Api), Reading::Present(FieldValues::one(ApiKey::PRODUCE)));
/// assert_eq!(call.read(Field::Outcome), Reading::Absent);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    number: CallNumber,
    connection: ConnectionNumber,
    fields: Fields,
    upstream: Option<ResponseSummary>,
    delivered: Delivered,
    explanation: Explanation,
    timing: CallTiming,
}

impl Call {
    /// The record of the call with that number on its session.
    ///
    /// `fields` are the fields of the call as the rules saw them at its end: who called, what the request asked,
    /// what the client context knew, and the truth.
    /// A call that never reached the backend has [no truth](Fields::without_truth).
    ///
    /// # Errors
    ///
    /// - [`CallError::NoConnection`] when `fields` do not have exactly one `connection`.
    ///   Every call runs on one session, and a session has its connection number from the moment it opens.
    /// - [`CallError::TruthPending`] when `fields` still wait for the backend. A record is of a call that ended.
    pub fn new(
        number: CallNumber,
        fields: Fields,
        delivered: Delivered,
        explanation: Explanation,
        timing: CallTiming,
    ) -> Result<Call, CallError> {
        let connection = match fields.whole_call(Field::Connection).present() {
            Some(values) => match values.iter().as_slice() {
                [FieldValue::Connection(connection)] => *connection,
                _ => return Err(CallError::NoConnection),
            },
            None => return Err(CallError::NoConnection),
        };
        if FieldGroup::Response
            .fields()
            .iter()
            .any(|field| fields.whole_call(*field).is_pending())
        {
            return Err(CallError::TruthPending);
        }
        Ok(Call {
            number,
            connection,
            fields,
            upstream: None,
            delivered,
            explanation,
            timing,
        })
    }

    /// The real cluster answered `upstream`, and the overlay of the backend changed that answer into the truth.
    #[must_use]
    pub fn with_upstream(mut self, upstream: ResponseSummary) -> Call {
        self.upstream = Some(upstream);
        self
    }

    /// Which call this is among all the calls of its engine.
    pub const fn id(&self) -> CallId {
        CallId::new(self.connection, self.number)
    }

    /// Which call of its session this is.
    pub const fn number(&self) -> CallNumber {
        self.number
    }

    /// The connection of the session the call ran on.
    pub const fn connection(&self) -> ConnectionNumber {
        self.connection
    }

    /// The fields of the call: the identity of its session, the request, the learned fields and the truth.
    pub const fn fields(&self) -> &Fields {
        &self.fields
    }

    /// What the real cluster answered, when the overlay of the backend changed it.
    ///
    /// The overlay is not a lie: its address translation and its emulated incidents are part of the truth.
    pub const fn upstream(&self) -> Option<&ResponseSummary> {
        self.upstream.as_ref()
    }

    /// What the client actually received.
    pub const fn delivered(&self) -> &Delivered {
        &self.delivered
    }

    /// Pass, or the lies that applied. It follows from the [explanation](Self::explanation).
    pub fn verdict(&self) -> Verdict {
        self.explanation.verdict()
    }

    /// Why the verdict came out this way: every rule the call touched.
    pub const fn explanation(&self) -> &Explanation {
        &self.explanation
    }

    /// When the call arrived, how long the backend took, and how long the client waited.
    pub const fn timing(&self) -> CallTiming {
        self.timing
    }
}

/// A call record answers with the fields it keeps.
impl FieldSource for Call {
    fn whole_call(&self, field: Field) -> Reading<FieldValues> {
        self.fields.whole_call(field)
    }

    fn parts(&self) -> &[PartFields] {
        self.fields.parts()
    }
}

/// Why the parts of a record are not a [`Call`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallError {
    /// The fields do not have exactly one `connection`, so nothing says which session the call ran on.
    NoConnection,
    /// The response fields still wait for the backend, and a record is of a call that ended.
    TruthPending,
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoConnection => "a call record needs the one `connection` its call ran on",
            Self::TruthPending => {
                "a call record is of a call that ended, and its response fields still wait for the backend"
            }
        })
    }
}

impl std::error::Error for CallError {}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Instant;
    use gaslightmq_kernel::prelude::*;

    use super::*;
    use crate::Answer;

    fn record(fields: Fields) -> Result<Call, CallError> {
        Call::new(
            CallNumber::FIRST,
            fields,
            Delivered::Nothing,
            Explanation::none(),
            CallTiming::arrived(Instant::ZERO),
        )
    }

    fn answered() -> Fields {
        Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::FETCH)
            .with(Outcome::Ok)
    }

    #[test]
    fn a_record_knows_which_call_it_is_of() {
        let call = record(answered()).unwrap();

        assert_eq!(call.connection(), ConnectionNumber::new(7));
        assert_eq!(call.number(), CallNumber::FIRST);
        assert_eq!(call.id(), "#7.1".parse().unwrap());
    }

    #[test]
    fn a_record_needs_the_one_connection_of_its_call() {
        let two = answered().with(ConnectionNumber::new(8));

        assert_eq!(record(Fields::new().with(Outcome::Ok)), Err(CallError::NoConnection));
        assert_eq!(record(two), Err(CallError::NoConnection));
        assert_eq!(
            CallError::NoConnection.to_string(),
            "a call record needs the one `connection` its call ran on"
        );
    }

    #[test]
    fn a_record_is_of_a_call_whose_truth_is_settled() {
        let pending = Fields::new().with(ConnectionNumber::new(7));

        assert_eq!(record(pending.clone()), Err(CallError::TruthPending));
        assert!(record(pending.clone().with(Outcome::Timeout)).is_ok());
        assert!(record(pending.without_truth()).is_ok());
        assert_eq!(
            CallError::TruthPending.to_string(),
            "a call record is of a call that ended, and its response fields still wait for the backend"
        );
    }

    #[test]
    fn a_record_answers_with_its_fields() {
        let partition: TopicPartition = "orders.eu/3".parse().unwrap();
        let fields = answered().with_part(PartFields::new(partition.clone()).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        let call = record(fields.clone()).unwrap();

        assert_eq!(call.fields(), &fields);
        for field in Field::ALL {
            assert_eq!(call.read(field), fields.read(field), "{field}");
        }
        assert_eq!(call.parts(), fields.parts());
        assert_eq!(call.whole_target().to_string(), "orders.eu/3");
    }

    #[test]
    fn a_record_keeps_what_the_real_cluster_answered() {
        let upstream = ResponseSummary::new(Answer::Ok);
        let call = record(answered()).unwrap();

        assert_eq!(call.upstream(), None);
        assert_eq!(call.with_upstream(upstream.clone()).upstream(), Some(&upstream));
    }

    #[test]
    fn a_record_without_a_rule_passes() {
        let call = record(answered()).unwrap();

        assert!(call.verdict().is_pass());
        assert!(call.explanation().is_empty());
        assert_eq!(call.delivered(), &Delivered::Nothing);
        assert_eq!(call.timing().arrived_at(), Instant::ZERO);
    }
}
