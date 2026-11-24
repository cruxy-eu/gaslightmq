//! What the tests of the traffic share: the connection of a client, whose calls end with a record.
//!
//! A traffic takes the records that an engine hands over, and a test writes them the way a session does.
//! A [`ClientConnection`] numbers its calls, and an [`EndingCall`] notes what each rule did to the call
//! before it becomes the record.
//!
//! Every call reaches the backend, and the backend answers it without an error.
//! A lie is therefore told after the response: the truth is `ok`, and the client receives something else.

use std::num::NonZeroU64;
use std::sync::Arc;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::RuleName;

/// One connection of a client. It numbers its calls in the order in which the test makes them.
pub struct ClientConnection {
    client: ClientId,
    connection: ConnectionNumber,
    next: CallNumber,
}

impl ClientConnection {
    /// The first connection that the proxy accepted, of the client with that `client.id`.
    pub fn first(client: ClientId) -> Self {
        Self::accepted(client, ConnectionNumber::new(1))
    }

    /// The second connection that the proxy accepted, of the client with that `client.id`.
    pub fn second(client: ClientId) -> Self {
        Self::accepted(client, ConnectionNumber::new(2))
    }

    fn accepted(client: ClientId, connection: ConnectionNumber) -> Self {
        Self {
            client,
            connection,
            next: CallNumber::FIRST,
        }
    }

    /// The next call of the connection: a Produce to that partition.
    pub fn produce(&mut self, partition: &TopicPartition) -> EndingCall {
        self.call(ApiKey::PRODUCE, Some(partition))
    }

    /// The next call of the connection: a Fetch from that partition.
    pub fn fetch(&mut self, partition: &TopicPartition) -> EndingCall {
        self.call(ApiKey::FETCH, Some(partition))
    }

    /// The next call of the connection: a Heartbeat, which names no partition.
    pub fn heartbeat(&mut self) -> EndingCall {
        self.call(ApiKey::HEARTBEAT, None)
    }

    fn call(&mut self, api: ApiKey, partition: Option<&TopicPartition>) -> EndingCall {
        let number = self.next;
        self.next = number.next();
        let fields = Fields::new()
            .with(self.connection)
            .with(self.client.clone())
            .with(api)
            .with(Outcome::Ok);
        EndingCall {
            number,
            fields,
            partition: partition.cloned(),
            rules: Vec::new(),
        }
    }
}

/// A call that the backend answered, on its way to its record: the rules that it touched, in priority order.
pub struct EndingCall {
    number: CallNumber,
    fields: Fields,
    partition: Option<TopicPartition>,
    rules: Vec<RuleExplanation>,
}

impl EndingCall {
    /// The selector of `rule` matched the call, and its trigger held the rule back.
    pub fn held_by(mut self, rule: &RuleName) -> Self {
        let trigger = Self::trigger(TriggerDecision::Held, TriggerChecks::new().with_count(false));
        let line = RuleExplanation::held(Self::recorded(rule), self.next_priority(), self.target(), trigger);
        self.rules.push(line.expect("a rule that held tells no lie"));
        self
    }

    /// `rule` fired, and its refusal applied: the client receives an error for what the call names.
    pub fn refused_by(self, rule: &RuleName) -> Self {
        let fate = LieFate::all_applied(self.refusal());
        self.lied(rule, fate)
    }

    /// `rule` fired, and its refusal lost everything to the refusal of `winner`, which has the higher priority.
    ///
    /// The winner has its line already: it refused the call first.
    pub fn refused_in_vain_by(self, rule: &RuleName, winner: &RuleName) -> Self {
        let lost = LostTarget::new(self.target(), Self::recorded(winner));
        let fate = LieFate::new(self.refusal(), None, vec![lost], Vec::new());
        self.lied(rule, fate.expect("a lie that lost its target went somewhere"))
    }

    /// The call ended, and this is its record.
    pub fn ended(self) -> Arc<Call> {
        let refused = self.rules.iter().any(RuleExplanation::applied);
        let mut delivered = ResponseSummary::new(Answer::Ok);
        let mut fields = self.fields;
        match (self.partition, refused) {
            (Some(partition), true) => {
                fields = fields.with_part(PartFields::new(partition.clone()).succeeded());
                delivered = delivered.with_part(partition, REFUSAL);
            }
            (Some(partition), false) => fields = fields.with_part(PartFields::new(partition).succeeded()),
            (None, true) => delivered = ResponseSummary::new(REFUSAL),
            (None, false) => {}
        }
        let explanation = Explanation::new(self.rules).expect("one line for each rule, and every winner has its own");
        let record = Call::new(
            self.number,
            fields,
            delivered.into(),
            explanation,
            CallTiming::arrived(Instant::ZERO),
        );
        Arc::new(record.expect("a call on a connection, with its truth"))
    }

    fn lied(mut self, rule: &RuleName, fate: LieFate) -> Self {
        let trigger = Self::trigger(TriggerDecision::Fired, TriggerChecks::new());
        let line = RuleExplanation::lied(Self::recorded(rule), self.next_priority(), self.target(), trigger, fate);
        self.rules.push(line.expect("a rule that fired tells its lie"));
        self
    }

    /// What the selector of every rule matches: the partition of the call, or the call when it names none.
    fn target(&self) -> Target {
        let parts = self.partition.clone().map(Part::from);
        Target::of_parts(parts).unwrap_or(Target::Call)
    }

    /// The refusal of every rule that lies, on what its selector matched.
    fn refusal(&self) -> TargetedLie {
        LieDefinition::from(RefusalLie::new(REFUSAL).at(Stage::AfterResponse)).on(self.target())
    }

    /// The rules of a call stand in the order in which the test names them.
    fn next_priority(&self) -> Priority {
        Priority::new(self.rules.len())
    }

    /// What a trigger decided. A traffic does not read the match number, so every call is the first match.
    fn trigger(decision: TriggerDecision, checks: TriggerChecks) -> TriggerExplanation {
        TriggerExplanation::new(decision, Some(NonZeroU64::MIN), checks).expect("a decision that fits its checks")
    }

    /// The name of the rule, as the record of a call keeps it.
    fn recorded(rule: &RuleName) -> RecordedRuleName {
        RecordedRuleName::new(rule.as_str()).expect("a record keeps the name of every rule")
    }
}

/// The error that every refusal of these tests answers with.
const REFUSAL: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
