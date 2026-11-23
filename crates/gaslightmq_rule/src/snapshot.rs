use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::Priority;
use gaslightmq_selector::{ApiIndex, Pass};
use gaslightmq_trigger::SessionCounters;

use crate::{ArmedRule, PassEvaluation, RuleEvaluation, RuleName};

/// The armed rules at one moment, in priority order.
///
/// The engine builds a snapshot whenever a rule is armed, disarmed or changed, and publishes it to every session.
/// A snapshot never changes.
/// A call in flight finishes on the snapshot it started with, and the next call uses the new one.
///
/// # Order
///
/// The rules come in the order in which they win a conflict: the first rule beats every rule after it.
/// The order comes from outside: the live set first, then the rules of the active scenario as its mode orders them.
/// A snapshot only keeps it.
///
/// # Evaluating a call
///
/// [`evaluate`](Self::evaluate) runs one pass for one call.
/// It finds the rules whose selector matches, through an index by API for each pass,
/// evaluates the trigger of each, and says what every rule that fired does.
/// It resolves no conflict between lies, and it applies nothing.
///
/// The counters, budgets and dice of the rules live in their [armings](ArmedRule::arming), and not in the snapshot.
/// A new snapshot with the same armed rules therefore continues every count.
///
/// ```
/// use gaslightmq_clock::{Instant, TimeSpan};
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let none = (SelectorSet::default(), PresetSet::default());
/// let now = Instant::ZERO;
///
/// // "Refuse the 2nd Produce", above a delay on everything.
/// let refuse = Rule::new(
///     "Refuse the 2nd Produce".parse().expect("a rule name"),
///     Conditions::from(Condition::is(ApiKey::PRODUCE)),
///     RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
/// )
/// .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(2).expect("a match number")));
/// let delay = Rule::new(
///     "Delay 1 s".parse().expect("a rule name"),
///     Conditions::new(),
///     TimingLie::new().with_delay(TimeSpan::from_secs(1)),
/// );
/// let snapshot = Snapshot::new([
///     refuse.resolve(&none.0, &none.1).expect("a rule that resolves").arm(Seed::new(42), now),
///     delay.resolve(&none.0, &none.1).expect("a rule that resolves").arm(Seed::new(42), now),
/// ])?;
///
/// let partition: TopicPartition = "orders.eu/3".parse().expect("a topic partition");
/// let produce = Fields::new().with(ApiKey::PRODUCE).with_part(partition);
/// let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").expect("a counter key"));
///
/// // Both rules match the 1st Produce, and only the delay fires.
/// let first = snapshot.evaluate(Pass::Request, &produce, now, &mut session);
/// assert_eq!(first.len(), 2);
/// assert_eq!(first.lies().map(|(_, lie)| lie.to_string()).collect::<Vec<_>>(), ["+1 s"]);
///
/// // On the 2nd, the refusal fires too, and it comes first.
/// let second = snapshot.evaluate(Pass::Request, &produce, now, &mut session);
/// assert_eq!(
///     second.lies().map(|(_, lie)| lie.to_string()).collect::<Vec<_>>(),
///     ["NOT_LEADER_OR_FOLLOWER on orders.eu/3", "+1 s"]
/// );
/// # Ok::<(), SnapshotError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The armed rules, in priority order.
    rules: Vec<ArmedRule>,
    by_name: BTreeMap<RuleName, Priority>,
    /// The rules that are decided before forwarding, by API.
    request: ApiIndex<Priority>,
    /// The rules that wait for the truth, by API.
    response: ApiIndex<Priority>,
}

impl Snapshot {
    /// The snapshot of those armed rules, in priority order: the rule that wins every conflict first.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::DuplicateName`] when two of the rules have one name.
    /// An explanation names a rule, so one name is one rule.
    pub fn new(rules: impl IntoIterator<Item = ArmedRule>) -> Result<Self, SnapshotError> {
        let rules: Vec<ArmedRule> = rules.into_iter().collect();

        let mut by_name = BTreeMap::new();
        for (position, rule) in rules.iter().enumerate() {
            if by_name.insert(rule.name().clone(), Priority::new(position)).is_some() {
                return Err(SnapshotError::DuplicateName {
                    name: rule.name().clone(),
                });
            }
        }

        let index = |pass: Pass| -> ApiIndex<Priority> {
            let decided_in_pass = rules.iter().enumerate().filter(|(_, rule)| rule.rule().pass() == pass);
            decided_in_pass
                .map(|(position, rule)| (Priority::new(position), rule.rule().selector().clone()))
                .collect()
        };
        let request = index(Pass::Request);
        let response = index(Pass::Response);

        Ok(Self {
            rules,
            by_name,
            request,
            response,
        })
    }

    /// How many rules are armed.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether no rule is armed.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The armed rules, in priority order.
    pub fn iter(&self) -> std::slice::Iter<'_, ArmedRule> {
        self.rules.iter()
    }

    /// The armed rule of this name.
    pub fn get(&self, name: &RuleName) -> Option<&ArmedRule> {
        self.rule(self.priority_of(name)?)
    }

    /// The armed rule at this priority.
    pub fn rule(&self, priority: Priority) -> Option<&ArmedRule> {
        self.rules.get(priority.position())
    }

    /// Where the armed rule of this name stands.
    pub fn priority_of(&self, name: &RuleName) -> Option<Priority> {
        self.by_name.get(name).copied()
    }

    /// The rules of `pass` whose selector can match a call of `api`, in priority order.
    ///
    /// The selector of a candidate is not tested yet.
    /// Whoever decodes a request lazily asks the candidates which fields they test.
    pub fn candidates(&self, pass: Pass, api: ApiKey) -> impl Iterator<Item = &ArmedRule> {
        let candidates = self.index(pass).candidates(api);
        candidates.filter_map(|(priority, _)| self.rule(*priority))
    }

    /// Runs one evaluation pass for one call.
    ///
    /// 1. It finds the rules of `pass` whose selector matches `call`, with the target each one matched.
    /// 2. It evaluates the trigger of each: the call gets a match number from the shared counter or from `session`,
    ///    the die rolls, and the time window is checked against `now`.
    ///    A rule that fires sets one firing of its budget aside.
    /// 3. It says what each rule that fired does: its lie on its target, or the incidents on the subjects of the call.
    ///
    /// The result holds every rule that matched, in priority order.
    /// The caller resolves the lies, passes the incidents on, and settles every firing.
    ///
    /// Run the [request pass](Pass::Request) before forwarding.
    /// Run the [response pass](Pass::Response) once `call` has the truth:
    /// a rule whose selector still waits for a response field is left out.
    pub fn evaluate<S: FieldSource + ?Sized>(
        &self,
        pass: Pass,
        call: &S,
        now: Instant,
        session: &mut SessionCounters,
    ) -> PassEvaluation {
        let mut rules = Vec::new();
        for (priority, target) in self.index(pass).matches(call) {
            if let Some(rule) = self.rule(*priority) {
                rules.push(RuleEvaluation::of(rule, *priority, target, call, now, session));
            }
        }
        PassEvaluation::new(pass, rules)
    }

    /// The rules that wait for the truth of `call` at `now`, in priority order.
    ///
    /// These are the rules of the response pass whose conditions on the request hold.
    /// When a silence or a hang-up ends the call before forwarding, the response pass never runs,
    /// and the explanation of the call lists these rules as pre-empted.
    /// They count no match, because nobody evaluated them.
    ///
    /// A rule whose lifetime has ended at `now` is not among them, even before the engine disarms it.
    /// It could not fire anymore, so no lie took anything from it.
    pub fn waiting<S: FieldSource + ?Sized>(&self, call: &S, now: Instant) -> Vec<&ArmedRule> {
        let waits =
            |rule: &&ArmedRule| !rule.arming().has_ended(now) && rule.rule().selector().evaluate(call).is_pending();
        // A call always has one API. For a source that says otherwise, every selector decides for itself.
        let apis = call.read(Field::Api).present();
        if let Some([FieldValue::Api(api)]) = apis.as_ref().map(|apis| apis.iter().as_slice()) {
            self.candidates(Pass::Response, *api).filter(waits).collect()
        } else {
            let of_the_response_pass = self.rules.iter().filter(|rule| rule.rule().pass() == Pass::Response);
            of_the_response_pass.filter(waits).collect()
        }
    }

    fn index(&self, pass: Pass) -> &ApiIndex<Priority> {
        match pass {
            Pass::Request => &self.request,
            Pass::Response => &self.response,
        }
    }
}

impl<'a> IntoIterator for &'a Snapshot {
    type Item = &'a ArmedRule;
    type IntoIter = std::slice::Iter<'a, ArmedRule>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Why armed rules are not a [`Snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotError {
    /// Two of the rules have the same name.
    DuplicateName { name: RuleName },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateName { name } => write!(f, "two rules are named {:?}", name.as_str()),
        }
    }
}

impl std::error::Error for SnapshotError {}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    use super::*;
    use crate::{FiredAction, HandFiring, PresetSet, Rule};

    const NOW: Instant = Instant::ZERO;

    fn name(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    fn count(value: u64) -> Count {
        Count::new(value).unwrap()
    }

    fn nth(number: u64) -> Trigger {
        TriggerParts::new().with_nth(MatchNumber::new(number).unwrap()).into()
    }

    fn arm(rule: &Rule) -> ArmedRule {
        rule.resolve(&SelectorSet::default(), &PresetSet::default())
            .unwrap()
            .arm(Seed::new(42), NOW)
    }

    fn snapshot<const N: usize>(rules: [&Rule; N]) -> Snapshot {
        Snapshot::new(rules.map(arm)).unwrap()
    }

    fn session() -> SessionCounters {
        SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").unwrap())
    }

    fn api(api: ApiKey) -> Conditions {
        Condition::is(api).into()
    }

    fn orders(api: ApiKey) -> Conditions {
        Conditions::new()
            .with(Condition::is(api))
            .with(Condition::parse(Field::Topic, "orders.*").unwrap())
    }

    fn succeeded(api: ApiKey) -> Conditions {
        Conditions::new()
            .with(Condition::is(api))
            .with(Condition::is(Outcome::Ok))
    }

    fn flap() -> RefusalLie {
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)
    }

    fn delay() -> TimingLie {
        TimingLie::new().with_delay(TimeSpan::from_millis(750))
    }

    fn lost() -> RefusalLie {
        RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse)
    }

    fn leader_moves() -> IncidentDefinition {
        IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        }
    }

    fn rule(text: &str, selector: Conditions, action: impl Into<crate::ActionRef>) -> Rule {
        Rule::new(name(text), selector, action)
    }

    fn partition(text: &str) -> TopicPartition {
        text.parse().unwrap()
    }

    /// A Produce for two orders partitions and one other.
    fn produce() -> Fields {
        Fields::new()
            .with(Endpoint::Broker(BrokerId::from(2)))
            .with(ClientId::new("order-ingest").unwrap())
            .with(ApiKey::PRODUCE)
            .with_part(partition("orders.eu/3"))
            .with_part(partition("orders.eu/4"))
            .with_part(partition("payments.settled/0"))
    }

    fn heartbeat() -> Fields {
        Fields::new()
            .with(Endpoint::Broker(BrokerId::from(2)))
            .with(ApiKey::HEARTBEAT)
            .with(GroupId::new("analytics-etl").unwrap())
    }

    fn names(pass: &PassEvaluation) -> Vec<&str> {
        pass.rules().iter().map(|rule| rule.name().as_str()).collect()
    }

    fn lies(pass: &PassEvaluation) -> Vec<String> {
        pass.lies().map(|(_, lie)| lie.to_string()).collect()
    }

    fn outcomes(pass: &PassEvaluation) -> Vec<TriggerOutcome> {
        pass.rules().iter().map(|rule| rule.rolled().outcome()).collect()
    }

    #[test]
    fn sessions_on_different_threads_share_a_snapshot() {
        fn shared<T: Send + Sync>() {}
        fn handed_over<T: Send>() {}

        shared::<Snapshot>();
        shared::<ArmedRule>();
        // A call in flight keeps its evaluation, on whichever thread its session runs.
        handed_over::<PassEvaluation>();
        handed_over::<RuleEvaluation>();
    }

    #[test]
    fn a_priority_is_a_position_and_the_first_is_the_highest() {
        let first = Priority::HIGHEST;
        let third = Priority::new(2);

        assert_eq!(first, Priority::new(0));
        assert_eq!(third.position(), 2);
        assert!(first.outranks(third));
        assert!(!third.outranks(first));
        assert!(!third.outranks(third));
        assert!(first < third);
        assert_eq!([first.to_string(), third.to_string()], ["#1", "#3"]);
        assert_eq!(Priority::new(usize::MAX).to_string(), format!("#{}", usize::MAX));
    }

    #[test]
    fn a_snapshot_keeps_its_rules_in_the_order_it_is_given() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap());
        let slow = rule("Slow", Conditions::new(), delay());
        let snapshot = snapshot([&refuse, &slow]);

        assert_eq!(snapshot.len(), 2);
        assert!(!snapshot.is_empty());
        assert_eq!(
            snapshot.iter().map(ArmedRule::name).collect::<Vec<_>>(),
            [&name("Refuse"), &name("Slow")]
        );
        assert_eq!((&snapshot).into_iter().count(), 2);
        assert_eq!(snapshot.priority_of(&name("Refuse")), Some(Priority::HIGHEST));
        assert_eq!(snapshot.priority_of(&name("Slow")), Some(Priority::new(1)));
        assert_eq!(snapshot.priority_of(&name("Fast")), None);
        assert_eq!(snapshot.get(&name("Slow")).map(ArmedRule::name), Some(&name("Slow")));
        assert_eq!(
            snapshot.rule(Priority::new(1)).map(ArmedRule::name),
            Some(&name("Slow"))
        );
        assert!(snapshot.get(&name("Fast")).is_none());
        assert!(snapshot.rule(Priority::new(2)).is_none());
    }

    #[test]
    fn a_snapshot_without_rules_matches_nothing() {
        let snapshot = Snapshot::default();

        assert!(snapshot.is_empty());
        assert_eq!(Snapshot::new([]).unwrap().len(), 0);
        assert!(
            snapshot
                .evaluate(Pass::Request, &produce(), NOW, &mut session())
                .is_empty()
        );
        assert!(snapshot.waiting(&produce(), NOW).is_empty());
    }

    #[test]
    fn two_armed_rules_with_one_name_are_an_error() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap());
        let twice = rule("Refuse", Conditions::new(), delay());

        let error = Snapshot::new([arm(&refuse), arm(&twice)]).unwrap_err();

        assert_eq!(error, SnapshotError::DuplicateName { name: name("Refuse") });
    }

    #[test]
    fn the_candidates_of_an_api_are_the_rules_of_the_pass_that_can_match_it() {
        let snapshot = snapshot([
            &rule("Refuse produce", api(ApiKey::PRODUCE), flap()),
            &rule("Lost produce", succeeded(ApiKey::PRODUCE), lost()),
            &rule("Slow", Conditions::new(), delay()),
            &rule("Refuse fetch", api(ApiKey::FETCH), flap()),
        ]);
        let candidates = |pass, api| -> Vec<&str> {
            let candidates = snapshot.candidates(pass, api);
            candidates.map(|rule| rule.name().as_str()).collect()
        };

        assert_eq!(candidates(Pass::Request, ApiKey::PRODUCE), ["Refuse produce", "Slow"]);
        assert_eq!(candidates(Pass::Request, ApiKey::FETCH), ["Slow", "Refuse fetch"]);
        assert_eq!(candidates(Pass::Response, ApiKey::PRODUCE), ["Lost produce"]);
        assert_eq!(candidates(Pass::Response, ApiKey::FETCH), [""; 0]);
    }

    #[test]
    fn a_pass_holds_the_rules_that_matched_in_priority_order_with_their_targets() {
        let snapshot = snapshot([
            &rule("Refuse fetch", api(ApiKey::FETCH), flap()),
            &rule("Refuse orders", orders(ApiKey::PRODUCE), flap()),
            &rule("Slow", Conditions::new(), delay()),
        ]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());

        assert_eq!(pass.pass(), Pass::Request);
        assert_eq!(pass.len(), 2);
        assert_eq!(names(&pass), ["Refuse orders", "Slow"]);
        let [refuse, slow] = pass.rules() else {
            panic!("two rules matched");
        };
        assert_eq!(refuse.priority(), Priority::new(1));
        assert_eq!(refuse.target().to_string(), "orders.eu/3, orders.eu/4");
        assert_eq!(slow.priority(), Priority::new(2));
        assert_eq!(
            slow.target().to_string(),
            "orders.eu/3, orders.eu/4, payments.settled/0"
        );
        assert!(refuse.rule().is(snapshot.get(&name("Refuse orders")).unwrap()));
    }

    #[test]
    fn a_lie_gets_the_target_the_selector_matched() {
        let snapshot = snapshot([&rule("Refuse orders", orders(ApiKey::PRODUCE), flap())]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());
        let refuse = &pass.rules()[0];

        assert!(refuse.fired());
        assert_eq!(refuse.rolled().outcome(), TriggerOutcome::Fired);
        assert_eq!(
            refuse.fired_action(),
            Some(&FiredAction::Lie(
                LieDefinition::from(flap()).on(refuse.target().clone())
            ))
        );
        assert_eq!(
            refuse.lie().map(ToString::to_string),
            Some("NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.eu/4".to_owned())
        );
        assert_eq!(refuse.incidents(), []);
        assert_eq!(refuse.missing_subject(), None);
    }

    #[test]
    fn a_lie_on_the_whole_call_keeps_what_the_selector_matched_next_to_it() {
        let snapshot = snapshot([&rule("Slow orders", orders(ApiKey::PRODUCE), delay())]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());
        let slow = &pass.rules()[0];

        assert_eq!(slow.target().to_string(), "orders.eu/3, orders.eu/4");
        assert_eq!(slow.lie().map(TargetedLie::target), Some(&Target::Call));
        assert_eq!(lies(&pass), ["+750 ms"]);
    }

    #[test]
    fn a_rule_that_does_not_fire_is_still_in_the_pass() {
        let snapshot = snapshot([&rule("Refuse the 2nd", api(ApiKey::PRODUCE), flap()).with_trigger(nth(2))]);
        let mut session = session();

        let first = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        let held = &first.rules()[0];
        assert!(!held.fired());
        assert_eq!(held.rolled().outcome(), TriggerOutcome::Held);
        assert_eq!(held.rolled().match_number(), Some(MatchNumber::new(1).unwrap()));
        assert_eq!(held.fired_action(), None);
        assert_eq!(held.lie(), None);
        assert!(!held.is_unsettled());
        assert_eq!(first.fired().count(), 0);
        assert_eq!(lies(&first), [""; 0]);

        let second = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&second), [TriggerOutcome::Fired]);
        assert_eq!(second.fired().count(), 1);
    }

    #[test]
    fn the_lies_of_a_pass_come_in_priority_order_with_their_priority() {
        let snapshot = snapshot([
            &rule("Slow", Conditions::new(), delay()),
            &rule("Never", api(ApiKey::PRODUCE), LieDefinition::Silence).with_trigger(Trigger::Manual),
            &rule("Election", orders(ApiKey::PRODUCE), leader_moves()),
            &rule("Refuse orders", orders(ApiKey::PRODUCE), flap()),
        ]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());

        assert_eq!(names(&pass), ["Slow", "Never", "Election", "Refuse orders"]);
        assert_eq!(
            pass.lies()
                .map(|(priority, lie)| (priority.position(), lie.to_string()))
                .collect::<Vec<_>>(),
            [
                (0, "+750 ms".to_owned()),
                (3, "NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.eu/4".to_owned())
            ]
        );
        assert_eq!(
            pass.fired().map(|rule| rule.name().as_str()).collect::<Vec<_>>(),
            ["Slow", "Election", "Refuse orders"]
        );
    }

    #[test]
    fn an_incident_without_a_subject_takes_it_from_the_parts_the_selector_matched() {
        let snapshot = snapshot([&rule("Election", orders(ApiKey::PRODUCE), leader_moves())]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());

        let moved = |text: &str| Incident::LeaderMoves {
            partition: partition(text),
            to: None,
        };
        assert_eq!(
            pass.rules()[0].incidents(),
            [moved("orders.eu/3"), moved("orders.eu/4")]
        );
        assert_eq!(
            pass.incidents().collect::<Vec<_>>(),
            [&moved("orders.eu/3"), &moved("orders.eu/4")]
        );
        assert_eq!(pass.rules()[0].lie(), None);
        assert_eq!(lies(&pass), [""; 0]);
    }

    #[test]
    fn an_incident_that_names_its_subject_ignores_the_call() {
        let named = IncidentDefinition::LeaderMoves {
            partition: Some(partition("payments.settled/0")),
            to: Some(BrokerId::from(1)),
        };
        let snapshot = snapshot([&rule("Election", orders(ApiKey::PRODUCE), named)]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());

        assert_eq!(
            pass.rules()[0].incidents(),
            [Incident::LeaderMoves {
                partition: partition("payments.settled/0"),
                to: Some(BrokerId::from(1)),
            }]
        );
    }

    #[test]
    fn an_incident_takes_the_broker_the_groups_and_the_transactional_ids_from_the_fields_of_the_call() {
        let down = IncidentDefinition::BrokerGoesDown {
            broker: None,
            failover: true,
        };
        let coordinator_moves = IncidentDefinition::CoordinatorMoves {
            coordinator: None,
            to: None,
        };
        let topic_deleted = IncidentDefinition::TopicDeleted { topic: None };
        let snapshot = snapshot([
            &rule("Broker down", Conditions::new(), down),
            &rule("Coordinators move", Conditions::new(), coordinator_moves),
            &rule("Topic deleted", Conditions::new(), topic_deleted),
        ]);
        let group = |id: &str| GroupId::new(id).unwrap();
        let transactional_id = TransactionalId::new("payments-tx-1").unwrap();
        let orders = TopicName::new("orders.eu").unwrap();
        let call = Fields::new()
            .with(Endpoint::Broker(BrokerId::from(2)))
            .with(ApiKey::new(9_999))
            .with_learned(group("payments"))
            .with_learned(group("analytics-etl"))
            .with(transactional_id.clone())
            .with_part(orders.clone());

        let pass = snapshot.evaluate(Pass::Request, &call, NOW, &mut session());

        assert_eq!(
            pass.incidents().cloned().collect::<Vec<_>>(),
            [
                Incident::BrokerGoesDown {
                    broker: BrokerId::from(2),
                    failover: true
                },
                Incident::CoordinatorMoves {
                    coordinator: group("analytics-etl").into(),
                    to: None
                },
                Incident::CoordinatorMoves {
                    coordinator: group("payments").into(),
                    to: None
                },
                Incident::CoordinatorMoves {
                    coordinator: transactional_id.into(),
                    to: None
                },
                Incident::TopicDeleted { topic: orders },
            ]
        );
    }

    #[test]
    fn a_call_on_the_bootstrap_endpoint_offers_no_broker() {
        let down = IncidentDefinition::BrokerGoesUnresponsive { broker: None };
        let snapshot = snapshot([&rule("Broker hangs", Conditions::new(), down)]);
        let call = Fields::new().with(Endpoint::Bootstrap).with(ApiKey::METADATA);

        let pass = snapshot.evaluate(Pass::Request, &call, NOW, &mut session());

        assert_eq!(
            pass.rules()[0].missing_subject(),
            Some(&IncidentError::MissingSubject {
                incident: IncidentKind::BrokerGoesUnresponsive,
                subject: "broker",
            })
        );
        assert_eq!(outcomes(&pass), [TriggerOutcome::NothingToActOn]);
    }

    #[test]
    fn the_record_and_the_evaluation_agree_on_whether_a_rule_fired() {
        let snapshot = snapshot([
            &rule("Slow", Conditions::new(), delay()),
            &rule("Never", Conditions::new(), LieDefinition::Silence).with_trigger(Trigger::Manual),
            &rule("Refuse the 2nd", Conditions::new(), flap()).with_trigger(nth(2)),
            &rule("Election", Conditions::new(), leader_moves()),
            &rule("Refuse once", Conditions::new(), flap()).with_lifetime(Lifetime::Firings(count(1))),
        ]);
        let mut session = session();
        let mut seen = Vec::new();

        // A Produce offers a partition to the election, and a Heartbeat offers none.
        for call in [produce(), heartbeat(), produce(), heartbeat()] {
            let mut pass = snapshot.evaluate(Pass::Request, &call, NOW, &mut session);
            for evaluated in &mut pass {
                assert_eq!(
                    evaluated.fired(),
                    evaluated.rolled().outcome().fired(),
                    "{}",
                    evaluated.name()
                );
                assert_eq!(evaluated.fired(), evaluated.is_unsettled(), "{}", evaluated.name());
                seen.push(evaluated.rolled().outcome());
                evaluated.confirm();
            }
        }

        for outcome in [
            TriggerOutcome::Fired,
            TriggerOutcome::Held,
            TriggerOutcome::NothingToActOn,
            TriggerOutcome::LifetimeOver,
        ] {
            assert!(seen.contains(&outcome), "{outcome:?}");
        }
    }

    #[test]
    fn an_incident_without_a_subject_on_the_call_does_not_happen_and_gives_its_firing_back() {
        let election = rule("Election", Conditions::new(), leader_moves()).with_lifetime(Lifetime::Firings(count(1)));
        let snapshot = snapshot([&election]);
        let armed = snapshot.get(&name("Election")).unwrap();
        let mut session = session();

        let mut pass = snapshot.evaluate(Pass::Request, &heartbeat(), NOW, &mut session);
        let missed = &mut pass.rules_mut()[0];

        // The trigger agreed, and the call counts as a match. The record does not say that the rule fired.
        assert_eq!(missed.rolled().outcome(), TriggerOutcome::NothingToActOn);
        assert!(!missed.rolled().outcome().fired());
        assert_eq!(missed.rolled().match_number(), Some(MatchNumber::new(1).unwrap()));
        assert_eq!(missed.rolled().budget().map(BudgetReading::left), Some(1));
        assert_eq!(
            missed.missing_subject(),
            Some(&IncidentError::MissingSubject {
                incident: IncidentKind::LeaderMoves,
                subject: "partition",
            })
        );
        // Nothing happened, so there is nothing to settle.
        assert!(!missed.fired());
        assert_eq!(missed.fired_action(), None);
        assert_eq!(missed.incidents(), []);
        assert!(!missed.is_unsettled());
        assert!(missed.take_firing().is_none());
        assert_eq!(pass.incidents().count(), 0);
        assert_eq!(armed.arming().budget().map(Budget::left), Some(1));

        // The next call that has a partition takes the firing.
        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(pass.incidents().count(), 3);
        assert_eq!(
            pass.rules()[0].rolled().match_number(),
            Some(MatchNumber::new(2).unwrap())
        );
    }

    #[test]
    fn confirming_a_firing_counts_it_for_the_lifetime() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap()).with_lifetime(Lifetime::Firings(count(2)));
        let snapshot = snapshot([&refuse]);
        let armed = snapshot.get(&name("Refuse")).unwrap();
        let mut session = session();

        let mut first = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        let fired = &mut first.rules_mut()[0];
        assert!(fired.is_unsettled());
        assert_eq!(fired.confirm().map(Confirmed::ends_lifetime), Some(false));
        // The evaluation keeps what it says, for the explanation of the call.
        assert!(!fired.is_unsettled());
        assert!(fired.fired());
        assert!(fired.lie().is_some());
        assert_eq!(fired.confirm(), None);

        let mut second = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(
            second.rules_mut()[0].confirm().map(Confirmed::ends_lifetime),
            Some(true)
        );
        assert!(armed.arming().has_ended(NOW));

        let third = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&third), [TriggerOutcome::LifetimeOver]);
        assert_eq!(third.rules()[0].rolled().match_number(), None);
        assert!(!third.rules()[0].fired());
    }

    #[test]
    fn withdrawing_a_firing_gives_it_back_to_the_budget() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap()).with_lifetime(Lifetime::Firings(count(1)));
        let snapshot = snapshot([&refuse]);
        let armed = snapshot.get(&name("Refuse")).unwrap();
        let mut session = session();

        let mut shadowed = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        // While the firing is set aside, another call finds the budget empty.
        let meanwhile = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&meanwhile), [TriggerOutcome::BudgetEmpty]);
        assert!(!meanwhile.rules()[0].fired());

        shadowed.rules_mut()[0].withdraw();
        assert!(!shadowed.rules()[0].is_unsettled());
        assert_eq!(armed.arming().budget().map(Budget::left), Some(1));
        // The evaluation keeps what it said: the rule fired, and another rule shadowed it.
        assert_eq!(outcomes(&shadowed), [TriggerOutcome::Fired]);
        assert!(shadowed.rules()[0].fired());

        let later = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&later), [TriggerOutcome::Fired]);
    }

    #[test]
    fn a_pass_that_is_dropped_gives_its_firings_back() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap()).with_lifetime(Lifetime::Firings(count(1)));
        let snapshot = snapshot([&refuse]);
        let armed = snapshot.get(&name("Refuse")).unwrap();

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());
        assert_eq!(armed.arming().budget().map(Budget::left), Some(0));
        drop(pass);

        assert_eq!(armed.arming().budget().map(Budget::left), Some(1));
        assert!(!armed.arming().has_ended(NOW));
    }

    #[test]
    fn the_firing_can_be_taken_out_to_settle_it_elsewhere() {
        let snapshot = snapshot([&rule("Refuse", api(ApiKey::PRODUCE), flap())]);

        let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());
        let mut rules = pass.into_rules();
        let firing = rules[0].take_firing().expect("the rule fired");

        assert!(!firing.by_hand());
        assert!(!firing.confirm().ends_lifetime());
        assert!(rules[0].take_firing().is_none());
    }

    #[test]
    fn a_hand_firing_of_a_lie_shows_in_the_next_matching_call() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap()).with_trigger(Trigger::Manual);
        let snapshot = snapshot([&refuse]);
        let mut session = session();

        let before = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&before), [TriggerOutcome::Held]);

        assert!(
            snapshot
                .get(&name("Refuse"))
                .unwrap()
                .fire_by_hand(NOW)
                .unwrap()
                .fired()
        );
        let mut next = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&next), [TriggerOutcome::FiredByHand]);
        assert_eq!(
            next.rules()[0].rolled().match_number(),
            Some(MatchNumber::new(2).unwrap())
        );
        let firing = next.rules_mut()[0].take_firing().expect("the rule fired");
        assert!(firing.by_hand());
        // The refusal applied, which uses the hand firing up.
        firing.confirm();

        let after = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&after), [TriggerOutcome::Held]);
    }

    #[test]
    fn a_hand_firing_of_a_lie_that_is_withdrawn_lies_on_the_next_matching_call() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap()).with_trigger(Trigger::Manual);
        let snapshot = snapshot([&refuse]);
        let armed = snapshot.get(&name("Refuse")).unwrap();
        let mut session = session();

        // Someone presses the pad once.
        assert!(matches!(armed.fire_by_hand(NOW), Ok(HandFiring::OnNextMatch)));
        assert_eq!(armed.arming().waiting_by_hand(), 1);

        // The next Produce takes the hand firing, and a rule with a higher priority shadows the refusal.
        let mut shadowed = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&shadowed), [TriggerOutcome::FiredByHand]);
        assert_eq!(armed.arming().waiting_by_hand(), 0);
        shadowed.rules_mut()[0].withdraw();

        // The press is not used up, so the Produce after it gets the refusal.
        assert_eq!(armed.arming().waiting_by_hand(), 1);
        let mut next = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&next), [TriggerOutcome::FiredByHand]);
        assert_eq!(
            lies(&next),
            ["NOT_LEADER_OR_FOLLOWER on orders.eu/3, orders.eu/4, payments.settled/0"]
        );
        assert!(next.rules_mut()[0].confirm().is_some());

        // One press is one lie.
        assert_eq!(armed.arming().waiting_by_hand(), 0);
        let after = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&after), [TriggerOutcome::Held]);
    }

    #[test]
    fn a_hand_firing_of_a_call_that_ended_early_lies_on_the_next_matching_call() {
        let refuse = rule("Refuse", api(ApiKey::PRODUCE), flap())
            .with_trigger(Trigger::Manual)
            .with_lifetime(Lifetime::Firings(count(1)));
        let snapshot = snapshot([&refuse]);
        let armed = snapshot.get(&name("Refuse")).unwrap();
        let mut session = session();
        assert!(armed.fire_by_hand(NOW).unwrap().fired());

        // The connection closes before the call settles its firing.
        let dropped = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&dropped), [TriggerOutcome::FiredByHand]);
        drop(dropped);

        // Both the firing of the budget and the press are back.
        assert_eq!(armed.arming().budget().map(Budget::left), Some(1));
        assert_eq!(armed.arming().waiting_by_hand(), 1);
        let mut next = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&next), [TriggerOutcome::FiredByHand]);
        assert_eq!(next.rules_mut()[0].confirm().map(Confirmed::ends_lifetime), Some(true));
        assert_eq!(armed.arming().waiting_by_hand(), 0);
    }

    #[test]
    fn a_rule_that_tests_the_truth_is_in_the_response_pass_only() {
        let snapshot = snapshot([
            &rule("Lost acknowledgement", succeeded(ApiKey::PRODUCE), lost()),
            &rule("Slow", api(ApiKey::PRODUCE), delay()),
        ]);
        let mut session = session();
        let asked = produce();

        let request = snapshot.evaluate(Pass::Request, &asked, NOW, &mut session);
        assert_eq!(names(&request), ["Slow"]);
        assert_eq!(request.rules()[0].priority(), Priority::new(1));

        // Before the backend answers, the response pass has nothing to decide.
        let too_early = snapshot.evaluate(Pass::Response, &asked, NOW, &mut session);
        assert!(too_early.is_empty());

        let answered = asked.with(Outcome::Ok);
        let later = NOW + TimeSpan::from_millis(14);
        let response = snapshot.evaluate(Pass::Response, &answered, later, &mut session);
        assert_eq!(response.pass(), Pass::Response);
        assert_eq!(names(&response), ["Lost acknowledgement"]);
        assert_eq!(response.rules()[0].priority(), Priority::HIGHEST);
        assert_eq!(
            lies(&response),
            ["REQUEST_TIMED_OUT after the response on orders.eu/3, orders.eu/4, payments.settled/0"]
        );
        // The rule of the request pass ran once, and the rule of the response pass too.
        let matches = |rule: &str| snapshot.get(&name(rule)).unwrap().arming().counter().matches();
        assert_eq!((matches("Slow"), matches("Lost acknowledgement")), (1, 1));
    }

    #[test]
    fn the_response_pass_targets_the_parts_whose_truth_passes() {
        let snapshot = snapshot([&rule("Lost acknowledgement", succeeded(ApiKey::PRODUCE), lost())]);
        let answered = Fields::new()
            .with(ApiKey::PRODUCE)
            .with_part(PartFields::new(partition("orders.eu/3")).succeeded())
            .with_part(PartFields::new(partition("orders.eu/4")).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER));

        let response = snapshot.evaluate(Pass::Response, &answered, NOW, &mut session());

        assert_eq!(response.rules()[0].target().to_string(), "orders.eu/3");

        let failed = Fields::new().with(ApiKey::PRODUCE).with(Outcome::Error);
        assert!(
            snapshot
                .evaluate(Pass::Response, &failed, NOW, &mut session())
                .is_empty()
        );
    }

    #[test]
    fn the_rules_that_wait_for_the_truth_are_those_whose_request_conditions_hold() {
        let snapshot = snapshot([
            &rule("Lost produce", succeeded(ApiKey::PRODUCE), lost()),
            &rule("Slow", Conditions::new(), delay()),
            &rule("Lost fetch", succeeded(ApiKey::FETCH), lost()),
            &rule("Slow success", Condition::is(Outcome::Ok).into(), delay()),
        ]);
        let waiting = |call: &Fields| -> Vec<String> {
            let waiting = snapshot.waiting(call, NOW);
            waiting.into_iter().map(|rule| rule.name().to_string()).collect()
        };

        assert_eq!(waiting(&produce()), ["Lost produce", "Slow success"]);
        assert_eq!(waiting(&heartbeat()), ["Slow success"]);
        // Once the truth is there, nobody waits.
        assert_eq!(waiting(&produce().with(Outcome::Ok)), [""; 0]);
        assert_eq!(waiting(&produce().without_truth()), [""; 0]);
        // A source without one API asks every rule of the response pass.
        assert_eq!(waiting(&Fields::new()), ["Slow success"]);
        assert_eq!(
            waiting(&Fields::new().with(ApiKey::PRODUCE).with(ApiKey::FETCH)),
            ["Lost produce", "Lost fetch", "Slow success"]
        );
        // Waiting counts no match.
        assert_eq!(
            snapshot
                .get(&name("Lost produce"))
                .map(|rule| rule.arming().counter().matches()),
            Some(0)
        );
    }

    #[test]
    fn a_rule_whose_lifetime_ended_does_not_wait_for_the_truth() {
        let half_a_minute = Lifetime::Duration(TimeSpan::from_secs(30));
        let snapshot = snapshot([
            &rule("Lost for 30 s", succeeded(ApiKey::PRODUCE), lost()).with_lifetime(half_a_minute),
            &rule("Lost once", succeeded(ApiKey::PRODUCE), lost()).with_lifetime(Lifetime::Firings(count(1))),
            &rule("Lost", succeeded(ApiKey::PRODUCE), lost()),
        ]);
        let waiting = |call: &Fields, now: Instant| -> Vec<String> {
            let waiting = snapshot.waiting(call, now);
            waiting.into_iter().map(|rule| rule.name().to_string()).collect()
        };
        let later = NOW + TimeSpan::from_secs(30);

        assert_eq!(waiting(&produce(), NOW), ["Lost for 30 s", "Lost once", "Lost"]);
        // The duration passed, and the engine has not disarmed the rule yet.
        assert_eq!(waiting(&produce(), later), ["Lost once", "Lost"]);
        // A source without one API leaves the rule out too.
        assert_eq!(
            waiting(&Fields::new().with(ApiKey::PRODUCE).with(ApiKey::FETCH), later),
            ["Lost once", "Lost"]
        );

        // A call spends the one firing of the second rule, which ends its lifetime.
        let mut response = snapshot.evaluate(Pass::Response, &produce().with(Outcome::Ok), NOW, &mut session());
        for evaluated in &mut response {
            evaluated.confirm();
        }
        assert_eq!(waiting(&produce(), NOW), ["Lost for 30 s", "Lost"]);
    }

    #[test]
    fn a_rule_that_counts_per_session_counts_each_session_on_its_own() {
        let first_two = TriggerParts::new().with_first(count(2)).with_per(Per::Session);
        let snapshot = snapshot([&rule("Stale", api(ApiKey::PRODUCE), flap()).with_trigger(first_two)]);
        let mut one = session();
        let mut other = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #2").unwrap());
        let fired = |session: &mut SessionCounters| {
            let pass = snapshot.evaluate(Pass::Request, &produce(), NOW, session);
            pass.rules()[0].fired()
        };

        assert_eq!([fired(&mut one), fired(&mut one), fired(&mut one)], [true, true, false]);
        assert_eq!(
            [fired(&mut other), fired(&mut other), fired(&mut other)],
            [true, true, false]
        );
    }

    #[test]
    fn a_new_snapshot_of_the_same_armed_rules_continues_their_counts() {
        let refuse = arm(&rule("Refuse the 2nd", api(ApiKey::PRODUCE), flap()).with_trigger(nth(2)));
        let slow = arm(&rule("Slow", Conditions::new(), delay()));
        let mut session = session();

        let before = Snapshot::new([refuse.clone()]).unwrap();
        let first = before.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(outcomes(&first), [TriggerOutcome::Held]);

        // Someone arms another rule above it, and the engine publishes a new snapshot.
        let after = Snapshot::new([slow, refuse]).unwrap();
        let second = after.evaluate(Pass::Request, &produce(), NOW, &mut session);
        assert_eq!(names(&second), ["Slow", "Refuse the 2nd"]);
        assert_eq!(outcomes(&second), [TriggerOutcome::Fired, TriggerOutcome::Fired]);
        assert_eq!(
            second.rules()[1].rolled().match_number(),
            Some(MatchNumber::new(2).unwrap())
        );
    }

    #[test]
    fn a_time_window_opens_at_the_instant_the_call_is_evaluated() {
        let after_10s = TriggerParts::new().with_window(TimeWindow::after(TimeSpan::from_secs(10)));
        let snapshot = snapshot([&rule("Late", api(ApiKey::PRODUCE), flap()).with_trigger(after_10s)]);
        let mut session = session();
        let mut at = |seconds: u64| {
            let now = NOW + TimeSpan::from_secs(seconds);
            let pass = snapshot.evaluate(Pass::Request, &produce(), now, &mut session);
            (pass.rules()[0].rolled().window_open(), pass.rules()[0].fired())
        };

        assert_eq!(at(9), (Some(false), false));
        assert_eq!(at(10), (Some(true), true));
    }

    #[test]
    fn a_pass_iterates_over_its_rules() {
        let snapshot = snapshot([
            &rule("Refuse", api(ApiKey::PRODUCE), flap()),
            &rule("Slow", Conditions::new(), delay()),
        ]);

        let mut pass = snapshot.evaluate(Pass::Request, &produce(), NOW, &mut session());

        assert_eq!((&pass).into_iter().count(), 2);
        for rule in &mut pass {
            rule.withdraw();
        }
        assert!(pass.rules().iter().all(|rule| !rule.is_unsettled()));
        assert_eq!(
            pass.into_iter().map(|rule| rule.name().to_string()).collect::<Vec<_>>(),
            ["Refuse", "Slow"]
        );
    }
}
