use std::collections::BTreeSet;
use std::fmt;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::condition::Holds;
use crate::conditions::write_conjunction;
use crate::{Condition, Conditions, Scope};

/// Every condition that applies, in one list: what matches a call.
///
/// A [`Selector`](crate::Selector) is a definition, and it can include another by name.
/// The effective selector is what is left once the names are resolved:
/// the conditions of the selector, of every selector it includes, and of a scope next to it.
/// All of them combine with **and**, and the list can test one field more than once.
///
/// | From | Through |
/// |---|---|
/// | A selector name or inline conditions | [`SelectorSet::resolve`](crate::SelectorSet::resolve) |
/// | [`Conditions`], a [`Scope`] or one [`Condition`] | [`From`] |
/// | A selector **and** a scope | [`and`](Self::and) |
///
/// # What a match yields
///
/// [`evaluate`](Self::evaluate) tests the conditions against a [`FieldSource`] and gives the [`Target`].
///
/// - `is not`, and every condition on a field that the call has once, test the whole call.
/// - A positive condition on `topic`, `partition`, `outcome` or `error.code` tests each [part](Part) on its own.
///   The parts that pass all of those conditions are the target.
///   `topic is orders.eu` with `partition is 3` matches `orders.eu/3`,
///   and not a request that carries `orders.eu/1` and `payments.settled/3`.
/// - Without such a condition, the target is every part of the call.
/// - On a call without parts, such as a `Heartbeat`, the target is the whole call.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let orders = EffectiveSelector::from(Condition::parse(Field::Topic, "orders.*")?)
///     .and(Scope::client(ClientId::new("order-ingest").expect("a client ID")));
/// assert_eq!(orders.to_string(), "topic matches orders.* and client.id is order-ingest");
/// assert_eq!(orders.pass(), Pass::Request);
///
/// let produce = Fields::new()
///     .with(ClientId::new("order-ingest").expect("a client ID"))
///     .with(ApiKey::PRODUCE)
///     .with_part("orders.eu/3".parse::<TopicPartition>().expect("a partition"))
///     .with_part("payments.settled/0".parse::<TopicPartition>().expect("a partition"));
///
/// // The lie of a rule with this selector applies to the `orders.eu` partition only.
/// let target = orders.evaluate(&produce).into_target().expect("the Produce names orders.eu");
/// assert_eq!(target.to_string(), "orders.eu/3");
/// # Ok::<(), ConditionError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct EffectiveSelector {
    conditions: Vec<Condition>,
}

impl EffectiveSelector {
    /// No condition, which matches every call.
    pub fn new() -> Self {
        Self::default()
    }

    /// This selector **and** `other`: the conditions of both.
    ///
    /// The effective selector of a rule is its selector and its scope.
    #[must_use]
    pub fn and(mut self, other: impl Into<EffectiveSelector>) -> Self {
        self.conditions.extend(other.into().conditions);
        self
    }

    /// The conditions, which combine with **and**.
    /// Those of an included selector come before those of the selector that includes it.
    pub fn conditions(&self) -> &[Condition] {
        &self.conditions
    }

    /// The fields that the conditions test, each one once and in order.
    pub fn fields(&self) -> impl Iterator<Item = Field> + use<> {
        let fields: BTreeSet<Field> = self.conditions.iter().map(Condition::field).collect();
        fields.into_iter()
    }

    /// The evaluation pass that can decide this selector.
    ///
    /// A selector that tests a response field waits for the truth, so it needs the response pass.
    /// Every other selector is decided before forwarding.
    pub fn pass(&self) -> Pass {
        if self.fields().any(|field| field.group() == FieldGroup::Response) {
            Pass::Response
        } else {
            Pass::Request
        }
    }

    /// Whether the conditions only test connection fields, so the selector says *who* something applies to.
    pub fn is_scope(&self) -> bool {
        self.fields().all(|field| field.group() == FieldGroup::Connection)
    }

    /// The APIs whose calls the selector can match, for an [index by API](crate::ApiIndex).
    ///
    /// `api is` and `api is any of` name them.
    /// A pattern or an `is not` on `api` narrows nothing here, and the selector still tests it on every call.
    pub fn apis(&self) -> Apis {
        let mut apis = Apis::Any;
        for condition in &self.conditions {
            let named: BTreeSet<ApiKey> = match condition {
                Condition::Is(FieldValue::Api(api)) => BTreeSet::from([*api]),
                Condition::IsAnyOf(values) if values.field() == Field::Api => values
                    .iter()
                    .filter_map(|value| match value {
                        FieldValue::Api(api) => Some(*api),
                        _ => None,
                    })
                    .collect(),
                _ => continue,
            };
            apis = match apis {
                Apis::Any => Apis::Only(named),
                Apis::Only(so_far) => Apis::Only(so_far.intersection(&named).copied().collect()),
            };
        }
        apis
    }

    /// Tests the conditions against a call, and gives the [`Target`] when all of them hold.
    ///
    /// The result is [`Match::Pending`] while a condition tests a response field that the call does not have yet,
    /// unless another condition already fails.
    /// See [what a match yields](Self#what-a-match-yields).
    pub fn evaluate<S: FieldSource + ?Sized>(&self, source: &S) -> Match {
        let on_whole_call = || self.conditions.iter().filter(|condition| !condition.is_per_part());
        let on_each_part = || self.conditions.iter().filter(|condition| condition.is_per_part());

        let call = on_whole_call().fold(Holds::Yes, |holds, condition| {
            holds.and(condition.test(source.read(condition.field())))
        });
        if call == Holds::No {
            return Match::NotMatched;
        }

        let parts = source.parts();
        if parts.is_empty() || on_each_part().next().is_none() {
            // Nothing singles out a part. On a call without parts, a condition on a part reads the whole call:
            // it has no `topic` and no `partition`, and its truth is the truth of the call.
            let all = on_each_part().fold(call, |holds, condition| {
                holds.and(condition.test(source.read(condition.field())))
            });
            return match all {
                Holds::Yes => Match::Matched(source.whole_target()),
                Holds::No => Match::NotMatched,
                Holds::Unknown => Match::Pending,
            };
        }

        let mut matched = Vec::new();
        let mut waiting = call == Holds::Unknown;
        let mut any_left = false;
        for part in parts {
            let holds = on_each_part().fold(Holds::Yes, |holds, condition| {
                holds.and(condition.test(source.read_in(part, condition.field())))
            });
            match holds {
                Holds::Yes => {
                    matched.push(part.part().clone());
                    any_left = true;
                }
                Holds::Unknown => {
                    waiting = true;
                    any_left = true;
                }
                Holds::No => {}
            }
        }
        match Target::of_parts(matched) {
            _ if !any_left => Match::NotMatched,
            Some(target) if !waiting => Match::Matched(target),
            _ => Match::Pending,
        }
    }
}

impl From<Condition> for EffectiveSelector {
    fn from(condition: Condition) -> Self {
        Self {
            conditions: vec![condition],
        }
    }
}

impl From<Conditions> for EffectiveSelector {
    fn from(conditions: Conditions) -> Self {
        conditions.into_iter().collect()
    }
}

impl From<Scope> for EffectiveSelector {
    fn from(scope: Scope) -> Self {
        scope.into_conditions().into()
    }
}

/// Conditions in a list keep their order, and can test one field more than once.
impl FromIterator<Condition> for EffectiveSelector {
    fn from_iter<I: IntoIterator<Item = Condition>>(conditions: I) -> Self {
        Self {
            conditions: conditions.into_iter().collect(),
        }
    }
}

/// Writes the conditions with `and` between them, or `every call` when there is none.
impl fmt::Display for EffectiveSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_conjunction(f, self.conditions.iter())
    }
}

/// What testing a selector against a call gives.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let succeeded = EffectiveSelector::from(Condition::is(Outcome::Ok));
/// let asked = Fields::new().with(ApiKey::HEARTBEAT);
///
/// // The backend did not answer yet, so nobody knows.
/// assert_eq!(succeeded.evaluate(&asked), Match::Pending);
/// assert_eq!(succeeded.evaluate(&asked.clone().with(Outcome::Ok)), Match::Matched(Target::Call));
/// assert_eq!(succeeded.evaluate(&asked.with(Outcome::Timeout)), Match::NotMatched);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Match {
    /// Every condition holds, and this is the part of the call the selector matched.
    Matched(Target),
    /// A condition does not hold.
    NotMatched,
    /// No condition fails, and one tests a response field that the call does not have yet.
    /// The response pass decides it.
    Pending,
}

impl Match {
    /// Whether every condition holds.
    pub const fn is_matched(&self) -> bool {
        matches!(self, Match::Matched(_))
    }

    /// Whether the match waits for the truth.
    pub const fn is_pending(&self) -> bool {
        matches!(self, Match::Pending)
    }

    /// What the selector matched, or `None` when it did not match or does not know yet.
    pub const fn target(&self) -> Option<&Target> {
        match self {
            Match::Matched(target) => Some(target),
            Match::NotMatched | Match::Pending => None,
        }
    }

    /// What the selector matched, or `None` when it did not match or does not know yet.
    pub fn into_target(self) -> Option<Target> {
        match self {
            Match::Matched(target) => Some(target),
            Match::NotMatched | Match::Pending => None,
        }
    }
}

/// One of the two times the pipeline evaluates selectors for a call.
///
/// | Pass | Runs | Decides a selector that |
/// |---|---|---|
/// | `Request` | Before forwarding. | Only tests connection, request and learned fields. |
/// | `Response` | After the backend answers. | Also tests a response field, such as `outcome`. |
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let produce = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE));
/// assert_eq!(produce.pass(), Pass::Request);
/// assert_eq!(produce.and(Condition::is(Outcome::Ok)).pass(), Pass::Response);
/// assert!(Pass::Request < Pass::Response);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pass {
    /// The pass before forwarding, when the truth is not known yet.
    Request,
    /// The pass after the backend answers.
    Response,
}

impl Pass {
    /// Both passes, in the order they run.
    pub const ALL: [Pass; 2] = [Pass::Request, Pass::Response];

    /// The name of the pass in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            Pass::Request => "request",
            Pass::Response => "response",
        }
    }
}

impl fmt::Display for Pass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// The APIs whose calls a selector can match.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let group_apis = EffectiveSelector::from(Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP])?);
/// assert!(group_apis.apis().contains(ApiKey::JOIN_GROUP));
/// assert!(!group_apis.apis().contains(ApiKey::PRODUCE));
/// assert_eq!(EffectiveSelector::new().apis(), Apis::Any);
/// # Ok::<(), ValueSetError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Apis {
    /// No condition names an API, so a call of any API can match.
    Any,
    /// Only a call of one of these APIs can match.
    /// The set is empty when the conditions contradict each other, and then no call matches.
    Only(BTreeSet<ApiKey>),
}

impl Apis {
    /// Whether a call of `api` can match.
    pub fn contains(&self, api: ApiKey) -> bool {
        match self {
            Apis::Any => true,
            Apis::Only(apis) => apis.contains(&api),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> ClientId {
        ClientId::new(id).unwrap()
    }

    fn pattern(field: Field, text: &str) -> Condition {
        Condition::matches(field, text.parse().unwrap())
    }

    #[test]
    fn a_new_selector_has_no_condition_and_matches_every_call() {
        let every_call = EffectiveSelector::new();

        assert_eq!(every_call.conditions(), []);
        assert_eq!(every_call.to_string(), "every call");
        assert_eq!(every_call.pass(), Pass::Request);
        assert!(every_call.is_scope());
        assert_eq!(every_call.evaluate(&Fields::new()), Match::Matched(Target::Call));
    }

    #[test]
    fn and_keeps_the_conditions_of_both_in_order() {
        let orders = pattern(Field::Topic, "orders.*");
        let eu = pattern(Field::Topic, "*.eu");
        let ingest = Condition::is(client("order-ingest"));

        let effective = EffectiveSelector::from(orders.clone())
            .and(eu.clone())
            .and(Scope::client(client("order-ingest")));

        assert_eq!(effective.conditions(), [orders, eu, ingest]);
        assert_eq!(
            effective.to_string(),
            "topic matches orders.* and topic matches *.eu and client.id is order-ingest"
        );
        assert_eq!(effective.fields().collect::<Vec<_>>(), [Field::ClientId, Field::Topic]);
    }

    #[test]
    fn an_effective_selector_converts_from_every_kind_of_conditions() {
        let produce = Condition::is(ApiKey::PRODUCE);
        let conditions = Conditions::new()
            .with(produce.clone())
            .with(pattern(Field::Topic, "orders.*"));

        assert_eq!(
            EffectiveSelector::from(produce.clone()).conditions(),
            std::slice::from_ref(&produce)
        );
        assert_eq!(
            EffectiveSelector::from(conditions.clone()).conditions(),
            conditions.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            EffectiveSelector::from(Scope::all()).conditions(),
            EffectiveSelector::new().conditions()
        );
        assert_eq!(
            [produce.clone(), produce.clone()]
                .into_iter()
                .collect::<EffectiveSelector>()
                .conditions(),
            [produce.clone(), produce]
        );
    }

    #[test]
    fn a_selector_that_tests_a_response_field_needs_the_response_pass() {
        for field in Field::ALL {
            let effective = EffectiveSelector::from(Condition::is(ApiKey::PRODUCE)).and(pattern(field, "*"));
            let expected = if field.group() == FieldGroup::Response {
                Pass::Response
            } else {
                Pass::Request
            };
            assert_eq!(effective.pass(), expected, "{field}");
        }
    }

    #[test]
    fn is_not_on_a_response_field_needs_the_response_pass_too() {
        let effective = EffectiveSelector::from(Condition::is_not(ErrorCode::NOT_LEADER_OR_FOLLOWER));

        assert_eq!(effective.pass(), Pass::Response);
    }

    #[test]
    fn the_passes_have_names_and_an_order() {
        assert_eq!(Pass::ALL.map(Pass::name), ["request", "response"]);
        assert_eq!(Pass::Response.to_string(), "response");
        assert_eq!(format!("{:>9}|", Pass::Request), "  request|");
        assert!(Pass::ALL.is_sorted());
    }

    #[test]
    fn a_selector_over_connection_fields_is_a_scope() {
        let who = EffectiveSelector::from(Scope::client(client("order-ingest")));

        assert!(who.is_scope());
        assert!(!who.clone().and(Condition::is(ApiKey::PRODUCE)).is_scope());
        assert!(who.and(Condition::is_not(client("kafka-ui"))).is_scope());
    }

    #[test]
    fn is_and_is_any_of_name_the_apis_a_selector_can_match() {
        let only = |apis: &[ApiKey]| Apis::Only(apis.iter().copied().collect());
        let group_apis = || Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP]).unwrap();

        assert_eq!(
            EffectiveSelector::from(Condition::is(ApiKey::PRODUCE)).apis(),
            only(&[ApiKey::PRODUCE])
        );
        assert_eq!(
            EffectiveSelector::from(group_apis()).apis(),
            only(&[ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP])
        );
        assert_eq!(
            EffectiveSelector::from(group_apis())
                .and(Condition::is(ApiKey::SYNC_GROUP))
                .apis(),
            only(&[ApiKey::SYNC_GROUP])
        );
        assert_eq!(
            EffectiveSelector::from(group_apis())
                .and(Condition::is(ApiKey::PRODUCE))
                .apis(),
            only(&[])
        );
    }

    #[test]
    fn nothing_else_narrows_the_apis() {
        let cases = [
            EffectiveSelector::new(),
            pattern(Field::Api, "*Group*").into(),
            Condition::is_not(ApiKey::PRODUCE).into(),
            pattern(Field::Topic, "orders.*").into(),
            Condition::is_any_of([client("a"), client("b")]).unwrap().into(),
        ];

        for effective in cases {
            assert_eq!(effective.apis(), Apis::Any, "{effective}");
        }
        assert_eq!(
            EffectiveSelector::from(pattern(Field::Api, "*Group*"))
                .and(Condition::is(ApiKey::JOIN_GROUP))
                .apis(),
            Apis::Only(BTreeSet::from([ApiKey::JOIN_GROUP]))
        );
    }

    #[test]
    fn apis_say_which_calls_can_match() {
        let produce = Apis::Only(BTreeSet::from([ApiKey::PRODUCE]));

        assert!(produce.contains(ApiKey::PRODUCE));
        assert!(!produce.contains(ApiKey::FETCH));
        assert!(Apis::Any.contains(ApiKey::FETCH));
        assert!(!Apis::Only(BTreeSet::new()).contains(ApiKey::PRODUCE));
    }

    #[test]
    fn a_match_gives_its_target_only_when_it_matched() {
        let matched = Match::Matched(Target::Call);

        assert!(matched.is_matched() && !matched.is_pending());
        assert_eq!(matched.target(), Some(&Target::Call));
        assert_eq!(matched.into_target(), Some(Target::Call));
        for other in [Match::NotMatched, Match::Pending] {
            assert!(!other.is_matched());
            assert_eq!(other.is_pending(), other == Match::Pending);
            assert_eq!(other.target(), None);
            assert_eq!(other.into_target(), None);
        }
    }
}
