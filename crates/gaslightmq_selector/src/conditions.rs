use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fmt;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::{Condition, SelectorError};

/// A set of conditions, which combine with **and**: the `match` of a selector, a filter, a scope.
///
/// A set has at most one condition per field, as the map in a file has one entry per field.
/// A new condition on a field replaces the one that was there.
/// To test one field twice, [include](crate::Selector::including) a selector, or combine two sets in an
/// [`EffectiveSelector`](crate::EffectiveSelector).
///
/// The conditions are in the order of their fields, so two sets with the same conditions are equal.
/// A set without a condition matches every call.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let order_producers = Conditions::new()
///     .with(Condition::parse(Field::Topic, "orders.*")?)
///     .with(Condition::is(ApiKey::PRODUCE));
///
/// assert_eq!(order_producers.to_string(), "api is Produce and topic matches orders.*");
/// assert_eq!(order_producers.get(Field::Api), Some(&Condition::is(ApiKey::PRODUCE)));
/// assert!(!order_producers.is_scope());
/// # Ok::<(), SelectorError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Conditions(BTreeMap<Field, Condition>);

impl Conditions {
    /// No condition, which matches every call.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same set with `condition`, which replaces the condition on the same field.
    #[must_use]
    pub fn with(mut self, condition: Condition) -> Self {
        self.insert(condition);
        self
    }

    /// Adds `condition`, and returns the condition on the same field that it replaces.
    pub fn insert(&mut self, condition: Condition) -> Option<Condition> {
        self.0.insert(condition.field(), condition)
    }

    /// Removes the condition on `field`, and returns it.
    pub fn remove(&mut self, field: Field) -> Option<Condition> {
        self.0.remove(&field)
    }

    /// The condition on `field`.
    pub fn get(&self, field: Field) -> Option<&Condition> {
        self.0.get(&field)
    }

    /// The conditions in the order of their fields.
    pub fn iter(&self) -> btree_map::Values<'_, Field, Condition> {
        self.0.values()
    }

    /// The fields that the conditions test, in order.
    pub fn fields(&self) -> impl Iterator<Item = Field> + '_ {
        self.0.keys().copied()
    }

    /// How many conditions there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there is no condition, so the set matches every call.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the conditions only test connection fields, so the set says *who* something applies to.
    /// A set without a condition is the scope of all clients.
    pub fn is_scope(&self) -> bool {
        self.first_outside_scope().is_none()
    }

    fn first_outside_scope(&self) -> Option<Field> {
        self.fields().find(|field| field.group() != FieldGroup::Connection)
    }
}

impl From<Condition> for Conditions {
    fn from(condition: Condition) -> Self {
        Self::new().with(condition)
    }
}

/// A later condition replaces an earlier one on the same field.
impl FromIterator<Condition> for Conditions {
    fn from_iter<I: IntoIterator<Item = Condition>>(conditions: I) -> Self {
        let mut set = Self::new();
        set.extend(conditions);
        set
    }
}

/// A later condition replaces an earlier one on the same field.
impl Extend<Condition> for Conditions {
    fn extend<I: IntoIterator<Item = Condition>>(&mut self, conditions: I) {
        for condition in conditions {
            self.insert(condition);
        }
    }
}

impl IntoIterator for Conditions {
    type Item = Condition;
    type IntoIter = btree_map::IntoValues<Field, Condition>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_values()
    }
}

impl<'a> IntoIterator for &'a Conditions {
    type Item = &'a Condition;
    type IntoIter = btree_map::Values<'a, Field, Condition>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Writes the conditions with `and` between them, or `every call` when there is none.
impl fmt::Display for Conditions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_conjunction(f, self.iter())
    }
}

pub(crate) fn write_conjunction<'a>(
    f: &mut fmt::Formatter<'_>,
    conditions: impl Iterator<Item = &'a Condition>,
) -> fmt::Result {
    let mut any = false;
    for condition in conditions {
        if any {
            f.write_str(" and ")?;
        }
        fmt::Display::fmt(condition, f)?;
        any = true;
    }
    if any { Ok(()) } else { f.write_str("every call") }
}

/// A set of conditions that only tests connection fields.
/// It answers the question *who does this apply to?*
///
/// A scope is not a concept of its own: it is a selector like any other.
/// The type exists because a rule and a pad layer name a scope next to a selector,
/// and a scope that tests a topic would be a mistake there.
///
/// | Scope | Conditions | Built with |
/// |---|---|---|
/// | All clients | None. | [`all`](Self::all) |
/// | One client | `client.id is order-ingest` | [`client`](Self::client) |
/// | One client instance | `client.id is order-ingest` and `host is 10.0.4.27` | [`client_instance`](Self::client_instance) |
/// | One connection | `connection is #7` | [`connection`](Self::connection) |
/// | One broker endpoint | `endpoint is broker 2` | [`endpoint`](Self::endpoint) |
/// | Any other | Any conditions on connection fields. | [`new`](Self::new) |
///
/// A scope on one connection ends with that connection.
/// A client instance follows a client across reconnects.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let ingest = Scope::client(ClientId::new("order-ingest").expect("a client ID"));
/// assert_eq!(ingest.to_string(), "client.id is order-ingest");
///
/// let not_a_scope = Scope::new(Condition::is(ApiKey::PRODUCE).into());
/// assert_eq!(not_a_scope, Err(SelectorError::NotAConnectionField { field: Field::Api }));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scope(Conditions);

impl Scope {
    /// The scope of those conditions.
    ///
    /// # Errors
    ///
    /// [`SelectorError::NotAConnectionField`] with the first field that does not describe the connection.
    pub fn new(conditions: Conditions) -> Result<Self, SelectorError> {
        match conditions.first_outside_scope() {
            Some(field) => Err(SelectorError::NotAConnectionField { field }),
            None => Ok(Self(conditions)),
        }
    }

    /// All clients: no condition.
    pub fn all() -> Self {
        Self::default()
    }

    /// One client: every connection that carries this `client.id`.
    pub fn client(id: ClientId) -> Self {
        Self(Condition::is(id).into())
    }

    /// One client instance: this `client.id` from this host, across reconnects.
    pub fn client_instance(id: ClientId, host: ClientHost) -> Self {
        Self(Conditions::new().with(Condition::is(id)).with(Condition::is(host)))
    }

    /// One connection, for as long as it lives.
    pub fn connection(number: ConnectionNumber) -> Self {
        Self(Condition::is(number).into())
    }

    /// One endpoint: every connection that a client opened to it.
    pub fn endpoint(endpoint: Endpoint) -> Self {
        Self(Condition::is(endpoint).into())
    }

    /// Whether the scope is all clients.
    pub fn is_all(&self) -> bool {
        self.0.is_empty()
    }

    /// The conditions of the scope.
    pub fn conditions(&self) -> &Conditions {
        &self.0
    }

    /// The conditions of the scope.
    pub fn into_conditions(self) -> Conditions {
        self.0
    }
}

impl TryFrom<Conditions> for Scope {
    type Error = SelectorError;

    fn try_from(conditions: Conditions) -> Result<Self, Self::Error> {
        Self::new(conditions)
    }
}

impl From<Scope> for Conditions {
    fn from(scope: Scope) -> Self {
        scope.0
    }
}

/// Writes the conditions with `and` between them, or `every call` for all clients.
impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> ClientId {
        ClientId::new(id).unwrap()
    }

    fn host(text: &str) -> ClientHost {
        text.parse().unwrap()
    }

    fn order_producers() -> Conditions {
        Conditions::new()
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::parse(Field::Topic, "orders.*").unwrap())
    }

    #[test]
    fn a_new_set_has_no_condition() {
        let none = Conditions::new();

        assert!(none.is_empty());
        assert_eq!(none.len(), 0);
        assert_eq!(none.iter().count(), 0);
        assert_eq!(none.to_string(), "every call");
        assert_eq!(none, Conditions::default());
    }

    #[test]
    fn the_conditions_are_in_the_order_of_their_fields() {
        let written_backwards = Conditions::new()
            .with(Condition::parse(Field::Topic, "orders.*").unwrap())
            .with(Condition::is(ApiKey::PRODUCE))
            .with(Condition::is(client("order-ingest")));

        assert_eq!(
            written_backwards.to_string(),
            "client.id is order-ingest and api is Produce and topic matches orders.*"
        );
        assert_eq!(
            written_backwards.fields().collect::<Vec<_>>(),
            [Field::ClientId, Field::Api, Field::Topic]
        );
        assert_eq!(written_backwards.len(), 3);
        assert_eq!(
            written_backwards,
            order_producers().with(Condition::is(client("order-ingest")))
        );
    }

    #[test]
    fn a_new_condition_replaces_the_one_on_the_same_field() {
        let mut conditions = order_producers();

        let replaced = conditions.insert(Condition::is(ApiKey::FETCH));

        assert_eq!(replaced, Some(Condition::is(ApiKey::PRODUCE)));
        assert_eq!(conditions.get(Field::Api), Some(&Condition::is(ApiKey::FETCH)));
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions.insert(Condition::is(Acks::ALL)), None);
        assert_eq!(conditions.len(), 3);
    }

    #[test]
    fn a_condition_can_be_removed() {
        let mut conditions = order_producers();

        assert_eq!(conditions.remove(Field::Api), Some(Condition::is(ApiKey::PRODUCE)));
        assert_eq!(conditions.remove(Field::Api), None);
        assert_eq!(conditions.get(Field::Api), None);
        assert_eq!(conditions.to_string(), "topic matches orders.*");
    }

    #[test]
    fn a_set_collects_conditions() {
        let collected: Conditions = [
            Condition::is(ApiKey::FETCH),
            Condition::parse(Field::Topic, "orders.*").unwrap(),
            Condition::is(ApiKey::PRODUCE),
        ]
        .into_iter()
        .collect();

        assert_eq!(collected, order_producers());
        assert_eq!(
            collected.clone().into_iter().collect::<Vec<_>>(),
            collected.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!((&collected).into_iter().count(), 2);
        assert_eq!(
            Conditions::from(Condition::is(ApiKey::PRODUCE)).to_string(),
            "api is Produce"
        );
    }

    #[test]
    fn a_set_that_only_tests_connection_fields_is_a_scope() {
        let who = Conditions::new()
            .with(Condition::is(client("order-ingest")))
            .with(Condition::is_not(host("10.0.4.27")));

        assert!(who.is_scope());
        assert!(Conditions::new().is_scope());
        assert!(!order_producers().is_scope());
        assert!(
            !who.clone()
                .with(Condition::is(GroupId::new("payments").unwrap()))
                .is_scope()
        );
        assert!(!who.with(Condition::is(Outcome::Ok)).is_scope());
    }

    #[test]
    fn the_common_scopes_have_the_conditions_of_the_spec() {
        let cases = [
            (Scope::all(), "every call"),
            (Scope::client(client("order-ingest")), "client.id is order-ingest"),
            (
                Scope::client_instance(client("order-ingest"), host("10.0.4.27")),
                "host is 10.0.4.27 and client.id is order-ingest",
            ),
            (Scope::connection(ConnectionNumber::new(7)), "connection is #7"),
            (
                Scope::endpoint(Endpoint::Broker(BrokerId::from(2))),
                "endpoint is broker 2",
            ),
        ];

        for (scope, conditions) in cases {
            assert_eq!(scope.to_string(), conditions);
            assert!(scope.conditions().is_scope());
        }
        assert!(Scope::all().is_all());
        assert!(!Scope::connection(ConnectionNumber::new(7)).is_all());
        assert_eq!(Scope::default(), Scope::all());
    }

    #[test]
    fn a_scope_only_tests_connection_fields() {
        for field in Field::ALL {
            let condition = Condition::matches(field, "*".parse().unwrap());
            let scope = Scope::new(condition.clone().into());

            if field.group() == FieldGroup::Connection {
                assert_eq!(scope.map(Scope::into_conditions), Ok(condition.into()), "{field}");
            } else {
                assert_eq!(scope, Err(SelectorError::NotAConnectionField { field }), "{field}");
            }
        }
    }

    #[test]
    fn a_scope_names_the_first_field_that_is_not_of_the_connection() {
        let conditions = order_producers().with(Condition::is(client("order-ingest")));

        assert_eq!(
            Scope::try_from(conditions),
            Err(SelectorError::NotAConnectionField { field: Field::Api })
        );
    }

    #[test]
    fn a_scope_converts_into_its_conditions() {
        let scope = Scope::client(client("order-ingest"));

        assert_eq!(
            Conditions::from(scope.clone()),
            Conditions::from(Condition::is(client("order-ingest")))
        );
        assert_eq!(Scope::new(scope.conditions().clone()), Ok(scope));
    }
}
