//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the kernel and of the fields.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_selector::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let pattern: Glob = Glob::new("orders.*").unwrap();
        let values: ValueSet = ValueSet::new([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP]).unwrap();
        let condition: Condition = Condition::matches(Field::Topic, pattern.clone());
        let operator: Operator = condition.operator();
        let conditions: Conditions = Conditions::new()
            .with(condition.clone())
            .with(Condition::IsAnyOf(values.clone()));
        let scope: Scope = Scope::client(ClientId::new("order-ingest").unwrap());
        let name: SelectorName = SelectorName::new("Group orders").unwrap();
        let selector: Selector = Selector::new(name.clone(), conditions.clone());
        let reference: SelectorRef = SelectorRef::Named(name.clone());
        let set: SelectorSet = SelectorSet::new([selector.clone()]).unwrap();
        let effective: EffectiveSelector = set.resolve(&reference).unwrap().and(scope.clone());
        let pass: Pass = effective.pass();
        let apis: Apis = effective.apis();
        let call = Fields::new()
            .with(ClientId::new("order-ingest").unwrap())
            .with(ApiKey::JOIN_GROUP)
            .with_part(TopicName::new("orders.eu").unwrap());
        let matched: Match = effective.evaluate(&call);
        let index: ApiIndex<SelectorName> = ApiIndex::new([(name.clone(), effective.clone())]);

        vec![
            pattern.to_string(),
            values.to_string(),
            condition.to_string(),
            operator.to_string(),
            conditions.to_string(),
            scope.to_string(),
            name.to_string(),
            selector.name().to_string(),
            format!("{reference:?}"),
            set.len().to_string(),
            effective.to_string(),
            pass.to_string(),
            format!("{apis:?}"),
            format!("{matched:?}"),
            index.matches(&call).count().to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let name: SelectorName = "Order producers".parse().unwrap();
        vec![
            message_of(GlobError::Empty),
            message_of(SelectorNameError::Empty),
            message_of(OperatorError::Unknown {
                name: "isnt".to_owned(),
            }),
            message_of(ValueSetError::Empty),
            message_of(ConditionError::InvalidValue {
                error: Field::Topic.parse_value("").unwrap_err(),
            }),
            message_of(ScopeError::NotAConnectionField { field: Field::Topic }),
            message_of(SelectorSetError::UnknownSelector { name }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 15);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "a pattern is empty",
            "selector name \"\" is empty",
            "no operator is named \"isnt\"",
            "a condition needs at least one value",
            "topic: topic name is empty",
            "a scope only tests connection fields, and topic is a request field",
            "no selector is named \"Order producers\"",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_match_a_call() {
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_selector::prelude::*;

    let selector = EffectiveSelector::from(Condition::parse(Field::Topic, "orders.*").unwrap());
    let call = Fields::new()
        .with(ApiKey::PRODUCE)
        .with_part("orders.eu/3".parse::<TopicPartition>().unwrap());

    assert_eq!(selector.evaluate(&call), Match::Matched(call.whole_target()));
}
