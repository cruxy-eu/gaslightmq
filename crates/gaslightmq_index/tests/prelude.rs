//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the preludes of the crates whose words an index meets: the call record, its fields and the selector.
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

    use gaslightmq_call::prelude::*;
    use gaslightmq_clock::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_index::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_selector::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let fields = Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::PRODUCE)
            .with(Outcome::Ok)
            .with_part(TopicName::new("orders.eu").expect("a topic name"))
            .with_part(TopicName::new("orders.us").expect("a topic name"));
        let arrived = CallTiming::arrived(gaslightmq_clock::Instant::ZERO);
        let call = Call::new(
            CallNumber::FIRST,
            fields,
            Delivered::Nothing,
            Explanation::none(),
            arrived,
        )
        .expect("a call that ended on a connection");

        let configuration: IndexConfiguration = IndexConfiguration::new();
        let mut index: CallIndex = CallIndex::new(configuration.clone());
        index.add(&call);
        let facets: Facets = index.facets(&EffectiveSelector::new());
        let facet: Facet = index.facet(Field::Topic, &EffectiveSelector::new());
        let value: Option<&FacetValue> = facet.values().first();
        let pattern: Option<&SuggestedPattern> = facet.patterns().first();

        vec![
            format!("{configuration:?}"),
            index.calls().to_string(),
            facets.len().to_string(),
            facet.to_string(),
            value.map_or_default(ToString::to_string),
            pattern.map_or_default(ToString::to_string),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 6);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_is_enough_to_group_the_calls_of_an_index() {
    use gaslightmq_index::prelude::*;
    use gaslightmq_selector::prelude::*;

    let index = CallIndex::new(IndexConfiguration::new());
    let facets: Facets = index.facets(&EffectiveSelector::new());

    assert_eq!(facets.calls(), 0);
    assert!(facets.iter().all(Facet::is_empty));
}
