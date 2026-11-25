//! The prelude holds every type, and its names clash with none of `std`, of the clock,
//! or of the preludes that code imports next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the prelude of every crate whose words meet in live traffic:
/// the records it takes, the selectors of its subscriptions, and the crates that evaluate what a record explains.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_and_every_neighbour {
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
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_traffic::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let size: LiveWindowSize = LiveWindowSize::new(64).expect("a window of 64 calls");
        let mut configuration: TrafficConfiguration = TrafficConfiguration::new();
        configuration.live_window = size;
        let mut traffic: Traffic = Traffic::new(configuration);
        let subscription: SubscriptionId = traffic.subscribe(gaslightmq_selector::EffectiveSelector::new());
        let window: &LiveWindow = traffic.live_window();
        let statistics: &Statistics = traffic.statistics();
        let rule: RuleStatistics = statistics.rule(&"leader-flap".parse().expect("a rule name"));
        let seen: &SeenNames = traffic.seen_names();

        vec![
            size.to_string(),
            subscription.to_string(),
            window.len().to_string(),
            statistics.calls().to_string(),
            rule.firings().to_string(),
            seen.clients().count().to_string(),
        ]
    }

    /// The error of the prelude, named without a path.
    ///
    /// A message says everything, so the error has no source.
    pub fn every_error() -> Vec<String> {
        let error: LiveWindowSizeError = LiveWindowSizeError::Zero;
        assert!(std::error::Error::source(&error).is_none(), "{error}");
        vec![error.to_string()]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_and_every_neighbour() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_item(),
        ["64 calls", "subscription 1", "0", "0", "0", "0"]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_error(),
        ["a live window keeps at least one call"]
    );
}
