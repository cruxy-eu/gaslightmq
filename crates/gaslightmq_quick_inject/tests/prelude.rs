//! The prelude holds every item, and its names clash with none of `std` or of the preludes next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that a host imports next to a sentence: everything a rule is made of.
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

    use gaslightmq_clock::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_quick_inject::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let configuration: QuickInjectConfiguration = QuickInjectConfiguration::default();
        let quick_inject: QuickInject = QuickInject::new(configuration);
        let known: KnownNames = KnownNames::new();
        let sentence: Sentence = quick_inject.read("delay prodcue", &known);
        let unread: &Unread = &sentence.unread()[0];
        let span: Span = unread.span();
        let typing: Sentence = quick_inject.read("thr", &known);
        let completion: &Completion = &typing.completions()[0];
        let kind: CompletionKind = completion.kind();

        vec![
            quick_inject.configuration().completion_limit.to_string(),
            known.topics().count().to_string(),
            sentence.text().to_owned(),
            unread.word().to_owned(),
            span.to_string(),
            completion.word().to_owned(),
            kind.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(
        items,
        ["5", "0", "delay prodcue", "prodcue", "6..13", "throttle", "lie"]
    );
}
