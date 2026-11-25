//! A store in memory does what the contract asks of every store.

use futures::executor::block_on;
use gaslightmq_definition_api::prelude::*;
use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_definition_memory::MemoryStore;

/// What a team says when it starts its day, under a name.
#[derive(Debug, Clone, PartialEq)]
struct Greeting {
    name: String,
    text: String,
}

impl Greeting {
    fn new(name: &str, text: &str) -> Self {
        Self {
            name: name.to_owned(),
            text: text.to_owned(),
        }
    }
}

impl Definition for Greeting {
    type Name = String;

    const LABEL: &'static str = "greeting";

    fn name(&self) -> &String {
        &self.name
    }
}

#[test]
fn a_store_in_memory_keeps_the_contract() {
    let greetings = Samples::new(
        Greeting::new("morning", "Good morning"),
        Greeting::new("morning", "Good morning, everyone"),
        Greeting::new("evening", "Good evening"),
    );

    block_on(assert_store_contract(MemoryStore::new, &greetings));
}
