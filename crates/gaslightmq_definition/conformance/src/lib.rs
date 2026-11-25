//! What every store of definitions does, as one check that the tests of any store run.
//!
//! The [store contract](gaslightmq_definition_api::Store) says what a store keeps to, whatever stands behind it
//! and whichever kind of definition it holds.
//! This crate holds that behaviour once: [`assert_store_contract`] takes a way to make an empty store,
//! and fails when the store does not do what the contract asks.
//!
//! | Item | Is |
//! |---|---|
//! | [`assert_store_contract`] | The check: every behaviour of the contract, each on a store of its own. |
//! | [`Samples`] | The definitions that the check stores. Whoever runs the check hands them in. |
//!
//! The crate knows no kind of definition.
//! Each kind has a conformance crate of its own, with the samples of its kind and the check of its store trait
//! on top of this one.
//!
//! ```
//! use std::collections::BTreeMap;
//! use std::sync::Mutex;
//!
//! use futures::executor::block_on;
//! use gaslightmq_definition_api::prelude::*;
//! use gaslightmq_definition_conformance::{Samples, assert_store_contract};
//!
//! /// What a team says when it starts its day, under a name.
//! #[derive(Debug, Clone, PartialEq)]
//! struct Greeting {
//!     name: String,
//!     text: String,
//! }
//!
//! impl Definition for Greeting {
//!     type Name = String;
//!
//!     const LABEL: &'static str = "greeting";
//!
//!     fn name(&self) -> &String {
//!         &self.name
//!     }
//! }
//!
//! /// A store of greetings that someone wrote for a test.
//! #[derive(Default)]
//! struct Greetings(Mutex<BTreeMap<String, Greeting>>);
//!
//! impl Store<Greeting> for Greetings {
//!     async fn list(&self) -> Result<Vec<Greeting>, StoreReadError> {
//!         Ok(self.0.lock().expect("no test panicked").values().cloned().collect())
//!     }
//!
//!     async fn get(&self, name: &String) -> Result<Option<Greeting>, StoreReadError> {
//!         Ok(self.0.lock().expect("no test panicked").get(name).cloned())
//!     }
//!
//!     async fn create(&self, greeting: Greeting) -> Result<(), StoreCreateError> {
//!         let mut greetings = self.0.lock().expect("no test panicked");
//!         if greetings.contains_key(greeting.name()) {
//!             return Err(StoreCreateError::NameTaken);
//!         }
//!         greetings.insert(greeting.name().clone(), greeting);
//!         Ok(())
//!     }
//!
//!     async fn update(&self, greeting: Greeting) -> Result<(), StoreChangeError> {
//!         let mut greetings = self.0.lock().expect("no test panicked");
//!         let held = greetings.get_mut(greeting.name()).ok_or(StoreChangeError::NotFound)?;
//!         *held = greeting;
//!         Ok(())
//!     }
//!
//!     async fn delete(&self, name: &String) -> Result<(), StoreChangeError> {
//!         let removed = self.0.lock().expect("no test panicked").remove(name);
//!         removed.map(drop).ok_or(StoreChangeError::NotFound)
//!     }
//! }
//!
//! let greeting = |name: &str, text: &str| Greeting { name: name.to_owned(), text: text.to_owned() };
//! let greetings = Samples::new(
//!     greeting("morning", "Good morning"),
//!     greeting("morning", "Good morning, everyone"),
//!     greeting("evening", "Good evening"),
//! );
//!
//! // The check is a future, and the test of a store drives it with the executor it has.
//! block_on(assert_store_contract(Greetings::default, &greetings));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the store contract alone.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

mod contract;
mod samples;

pub use contract::assert_store_contract;
pub use samples::Samples;
